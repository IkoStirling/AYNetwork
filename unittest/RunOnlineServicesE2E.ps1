param(
    [Parameter(Mandatory = $true)][string]$SessionServer,
    [Parameter(Mandatory = $true)][string]$OnlineProbe
)

$ErrorActionPreference = "Stop"
$tempRoot = Join-Path ([System.IO.Path]::GetTempPath()) (
    "aynetwork-online-service-" + [Guid]::NewGuid().ToString("N"))
New-Item -ItemType Directory -Path $tempRoot | Out-Null
$serverOut = Join-Path $tempRoot "server.out.log"
$serverErr = Join-Path $tempRoot "server.err.log"
$probeOut = Join-Path $tempRoot "probe.out.log"
$probeErr = Join-Path $tempRoot "probe.err.log"
$credentials = Join-Path $tempRoot "players.txt"
$server = $null
$savedEnvironment = @{}

function Get-FreeTcpPort {
    $listener = [System.Net.Sockets.TcpListener]::new(
        [System.Net.IPAddress]::Loopback, 0)
    $listener.Start()
    try { return ([System.Net.IPEndPoint]$listener.LocalEndpoint).Port }
    finally { $listener.Stop() }
}

function Get-FreeUdpPort {
    $client = [System.Net.Sockets.UdpClient]::new(0)
    try { return ([System.Net.IPEndPoint]$client.Client.LocalEndPoint).Port }
    finally { $client.Dispose() }
}

function Read-Log([string]$Path) {
    if (!(Test-Path -LiteralPath $Path)) { return "" }
    $stream = $null
    $reader = $null
    try {
        $stream = New-Object System.IO.FileStream(
            $Path, [System.IO.FileMode]::Open, [System.IO.FileAccess]::Read,
            [System.IO.FileShare]::ReadWrite)
        $reader = New-Object System.IO.StreamReader($stream)
        return $reader.ReadToEnd()
    }
    catch { return "" }
    finally {
        if ($reader) { $reader.Dispose() }
        elseif ($stream) { $stream.Dispose() }
    }
}

try {
    $ownerToken = "owner-" + ("a" * 58)
    $guestToken = "guest-" + ("b" * 58)
    $fleetToken = "fleet-" + ("c" * 58)
    Set-Content -LiteralPath $credentials -Value @(
        "online-owner $ownerToken",
        "online-guest $guestToken"
    ) -Encoding ASCII
    $environment = @{
        AY_ONLINE_ENABLE = "1"
        AY_ONLINE_CREDENTIALS_FILE = $credentials
        AY_ONLINE_SERVER_TOKEN = $fleetToken
        AY_ONLINE_OWNER_TOKEN = $ownerToken
        AY_ONLINE_GUEST_TOKEN = $guestToken
    }
    foreach ($entry in $environment.GetEnumerator()) {
        $savedEnvironment[$entry.Key] = [Environment]::GetEnvironmentVariable(
            $entry.Key, "Process")
        [Environment]::SetEnvironmentVariable(
            $entry.Key, $entry.Value, "Process")
    }

    $httpPort = Get-FreeTcpPort
    $signalPort = Get-FreeUdpPort
    $server = Start-Process -FilePath $SessionServer -ArgumentList @(
        "127.0.0.1", [string]$httpPort, "127.0.0.1", [string]$signalPort
    ) -RedirectStandardOutput $serverOut -RedirectStandardError $serverErr `
      -PassThru -WindowStyle Hidden
    $null = $server.Handle

    $deadline = [DateTime]::UtcNow.AddSeconds(10)
    while ([DateTime]::UtcNow -lt $deadline) {
        if ($server.HasExited) {
            throw "SessionServer exited early: $(Read-Log $serverErr)"
        }
        if ((Read-Log $serverOut).Contains("online=enabled")) { break }
        Start-Sleep -Milliseconds 50
    }
    if (!(Read-Log $serverOut).Contains("online=enabled")) {
        throw "Online SessionServer did not become ready: $(Read-Log $serverOut)"
    }

    $probe = Start-Process -FilePath $OnlineProbe -ArgumentList @(
        "127.0.0.1", [string]$httpPort
    ) -RedirectStandardOutput $probeOut -RedirectStandardError $probeErr `
      -PassThru -Wait -WindowStyle Hidden
    $probeText = Read-Log $probeOut
    if ($probe.ExitCode -ne 0 -or
        !$probeText.Contains("AY_ONLINE_RESULT state=passed")) {
        throw "OnlineProbe failed ($($probe.ExitCode)): $probeText $(Read-Log $probeErr) server=$(Read-Log $serverOut) $(Read-Log $serverErr)"
    }
    Write-Output $probeText.Trim()
}
finally {
    if ($server -and !$server.HasExited) {
        Stop-Process -Id $server.Id -Force -ErrorAction SilentlyContinue
        $server.WaitForExit(5000) | Out-Null
    }
    foreach ($entry in $savedEnvironment.GetEnumerator()) {
        [Environment]::SetEnvironmentVariable(
            $entry.Key, $entry.Value, "Process")
    }
    Remove-Item -LiteralPath $tempRoot -Recurse -Force -ErrorAction SilentlyContinue
}
