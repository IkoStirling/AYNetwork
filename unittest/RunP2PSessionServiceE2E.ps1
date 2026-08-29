param(
    [Parameter(Mandatory = $true)][string]$SessionServer,
    [Parameter(Mandatory = $true)][string]$SessionProbe,
    [switch]$Production
)

$ErrorActionPreference = "Stop"
$tempRoot = Join-Path ([System.IO.Path]::GetTempPath()) (
    "aynetwork-session-service-" + [Guid]::NewGuid().ToString("N"))
New-Item -ItemType Directory -Path $tempRoot | Out-Null
$serverOut = Join-Path $tempRoot "server.out.log"
$serverErr = Join-Path $tempRoot "server.err.log"
$probeOut = Join-Path $tempRoot "probe.out.log"
$probeErr = Join-Path $tempRoot "probe.err.log"
$server = $null
$savedEnvironment = @{}

# CTest launched from some VS/Codex environments can inherit both `Path` and
# `PATH`. Windows accepts that environment block, but Windows PowerShell's
# Start-Process copies it into a case-insensitive dictionary and throws before
# spawning the child. Keep the most complete value under one canonical key.
$processEnvironment = [Environment]::GetEnvironmentVariables("Process")
$pathEntries = @($processEnvironment.GetEnumerator() | Where-Object {
    [string]$_.Key -ieq "Path"
})
if ($pathEntries.Count -gt 1) {
    $pathValue = ($pathEntries | Sort-Object {
        ([string]$_.Value).Length
    } -Descending | Select-Object -First 1).Value
    foreach ($entry in $pathEntries) {
        [Environment]::SetEnvironmentVariable(
            [string]$entry.Key, $null, "Process")
    }
    [Environment]::SetEnvironmentVariable("Path", $pathValue, "Process")
}

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
    if ($Production) {
        $productionEnvironment = @{
            AY_SESSION_PRODUCTION = "1"
            AY_SESSION_DB = (Join-Path $tempRoot "sessions.db")
            AY_SESSION_STATE_KEY = ("1a" * 32)
            AY_SESSION_TICKET_KEY_FILE = (Join-Path $tempRoot "ticket.key")
            AY_SESSION_ADMISSION_TOKEN = ("admission-test-" + ("a" * 50))
            AY_SESSION_AUDIT_FILE = (Join-Path $tempRoot "audit.jsonl")
            AY_SESSION_HTTP_BIND = "127.0.0.1"
            AY_SESSION_SIGNALING_TRANSPORT = "websocket"
            AY_SESSION_DRAIN_SECONDS = "0"
        }
        foreach ($entry in $productionEnvironment.GetEnumerator()) {
            $savedEnvironment[$entry.Key] = [Environment]::GetEnvironmentVariable(
                $entry.Key, "Process")
            [Environment]::SetEnvironmentVariable(
                $entry.Key, $entry.Value, "Process")
        }
    }
    $httpPort = Get-FreeTcpPort
    $signalPort = if ($Production) { $httpPort } else { Get-FreeUdpPort }
    $signalAddress = if ($Production) {
        "ws://127.0.0.1/v1/signaling"
    } else { "127.0.0.1" }
    $server = Start-Process -FilePath $SessionServer -ArgumentList @(
        "127.0.0.1", [string]$httpPort, $signalAddress, [string]$signalPort
    ) -RedirectStandardOutput $serverOut -RedirectStandardError $serverErr `
      -PassThru -WindowStyle Hidden
    $null = $server.Handle

    $deadline = [DateTime]::UtcNow.AddSeconds(10)
    while ([DateTime]::UtcNow -lt $deadline) {
        if ($server.HasExited) {
            throw "SessionServer exited early: $(Read-Log $serverErr)"
        }
        if ((Read-Log $serverOut).Contains("AY_SESSION_SERVER state=ready")) {
            break
        }
        Start-Sleep -Milliseconds 50
    }
    if (!(Read-Log $serverOut).Contains("AY_SESSION_SERVER state=ready")) {
        throw "SessionServer did not become ready"
    }
    if ($Production -and
        !(Read-Log $serverOut).Contains("mode=production store=sqlite")) {
        throw "SessionServer did not enter durable production mode: $(Read-Log $serverOut)"
    }

    $probe = Start-Process -FilePath $SessionProbe -ArgumentList @(
        "127.0.0.1", [string]$httpPort
    ) -RedirectStandardOutput $probeOut -RedirectStandardError $probeErr `
      -PassThru -Wait -WindowStyle Hidden
    $probeText = Read-Log $probeOut
    if ($probe.ExitCode -ne 0 -or
        !$probeText.Contains("AY_SESSION_RESULT state=passed")) {
        throw "SessionProbe failed ($($probe.ExitCode)): $probeText $(Read-Log $probeErr) server=$(Read-Log $serverOut) $(Read-Log $serverErr)"
    }
    if ($Production) {
        foreach ($required in @(
            $env:AY_SESSION_DB,
            $env:AY_SESSION_TICKET_KEY_FILE,
            $env:AY_SESSION_AUDIT_FILE)) {
            if (!(Test-Path -LiteralPath $required)) {
                throw "Production artifact missing: $required"
            }
        }
        $audit = Get-Content -LiteralPath $env:AY_SESSION_AUDIT_FILE -Raw
        if (!$audit.Contains('"status":200') -or
            $audit.Contains($env:AY_SESSION_ADMISSION_TOKEN)) {
            throw "Production audit is missing success events or leaked admission secret"
        }
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
