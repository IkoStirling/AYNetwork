param(
    [Parameter(Mandatory = $true)][string]$SessionServer,
    [Parameter(Mandatory = $true)][string]$OnlineProbe,
    [switch]$Production
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

function Convert-HexBytes([string]$Text) {
    if (($Text.Length % 2) -ne 0) { throw "Invalid hex byte string" }
    $bytes = New-Object byte[] ($Text.Length / 2)
    for ($index = 0; $index -lt $bytes.Length; ++$index) {
        $bytes[$index] = [Convert]::ToByte($Text.Substring($index * 2, 2), 16)
    }
    return $bytes
}

function Convert-Base64Url([byte[]]$Bytes) {
    return [Convert]::ToBase64String($Bytes).TrimEnd('=').Replace('+', '-').Replace('/', '_')
}

function New-PlayerAccessToken(
    [string]$PeerId, [long]$IssuedAt, [long]$ExpiresAt, [string]$KeyHex) {
    $nonce = New-Object byte[] 16
    [System.Security.Cryptography.RandomNumberGenerator]::Create().GetBytes($nonce)
    $peer = Convert-Base64Url ([Text.Encoding]::UTF8.GetBytes($PeerId))
    $encodedNonce = Convert-Base64Url $nonce
    $payload = "ay1.$peer.$IssuedAt.$ExpiresAt.$encodedNonce"
    $hmac = New-Object System.Security.Cryptography.HMACSHA256
    try {
        $hmac.Key = Convert-HexBytes $KeyHex
        $signature = $hmac.ComputeHash([Text.Encoding]::UTF8.GetBytes($payload))
        return "$payload.$(Convert-Base64Url $signature)"
    }
    finally { $hmac.Dispose() }
}

try {
    $authKey = "2b" * 32
    if ($Production) {
        $issuedAt = [DateTimeOffset]::UtcNow.ToUnixTimeSeconds()
        $ownerToken = New-PlayerAccessToken `
            "online-owner" $issuedAt ($issuedAt + 300) $authKey
        $guestToken = New-PlayerAccessToken `
            "online-guest" $issuedAt ($issuedAt + 300) $authKey
    }
    else {
        $ownerToken = "owner-" + ("a" * 58)
        $guestToken = "guest-" + ("b" * 58)
    }
    $fleetToken = "fleet-" + ("c" * 58)
    $environment = @{
        AY_ONLINE_ENABLE = "1"
        AY_ONLINE_SERVER_TOKEN = $fleetToken
        AY_ONLINE_OWNER_TOKEN = $ownerToken
        AY_ONLINE_GUEST_TOKEN = $guestToken
    }
    if ($Production) {
        $environment.AY_SESSION_PRODUCTION = "1"
        $environment.AY_SESSION_DB = Join-Path $tempRoot "sessions.db"
        $environment.AY_SESSION_STATE_KEY = "1a" * 32
        $environment.AY_SESSION_TICKET_KEY_FILE = Join-Path $tempRoot "ticket.key"
        $environment.AY_SESSION_ADMISSION_TOKEN = "admission-test-" + ("a" * 50)
        $environment.AY_SESSION_AUDIT_FILE = Join-Path $tempRoot "audit.jsonl"
        $environment.AY_SESSION_HTTP_BIND = "127.0.0.1"
        $environment.AY_ONLINE_DB = $environment.AY_SESSION_DB
        $environment.AY_ONLINE_AUTH_KEY = $authKey
    }
    else {
        Set-Content -LiteralPath $credentials -Value @(
            "online-owner $ownerToken",
            "online-guest $guestToken"
        ) -Encoding ASCII
        $environment.AY_ONLINE_CREDENTIALS_FILE = $credentials
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
    if ($Production -and
        !(Read-Log $serverOut).Contains(
            "mode=production store=sqlite online=enabled online_store=sqlite")) {
        throw "Online SessionServer did not use durable production mode: $(Read-Log $serverOut)"
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
    if ($Production) {
        foreach ($required in @(
            $env:AY_SESSION_DB,
            $env:AY_SESSION_TICKET_KEY_FILE,
            $env:AY_SESSION_AUDIT_FILE)) {
            if (!(Test-Path -LiteralPath $required)) {
                throw "Production Online Services artifact missing: $required"
            }
        }
        $audit = Get-Content -LiteralPath $env:AY_SESSION_AUDIT_FILE -Raw
        if (!$audit.Contains('"status":200') -or
            $audit.Contains($ownerToken) -or $audit.Contains($guestToken) -or
            $audit.Contains($fleetToken)) {
            throw "Production Online Services audit is incomplete or leaked a bearer"
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
