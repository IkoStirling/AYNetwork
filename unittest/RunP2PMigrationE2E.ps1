param(
    [Parameter(Mandatory = $true)][string]$SignalingServer,
    [Parameter(Mandatory = $true)][string]$SmokePeer,
    [ValidateSet("normal", "reject", "drop")][string]$Scenario = "normal",
    [int]$TimeoutSeconds = 70
)

$ErrorActionPreference = "Stop"
$work = Join-Path ([System.IO.Path]::GetTempPath()) (
    "aynetwork-p2p-migration-" + [Guid]::NewGuid().ToString("N"))
[void](New-Item -ItemType Directory -Path $work)
$processes = @()

function Read-Log([string]$name) {
    $stdout = Join-Path $work ($name + ".out.log")
    $stderr = Join-Path $work ($name + ".err.log")
    $text = ""
    foreach ($path in @($stdout, $stderr)) {
        if (-not (Test-Path -LiteralPath $path)) {
            continue
        }
        $stream = $null
        $reader = $null
        try {
            $stream = New-Object System.IO.FileStream(
                $path, [System.IO.FileMode]::Open, [System.IO.FileAccess]::Read,
                [System.IO.FileShare]::ReadWrite)
            $reader = New-Object System.IO.StreamReader($stream)
            $text += $reader.ReadToEnd()
        } finally {
            if ($reader) { $reader.Dispose() }
            elseif ($stream) { $stream.Dispose() }
        }
    }
    return $text
}

function Start-Logged(
    [string]$name, [string]$filePath, [string[]]$arguments) {
    $stdout = Join-Path $work ($name + ".out.log")
    $stderr = Join-Path $work ($name + ".err.log")
    $process = Start-Process -FilePath $filePath -ArgumentList $arguments `
        -RedirectStandardOutput $stdout -RedirectStandardError $stderr `
        -NoNewWindow -PassThru
    # Windows PowerShell 5 loses ExitCode unless the process handle is opened
    # before the child exits.
    $null = $process.Handle
    $script:processes += $process
    return $process
}

function Set-CommonPeerEnvironment([int]$expectedFailure) {
    $remove = @(
        "AY_P2P_SIGNAL_ROOM", "AY_P2P_SIGNAL_TOKEN", "AY_P2P_STUN",
        "AY_P2P_TURN", "AY_P2P_TURN_USER", "AY_P2P_TURN_PASS",
        "AY_P2P_MIGRATION_REJECT_PREPARE",
        "AY_P2P_MIGRATION_DROP_AFTER_PREPARE"
    )
    foreach ($name in $remove) {
        Remove-Item ("Env:" + $name) -ErrorAction SilentlyContinue
    }
    $env:AY_P2P_ICE_POLICY = "direct"
    $env:AY_P2P_ALLOW_PRIVATE = "true"
    $env:AY_P2P_EXPECT_PATH = "any"
    $env:AY_P2P_HOST_MIGRATION = "true"
    $env:AY_P2P_MIGRATION_MEMBERS = "3"
    $env:AY_P2P_HOST_TIMEOUT_SECONDS = "35"
    $env:AY_P2P_EXPECT_MIGRATION_FAILURE = [string]$expectedFailure
}

function Assert-Contains([string]$name, [string]$needle) {
    $text = Read-Log $name
    if (-not $text.Contains($needle)) {
        throw ("{0} did not contain '{1}'.`n{2}" -f $name, $needle, $text)
    }
}

try {
    $probe = New-Object System.Net.Sockets.UdpClient(0)
    $signalPort = ([System.Net.IPEndPoint]$probe.Client.LocalEndPoint).Port
    $probe.Close()
    $virtualPort = Get-Random -Minimum 12000 -Maximum 32000

    $signal = Start-Logged "signaling" $SignalingServer @(
        "127.0.0.1", [string]$signalPort)
    Start-Sleep -Milliseconds 350
    if ($signal.HasExited) {
        throw ("signaling server exited early.`n" + (Read-Log "signaling"))
    }

    $expectedFailure = switch ($Scenario) {
        "reject" { 11 }
        # GNS can keep a closed ICE route logically alive beyond AYNetwork's
        # bounded 3 s Prepare window, so a dropped voter deterministically
        # resolves as PrepareTimeout rather than waiting for ICE failure.
        "drop" { 13 }
        default { 0 }
    }

    Set-CommonPeerEnvironment $expectedFailure
    $hostProcess = Start-Logged "host" $SmokePeer @(
        "host", "migration-host", "127.0.0.1",
        [string]$signalPort, [string]$virtualPort)
    Start-Sleep -Milliseconds 300

    Set-CommonPeerEnvironment $expectedFailure
    $joinAProcess = Start-Logged "join-a" $SmokePeer @(
        "join", "migration-a", "migration-host", "127.0.0.1",
        [string]$signalPort, [string]$virtualPort)
    Start-Sleep -Milliseconds 250

    Set-CommonPeerEnvironment $expectedFailure
    if ($Scenario -eq "reject") {
        $env:AY_P2P_MIGRATION_REJECT_PREPARE = "true"
    } elseif ($Scenario -eq "drop") {
        $env:AY_P2P_MIGRATION_DROP_AFTER_PREPARE = "true"
    }
    $joinBProcess = Start-Logged "join-b" $SmokePeer @(
        "join", "migration-b", "migration-host", "127.0.0.1",
        [string]$signalPort, [string]$virtualPort)

    $deadline = [DateTime]::UtcNow.AddSeconds($TimeoutSeconds)
    while ((-not $hostProcess.HasExited -or -not $joinAProcess.HasExited -or
            -not $joinBProcess.HasExited) -and [DateTime]::UtcNow -lt $deadline) {
        Start-Sleep -Milliseconds 100
        $hostProcess.Refresh()
        $joinAProcess.Refresh()
        $joinBProcess.Refresh()
    }
    if (-not $hostProcess.HasExited -or -not $joinAProcess.HasExited -or
        -not $joinBProcess.HasExited) {
        throw "migration processes exceeded timeout"
    }

    foreach ($entry in @(
        @{ Name = "host"; Process = $hostProcess },
        @{ Name = "join-a"; Process = $joinAProcess },
        @{ Name = "join-b"; Process = $joinBProcess })) {
        $entry.Process.WaitForExit()
        if ($entry.Process.ExitCode -ne 0) {
            throw ("{0} exited with {1}.`n{2}" -f
                $entry.Name, $entry.Process.ExitCode, (Read-Log $entry.Name))
        }
    }

    if ($Scenario -eq "normal") {
        Assert-Contains "host" "phase=departed"
        Assert-Contains "join-a" "phase=complete"
        Assert-Contains "join-b" "phase=complete"
    } elseif ($Scenario -eq "reject") {
        Assert-Contains "host" "phase=aborted reason=11"
        Assert-Contains "join-a" "phase=aborted reason=11"
        Assert-Contains "join-b" "phase=aborted reason=11"
    } else {
        Assert-Contains "host" "phase=aborted reason=13"
        Assert-Contains "join-a" "phase=aborted reason=13"
        Assert-Contains "join-b" "phase=drop-after-prepare"
    }

    Write-Output ("AYNetwork P2P migration E2E passed: " + $Scenario)
} catch {
    foreach ($name in @("host", "join-a", "join-b", "signaling")) {
        $text = Read-Log $name
        if ($text) {
            Write-Output ("===== " + $name + " =====")
            Write-Output $text
        }
    }
    throw
} finally {
    foreach ($process in $processes) {
        try {
            $process.Refresh()
            if (-not $process.HasExited) {
                Stop-Process -Id $process.Id -Force -ErrorAction SilentlyContinue
            }
        } catch {
        }
    }
    Remove-Item -LiteralPath $work -Recurse -Force -ErrorAction SilentlyContinue
}
