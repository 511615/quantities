param(
    [ValidateSet("start", "stop", "restart", "status", "smoke")]
    [string]$Action = "status",
    [int]$Port = 8000,
    [string]$BindHost = "127.0.0.1"
)

$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = Split-Path -Parent $scriptDir
$repoRoot = Split-Path -Parent $repoRoot
$tmpDir = Join-Path $repoRoot ".tmp"
$statePath = Join-Path $tmpDir "backend-runtime-state.json"
$stdoutPath = Join-Path $repoRoot "backend-dev.out.log"
$stderrPath = Join-Path $repoRoot "backend-dev.err.log"
$pythonPath = $null

if (-not (Test-Path $tmpDir)) {
    New-Item -ItemType Directory -Path $tmpDir | Out-Null
}

function Get-PythonCommandPath {
    try {
        $command = Get-Command python -ErrorAction Stop
        return $command.Source
    }
    catch {
        return $null
    }
}

function Get-ManagedBackendExecutablePath {
    $state = Get-State
    if ($null -ne $state -and $state.pid) {
        $process = Get-CimInstance Win32_Process -Filter "ProcessId = $($state.pid)" -ErrorAction SilentlyContinue
        if ($null -ne $process -and -not [string]::IsNullOrWhiteSpace($process.ExecutablePath)) {
            return $process.ExecutablePath
        }
    }

    try {
        $listener = Get-NetTCPConnection -State Listen -LocalPort $Port -ErrorAction Stop | Select-Object -First 1
        if ($null -ne $listener) {
            $process = Get-CimInstance Win32_Process -Filter "ProcessId = $($listener.OwningProcess)" -ErrorAction SilentlyContinue
            if ($null -ne $process -and -not [string]::IsNullOrWhiteSpace($process.ExecutablePath) -and (Is-ManagedBackendProcess ([int]$listener.OwningProcess))) {
                return $process.ExecutablePath
            }
        }
    }
    catch {
        return $null
    }

    return $null
}

function Resolve-PythonExecutable {
    $candidates = @(
        (Get-ManagedBackendExecutablePath),
        (Get-PythonCommandPath),
        (Join-Path $repoRoot ".venv\\Scripts\\python.exe")
    ) | Where-Object { -not [string]::IsNullOrWhiteSpace($_) } | Select-Object -Unique

    foreach ($candidate in $candidates) {
        if (Test-Path $candidate -ErrorAction SilentlyContinue) {
            return $candidate
        }
    }
    throw "No usable Python runtime was found."
}
function Get-State {
    if (-not (Test-Path $statePath)) {
        return $null
    }
    return Get-Content -Path $statePath -Raw | ConvertFrom-Json
}

function Save-State([int]$PidValue) {
    $state = [ordered]@{
        pid = $PidValue
        host = $BindHost
        port = $Port
        repo_root = $repoRoot
        stdout_log = $stdoutPath
        stderr_log = $stderrPath
        updated_at = (Get-Date).ToString("o")
    }
    $state | ConvertTo-Json | Set-Content -Path $statePath -Encoding UTF8
}

function Remove-State {
    if (Test-Path $statePath) {
        Remove-Item -LiteralPath $statePath -Force
    }
}

function Get-ProcessCommandLine([int]$PidValue) {
    $process = Get-CimInstance Win32_Process -Filter "ProcessId = $PidValue" -ErrorAction SilentlyContinue
    if ($null -eq $process) {
        return $null
    }
    return $process.CommandLine
}

function Is-ManagedBackendProcess([int]$PidValue) {
    $commandLine = Get-ProcessCommandLine $PidValue
    if ([string]::IsNullOrWhiteSpace($commandLine)) {
        return $false
    }
    return (
        $commandLine -match "quant_platform\.webapi\.main" -or
        $commandLine -match "quant_platform\.webapi\.app:create_app" -or
        $commandLine -match "quant-platform-web"
    )
}

function Stop-BackendProcess([int]$PidValue) {
    $process = Get-Process -Id $PidValue -ErrorAction SilentlyContinue
    if ($null -eq $process) {
        return
    }
    Stop-Process -Id $PidValue -Force
}

function Stop-ManagedBackend {
    $stopped = @()
    $state = Get-State
    if ($null -ne $state -and $state.pid) {
        $pidValue = [int]$state.pid
        if (Get-Process -Id $pidValue -ErrorAction SilentlyContinue) {
            Stop-BackendProcess $pidValue
            $stopped += $pidValue
        }
    }

    try {
        $listeners = Get-NetTCPConnection -State Listen -LocalPort $Port -ErrorAction Stop
    }
    catch {
        $listeners = @()
    }

    foreach ($listener in $listeners) {
        $pidValue = [int]$listener.OwningProcess
        if ($stopped -contains $pidValue) {
            continue
        }
        if (Is-ManagedBackendProcess $pidValue) {
            Stop-BackendProcess $pidValue
            $stopped += $pidValue
        }
    }

    Remove-State
    return $stopped
}

function Wait-ForHealth {
    $healthUrl = "http://$BindHost`:$Port/health"
    for ($index = 0; $index -lt 50; $index++) {
        try {
            $response = Invoke-RestMethod -Uri $healthUrl -Method Get -TimeoutSec 2
            if ($response.status -eq "ok") {
                return $true
            }
        }
        catch {
            Start-Sleep -Milliseconds 200
        }
    }
    return $false
}

function Start-ManagedBackend {
    $srcPath = Join-Path $repoRoot "src"
    $launcherPath = Join-Path $scriptDir "run_backend.py"
    $env:QUANT_PLATFORM_WEB_HOST = $BindHost
    $env:QUANT_PLATFORM_WEB_PORT = "$Port"
    if ([string]::IsNullOrWhiteSpace($env:PYTHONPATH)) {
        $env:PYTHONPATH = $srcPath
    }
    elseif (-not (($env:PYTHONPATH -split ';') -contains $srcPath)) {
        $env:PYTHONPATH = "$srcPath;$($env:PYTHONPATH)"
    }
    $process = Start-Process `
        -FilePath $pythonPath `
        -ArgumentList '-u', $launcherPath `
        -WorkingDirectory $repoRoot `
        -RedirectStandardOutput $stdoutPath `
        -RedirectStandardError $stderrPath `
        -PassThru

    Save-State $process.Id
    if (-not (Wait-ForHealth)) {
        Stop-BackendProcess $process.Id
        Remove-State
        throw "Backend failed to become healthy on http://$BindHost`:$Port."
    }
    return $process
}

function Show-Status {
    $state = Get-State
    $portOwner = $null
    try {
        $listener = Get-NetTCPConnection -State Listen -LocalPort $Port -ErrorAction Stop | Select-Object -First 1
        if ($null -ne $listener) {
            $portOwner = [int]$listener.OwningProcess
        }
    }
    catch {
        $portOwner = $null
    }

    $payload = [ordered]@{
        host = $BindHost
        port = $Port
        state = $state
        listening_pid = $portOwner
        listening_command = if ($null -ne $portOwner) { Get-ProcessCommandLine $portOwner } else { $null }
        health = $false
    }

    try {
        $response = Invoke-RestMethod -Uri "http://$BindHost`:$Port/health" -Method Get -TimeoutSec 2
        $payload.health = ($response.status -eq "ok")
    }
    catch {
        $payload.health = $false
    }

    $payload | ConvertTo-Json -Depth 6
}

$pythonPath = Resolve-PythonExecutable

switch ($Action) {
    "stop" {
        $stopped = Stop-ManagedBackend
        [ordered]@{ action = "stop"; stopped_pids = $stopped } | ConvertTo-Json
    }
    "start" {
        $null = Stop-ManagedBackend
        $process = Start-ManagedBackend
        [ordered]@{
            action = "start"
            pid = $process.Id
            base_url = "http://$BindHost`:$Port"
            stdout_log = $stdoutPath
            stderr_log = $stderrPath
        } | ConvertTo-Json
    }
    "restart" {
        $stopped = Stop-ManagedBackend
        $process = Start-ManagedBackend
        [ordered]@{
            action = "restart"
            stopped_pids = $stopped
            pid = $process.Id
            base_url = "http://$BindHost`:$Port"
            stdout_log = $stdoutPath
            stderr_log = $stderrPath
        } | ConvertTo-Json
    }
    "status" {
        Show-Status
    }
    "smoke" {
        & $pythonPath (Join-Path $scriptDir "backend_smoke.py") "http://$BindHost`:$Port"
    }
}
