param(
    [ValidateSet("dev", "deploy")]
    [string]$Mode = "dev",
    [string]$FrontendHost = "127.0.0.1",
    [int]$FrontendPort = 5173,
    [string]$BackendHost = "127.0.0.1",
    [int]$BackendPort = 8015,
    [switch]$InstallDeps
)

$ErrorActionPreference = "Stop"
$ProgressPreference = "SilentlyContinue"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = Split-Path -Parent $scriptDir
$repoRoot = Split-Path -Parent $repoRoot
$webRoot = Join-Path $repoRoot "apps\web"
$frontendOutLog = Join-Path $webRoot "frontend-start-local.out.log"
$frontendErrLog = Join-Path $webRoot "frontend-start-local.err.log"
$frontendStatePath = Join-Path $repoRoot ".tmp\frontend-runtime-state.json"
$backendControlOutLog = Join-Path $repoRoot ".tmp\start-local-backend-control.out.log"
$backendControlErrLog = Join-Path $repoRoot ".tmp\start-local-backend-control.err.log"
$viteCommandLinePattern = "*vite*--host $FrontendHost --port $FrontendPort*"
$npmCommandLinePattern = "*npm.cmd run dev -- --host $FrontendHost --port $FrontendPort*"

function Ensure-PathExists([string]$PathValue, [string]$Message) {
    if (-not (Test-Path $PathValue)) {
        throw $Message
    }
}

function Get-PythonExecutable {
    $candidate = Join-Path $repoRoot ".venv\Scripts\python.exe"
    if (Test-Path $candidate) {
        return $candidate
    }
    $command = Get-Command python -ErrorAction SilentlyContinue
    if ($null -ne $command) {
        return $command.Source
    }
    throw "Python runtime not found. Expected .venv\Scripts\python.exe or python on PATH."
}

function Save-FrontendState([int]$PidValue) {
    $state = [ordered]@{
        pid = $PidValue
        host = $FrontendHost
        port = $FrontendPort
        mode = $Mode
        updated_at = (Get-Date).ToString("o")
        stdout_log = $frontendOutLog
        stderr_log = $frontendErrLog
    }
    $state | ConvertTo-Json | Set-Content -Path $frontendStatePath -Encoding UTF8
}

function Remove-FrontendState {
    if (Test-Path $frontendStatePath) {
        Remove-Item -LiteralPath $frontendStatePath -Force
    }
}

function Stop-FrontendIfRunning {
    $candidates = Get-CimInstance Win32_Process -ErrorAction SilentlyContinue | Where-Object {
        ($_.CommandLine -like $viteCommandLinePattern) -or
        ($_.CommandLine -like $npmCommandLinePattern)
    }

    foreach ($candidate in $candidates) {
        try {
            Stop-Process -Id $candidate.ProcessId -Force -ErrorAction Stop
        }
        catch {
        }
    }

    Remove-FrontendState
}

function Wait-ForUrl([string]$Url, [int]$Attempts = 60, [int]$DelayMs = 500) {
    for ($index = 0; $index -lt $Attempts; $index++) {
        try {
            $response = Invoke-WebRequest -Uri $Url -TimeoutSec 3
            if ($response.StatusCode -ge 200 -and $response.StatusCode -lt 500) {
                return $true
            }
        }
        catch {
            Start-Sleep -Milliseconds $DelayMs
        }
    }
    return $false
}

function Invoke-Npm([string[]]$Arguments) {
    $cmd = Get-Command npm.cmd -ErrorAction SilentlyContinue
    if ($null -eq $cmd) {
        throw "npm.cmd not found. Install Node.js and ensure npm.cmd is on PATH."
    }
    & $cmd.Source @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "npm command failed: npm.cmd $($Arguments -join ' ')"
    }
}

function Start-FrontendDevServer {
    Stop-FrontendIfRunning
    if (Test-Path $frontendOutLog) {
        Remove-Item -LiteralPath $frontendOutLog -Force
    }
    if (Test-Path $frontendErrLog) {
        Remove-Item -LiteralPath $frontendErrLog -Force
    }

    $process = Start-Process `
        -FilePath "C:\Windows\System32\cmd.exe" `
        -ArgumentList "/c", "npm.cmd run dev -- --host $FrontendHost --port $FrontendPort" `
        -WorkingDirectory $webRoot `
        -RedirectStandardOutput $frontendOutLog `
        -RedirectStandardError $frontendErrLog `
        -PassThru

    Save-FrontendState $process.Id

    $frontendUrl = "http://$FrontendHost`:$FrontendPort"
    if (-not (Wait-ForUrl -Url $frontendUrl)) {
        try {
            Stop-Process -Id $process.Id -Force -ErrorAction Stop
        }
        catch {
        }
        Remove-FrontendState
        throw "Frontend dev server failed to become ready on $frontendUrl."
    }

    return $process
}

Ensure-PathExists $repoRoot "Repository root not found."
Ensure-PathExists $webRoot "Frontend app directory not found at apps\web."

$tmpDir = Join-Path $repoRoot ".tmp"
if (-not (Test-Path $tmpDir)) {
    New-Item -ItemType Directory -Path $tmpDir | Out-Null
}

$pythonPath = Get-PythonExecutable
$env:QUANT_PLATFORM_WEB_HOST = $BackendHost
$env:QUANT_PLATFORM_WEB_PORT = "$BackendPort"

if ($InstallDeps) {
    & $pythonPath -m pip install -e ".[dev]"
    if ($LASTEXITCODE -ne 0) {
        throw "Python dependency installation failed."
    }
    Invoke-Npm @("install")
}

if ($Mode -eq "deploy") {
    Push-Location $webRoot
    try {
        Invoke-Npm @("run", "build")
    }
    finally {
        Pop-Location
    }
}

$backendControlScript = Join-Path $scriptDir "backend_control.ps1"
Ensure-PathExists $backendControlScript "Backend control script not found."

if (Test-Path $backendControlOutLog) {
    Remove-Item -LiteralPath $backendControlOutLog -Force
}
if (Test-Path $backendControlErrLog) {
    Remove-Item -LiteralPath $backendControlErrLog -Force
}

$backendControlProcess = Start-Process `
    -FilePath "C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe" `
    -ArgumentList "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-File", $backendControlScript, "-Action", "restart", "-Port", "$BackendPort", "-BindHost", $BackendHost `
    -WorkingDirectory $repoRoot `
    -RedirectStandardOutput $backendControlOutLog `
    -RedirectStandardError $backendControlErrLog `
    -PassThru `
    -Wait

if ($backendControlProcess.ExitCode -ne 0) {
    throw "Backend control script failed. Check $backendControlOutLog and $backendControlErrLog."
}

$backendUrl = "http://$BackendHost`:$BackendPort"
if (-not (Wait-ForUrl -Url "$backendUrl/health" -Attempts 40 -DelayMs 250)) {
    throw "Backend failed to become healthy on $backendUrl."
}

$frontendUrl = $null
$frontendProcess = $null

if ($Mode -eq "dev") {
    Push-Location $webRoot
    try {
        $frontendProcess = Start-FrontendDevServer
    }
    finally {
        Pop-Location
    }
    $frontendUrl = "http://$FrontendHost`:$FrontendPort"
}
else {
    $frontendUrl = $backendUrl
}

$result = [ordered]@{
    mode = $Mode
    frontend_url = $frontendUrl
    backend_url = $backendUrl
    frontend_pid = if ($null -ne $frontendProcess) { $frontendProcess.Id } else { $null }
    frontend_log = if ($Mode -eq "dev") { $frontendOutLog } else { $null }
    backend_health = "$backendUrl/health"
} | ConvertTo-Json -Compress

Write-Output $result
exit 0
