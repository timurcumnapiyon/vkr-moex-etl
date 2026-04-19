$ErrorActionPreference = "Stop"

$RepoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $RepoRoot

$LogDir = Join-Path $RepoRoot "logs\health"
New-Item -Path $LogDir -ItemType Directory -Force | Out-Null
$LogFile = Join-Path $LogDir ("daily_health_{0}.log" -f (Get-Date -Format "yyyy-MM-dd_HH-mm-ss"))

function Write-Log {
    param([string]$Message)
    $line = "[{0}] {1}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), $Message
    $line | Tee-Object -FilePath $LogFile -Append
}

function Resolve-Python {
    $candidates = @(
        (Join-Path $RepoRoot ".venv_clean\Scripts\python.exe"),
        (Join-Path $RepoRoot ".venv\Scripts\python.exe")
    )
    foreach ($p in $candidates) {
        if (Test-Path $p) { return $p }
    }
    throw "Python venv не найден (.venv_clean или .venv)."
}

$pythonExe = Resolve-Python
$env:PREFECT_API_URL = "http://127.0.0.1:4200/api"

Write-Log "Ежедневный health-check запущен."

Write-Log "Статусы Docker-контейнеров:"
docker ps --format "table {{.Names}}\t{{.Status}}" | Tee-Object -FilePath $LogFile -Append | Out-Null

Write-Log "Состояние очереди Prefect:"
& $pythonExe -m prefect work-queue inspect default --pool vkr-docker-pool | Tee-Object -FilePath $LogFile -Append | Out-Null

Write-Log "Проверка Late run (первые 10):"
& $pythonExe -m prefect flow-run ls --state LATE --limit 10 | Tee-Object -FilePath $LogFile -Append | Out-Null

Write-Log "Проверка Failed/Crash run (первые 10):"
& $pythonExe -m prefect flow-run ls --state FAILED --state CRASHED --limit 10 | Tee-Object -FilePath $LogFile -Append | Out-Null

Write-Log "Ежедневный health-check завершен."
Write-Host "Лог health-check: $LogFile"
