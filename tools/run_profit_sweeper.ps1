# tools/run_profit_sweeper.ps1
# Runs the auto_profit_sweeper.py in a resilient way with logging and single-instance lock.

param(
  [string]$BaseDir = "C:\Users\nolan\Desktop\Base 44",
  [string]$VenvAct = ".\.venv\Scripts\Activate.ps1",
  [string]$PyFile  = "tools\auto_profit_sweeper.py"
)

$ErrorActionPreference = "Stop"
Set-Location $BaseDir

# Ensure logs folder
$LogDir = Join-Path $BaseDir "logs\profit_sweeper"
New-Item -ItemType Directory -Force -Path $LogDir | Out-Null

# Single instance via lock file + mutex
$LockFile = Join-Path $LogDir "profit_sweeper.lock"
$MutexName = "Global\Base44_ProfitSweeper_Mutex"
$mutex = New-Object System.Threading.Mutex($false, $MutexName, [ref]$createdNew)
if (-not $createdNew) {
  Write-Host "[run_profit_sweeper] Another instance is running (mutex). Exiting."
  exit 0
}
try {
  Set-Content -Path $LockFile -Value "$PID" -Encoding ascii
} catch {}

# Activate venv
& $VenvAct | Out-Null

# Daily log file
$Stamp  = Get-Date -Format "yyyy-MM-dd"
$Log    = Join-Path $LogDir "$Stamp.log"

# Rotate: keep last 14 daily logs
Get-ChildItem $LogDir -Filter "*.log" | Sort-Object LastWriteTime -Descending | Select-Object -Skip 14 | Remove-Item -Force -ErrorAction SilentlyContinue

# Run loop with backoff on crash
$Backoff = 5
while ($true) {
  $ts = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
  "[$ts] runner: starting $PyFile" | Tee-Object -FilePath $Log -Append | Out-Null
  try {
    # Start Python script, stream output to log
    & python $PyFile 2>&1 | Tee-Object -FilePath $Log -Append
    $code = $LASTEXITCODE
  } catch {
    $code = 1
    "[$(Get-Date -Format "yyyy-MM-dd HH:mm:ss")] runner/error: $_" | Tee-Object -FilePath $Log -Append | Out-Null
  }

  if ($code -eq 0) {
    "[$(Get-Date -Format "yyyy-MM-dd HH:mm:ss")] runner: script exited cleanly. Restarting in 5s..." | Tee-Object -FilePath $Log -Append | Out-Null
    Start-Sleep -Seconds 5
    continue
  } else {
    "[$(Get-Date -Format "yyyy-MM-dd HH:mm:ss")] runner: script crashed (code $code). Backoff ${Backoff}s..." | Tee-Object -FilePath $Log -Append | Out-Null
    Start-Sleep -Seconds $Backoff
    $Backoff = [Math]::Min($Backoff * 2, 300)  # cap at 5 min
  }
}
# Release mutex on exit
$mutex.ReleaseMutex() | Out-Null
