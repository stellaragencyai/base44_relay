param(
  [string]$BaseDir = "C:\Users\nolan\Desktop\Base 44",
  [int]$RestartDelaySec = 5
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

# Paths
$VenvPy = Join-Path $BaseDir ".venv\Scripts\python.exe"
$Logs   = Join-Path $BaseDir "logs"
New-Item -ItemType Directory -Force -Path $Logs | Out-Null

# Sanity
if (-not (Test-Path $VenvPy)) {
  Write-Error "Venv python not found: $VenvPy"
  exit 1
}

# Helper: persistent tab that restarts the target on exit and tees to a log
function New-TabWatch {
  param(
    [Parameter(Mandatory)] [string]$Title,
    [Parameter(Mandatory)] [string]$Cmd,       # e.g. "-m bots.executor_v1"
    [Parameter(Mandatory)] [string]$LogFile    # full path to log
  )
  $escapedLog = $LogFile.Replace("'", "''")
  $psCmd = @"
`$Host.UI.RawUI.WindowTitle = '$Title'
cd '$BaseDir'
while (`$true) {
  Write-Host ('== {0} @ {1} ==' -f '$Title', (Get-Date -Format o)) -ForegroundColor Yellow
  & '$VenvPy' $Cmd *>&1 | Tee-Object -FilePath '$escapedLog' -Append
  Write-Warning 'Process exited. Restarting in $RestartDelaySec seconds...'
  Start-Sleep -Seconds $RestartDelaySec
}
"@
  Start-Process powershell -ArgumentList @("-NoExit","-NoLogo","-Command", $psCmd) -WindowStyle Normal | Out-Null
}

# Helper: one-shot tab (no restart), still logs
function New-TabOnce {
  param(
    [Parameter(Mandatory)] [string]$Title,
    [Parameter(Mandatory)] [string]$Cmd,
    [Parameter(Mandatory)] [string]$LogFile
  )
  $escapedLog = $LogFile.Replace("'", "''")
  $psCmd = @"
`$Host.UI.RawUI.WindowTitle = '$Title'
cd '$BaseDir'
Write-Host ('== {0} @ {1} ==' -f '$Title', (Get-Date -Format o)) -ForegroundColor Yellow
& '$VenvPy' $Cmd *>&1 | Tee-Object -FilePath '$escapedLog' -Append
"@
  Start-Process powershell -ArgumentList @("-NoExit","-NoLogo","-Command", $psCmd) -WindowStyle Normal | Out-Null
}

# Decide which executor module we’re using
$ExecutorModule = if (Test-Path (Join-Path $BaseDir "bots\executor_v1.py")) { "-m bots.executor_v1" } else { "-m bots.trade_executor" }

# 1) Relay (Flask) — persistent
New-TabWatch -Title "relay" -Cmd "-m relay.base44_relay" -LogFile (Join-Path $Logs "relay.log")

# 2) ngrok (optional, no restart loop)
if (Get-Command ngrok -ErrorAction SilentlyContinue) {
  $ngrokCmd = @"
`$Host.UI.RawUI.WindowTitle = 'ngrok'
cd '$BaseDir'
ngrok http http://127.0.0.1:5000  *>&1 | Tee-Object -FilePath '$(Join-Path $Logs "ngrok.log")' -Append
"@
  Start-Process powershell -ArgumentList @("-NoExit","-NoLogo","-Command", $ngrokCmd) -WindowStyle Normal | Out-Null
}

# 3) Notifier heartbeat (one-shot)
New-TabOnce -Title "notifier" -Cmd "-m core.notifier_bot --ping session_boot" -LogFile (Join-Path $Logs "notifier.log")

# 4) Risk Daemon (persistent)
New-TabWatch -Title "risk_daemon" -Cmd "-m core.risk_daemon" -LogFile (Join-Path $Logs "risk_daemon.log")

# 5) TP/SL Manager (persistent)
New-TabWatch -Title "tp_sl_manager" -Cmd "-m bots.tp_sl_manager" -LogFile (Join-Path $Logs "tpsl.log")

# 6) Trade Executor (persistent) — v1 if present
New-TabWatch -Title "executor" -Cmd $ExecutorModule -LogFile (Join-Path $Logs "executor.log")

# 7) PnL snapshot (one-shot)
New-TabOnce -Title "pnl_snapshot" -Cmd "-m bots.pnl_daily --snapshot" -LogFile (Join-Path $Logs "pnl_snapshot.log")

Write-Host "Launched. Windows per service; logs in $Logs"
