param(
  [string]$BaseDir = "C:\Users\nolan\Desktop\Base 44",
  [int]$RestartDelaySec = 5
)

function New-Tab {
  param([string]$Title, [string]$Cmd)
  Start-Process powershell -ArgumentList @(
    "-NoExit","-Command",
    "Write-Host '== $Title =='; cd '$BaseDir'; .\.venv\Scripts\python.exe $Cmd"
  )
}

# 0) Sanity
if (-not (Test-Path (Join-Path $BaseDir ".venv\Scripts\python.exe"))) {
  Write-Error "Venv python not found. Check $BaseDir\.venv"
  exit 1
}

# 1) Bring up relay (Flask) in its own console
Start-Process powershell -ArgumentList @(
  "-NoExit","-Command",
  "cd '$BaseDir'; .\.venv\Scripts\python.exe -m relay.base44_relay"
) -WindowStyle Normal

# 2) ngrok (only if you need the public URL)
if (Get-Command ngrok -ErrorAction SilentlyContinue) {
  Start-Process powershell -ArgumentList @(
    "-NoExit","-Command",
    "cd '$BaseDir'; ngrok http http://127.0.0.1:5000"
  ) -WindowStyle Normal
}

# 3) Notifier heartbeat
New-Tab -Title "notifier" -Cmd "-m core.notifier_bot --ping session_boot"

# 4) Risk Daemon (auto breaker)
New-Tab -Title "risk_daemon" -Cmd "-m core.risk_daemon"

# 5) TP/SL Manager (your current 5-rung version)
#    Replace module name below if you renamed it.
New-Tab -Title "tp_sl_manager" -Cmd "-m bots.tp_sl_manager"

# 6) Trade Executor (breaker-enforced)
New-Tab -Title "trade_executor" -Cmd "-m bots.trade_executor"

# 7) PnL snapshot now (optional)
New-Tab -Title "pnl_snapshot" -Cmd "-m bots.pnl_daily --snapshot"

Write-Host "Launched. Each service has its own window."
