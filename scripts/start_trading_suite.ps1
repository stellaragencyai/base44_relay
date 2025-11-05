param(
  [string]$BaseDir = "C:\Users\nolan\Desktop\Base 44"
)

$py = Join-Path $BaseDir ".venv\Scripts\python.exe"
if (-not (Test-Path $py)) { $py = "python" }

function Launch($title, $cmdArgs) {
  Start-Process -FilePath "powershell.exe" -ArgumentList @("-NoExit","-Command","$cmdArgs") -WorkingDirectory $BaseDir -WindowStyle Minimized -Verb RunAs
  Start-Sleep -Milliseconds 300
}

# Notifier (heartbeats + TG relay)
Launch "Notifier" "$py -m core.notifier_bot --heartbeat 60"

# Risk Daemon (DD cap 10%)
$env:RISK_MAX_DD_PCT = "10.0"
Launch "RiskDaemon" "$py -m core.risk_daemon"

# PnL Logger
Launch "PnLLogger" "$py -m bots.pnl_logger"

# TP/SL Manager (5 equal TPs + auto-resize)
Launch "TPSL" "$py -m bots.tp_sl_manager"

Write-Host "Launched: notifier, risk_daemon, pnl_logger, tp_sl_manager" -ForegroundColor Green
