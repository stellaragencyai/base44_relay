# scripts/start_all_monitors.ps1
param(
  [string]$BaseDir = "C:\Users\nolan\Desktop\Base 44"
)

$venv = Join-Path $BaseDir ".venv\Scripts\Activate.ps1"
$bots = Join-Path $BaseDir "bots"

$procs = @(
  @{ name="pnl_logger";      file="pnl_logger.py" }
  @{ name="trade_historian"; file="trade_historian.py" }
  @{ name="risk_daemon";     file="risk_daemon.py" }
  @{ name="tp_sl_manager";   file="tp_sl_manager.py" }
  @{ name="relay_watchdog";  file="relay_watchdog.py" }  # ← renamed
)

foreach ($p in $procs) {
  $title = "B44 - $($p.name)"
  Start-Process powershell -ArgumentList @(
    "-NoExit",
    "-Command", "& { $host.ui.RawUI.WindowTitle = '$title'; Set-Location '$bots'; . '$venv'; python '$($p.file)' }"
  ) -WindowStyle Minimized -WorkingDirectory $bots -Verb RunAs
  Start-Sleep -Milliseconds 400
}
Write-Host "Launched monitors in separate terminals."

# Tip: start Coach instances separately (master or per sub) as needed:
#   powershell -NoExit -Command "& { Set-Location '$bots'; . '$venv'; python coach.py }"
#   powershell -NoExit -Command "& { Set-Location '$bots'; . '$venv'; $env:COACH_MEMBER_ID='302355261'; python coach.py }"
