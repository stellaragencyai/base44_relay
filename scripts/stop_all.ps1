param(
  [string]$BaseDir = "C:\Users\nolan\Desktop\Base 44"
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

# Kill python modules we launched (match by -m)
$patterns = @(
  "-m relay.base44_relay",
  "-m core.risk_daemon",
  "-m bots.tp_sl_manager",
  "-m bots.executor_v1",
  "-m bots.trade_executor",
  "-m bots.pnl_daily",
  "ngrok http http://127.0.0.1:5000"
)

# Kill python child shells we started
function Stop-ByPattern {
  param([string]$pat)
  Get-CimInstance Win32_Process |
    Where-Object {
      ($_.Name -match "python\.exe" -or $_.Name -match "ngrok\.exe" -or $_.Name -match "powershell\.exe") -and
      ($_.CommandLine -like "*$pat*")
    } |
    ForEach-Object {
      try {
        Stop-Process -Id $_.ProcessId -Force -ErrorAction Stop
        Write-Host ("Stopped PID {0}  [{1}]" -f $_.ProcessId, $pat)
      } catch {}
    }
}

$patterns | ForEach-Object { Stop-ByPattern $_ }

# As a courtesy, kill any orphaned python from our venv in BaseDir
$venv = Join-Path $BaseDir ".venv\Scripts\python.exe"
Get-CimInstance Win32_Process | Where-Object { $_.ExecutablePath -eq $venv } | ForEach-Object {
  try { Stop-Process -Id $_.ProcessId -Force; Write-Host "Stopped orphaned venv python PID $($_.ProcessId)" } catch {}
}

Write-Host "All targets stopped (as much as Windows allows)."
