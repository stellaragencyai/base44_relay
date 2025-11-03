param(
  [Parameter(Mandatory=$true)][string]$SubLabel,
  [ValidateSet("tp","risk","both")][string]$Bot="both"
)
$Base = "C:\Users\nolan\Desktop\Base 44"
$env:PYTHONUTF8=1
Set-Location $Base
.\.venv\Scripts\Activate.ps1 | Out-Null

# Load UID from registry
$map = Get-Content "$Base\registry\sub_map.json" -Raw | ConvertFrom-Json
$sub = $map.subs.$SubLabel
if (-not $sub) { Write-Host "Unknown sub label $SubLabel"; exit 1 }
$uid = $sub.uid
if (-not $uid) { Write-Host "$SubLabel has empty uid in registry/sub_map.json"; exit 1 }

$env:BYBIT_SUB_UID = $uid
Write-Host "Launching for $SubLabel (uid=$uid) ..."

if ($Bot -eq "risk" -or $Bot -eq "both") {
  Start-Process -NoNewWindow -FilePath python -ArgumentList "bots\risk_daemon.py"
}
if ($Bot -eq "tp" -or $Bot -eq "both") {
  Start-Process -NoNewWindow -FilePath python -ArgumentList "bots\tp_sl_manager.py"
}
