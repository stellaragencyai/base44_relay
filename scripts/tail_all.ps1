param(
  [string]$BaseDir = "C:\Users\nolan\Desktop\Base 44"
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$Logs = Join-Path $BaseDir "logs"
$new = New-Item -ItemType Directory -Force -Path $Logs; $null = $new

$files = @(
  "relay.log",
  "risk_daemon.log",
  "tpsl.log",
  "executor.log",
  "pnl_snapshot.log",
  "ngrok.log"
)

function Tail-One {
  param([string]$title, [string]$path)
  $ps = @"
`$Host.UI.RawUI.WindowTitle = '$title'
if (Test-Path '$path') {
  Write-Host "== tailing: $path =="
  Get-Content -Path '$path' -Wait -Tail 200
} else {
  Write-Host "== $path not found (yet). Waiting... =="
  while (-not (Test-Path '$path')) { Start-Sleep -Seconds 1 }
  Get-Content -Path '$path' -Wait -Tail 200
}
"@
  Start-Process powershell -ArgumentList @("-NoExit","-NoLogo","-Command",$ps) | Out-Null
}

foreach ($f in $files) {
  Tail-One -title $f -path (Join-Path $Logs $f)
}

Write-Host "Opened tail windows for: $($files -join ', ')"
