. "$PSScriptRoot\env.ps1"

Write-Host "Tailing relay.log, executor.log, tpsl.log — Ctrl+C to quit."
Get-Content "$env:B44_LOGS\relay.log" -Wait | Write-Host -ForegroundColor Yellow &
Get-Content "$env:B44_LOGS\executor.log" -Wait | Write-Host -ForegroundColor Cyan &
Get-Content "$env:B44_LOGS\tpsl.log" -Wait | Write-Host -ForegroundColor Green
