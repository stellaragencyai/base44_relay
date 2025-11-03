# scripts/register_profit_sweeper.ps1
# Creates/updates a Windows Scheduled Task to run the profit sweeper 24/7.

param(
  [string]$TaskName = "Base44_ProfitSweeper",
  [string]$BaseDir  = "C:\Users\nolan\Desktop\Base 44",
  [string]$Runner   = "tools\run_profit_sweeper.ps1"
)

$ErrorActionPreference = "Stop"

$RunnerPath = Join-Path $BaseDir $Runner
if (-not (Test-Path $RunnerPath)) {
  Write-Host "Runner not found: $RunnerPath"
  exit 1
}

# Action: PowerShell hidden, bypass policy, run the runner
$ps = "$env:SystemRoot\System32\WindowsPowerShell\v1.0\powershell.exe"
$arg = "-NoProfile -WindowStyle Hidden -ExecutionPolicy Bypass -File `"$RunnerPath`" -BaseDir `"$BaseDir`""

$action   = New-ScheduledTaskAction -Execute $ps -Argument $arg -WorkingDirectory $BaseDir

# Triggers: at logon AND at startup
$trigLogon   = New-ScheduledTaskTrigger -AtLogOn
$trigStartup = New-ScheduledTaskTrigger -AtStartup

# Settings: restart on failure, run even on battery, no stop, 24/7
$settings = New-ScheduledTaskSettingsSet `
  -AllowStartIfOnBatteries `
  -DontStopIfGoingOnBatteries `
  -ExecutionTimeLimit (New-TimeSpan -Hours 0) `
  -MultipleInstances IgnoreNew `
  -RestartCount 999 `
  -RestartInterval (New-TimeSpan -Minutes 1) `
  -StartWhenAvailable

# Register or update
try {
  $task = Get-ScheduledTask -TaskName $TaskName -ErrorAction Stop
  Set-ScheduledTask -TaskName $TaskName -Action $action -Trigger @($trigLogon,$trigStartup) -Settings $settings | Out-Null
  Write-Host "Updated scheduled task '$TaskName'."
} catch {
  Register-ScheduledTask -TaskName $TaskName -Action $action -Trigger @($trigLogon,$trigStartup) -Settings $settings -Description "Runs Base44 auto_profit_sweeper 24/7" -User $env:USERNAME | Out-Null
  Write-Host "Created scheduled task '$TaskName'."
}

# Kick it now
Start-ScheduledTask -TaskName $TaskName
Write-Host "Started '$TaskName'. Use 'Get-ScheduledTask -TaskName $TaskName' to check."
