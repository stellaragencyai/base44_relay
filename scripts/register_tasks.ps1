# scripts/register_tasks.ps1
param(
  [string]$BaseDir = "C:\Users\nolan\Desktop\Base 44"
)

$BotsDir = Join-Path $BaseDir "bots"
$VenvAct = Join-Path $BaseDir ".venv\Scripts\Activate.ps1"
$RegDir  = Join-Path $BaseDir "registry"
$SubCsv  = Join-Path $RegDir "sub_uids.csv"      # optional
$SubMap  = Join-Path $RegDir "sub_map.json"      # optional (names/roles/tiers)

function Register-Or-UpdateTask {
  param(
    [string]$TaskName,
    [string]$Command,
    [string]$Arguments,
    [string]$WorkingDirectory
  )
  $action    = New-ScheduledTaskAction -Execute $Command -Argument $Arguments -WorkingDirectory $WorkingDirectory
  $trigger   = New-ScheduledTaskTrigger -AtLogOn
  $settings  = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -MultipleInstances IgnoreNew -StartWhenAvailable
  $principal = New-ScheduledTaskPrincipal -UserId $env:USERNAME -LogonType Interactive -RunLevel Highest

  try {
    if (Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue) {
      Set-ScheduledTask -TaskName $TaskName -Action $action -Trigger $trigger -Settings $settings -Principal $principal
      Write-Host "Updated task $TaskName"
    } else {
      Register-ScheduledTask -TaskName $TaskName -Action $action -Trigger $trigger -Settings $settings -Principal $principal
      Write-Host "Registered task $TaskName"
    }
  } catch {
    Write-Host "Error for $TaskName: $($_.Exception.Message)"
  }
}

# Helper: wrap a PowerShell inner command into a clean -Command argument
function Get-PSArgs {
  param(
    [string]$InnerCommand
  )
  # We pass the command inside double-quotes so PowerShell parses it:
  #   -WindowStyle ... -Command "<inner>"
  return "-WindowStyle Minimized -NoProfile -ExecutionPolicy Bypass -Command ""$InnerCommand"""
}

# Helper: sanitize a string to be a safe scheduled task name
function ConvertTo-SafeTaskName {
  param([string]$Name)
  # Replace anything not alnum, underscore, or hyphen with underscore
  $safe = ($Name -replace '[^A-Za-z0-9_\-]', '_')
  # Trim to something reasonable (Task Scheduler supports long names but keep tidy)
  if ($safe.Length -gt 120) { $safe = $safe.Substring(0,120) }
  return $safe
}

# ── 1) Core monitor bots (no trade placement; safe to autostart)
$monitors = @(
  @{ Name="B44_pnl_logger";      Script="pnl_logger.py" },
  @{ Name="B44_trade_historian"; Script="trade_historian.py" },
  @{ Name="B44_risk_daemon";     Script="risk_daemon.py" },
  @{ Name="B44_tp_sl_manager";   Script="tp_sl_manager.py" },
  @{ Name="B44_relay_watchdog";  Script="relay_watchdog.py" }
)

foreach ($it in $monitors) {
  $taskName = ConvertTo-SafeTaskName $it.Name
  $inner = "& { Set-Location '$BotsDir'; . '$VenvAct'; python '$($it.Script)' }"
  $taskArgs = Get-PSArgs -InnerCommand $inner
  Register-Or-UpdateTask -TaskName $taskName -Command "powershell.exe" -Arguments $taskArgs -WorkingDirectory $BotsDir
}

# ── 2) Coach (master) — no MEMBER_ID env -> master scope
$coachMasterName = ConvertTo-SafeTaskName "B44_coach_master"
$innerMaster = "& { Set-Location '$BotsDir'; . '$VenvAct'; Remove-Item Env:\COACH_MEMBER_ID -ErrorAction SilentlyContinue; python 'coach.py' }"
$coachMasterArgs = Get-PSArgs -InnerCommand $innerMaster
Register-Or-UpdateTask -TaskName $coachMasterName -Command "powershell.exe" -Arguments $coachMasterArgs -WorkingDirectory $BotsDir

# ── 3) Coach (per-sub) — discover sub_uids from registry\sub_uids.csv; label from sub_map.json if present
$subs = @()
if (Test-Path $SubCsv) {
  try {
    $raw = Import-Csv -Path $SubCsv
    foreach ($row in $raw) {
      $uid = $row.sub_uid
      if ($uid -and $uid.Trim().Length -gt 0) {
        $subs += $uid.Trim()
      }
    }
    $subs = $subs | Sort-Object -Unique
  } catch {
    Write-Host "Warning: Failed to read $SubCsv — $($_.Exception.Message)"
  }
}

# Optional: name map from sub_map.json ({ "uid": "Name" } or { "uid": { "name": "..." } })
$nameMap = @{}
if (Test-Path $SubMap) {
  try {
    $json = Get-Content -Raw -Path $SubMap | ConvertFrom-Json
    $keys = $json.PSObject.Properties.Name
    foreach ($k in $keys) {
      $v = $json.$k
      if ($v -is [string]) {
        $nameMap[$k] = $v
      } elseif ($v -is [psobject]) {
        if ($v.PSObject.Properties.Name -contains "name") {
          $nameMap[$k] = $v.name
        } else {
          $nameMap[$k] = "$k"
        }
      }
    }
  } catch {
    Write-Host "Warning: Failed to parse $SubMap — $($_.Exception.Message)"
  }
}

foreach ($uid in $subs) {
  $label = $uid
  if ($nameMap.ContainsKey($uid) -and $nameMap[$uid]) {
    $safeName = ($nameMap[$uid] -replace '[^a-zA-Z0-9_\-]', '_').Trim()
    if ($safeName) { $label = "$safeName-$uid" }
  }
  $taskName = ConvertTo-SafeTaskName ("B44_coach_sub_" + $label)

  # IMPORTANT: escape the dollar sign so $env:COACH_MEMBER_ID is set at runtime, not now
  $innerSub = "& { Set-Location '$BotsDir'; . '$VenvAct'; `\$env:COACH_MEMBER_ID='$uid'; python 'coach.py' }"
  $argsSub  = Get-PSArgs -InnerCommand $innerSub

  Register-Or-UpdateTask -TaskName $taskName -Command "powershell.exe" -Arguments $argsSub -WorkingDirectory $BotsDir
}

Write-Host "All tasks registered/updated."

# Funding & Fees Tracker — master
$taskNameFFM = "B44_funding_fees_master"
$innerFFM = "& { Set-Location '$BotsDir'; . '$VenvAct'; Remove-Item Env:\FF_MEMBER_ID -ErrorAction SilentlyContinue; python 'funding_fees_tracker.py' }"
$argsFFM = "-WindowStyle Minimized -NoProfile -ExecutionPolicy Bypass -Command ""$innerFFM"""
Register-Or-UpdateTask -TaskName $taskNameFFM -Command "powershell.exe" -Arguments $argsFFM -WorkingDirectory $BotsDir

# Funding & Fees Tracker — per-sub (duplicate this block for each UID you want)
$uid = "302355261"   # <-- replace with a real UID
$taskNameFFS = "B44_funding_fees_sub_$uid"
$innerFFS = "& { Set-Location '$BotsDir'; . '$VenvAct'; `\$env:FF_MEMBER_ID='$uid'; python 'funding_fees_tracker.py' }"
$argsFFS  = "-WindowStyle Minimized -NoProfile -ExecutionPolicy Bypass -Command ""$innerFFS"""
Register-Or-UpdateTask -TaskName $taskNameFFS -Command "powershell.exe" -Arguments $argsFFS -WorkingDirectory $BotsDir
