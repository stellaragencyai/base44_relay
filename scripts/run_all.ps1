param(
  [string]$BaseDir = "C:\Users\nolan\Desktop\Base 44",
  [int]$RestartDelaySec = 5,
  [int]$MaxCrashStreak = 6,
  [switch]$SkipNgrok
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

# Paths
$VenvPy = Join-Path $BaseDir ".venv\Scripts\python.exe"
$Logs   = Join-Path $BaseDir "logs"
New-Item -ItemType Directory -Force -Path $Logs | Out-Null

if (-not (Test-Path $VenvPy)) {
  Write-Error "Venv python not found: $VenvPy"
  exit 1
}

$env:PYTHONUNBUFFERED = "1"

function Add-LogHeader {
  param([string]$File)
  "==== $(Get-Date -Format s) START ====" | Add-Content -Path $File
}

function New-TabWatch {
  param(
    [Parameter(Mandatory)] [string]$Title,
    [Parameter(Mandatory)] [string]$Cmd,       # e.g. "-m bots.auto_executor"
    [Parameter(Mandatory)] [string]$LogFile
  )
  Add-LogHeader -File $LogFile
  $escapedLog = $LogFile.Replace("'", "''")
  $psCmd = @"
`$Host.UI.RawUI.WindowTitle = "$Title"
cd "$BaseDir"
`$crashStreak = 0
while (`$true) {
  Write-Host ("== {0} @ {1} ==" -f "$Title", (Get-Date -Format o)) -ForegroundColor Yellow
  try {
    & "$VenvPy" $Cmd *>&1 | Tee-Object -FilePath "$escapedLog" -Append
    `$exitCode = `$LASTEXITCODE
  } catch {
    `$exitCode = -1
  }
  if (`$exitCode -eq 0) {
    Write-Host "$Title exited normally." -ForegroundColor Green
    break
  }
  `$crashStreak++
  if (`$crashStreak -ge $MaxCrashStreak) {
    Write-Warning "$Title crashed `$crashStreak times; giving up."
    break
  }
  `$delay = [Math]::Min($RestartDelaySec * [Math]::Pow(2, [Math]::Max(0, `$crashStreak - 1)), 60)
  Write-Warning ("$Title exited (code {0}). Restarting in {1}s..." -f `$exitCode, [int]`$delay)
  Start-Sleep -Seconds [int]`$delay
}
"@
  Start-Process powershell -ArgumentList @("-NoExit","-NoLogo","-Command", $psCmd) -WindowStyle Normal | Out-Null
}

function New-TabOnce {
  param(
    [Parameter(Mandatory)] [string]$Title,
    [Parameter(Mandatory)] [string]$Cmd,
    [Parameter(Mandatory)] [string]$LogFile
  )
  Add-LogHeader -File $LogFile
  $escapedLog = $LogFile.Replace("'", "''")
  $psCmd = @"
`$Host.UI.RawUI.WindowTitle = "$Title"
cd "$BaseDir"
Write-Host ("== {0} @ {1} ==" -f "$Title", (Get-Date -Format o)) -ForegroundColor Yellow
& "$VenvPy" $Cmd *>&1 | Tee-Object -FilePath "$escapedLog" -Append
"@
  Start-Process powershell -ArgumentList @("-NoExit","-NoLogo","-Command", $psCmd) -WindowStyle Normal | Out-Null
}

# --- presence checks for modules (so we don't pretend) ---
function Test-ModuleFile($RelPath) { Test-Path (Join-Path $BaseDir $RelPath) }

$HasRelay          = (Test-ModuleFile "relay\base44_relay.py")
$HasPortfolioGuard = (Test-ModuleFile "core\portfolio_guard.py") -or (Test-ModuleFile "bots\portfolio_guard.py")
$HasTpSl           = (Test-ModuleFile "bots\tp_sl_manager.py")
$HasExecutor       = (Test-ModuleFile "bots\auto_executor.py")
$HasWatchdog       = (Test-ModuleFile "ops\watchdog.py")

# 1) Relay (optional)
if ($HasRelay) {
  New-TabWatch -Title "relay" -Cmd "-m relay.base44_relay" -LogFile (Join-Path $Logs "relay.log")
} else {
  Write-Host "relay/base44_relay.py not found; skipping relay." -ForegroundColor DarkYellow
}

# 2) ngrok (optional)
if (-not $SkipNgrok -and (Get-Command ngrok -ErrorAction SilentlyContinue)) {
  $ngrokLog = Join-Path $Logs "ngrok.log"
  Add-LogHeader -File $ngrokLog
  $ngrokCmd = @"
`$Host.UI.RawUI.WindowTitle = "ngrok"
cd "$BaseDir"
ngrok http http://127.0.0.1:5000 *>&1 | Tee-Object -FilePath "$ngrokLog" -Append
"@
  Start-Process powershell -ArgumentList @("-NoExit","-NoLogo","-Command", $ngrokCmd) -WindowStyle Normal | Out-Null
}

# 3) Notifier heartbeat (one-shot)
New-TabOnce -Title "notifier" -Cmd "-m core.notifier_bot --ping session_boot" -LogFile (Join-Path $Logs "notifier.log")

# 4) Portfolio Guard (only if you actually run it as a loop/daemon)
if ($HasPortfolioGuard -and (Test-ModuleFile "bots\portfolio_guard.py")) {
  New-TabWatch -Title "portfolio_guard" -Cmd "-m bots.portfolio_guard" -LogFile (Join-Path $Logs "portfolio_guard.log")
} elseif ($HasPortfolioGuard) {
  Write-Host "core/portfolio_guard.py present but isn't a loop daemon. Skipping tab (it’s a library in your stack)." -ForegroundColor DarkYellow
} else {
  Write-Host "portfolio_guard not found; skipping." -ForegroundColor DarkYellow
}

# 5) TP/SL Manager
if ($HasTpSl) {
  New-TabWatch -Title "tp_sl_manager" -Cmd "-m bots.tp_sl_manager" -LogFile (Join-Path $Logs "tpsl.log")
} else {
  Write-Host "bots/tp_sl_manager.py not found; skipping." -ForegroundColor DarkYellow
}

# 6) Auto Executor
if ($HasExecutor) {
  New-TabWatch -Title "executor" -Cmd "-m bots.auto_executor" -LogFile (Join-Path $Logs "executor.log")
} else {
  Write-Host "bots/auto_executor.py not found; skipping." -ForegroundColor DarkYellow
}

# 7) Watchdog (recommended)
if ($HasWatchdog) {
  New-TabWatch -Title "watchdog" -Cmd "-m ops.watchdog" -LogFile (Join-Path $Logs "watchdog.log")
} else {
  Write-Host "ops/watchdog.py not found; skipping watchdog." -ForegroundColor DarkYellow
}

Write-Host "Launched. One window per service; logs in $Logs" -ForegroundColor Green
