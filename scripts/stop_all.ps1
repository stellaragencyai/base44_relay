$names = @("python", "python.exe")  # cheap but effective

$targets = @("base44_relay.py", "bots.executor_v1", "bots.tp_sl_manager")
$procs = Get-Process | Where-Object {
  $_.ProcessName -in $names -and ($_.Path -and (Get-Process -Id $_.Id -IncludeUserName -ErrorAction SilentlyContinue))
}

foreach ($p in $procs) {
  try {
    $cmdline = (Get-CimInstance Win32_Process -Filter "ProcessId=$($p.Id)").CommandLine
    if ($targets | Where-Object { $cmdline -match $_ }) {
      Write-Host "Stopping PID $($p.Id): $cmdline"
      Stop-Process -Id $p.Id -ErrorAction SilentlyContinue
    }
  } catch { }
}

Start-Sleep -Seconds 2

# Nuke survivors
$survivors = Get-Process | Where-Object {
  $_.ProcessName -in $names -and (
    $targets | Where-Object { ((Get-CimInstance Win32_Process -Filter "ProcessId=$($_.Id)").CommandLine) -match $_ }
  )
}
foreach ($s in $survivors) {
  try {
    Write-Warning "Killing stubborn PID $($s.Id)"
    Stop-Process -Id $s.Id -Force -ErrorAction SilentlyContinue
  } catch { }
}
Write-Host "Stopped."
