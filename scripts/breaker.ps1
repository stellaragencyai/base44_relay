param(
  [ValidateSet("on","off","status")]
  [string]$mode = "status"
)

$root = Split-Path -Parent $PSScriptRoot
$state = Join-Path $root ".state"
New-Item -ItemType Directory -Force -Path $state | Out-Null
$file = Join-Path $state "risk_state.json"

if ($mode -eq "on") {
  '{"breach": true, "ts": ' + [int][double]::Parse((Get-Date -UFormat %s)) + '}' | Set-Content -Path $file -Encoding UTF8
  Write-Host "Breaker ON -> $file"
  exit 0
}
elseif ($mode -eq "off") {
  '{"breach": false, "ts": ' + [int][double]::Parse((Get-Date -UFormat %s)) + '}' | Set-Content -Path $file -Encoding UTF8
  Write-Host "Breaker OFF -> $file"
  exit 0
}
else {
  if (Test-Path $file) {
    Get-Content $file
  } else {
    Write-Host '{"breach": false}'
  }
}
