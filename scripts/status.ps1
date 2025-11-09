. "$PSScriptRoot\env.ps1"

$relayBase = "http://$env:RELAY_HOST`:$env:RELAY_PORT"
$tok = $env:RELAY_TOKEN

function Get-Json($url) {
  try {
    Invoke-RestMethod -Method GET -Uri $url -Headers @{ "x-relay-token" = $tok } -TimeoutSec 8
  } catch {
    @{ ok = $false; error = $_.Exception.Message }
  }
}

# Relay up?
$health = Invoke-RestMethod -Method GET -Uri "$relayBase/health" -TimeoutSec 5 -ErrorAction SilentlyContinue
if ($null -eq $health) { Write-Host "Relay: DOWN" -ForegroundColor Red } else { Write-Host "Relay: UP ($($health.env) → $($health.bybit_base))" -ForegroundColor Green }

# Bybit signed check
$diag = Get-Json "$relayBase/diag/bybit"
if ($diag.ok -eq $false -and $diag.error) { Write-Host "Bybit: $($diag.error)" -ForegroundColor Red } else { Write-Host "Bybit: OK (signed v5)" -ForegroundColor Green }

# Equity (proxy via /getAccountData)
$accs = Get-Json "$relayBase/getAccountData"
if ($accs -is [System.Array]) {
  $tot = [Math]::Round(($accs | Measure-Object -Property equity -Sum).Sum, 2)
  Write-Host "Equity (sum of accounts): $tot" -ForegroundColor Cyan
} else {
  Write-Host "Equity fetch failed: $($accs.error)" -ForegroundColor DarkYellow
}

# Positions gross
$pos = Get-Json "$relayBase/bybit/positions?category=linear"
if ($pos.retCode -eq 0) {
  $gross = 0.0
  foreach ($p in $pos.result.list) { $gross += [math]::Abs([double]$p.size * [double]$p.avgPrice) }
  Write-Host ("Gross exposure (linear): {0:n2}" -f $gross) -ForegroundColor Cyan
} else {
  Write-Host "Positions fetch failed." -ForegroundColor DarkYellow
}

Write-Host "Status ping complete."
