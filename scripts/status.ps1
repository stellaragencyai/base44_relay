param(
  [string]$BaseDir = "C:\Users\nolan\Desktop\Base 44",
  [string]$RelayUrl = "http://127.0.0.1:5000",
  [string]$Token = ""
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

function Read-DotEnv {
  param([string]$Path)
  $envs = @{}
  if (Test-Path $Path) {
    Get-Content $Path | ForEach-Object {
      $line = $_.Trim()
      if ($line -eq "" -or $line.StartsWith("#")) { return }
      $idx = $line.IndexOf("=")
      if ($idx -gt 0) {
        $k = $line.Substring(0,$idx).Trim()
        $v = $line.Substring($idx+1).Trim('"').Trim("'")
        $envs[$k] = $v
      }
    }
  }
  return $envs
}

if (-not $Token) {
  $dotenv = Read-DotEnv (Join-Path $BaseDir ".env")
  if ($dotenv.ContainsKey("RELAY_TOKEN")) { $Token = $dotenv["RELAY_TOKEN"] }
}
if (-not $Token) {
  Write-Error "Relay token missing. Provide -Token or set RELAY_TOKEN in $BaseDir\.env"
  exit 1
}

$hdr = @{ "x-relay-token" = $Token }

# /health (no auth)
try {
  $h = Invoke-RestMethod -Method GET -Uri ($RelayUrl.TrimEnd('/') + "/health")
  "{0} | ENV={1} BASE={2}" -f (Get-Date -Format s), $h.env, $h.bybit_base | Write-Host
} catch {
  Write-Host "health check failed: $_" -ForegroundColor Red
}

# /status (auth)
try {
  $s = Invoke-RestMethod -Method GET -Headers $hdr -Uri ($RelayUrl.TrimEnd('/') + "/status")
  $pct = if ($s.equity -gt 0) { "{0:P1}" -f ($s.gross_exposure / $s.equity) } else { "0.0%" }
  Write-Host ("Breaker: {0} | Equity: {1:N2} | Gross: {2:N2} ({3})" -f ($s.breaker ? "ON" : "OFF"), $s.equity, $s.gross_exposure, $pct)
  if ($s.gross_by_symbol) {
    $top = $s.gross_by_symbol.GetEnumerator() | Sort-Object Value -Descending | Select-Object -First 6
    Write-Host "Top gross by symbol:"; $top | ForEach-Object { "{0,-8} {1,12:N2}" -f $_.Key, $_.Value | Write-Host }
  }
  if ($s.signals) {
    Write-Host "Recent signals:"
    $s.signals | Select-Object -Last 6 | ForEach-Object {
      $t = Get-Date ([DateTimeOffset]::FromUnixTimeMilliseconds($_.ts).DateTime) -Format "HH:mm:ss"
      "{0}  {1,-10}  {2,-6}  {3}" -f $t, $_.symbol, $_.signal, ($_.features_class) | Write-Host
    }
  }
} catch {
  Write-Host "status fetch failed: $_" -ForegroundColor Red
}

# Optional: heartbeat ping to Telegram
if ($env:PING_TG -and $env:PING_TG -in @("1","true","on","yes")) {
  try {
    $hb = Invoke-RestMethod -Method GET -Headers $hdr -Uri ($RelayUrl.TrimEnd('/') + "/heartbeat?note=cli_status")
    if ($hb.ok) { Write-Host "Heartbeat sent." }
  } catch {}
}
