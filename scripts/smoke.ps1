. "$PSScriptRoot\env.ps1"
$relayBase = "http://$env:RELAY_HOST`:$env:RELAY_PORT"
$tok = $env:RELAY_TOKEN

Invoke-RestMethod -Method GET -Uri "$relayBase/diag/time" | Out-Null
Invoke-RestMethod -Method GET -Headers @{ "x-relay-token" = $tok } -Uri "$relayBase/diag/wallet-normalized" | ConvertTo-Json -Depth 6
