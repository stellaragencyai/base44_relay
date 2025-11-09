# Base44 runtime env
$env:PYTHONUTF8="1"
$env:PYTHONIOENCODING="utf-8"

# Relay auth + Bybit
$env:RELAY_TOKEN="REDACTED_SUPER_TOKEN"
$env:BYBIT_ENV="mainnet"          # or "testnet"
# Optional if custom
#$env:BYBIT_BASE="https://api-testnet.bybit.com"
$env:BYBIT_API_KEY="YOUR_KEY"
$env:BYBIT_API_SECRET="YOUR_SECRET"

# Telegram (optional)
$env:TELEGRAM_BOT_TOKEN=""
$env:TELEGRAM_CHAT_ID=""

# Relay bind
$env:RELAY_HOST="127.0.0.1"
$env:RELAY_PORT="5000"

# Executor knobs
$env:EXEC_DRY_RUN="true"
$env:EXEC_QTY_USDT="5"
$env:EX_MAX_GROSS_PCT="0.6"
# Risk buckets + sessions
$env:EX_BUCKETS_JSON="cfg/risk_buckets.json"
$env:EX_SESSIONS_JSON="cfg/sessions.json"

# TP/SL Manager basics
$env:TP_DRY_RUN="true"
$env:TP_MANAGED_TAG="B44"
$env:TP_SYMBOL_WHITELIST="BTCUSDT,ETHUSDT"

# Paths (adjust if you like pain)
$env:B44_ROOT="$PSScriptRoot\.."
$env:B44_LOGS="$env:B44_ROOT\logs"
