import os, requests
from urllib.parse import quote

TOKEN = os.getenv("TELEGRAM_BOT_TOKEN") or "<BOT_TOKEN_WITH_COLON>"
CHAT  = os.getenv("TELEGRAM_CHAT_ID")   or "7776809236"

assert ":" in TOKEN, "Bad TELEGRAM_BOT_TOKEN (missing colon)."

for path in ("getMe", f"sendMessage?chat_id={CHAT}&text={quote('Base44 Matrix wired ✅')}"):
    url = f"https://api.telegram.org/bot{TOKEN}/{path}"
    r = requests.get(url, timeout=10)
    print(path, r.status_code, r.text)
