import os, requests
from urllib.parse import quote
from dotenv import load_dotenv

# Absolute path because your script lives in \tools and your .env is one level up
ENV_PATH = r"C:\Users\nolan\Desktop\Base 44\.env"
loaded = load_dotenv(dotenv_path=ENV_PATH)

TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT  = os.getenv("TELEGRAM_CHAT_ID")

if not loaded:
    raise SystemExit(f"Could not load .env from {ENV_PATH}")
if not TOKEN or ":" not in TOKEN:
    raise SystemExit(f"Bad TELEGRAM_BOT_TOKEN: {repr(TOKEN)} (expected a token with a colon)")
if not CHAT:
    raise SystemExit(f"Missing TELEGRAM_CHAT_ID (got {repr(CHAT)})")

# Quick sanity check
r1 = requests.get(f"https://api.telegram.org/bot{TOKEN}/getMe", timeout=10)
print("getMe:", r1.status_code, r1.text)

# Send a message
msg = "Base44 Matrix wired ✅"
r2 = requests.get(
    f"https://api.telegram.org/bot{TOKEN}/sendMessage",
    params={"chat_id": CHAT, "text": msg},
    timeout=10
)
print("sendMessage:", r2.status_code, r2.text)
