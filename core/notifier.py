import os, requests
from pathlib import Path
from dotenv import load_dotenv

# Load .env from project root or current folder
root = Path(__file__).resolve().parents[1]
for p in (root / ".env", Path(".") / ".env"):
    if p.exists():
        load_dotenv(p)
        break

TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT  = os.getenv("TELEGRAM_CHAT_ID")

def send_telegram(text: str):
    if not (TOKEN and CHAT): 
        return False, "Telegram not configured"
    try:
        r = requests.get(
            f"https://api.telegram.org/bot{TOKEN}/sendMessage",
            params={"chat_id": CHAT, "text": text},
            timeout=10
        )
        return r.ok, r.text
    except Exception as e:
        return False, str(e)
