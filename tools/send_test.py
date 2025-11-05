import os, requests
from urllib.parse import quote
from pathlib import Path
from dotenv import load_dotenv

# Try these in order: tools/.env, project root/.env
here = Path(__file__).resolve().parent
candidates = [
    here / ".env",
    here.parent / ".env",
]

loaded_from = None
for p in candidates:
    if p.exists():
        if load_dotenv(p):
            loaded_from = str(p)
            break

TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT  = os.getenv("TELEGRAM_CHAT_ID")

print("Loaded .env from:", loaded_from)
print("Token looks valid:", bool(TOKEN and ":" in TOKEN))
print("Chat ID present:", bool(CHAT))

if not loaded_from:
    raise SystemExit("Could not find a .env in any candidate path. Put one next to this script or one level up.")

if not (TOKEN and ":" in TOKEN):
    raise SystemExit(f"Bad TELEGRAM_BOT_TOKEN: {repr(TOKEN)} (expecting a token with a colon)")
if not CHAT:
    raise SystemExit(f"Missing TELEGRAM_CHAT_ID (got {repr(CHAT)})")

# Sanity check
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
