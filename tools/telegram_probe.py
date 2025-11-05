#!/usr/bin/env python3
import os, requests
from dotenv import load_dotenv
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]  # repo root
load_dotenv(ROOT / ".env")

tok = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
if not tok:
    raise SystemExit("Missing TELEGRAM_BOT_TOKEN in .env")

url = f"https://api.telegram.org/bot{tok}/getUpdates"
r = requests.get(url, timeout=10)
r.raise_for_status()
data = r.json()
if not data.get("ok"):
    print("Telegram error:", data)
    raise SystemExit(1)

found = set()
for upd in data.get("result", []):
    msg = upd.get("message") or upd.get("channel_post") or {}
    chat = msg.get("chat") or {}
    cid = chat.get("id")
    title = chat.get("title")
    uname = chat.get("username")
    if cid:
        found.add(cid)
        print(f"chat_id={cid}  title={title}  username={uname}")

if not found:
    print("No chats found. Send a message to the bot (or in the group) and rerun.")
