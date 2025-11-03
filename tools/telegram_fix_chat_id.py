#!/usr/bin/env python3
# tools/telegram_fix_chat_id.py
import os, re, json, shutil, requests
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ENV_PATH = ROOT / ".env"

def load_token():
    tok = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    if not tok and ENV_PATH.exists():
        for line in ENV_PATH.read_text(encoding="utf-8").splitlines():
            if line.strip().startswith("TELEGRAM_BOT_TOKEN="):
                tok = line.split("=",1)[1].strip()
                break
    if not tok:
        raise SystemExit("No TELEGRAM_BOT_TOKEN found. Put it in .env first.")
    if not re.match(r"^\d+:[A-Za-z0-9_-]{30,}$", tok):
        raise SystemExit("Token format looks wrong. Copy a fresh one from @BotFather.")
    return tok

def pick_chat_id(updates):
    # Prefer latest message/chat the bot saw
    chats = []
    for u in updates.get("result", []):
        msg = u.get("message") or u.get("channel_post") or u.get("edited_message") or u.get("edited_channel_post")
        if not msg: 
            continue
        chat = msg.get("chat") or {}
        cid = chat.get("id")
        ctype = chat.get("type")
        title = chat.get("title") or chat.get("username") or chat.get("first_name")
        if cid:
            chats.append((u.get("update_id", 0), cid, ctype, title))
    if not chats:
        return None
    # pick the one with the highest update_id (most recent)
    chats.sort(key=lambda x: x[0])
    _, cid, ctype, title = chats[-1]
    return cid

def write_env_chat_id(chat_id: int):
    if not ENV_PATH.exists():
        raise SystemExit(f".env not found at {ENV_PATH}")
    # Backup
    shutil.copy2(ENV_PATH, ENV_PATH.with_suffix(".env.bak"))
    lines = ENV_PATH.read_text(encoding="utf-8").splitlines()
    out = []
    found = False
    for line in lines:
        if line.strip().startswith("TELEGRAM_CHAT_ID="):
            out.append(f"TELEGRAM_CHAT_ID={chat_id}")
            found = True
        else:
            out.append(line)
    if not found:
        out.append(f"TELEGRAM_CHAT_ID={chat_id}")
    ENV_PATH.write_text("\n".join(out) + "\n", encoding="utf-8")
    return True

def main():
    token = load_token()
    base = f"https://api.telegram.org/bot{token}"
    # Sanity check token
    me = requests.get(f"{base}/getMe", timeout=15).json()
    if not me.get("ok"):
        raise SystemExit(f"getMe failed: {me}")
    print(f"Bot OK: @{me['result'].get('username')} id={me['result'].get('id')}")

    # Fetch updates (send your bot a message first!)
    updates = requests.get(f"{base}/getUpdates", timeout=20).json()
    # print(json.dumps(updates, indent=2))  # uncomment to see raw payload
    cid = pick_chat_id(updates)
    if cid is None:
        raise SystemExit("No chat_id found. Open the bot and send 'hi', then run this again.")
    print(f"Discovered chat_id: {cid}")
    write_env_chat_id(cid)
    print(f"Wrote TELEGRAM_CHAT_ID={cid} into {ENV_PATH} (backup saved to .env.bak)")

if __name__ == "__main__":
    main()
