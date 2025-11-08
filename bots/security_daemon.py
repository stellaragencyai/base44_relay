# bots/security_daemon.py
from flask import Flask, request, jsonify
import os
from core.logger import get_logger
from core.security.guardrail import verify_all
from core.security.timelock import update_metrics
from core.security.config import load

log = get_logger("security_daemon")
cfg = load()
app = Flask(__name__)

def _auth_ok(req):
    bearer = (req.headers.get("Authorization") or "").replace("Bearer ","").strip()
    return bearer and bearer == cfg.secret

@app.route("/status", methods=["GET"])
def status():
    return jsonify({"ok": True, "provider": "Base44 Security", "yubi": cfg.yubikey_method, "approver": cfg.approver_provider})

@app.route("/update_metrics", methods=["POST"])
def update():
    if not _auth_ok(request):
        return jsonify({"ok": False, "error": "unauthorized"}), 401
    data = request.get_json(force=True, silent=True) or {}
    gd = data.get("green_days")
    eq = data.get("equity_usd")
    update_metrics(green_days=gd, equity_usd=eq)
    return jsonify({"ok": True})

@app.route("/unlock", methods=["POST"])
def unlock():
    if not _auth_ok(request):
        return jsonify({"ok": False, "error": "unauthorized"}), 401
    data = request.get_json(force=True, silent=True) or {}
    action = str(data.get("action",""))
    amount = float(data.get("amount",0))
    sub_uid = str(data.get("sub_uid",""))
    meta = data.get("meta") or {}
    ok, sig = verify_all(action, amount, sub_uid, meta)
    return jsonify({"ok": ok, "token": sig if ok else "", "why": "" if ok else sig})

if __name__ == "__main__":
    app.run(host=cfg.bind, port=cfg.port)
