# tools/env_doctor.py
#!/usr/bin/env python3
import os, sys, json, importlib.util
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]  # repo root = Base 44
print(f"repo_root: {REPO}")
print(f"cwd      : {Path.cwd()}")

# Ensure repo root is importable
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

def check_exists(path):
    p = (REPO / path)
    ok = p.exists()
    print(f"{'✅' if ok else '❌'} exists: {path}")
    return ok

print("\n# Files")
check_exists("core/__init__.py")
check_exists("core/env_bootstrap.py")
check_exists("core/notifier_bot.py")
check_exists("core/base44_client.py")
check_exists("config/.env")

print("\n# Import core.env_bootstrap")
try:
    from core.env_bootstrap import *  # noqa
    print("✅ import core.env_bootstrap")
except Exception as e:
    print("❌ import core.env_bootstrap:", e)

print("\n# Read critical envs (after bootstrap)")
for k in ["BASE44_CORE_DIR","RELAY_TOKEN","RELAY_SECRET","BYBIT_API_KEY","BYBIT_API_SECRET"]:
    v = os.getenv(k)
    print(f"{'✅' if v else '❌'} env {k} = {'<set>' if v else '<missing>'}")

print("\n# Import notifier_bot.tg_send")
try:
    from core import notifier_bot as nb
    assert hasattr(nb, "tg_send")
    print("✅ core.notifier_bot import & tg_send present")
except Exception as e:
    try:
        from core import notifier_bot as nb2
        assert hasattr(nb2, "tg_send")
        print("🟡 fallback notifier_bot import OK (no package prefix)")
    except Exception as e2:
        print("❌ notifier_bot import failed:", e, "| fallback:", e2)

print("\n# Import base44_client")
try:
    from core import base44_client as b44
    print("✅ core.base44_client import OK")
except Exception as e:
    try:
        import base44_client as b44
        print("🟡 fallback base44_client import OK")
    except Exception as e2:
        print("❌ base44_client import failed:", e, "| fallback:", e2)

print("\n# Optional: quick client surface check")
try:
    for name in ["tg_send","bybit_proxy","get_balance_unified"]:
        print(f" - {name}: {'✅' if hasattr(b44, name) else '❌'}")
except Exception:
    pass

print("\nDone.")
