# core/env_bootstrap.py
import os, sys
from pathlib import Path

# Ensure core is importable if BASE44_CORE_DIR is set
core = os.getenv("BASE44_CORE_DIR")
if core and core not in sys.path:
    sys.path.insert(0, core)

# Load config/.env if present
try:
    from dotenv import load_dotenv  # from python-dotenv package
except Exception:
    load_dotenv = None

if load_dotenv:
    # repo root is one level above core/
    here = Path(__file__).resolve()
    repo_root = here.parents[1]
    env_path = repo_root / "config" / ".env"
    if env_path.exists():
        load_dotenv(env_path.as_posix())
