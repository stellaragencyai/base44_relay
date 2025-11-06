# core/env_bootstrap.py
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Env bootstrap for Base44 (idempotent, automation-ready).

What this does (quietly and without drama):
- Adds BASE44_CORE_DIR and the repo root to sys.path so `core.*` imports work everywhere.
- Loads environment files in a layered order with later files overriding earlier ones:
    1) <repo_root>/.env
    2) <repo_root>/config/.env
    3) <repo_root>/.env.local
    4) <repo_root>/config/.env.local
- Exposes common paths: ROOT, CONFIG_DIR, REGISTRY_DIR, LOGS_DIR, STATE_DIR.
- Ensures logs/, registry/, and .state/ exist.
- Provides small env helpers: env_bool, env_int, env_float, env_decimal, env_csv.
- Sets default TZ if missing (America/Phoenix), but won’t overwrite if you already set it.

Safe to import multiple times.
"""

from __future__ import annotations
import os
import sys
from pathlib import Path
from typing import List
from decimal import Decimal

# --------------------------------------------------------------------------------------
# Path wiring
# --------------------------------------------------------------------------------------
# Honor explicit override first
_CORE_HINT = os.getenv("BASE44_CORE_DIR")
if _CORE_HINT and _CORE_HINT not in sys.path:
    sys.path.insert(0, _CORE_HINT)

# Resolve repo root from this file’s location (…/core/env_bootstrap.py)
_THIS = Path(__file__).resolve()
ROOT = _THIS.parents[1]            # project root
CORE_DIR = ROOT / "core"
CONFIG_DIR = ROOT / "config"
REGISTRY_DIR = ROOT / "registry"
LOGS_DIR = ROOT / "logs"
STATE_DIR = ROOT / ".state"

# Make sure imports like `import core.something` work, regardless of CWD
for p in (str(ROOT), str(CORE_DIR)):
    if p not in sys.path:
        sys.path.insert(0, p)

# --------------------------------------------------------------------------------------
# Load dotenv in layered order (later files override earlier ones)
# --------------------------------------------------------------------------------------
try:
    from dotenv import load_dotenv  # type: ignore
except Exception:
    load_dotenv = None  # optional dependency

def _load_env_file(path: Path) -> None:
    if load_dotenv and path.exists():
        # Use override=True so later files in our sequence take precedence.
        load_dotenv(path.as_posix(), override=True)

def _load_env_layered() -> None:
    # 1) root .env
    _load_env_file(ROOT / ".env")
    # 2) config/.env
    _load_env_file(CONFIG_DIR / ".env")
    # 3) root .env.local
    _load_env_file(ROOT / ".env.local")
    # 4) config/.env.local
    _load_env_file(CONFIG_DIR / ".env.local")

# Only load once per interpreter session
if not os.environ.get("__BASE44_ENV_BOOTSTRAPPED__"):
    _load_env_layered()
    os.environ["__BASE44_ENV_BOOTSTRAPPED__"] = "1"

# --------------------------------------------------------------------------------------
# Ensure key directories exist
# --------------------------------------------------------------------------------------
for d in (LOGS_DIR, REGISTRY_DIR, STATE_DIR):
    try:
        d.mkdir(parents=True, exist_ok=True)
    except Exception:
        # Non-fatal if e.g. read-only FS; caller can handle missing dirs as needed
        pass

# --------------------------------------------------------------------------------------
# Defaults that shouldn’t stomp over explicit settings
# --------------------------------------------------------------------------------------
os.environ.setdefault("TZ", os.getenv("TZ", "America/Phoenix"))

# --------------------------------------------------------------------------------------
# Helper utilities for other modules (import-safe)
# --------------------------------------------------------------------------------------
def env_bool(name: str, default: bool = False) -> bool:
    v = (os.getenv(name, str(int(default))) or "").strip().lower()
    return v in {"1", "true", "yes", "on"}

def env_int(name: str, default: int = 0) -> int:
    try:
        return int((os.getenv(name, str(default)) or "").strip())
    except Exception:
        return default

def env_float(name: str, default: float = 0.0) -> float:
    try:
        return float((os.getenv(name, str(default)) or "").strip())
    except Exception:
        return default

def env_decimal(name: str, default: str = "0") -> Decimal:
    try:
        return Decimal((os.getenv(name, default) or default).strip())
    except Exception:
        return Decimal(default)

def env_csv(name: str, default: str = "") -> List[str]:
    raw = os.getenv(name, default) or default
    return [s.strip() for s in raw.split(",") if s.strip()]

__all__ = [
    # paths
    "ROOT", "CORE_DIR", "CONFIG_DIR", "REGISTRY_DIR", "LOGS_DIR", "STATE_DIR",
    # helpers
    "env_bool", "env_int", "env_float", "env_decimal", "env_csv",
]
