#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import sys
import io
import hashlib
import zipfile
import argparse
from pathlib import Path
from datetime import datetime

# ---------- Defaults ----------
DEFAULT_IGNORES = {
    ".git", ".hg", ".svn", ".DS_Store", "__pycache__", ".pytest_cache",
    ".mypy_cache", "node_modules", ".venv", "env", "venv", ".idea", ".vscode",
    "Logs", "logs", ".state"  # skip heavy runtime stuff by default
}
INCLUDE_EXTS = {".py", ".yaml", ".yml", ".json", ".toml", ".ini", ".cfg", ".ps1", ".sh", ".txt", ".md"}
MAX_FILE_MB_DEFAULT = 5  # safety: skip files larger than this (override with --max-mb)

SECRET_KEYWORDS = re.compile(r"(secret|password|passwd|token|key|apikey|api_key|bearer|auth|jwt|private)", re.I)

# ---------- Helpers ----------
def sha256_bytes(data: bytes) -> str:
    h = hashlib.sha256()
    h.update(data)
    return h.hexdigest()

def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()

def is_ignored(path: Path, ignore_dirs: set[str]) -> bool:
    parts = {p.name for p in path.resolve().parts}
    return any(name in ignore_dirs for name in parts)

def list_dir(path: Path):
    try:
        return sorted(path.iterdir(), key=lambda p: (not p.is_dir(), p.name.lower()))
    except PermissionError:
        return []

def make_unicode_tree(root: Path, ignore_dirs: set[str]) -> str:
    lines: list[str] = [f"Project structure for: {root.resolve()}"]
    def walk(d: Path, prefix: str = ""):
        entries = [p for p in list_dir(d) if p.name not in ignore_dirs]
        for i, p in enumerate(entries):
            is_last = (i == len(entries) - 1)
            connector = "└── " if is_last else "├── "
            lines.append(prefix + connector + p.name)
            if p.is_dir():
                child_prefix = prefix + ("    " if is_last else "│   ")
                walk(p, child_prefix)
    walk(root)
    return "\n".join(lines)

def redact_env(content: str) -> str:
    """Mask values that look like secrets, keep keys and formatting."""
    out_lines = []
    for line in content.splitlines():
        if not line.strip() or line.strip().startswith("#"):
            out_lines.append(line); continue
        if "=" not in line:
            out_lines.append(line); continue
        key, val = line.split("=", 1)
        if SECRET_KEYWORDS.search(key) or SECRET_KEYWORDS.search(val):
            out_lines.append(f"{key}=***REDACTED***")
        else:
            out_lines.append(line)
    return "\n".join(out_lines) + "\n"

def should_include_file(p: Path, include_exts: set[str]) -> bool:
    if p.name == ".env":  # handled separately (redacted)
        return False
    if p.suffix.lower() in include_exts:
        return True
    return False

# ---------- Main export ----------
def export(root: Path, out_name: str, ignore_dirs: set[str], include_exts: set[str],
           max_file_mb: int, include_env_original: bool) -> Path:
    ts = datetime.now().strftime("%Y%m%d_%H%M")
    zip_path = root / f"{out_name}_{ts}.zip"
    manifest_lines = []
    added_files = 0
    skipped_large = 0

    # Precompute tree
    tree_txt = make_unicode_tree(root, ignore_dirs)

    # Prepare zip
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        # Add structure file
        zf.writestr("structure_utf8.txt", tree_txt)

        # Add MANIFEST after we gather files
        # Walk project
        for dirpath, dirnames, filenames in os.walk(root):
            # prune ignored dirs
            dirnames[:] = [d for d in dirnames if d not in ignore_dirs]
            cur = Path(dirpath)

            for name in filenames:
                p = cur / name
                rel = p.relative_to(root)

                # special-case .env
                if name == ".env":
                    try:
                        data = p.read_text(encoding="utf-8", errors="ignore")
                    except Exception:
                        data = p.read_text(errors="ignore")
                    red = redact_env(data)
                    zf.writestr(str(rel) + ".redacted", red)
                    manifest_lines.append(f"REDACTED {rel}.redacted sha256:{sha256_bytes(red.encode('utf-8'))}")
                    if include_env_original:
                        # include original .env too (not recommended)
                        zf.write(p, str(rel))
                        manifest_lines.append(f"FILE     {rel} sha256:{sha256_file(p)}")
                        added_files += 2
                    else:
                        added_files += 1
                    continue

                # filter by extension
                if not should_include_file(p, include_exts):
                    continue

                # size guard
                try:
                    size_mb = p.stat().st_size / (1024 * 1024)
                except FileNotFoundError:
                    continue
                if size_mb > max_file_mb:
                    skipped_large += 1
                    continue

                # add file
                try:
                    zf.write(p, str(rel))
                    manifest_lines.append(f"FILE     {rel} sha256:{sha256_file(p)}")
                    added_files += 1
                except Exception as e:
                    manifest_lines.append(f"SKIPERR  {rel} err:{e}")

        # Finally, write MANIFEST
        manifest = io.StringIO()
        manifest.write("Base44 Export Manifest\n")
        manifest.write(f"Root: {root.resolve()}\n")
        manifest.write(f"Created: {datetime.now().isoformat()}\n")
        manifest.write(f"Total files: {added_files}\n")
        if skipped_large:
            manifest.write(f"Skipped (too large > {max_file_mb} MB): {skipped_large}\n")
        manifest.write("\nFiles:\n")
        for line in manifest_lines:
            manifest.write(line + "\n")
        zf.writestr("MANIFEST.txt", manifest.getvalue())

    return zip_path

def main():
    ap = argparse.ArgumentParser(description="Export Base44 project scripts/config to a single zip.")
    ap.add_argument("--root", default=".", help="Project root (default: current dir)")
    ap.add_argument("--name", default="Base44_export", help="Output zip base name")
    ap.add_argument("--max-mb", type=int, default=MAX_FILE_MB_DEFAULT, help="Max file size to include (MB)")
    ap.add_argument("--ignore", nargs="*", default=[], help="Extra directory names to ignore")
    ap.add_argument("--include-env", action="store_true", help="Also include original .env (in addition to .env.redacted)")
    args = ap.parse_args()

    root = Path(args.root).resolve()
    ignore_dirs = DEFAULT_IGNORES.union(set(args.ignore))

    zip_path = export(root, args.name, ignore_dirs, INCLUDE_EXTS, args.max_mb, args.include_env)
    print(f"Exported to: {zip_path}")

if __name__ == "__main__":
    main()
