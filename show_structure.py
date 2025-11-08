#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import argparse
from pathlib import Path
from typing import Iterable

DEFAULT_IGNORES = {
    ".git", ".hg", ".svn", ".DS_Store", "__pycache__", ".pytest_cache",
    ".mypy_cache", "node_modules", ".venv", "env", "venv", ".idea", ".vscode"
}

def list_dir(path: Path) -> Iterable[Path]:
    try:
        return sorted(path.iterdir(), key=lambda p: (not p.is_dir(), p.name.lower()))
    except PermissionError:
        return []

def make_ascii_tree(root: Path, max_depth: int | None, ignore: set[str]) -> str:
    lines: list[str] = [f"Project structure for: {root.resolve()}"]
    def walk(dir_path: Path, prefix: str = "", depth: int = 0):
        if max_depth is not None and depth > max_depth:
            return
        entries = [p for p in list_dir(dir_path) if p.name not in ignore]
        for i, p in enumerate(entries):
            is_last = (i == len(entries) - 1)
            connector = "+-- " if not is_last else "`-- "
            lines.append(prefix + connector + p.name)
            if p.is_dir():
                child_prefix = prefix + ("|   " if not is_last else "    ")
                walk(p, child_prefix, depth + 1)
    walk(root)
    return "\n".join(lines)

def make_unicode_tree(root: Path, max_depth: int | None, ignore: set[str]) -> str:
    lines: list[str] = [f"Project structure for: {root.resolve()}"]
    def walk(dir_path: Path, prefix: str = "", depth: int = 0):
        if max_depth is not None and depth > max_depth:
            return
        entries = [p for p in list_dir(dir_path) if p.name not in ignore]
        for i, p in enumerate(entries):
            is_last = (i == len(entries) - 1)
            connector = "├── " if not is_last else "└── "
            lines.append(prefix + connector + p.name)
            if p.is_dir():
                child_prefix = prefix + ("│   " if not is_last else "    ")
                walk(p, child_prefix, depth + 1)
    walk(root)
    return "\n".join(lines)

def main():
    ap = argparse.ArgumentParser(description="Print project folder/file tree.")
    ap.add_argument("--root", default=".", help="Root folder (default: current dir)")
    ap.add_argument("--max-depth", type=int, default=None, help="Limit recursion depth")
    ap.add_argument("--ignore", nargs="*", default=[], help="Extra names to ignore")
    args = ap.parse_args()

    root = Path(args.root).resolve()
    ignore = DEFAULT_IGNORES.union(set(args.ignore))

    ascii_tree = make_ascii_tree(root, args.max_depth, ignore)
    print(ascii_tree)  # ASCII-only, safe for Windows consoles and redirection

    # Also write a pretty UTF-8 version with a BOM so Notepad behaves
    unicode_tree = make_unicode_tree(root, args.max_depth, ignore)
    out_file = root / "structure_utf8.txt"
    with open(out_file, "w", encoding="utf-8-sig") as f:
        f.write(unicode_tree + "\n")

if __name__ == "__main__":
    main()
