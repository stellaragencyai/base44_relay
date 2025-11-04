#!/usr/bin/env python3
# tools/print_effective_config.py
import sys, json
from core.strategy_config import render_effective_config, ConfigError

def main():
    if len(sys.argv) < 2:
        print("Usage: python tools\\print_effective_config.py SUB7|SUB2|SUB1|MAIN|<uid>")
        sys.exit(1)
    target = sys.argv[1]
    try:
        cfg = render_effective_config(target)
        print(json.dumps(cfg, indent=2))
    except ConfigError as e:
        print(f"[config/error] {e}")
        sys.exit(2)

if __name__ == "__main__":
    main()
