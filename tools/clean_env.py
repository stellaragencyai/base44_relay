# clean_env.py
lines = {}
with open(".env", "r", encoding="utf-8") as f:
    for line in f:
        if "=" in line and not line.strip().startswith("#"):
            key = line.split("=", 1)[0].strip()
            lines[key] = line  # keeps the *last* version
        else:
            lines[hash(line)] = line  # preserve comments and blanks uniquely

with open(".env", "w", encoding="utf-8") as f:
    f.writelines(lines.values())
