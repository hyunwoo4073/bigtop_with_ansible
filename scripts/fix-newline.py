#!/usr/bin/env python3
from pathlib import Path

targets = [
    Path(".yamllint"),
    Path(".ansible-lint"),
    Path("requirements-dev.txt"),
    Path("Makefile"),
    Path("README.md"),
    Path("inventory/group_vars/all.yml.example"),
    Path("collections/requirements.yml"),
]

scan_dirs = [
    Path("playbooks"),
    Path("roles"),
    Path("scripts"),
    Path("docs"),
    Path("collections"),
]

text_suffixes = {
    ".yml",
    ".yaml",
    ".j2",
    ".sh",
    ".md",
    ".txt",
    ".cfg",
    ".ini",
}

for base in scan_dirs:
    if not base.exists():
        continue

    for path in base.rglob("*"):
        if path.is_file() and path.suffix in text_suffixes:
            targets.append(path)

fixed = 0

for file in sorted(set(targets)):
    if not file.exists():
        continue

    data = file.read_bytes()

    if not data:
        continue

    if b"\0" in data:
        continue

    if not data.endswith(b"\n"):
        file.write_bytes(data + b"\n")
        print(f"fixed newline: {file}")
        fixed += 1

if fixed == 0:
    print("OK: no missing EOF newlines")
else:
    print(f"OK: fixed {fixed} file(s)")
