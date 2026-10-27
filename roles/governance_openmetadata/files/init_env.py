#!/usr/bin/env python3
"""Create restricted first-run database credentials on the managed VM.

Do not write secrets to stdout or the Ansible controller. Never rotate on replay.
"""
from pathlib import Path
import os
import secrets
import sys
import tempfile

REQUIRED = ("MYSQL_ROOT_PASSWORD", "OM_DB_PASSWORD")


def generate(path):
    dest = Path(path)
    if dest.exists():
        if not dest.is_file() or dest.is_symlink():
            raise RuntimeError("Refusing non-regular environment file")
        content = dest.read_text(encoding="utf-8")
        keys = {line.split("=", 1)[0] for line in content.splitlines() if "=" in line}
        if not all(key in keys for key in REQUIRED):
            raise RuntimeError("Existing .env is missing required credentials; refusing overwrite")
        os.chmod(dest, 0o600)
        print("EXISTS")
        return

    data = "".join(f"{key}={secrets.token_hex(24)}\n" for key in REQUIRED)
    fd, temp = tempfile.mkstemp(prefix=".env-tmp-", dir=dest.parent)
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        # Atomic creation without replacing previously created credentials.
        os.link(temp, dest)
        print("CREATED")
    finally:
        os.unlink(temp)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("Usage: init_env.py /path/to/.env")
    generate(sys.argv[1])
