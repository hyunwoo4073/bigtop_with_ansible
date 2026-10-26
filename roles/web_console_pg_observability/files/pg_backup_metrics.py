#!/usr/bin/env python3
"""Publish local, read-only PostgreSQL backup health metrics to Node Exporter."""
import argparse
import json
import os
import re
import subprocess
import sys
import tempfile
import time
from pathlib import Path

PREFIX = "bigtop_web_console_pg_backup_"
MEASURES = {
    "collector_success": "1 if the observer completed its checks",
    "last_check_timestamp_seconds": "Unix time of latest collector run",
    "archive_count": "Number of managed PostgreSQL backup archives",
    "latest_archive_size_bytes": "Size in bytes of newest archive",
    "latest_archive_timestamp_seconds": "mtime of newest managed archive",
    "latest_archive_valid": "1 if pg_restore can read the latest archive",
    "last_valid_timestamp_seconds": "mtime of newest verified managed archive, or zero",
    "timer_active": "1 if backup timer is active",
}


def run_command(args, timeout):
    try:
        result = subprocess.run(args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                                timeout=timeout, check=False)
        return result.returncode == 0
    except (OSError, subprocess.TimeoutExpired):
        return False


def collect(config, now=None, verify=None, check_timer=None):
    """Collect observations; never consider missing backup files healthy."""
    now = int(time.time() if now is None else now)
    db = config["database"]
    root = Path(config["backup_dir"])
    valid_name = re.compile(r"^" + re.escape(db) + r"_\d{8}T\d{6}Z_\d+\.dump$")
    result = {name: 0 for name in MEASURES}
    result["last_check_timestamp_seconds"] = now
    result["collector_success"] = 1
    try:
        if verify is None:
            verify = lambda path: run_command(["pg_restore", "--list", str(path)],
                                               config.get("restore_timeout", 60))
        if check_timer is None:
            check_timer = lambda: run_command(["systemctl", "is-active", "--quiet",
                                               config["backup_timer"]], 10)
        result["timer_active"] = int(bool(check_timer()))
        if not root.is_dir():
            result["collector_success"] = 0
            return result
        candidates = []
        for path in root.iterdir():
            if not valid_name.fullmatch(path.name) or path.is_symlink() or not path.is_file():
                continue
            stat = path.stat()
            candidates.append((stat.st_mtime, stat.st_size, path))
        candidates.sort(key=lambda row: row[0], reverse=True)
        result["archive_count"] = len(candidates)
        if not candidates:
            return result
        mtime, size, latest = candidates[0]
        result["latest_archive_timestamp_seconds"] = int(mtime)
        result["latest_archive_size_bytes"] = size
        minimum = int(config.get("minimum_bytes", 1024))
        if size >= minimum and verify(latest):
            result["latest_archive_valid"] = 1
            result["last_valid_timestamp_seconds"] = int(mtime)
        # No silent fallback to an older archive: an invalid latest dump is unhealthy.
    except (OSError, ValueError, KeyError) as error:
        print("Backup observer check failed: " + str(error), file=sys.stderr)
        result["collector_success"] = 0
    return result


def format_metrics(values):
    lines = []
    for name, description in MEASURES.items():
        metric = PREFIX + name
        lines.append(f"# HELP {metric} {description}")
        lines.append(f"# TYPE {metric} gauge")
        lines.append(f"{metric} {int(values[name])}")
    return "\n".join(lines) + "\n"


def write_atomic(path, data):
    target = Path(path)
    fd, temporary = tempfile.mkstemp(prefix="." + target.name + ".", dir=target.parent)
    try:
        with os.fdopen(fd, "w", encoding="ascii") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.chmod(temporary, 0o644)
        os.replace(temporary, target)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    args = parser.parse_args()
    with open(args.config, encoding="utf-8") as stream:
        config = json.load(stream)
    values = collect(config)
    write_atomic(config["metric_file"], format_metrics(values))
    print("BACKUP METRICS OK" if values["collector_success"] else "BACKUP METRICS DEGRADED")
    return 0 if values["collector_success"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
