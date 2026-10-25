#!/usr/bin/env python3
"""Import historical action JSONL events into PostgreSQL idempotently.

Requires WEB_CONSOLE_DB_DSN and psycopg 3. Does not modify the JSONL file.
"""
import argparse
import hashlib
import json
import os
import uuid
from pathlib import Path


def event_id_for(item, line, line_no):
    provided = item.get("event_id")
    if provided:
        return str(uuid.UUID(str(provided)))
    digest = hashlib.sha256(f"{line_no}:".encode() + line).hexdigest()
    return str(uuid.uuid5(uuid.NAMESPACE_URL, "bigtop-web-console-log:" + digest))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--log", default="/var/log/bigtop-web-console/actions.log")
    args = parser.parse_args()
    dsn = os.environ.get("WEB_CONSOLE_DB_DSN", "")
    if not dsn:
        raise SystemExit("ERROR: WEB_CONSOLE_DB_DSN not set")
    import psycopg
    count, skipped = 0, 0
    with psycopg.connect(dsn, connect_timeout=3) as conn:
        with conn.cursor() as cur:
            for i, raw in enumerate(Path(args.log).read_bytes().splitlines(), 1):
                try:
                    item = json.loads(raw)
                    if not isinstance(item, dict):
                        skipped += 1
                        continue
                    event_id = event_id_for(item, raw, i)
                    item.setdefault("event_id", event_id)
                    cur.execute(
                        """INSERT INTO action_history
                          (event_id, recorded_at, action, host, service, ok, blocked, payload)
                          VALUES (%s::uuid, %s, %s, %s, %s, %s, %s, %s::jsonb)
                          ON CONFLICT (event_id) DO NOTHING""",
                        (event_id, str(item.get("recorded_at") or ""), item.get("action"),
                         item.get("host"), item.get("service"), item.get("ok"),
                         bool(item.get("blocked", False)), json.dumps(item, ensure_ascii=False)),
                    )
                    count += cur.rowcount
                except (ValueError, UnicodeDecodeError, json.JSONDecodeError, KeyError, TypeError):
                    skipped += 1
    print(f"Imported: {count}; invalid/skipped: {skipped}")


if __name__ == "__main__":
    main()
