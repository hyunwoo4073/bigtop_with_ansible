#!/usr/bin/env python3
"""Bootstrap dedicated catalog DB and generate unlogged config; local PostgreSQL only."""
import argparse
import hashlib
import os
from pathlib import Path
import pwd
import grp
import re
import secrets
import subprocess
import tempfile

NAME = re.compile(r"^[a-z][a-z0-9_]{0,30}$")

def ident(s):
    if not NAME.fullmatch(s):
        raise ValueError("invalid SQL identifier")
    return s


def ensure_secret(path):
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        try:
            with os.fdopen(fd, "w") as fh:
                fh.write(secrets.token_urlsafe(36) + "\n")
        except BaseException:
            path.unlink(missing_ok=True)
            raise
    if not path.is_file() or (path.stat().st_mode & 0o077):
        raise RuntimeError("secret must be a mode 0600 regular file")
    secret = path.read_text(encoding="utf-8").strip()
    if not re.fullmatch(r"[A-Za-z0-9_\-]{20,150}", secret):
        raise RuntimeError("invalid generated secret")
    return secret


def run_pg(sql):
    p = subprocess.run(["runuser", "-u", "postgres", "--", "psql", "-X", "-qAt", "-v", "ON_ERROR_STOP=1", "-d", "postgres"],
                       input=sql, text=True, capture_output=True, check=False)
    if p.returncode:
        raise RuntimeError("PostgreSQL bootstrap failed (inspect local PostgreSQL server logs)")
    return p.stdout.strip()


def create_db(secret_file, role, db):
    role, db = ident(role), ident(db)
    existed = secret_file.exists()
    password = ensure_secret(secret_file)
    changed = not existed
    if run_pg(f"SELECT 1 FROM pg_roles WHERE rolname='{role}';") != "1":
        # Secret is generated in restricted file, never logged by the caller.
        run_pg(f"CREATE ROLE {role} LOGIN PASSWORD '{password}';")
        changed = True
    dbowner = run_pg(f"SELECT pg_get_userbyid(datdba) FROM pg_database WHERE datname='{db}';")
    if not dbowner:
        run_pg(f"CREATE DATABASE {db} OWNER {role};")
        changed = True
    elif dbowner != role:
        raise RuntimeError("existing catalog DB has a different owner; refusing to take over")
    # Force local socket/TCP only, no postgres.conf modifications.
    print("CREATED" if changed else "UNCHANGED")


def render(secret_file, output, dbuser, dbname, dbport, warehouse, bind, port, group):
    ident(dbuser); ident(dbname)
    if not warehouse.startswith('hdfs://master:9000/data/olist/iceberg-rest'):
        raise ValueError("isolated HDFS warehouse required")
    secret = ensure_secret(secret_file)
    lines = [
        'gravitino.iceberg-rest.host = ' + bind,
        'gravitino.iceberg-rest.httpPort = ' + str(int(port)),
        'gravitino.iceberg-rest.catalog-backend = jdbc',
        'gravitino.iceberg-rest.catalog-backend-name = bigtop_iceberg_rest56',
        'gravitino.iceberg-rest.uri = jdbc:postgresql://127.0.0.1:' + str(int(dbport)) + '/' + dbname,
        'gravitino.iceberg-rest.jdbc-user = ' + dbuser,
        'gravitino.iceberg-rest.jdbc-password = ' + secret,
        'gravitino.iceberg-rest.jdbc-driver = org.postgresql.Driver',
        'gravitino.iceberg-rest.jdbc-initialize = true',
        'gravitino.iceberg-rest.warehouse = ' + warehouse,
    ]
    content = '\n'.join(lines) + '\n'
    output.parent.mkdir(parents=True, exist_ok=True)
    prior = output.read_text(encoding='utf-8') if output.exists() else None
    if prior == content:
        print("UNCHANGED")
        return
    # Do not disclose secret through Ansible output; atomic root-owned config replacement.
    fd, temp = tempfile.mkstemp(prefix='.iceberg-rest-', dir=str(output.parent), text=True)
    try:
        os.fchmod(fd, 0o640)
        os.fchown(fd, 0, grp.getgrnam(group).gr_gid)
        with os.fdopen(fd, 'w', encoding='utf-8') as f:
            f.write(content)
        os.replace(temp, output)
        print("CHANGED")
    finally:
        if os.path.exists(temp): os.unlink(temp)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--secret', type=Path, required=True)
    parser.add_argument('--role')
    parser.add_argument('--database')
    parser.add_argument('--render', action='store_true')
    parser.add_argument('--output', type=Path)
    parser.add_argument('--dbuser')
    parser.add_argument('--dbname')
    parser.add_argument('--dbport', type=int)
    parser.add_argument('--warehouse')
    parser.add_argument('--bind')
    parser.add_argument('--port', type=int)
    parser.add_argument('--group')
    args = parser.parse_args()
    if args.render:
        render(args.secret, args.output, args.dbuser, args.dbname, args.dbport,
               args.warehouse, args.bind, args.port, args.group)
    else:
        create_db(args.secret, args.role, args.database)

if __name__ == '__main__':
    main()
