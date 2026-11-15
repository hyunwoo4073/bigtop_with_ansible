#!/usr/bin/env python3
"""Create and verify root-only logical metadata backups on their source VM.

Only two fixed, existing databases are eligible for read-only dumps. The script
never restores, truncates, creates, drops, or alters a production database.
"""
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import socket
import stat
import subprocess
import sys

ROOT = Path('/var/backups/bigtop-step76')
ID_RE = re.compile(r'^[0-9]{8}T[0-9]{6}Z-[a-f0-9]{8}$')
DATABASES = {'pg': 'iceberg_rest56', 'mysql': 'openmetadata_db'}
FILES = {'pg': 'iceberg_rest56.pgcustom', 'mysql': 'openmetadata_db.sql'}


def run(argv, *, timeout=60):
    try:
        p = subprocess.run(argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                           timeout=timeout, check=False)
    except (OSError, subprocess.TimeoutExpired) as e:
        raise RuntimeError('Required program failed or timed out: ' + argv[0]) from e
    if p.returncode:
        raise RuntimeError('Required command returned nonzero: ' + argv[0])
    return p.stdout.decode('utf-8', errors='replace').strip()


def safe_directory(identifier, create=False):
    if not ID_RE.fullmatch(identifier):
        raise ValueError('Invalid backup identity')
    if ROOT.is_symlink():
        raise RuntimeError('Backup root cannot be a symbolic link')
    if create:
        ROOT.mkdir(mode=0o700, parents=True, exist_ok=True)
    if not ROOT.is_dir():
        raise RuntimeError('Backup root does not exist')
    if ROOT.stat().st_uid != 0 or stat.S_IMODE(ROOT.stat().st_mode) != 0o700:
        raise RuntimeError('Backup root must be owned by root with mode 0700')
    path = ROOT / identifier
    if path.is_symlink():
        raise RuntimeError('Backup directory cannot be a symbolic link')
    if create:
        path.mkdir(mode=0o700, exist_ok=True)
    if not path.is_dir():
        raise RuntimeError('Backup directory does not exist')
    if path.stat().st_uid != 0 or stat.S_IMODE(path.stat().st_mode) != 0o700:
        raise RuntimeError('Backup directory must be owned by root with mode 0700')
    return path


def docker_mysql_id():
    ids = run(['docker', 'ps', '-q', '--filter',
               'label=com.docker.compose.project=bigtop-openmetadata',
               '--filter', 'label=com.docker.compose.service=mysql']).splitlines()
    if len(ids) != 1:
        raise RuntimeError('Expected one running OpenMetadata MySQL container')
    cid = ids[0]
    actual_db = run(['docker', 'exec', cid, 'sh', '-c', 'printf %s "$MYSQL_DATABASE"'])
    if actual_db != DATABASES['mysql']:
        raise RuntimeError('OpenMetadata container database name does not match')
    return cid


def command_for(component):
    if component == 'pg':
        actual_db = run(['runuser', '-u', 'postgres', '--', 'psql', '-X', '-A', '-t', '-d',
                         'postgres', '-c',
                         "SELECT datname FROM pg_database WHERE datname='iceberg_rest56'"] )
        if actual_db != DATABASES['pg']:
            raise RuntimeError('Catalog PostgreSQL source database not found')
        return ['runuser', '-u', 'postgres', '--', 'pg_dump', '--format=custom',
                '--no-owner', '--no-privileges', '--dbname=iceberg_rest56']
    cid = docker_mysql_id()
    cmd = ('export MYSQL_PWD="$MYSQL_PASSWORD"; '
           'exec mysqldump --protocol=TCP -h 127.0.0.1 -u "$MYSQL_USER" '
           '--single-transaction --quick --routines --events --triggers '
           '--hex-blob --default-character-set=utf8mb4 --set-gtid-purged=OFF '
           '--column-statistics=0 --no-tablespaces "$MYSQL_DATABASE"')
    return ['docker', 'exec', cid, 'sh', '-c', cmd]


def sha_file(path):
    sha = hashlib.sha256()
    length = 0
    with path.open('rb') as src:
        for chunk in iter(lambda: src.read(1024 * 1024), b''):
            sha.update(chunk)
            length += len(chunk)
    return sha.hexdigest(), length


def validate_dump(component, path):
    if component == 'pg':
        with path.open('rb') as fh:
            if fh.read(5) != b'PGDMP':
                raise RuntimeError('PostgreSQL custom-format header is missing')
        listing = run(['pg_restore', '--list', str(path)], timeout=60)
        if not any('TABLE' in line and not line.startswith(';') for line in listing.splitlines()):
            raise RuntimeError('PostgreSQL dump has no table objects')
        return 'pg_restore_list_pass'
    with path.open('rb') as fh:
        header = fh.read(4096)
        if b'MySQL dump' not in header:
            raise RuntimeError('MySQL dump header is missing')
        if fh.seek(0, os.SEEK_END) < 100:
            raise RuntimeError('MySQL dump is unexpectedly small')
        fh.seek(max(fh.tell() - 16384, 0))
        tail = fh.read()
        if b'-- Dump completed on ' not in tail:
            raise RuntimeError('MySQL dump completion marker missing')
    return 'mysql_dump_header_footer_pass'


def atomic_json(path, doc):
    tmp = path.with_name(path.name + '.partial')
    fd = os.open(str(tmp), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as fh:
            json.dump(doc, fh, sort_keys=True, indent=2)
            fh.write('\n')
            fh.flush()
            os.fsync(fh.fileno())
        os.rename(tmp, path)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise


def backup(component, identifier):
    path = safe_directory(identifier, create=True)
    filename = FILES[component]
    dump = path / filename
    manifest = path / (filename + '.manifest.json')
    if dump.exists() or manifest.exists():
        raise RuntimeError('Backup already exists; refusing to overwrite this component')
    if shutil.disk_usage(path).free < 512 * 1024 * 1024:
        raise RuntimeError('Less than 512 MiB available for protected backup')
    partial = path / (filename + '.partial')
    fd = os.open(str(partial), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        cmd = command_for(component)
        with os.fdopen(fd, 'wb') as out:
            p = subprocess.run(cmd, stdout=out, stderr=subprocess.PIPE,
                               timeout=900, check=False)
            if p.returncode:
                raise RuntimeError('Database dump command failed (exit=' + str(p.returncode) + ')')
            out.flush()
            os.fsync(out.fileno())
        digest, size = sha_file(partial)
        if size < 1024:
            raise RuntimeError('Dump payload is unexpectedly small')
        check = validate_dump(component, partial)
        os.rename(partial, dump)
        doc = {
            'backup_id': identifier,
            'component': component,
            'database': DATABASES[component],
            'hostname': socket.gethostname(),
            'created_utc': dt.datetime.now(dt.timezone.utc).isoformat(),
            'archive_filename': filename,
            'sha256': digest,
            'bytes': size,
            'format_check': check,
            'scope': 'logical_db_only_no_elasticsearch_hdfs_or_off_host_copy',
        }
        atomic_json(manifest, doc)
    finally:
        partial.unlink(missing_ok=True)
    return report(component, identifier, doc, dump)


def report(component, identifier, doc, dump):
    return {'ok': True, 'backup_id': identifier, 'component': component,
            'database': doc['database'], 'node': doc['hostname'],
            'archive': str(dump), 'bytes': doc['bytes'], 'sha256': doc['sha256'],
            'format_check': doc['format_check'], 'verification': 'passed'}


def verify(component, identifier):
    directory = safe_directory(identifier, create=False)
    filename = FILES[component]
    dump = directory / filename
    manifest = directory / (filename + '.manifest.json')
    for path in (dump, manifest):
        if path.is_symlink() or not path.is_file():
            raise RuntimeError('Expected regular backup file missing or symlink detected')
        if stat.S_IMODE(path.stat().st_mode) != 0o600 or path.stat().st_uid != 0:
            raise RuntimeError('Backup file must be owned by root with mode 0600')
    doc = json.loads(manifest.read_text(encoding='utf-8'))
    if (doc.get('backup_id') != identifier or doc.get('component') != component or
            doc.get('database') != DATABASES[component] or
            doc.get('archive_filename') != filename):
        raise RuntimeError('Backup manifest identity mismatch')
    digest, length = sha_file(dump)
    if digest != doc.get('sha256') or length != doc.get('bytes'):
        raise RuntimeError('Backup checksum or size mismatch')
    checked = validate_dump(component, dump)
    if checked != doc.get('format_check'):
        raise RuntimeError('Backup format verification mismatch')
    return report(component, identifier, doc, dump)


def main(argv):
    if len(argv) != 5 or argv[1] not in ('backup', 'verify') or argv[2] not in DATABASES:
        print('Usage: step76_logical_backup.py backup|verify pg|mysql ID DB', file=sys.stderr)
        return 2
    action, component, identifier, dbname = argv[1:]
    if not ID_RE.fullmatch(identifier) or dbname != DATABASES[component]:
        print('Invalid identity, source, or database name', file=sys.stderr)
        return 2
    if os.geteuid() != 0:
        print('Requires root (backup destination owner)', file=sys.stderr)
        return 2
    try:
        result = backup(component, identifier) if action == 'backup' else verify(component, identifier)
        print(json.dumps(result, sort_keys=True))
        return 0
    except (RuntimeError, ValueError, OSError, json.JSONDecodeError,
            subprocess.TimeoutExpired) as e:
        # Do not log DB output, dump contents, or database credentials.
        print('STEP76_BACKUP_ERROR: ' + str(e), file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main(sys.argv))
