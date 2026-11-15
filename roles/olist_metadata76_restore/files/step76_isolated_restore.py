#!/usr/bin/env python3
"""Step76 Phase C: restore protected dumps into scratch databases only.

This script cannot accept a destination database from the caller. PostgreSQL
uses a new scratch database with a restricted temporary role; MySQL uses a
new scratch database and a one-time, restricted account in the existing MySQL
server container. Both scratch resources are cleaned after each proof.

No service restart, production DB restore, HDFS writes, or dump export.
"""
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import shutil
import stat
import subprocess
import sys

ROOT = Path('/var/backups/bigtop-step76')
ID_RE = re.compile(r'^[0-9]{8}T[0-9]{6}Z-[a-f0-9]{8}$')
DATABASE = {'pg': 'iceberg_rest56', 'mysql': 'openmetadata_db'}
FILENAME = {'pg': 'iceberg_rest56.pgcustom', 'mysql': 'openmetadata_db.sql'}
EVIDENCE = {'pg': 'restore-pg.proof.json', 'mysql': 'restore-mysql.proof.json'}


def run(args, *, input_bytes=None, timeout=90):
    try:
        p = subprocess.run(args, input=input_bytes, capture_output=True,
                           timeout=timeout, check=False)
    except (OSError, subprocess.TimeoutExpired) as e:
        raise RuntimeError('Tool missing or timed out: ' + args[0]) from e
    if p.returncode:
        # Do not print SQL, dumps, credentials, or command stderr.
        raise RuntimeError('Tool returned nonzero: ' + args[0]
                           + ' (exit=' + str(p.returncode) + ')')
    return p.stdout.decode('utf-8', errors='replace').strip()


def pg_sql(sql, db='postgres'):
    return run(['runuser', '-u', 'postgres', '--', 'psql', '-X', '-q', '-A', '-t',
                '-v', 'ON_ERROR_STOP=1', '-d', db, '-c', sql], timeout=120)


def pg_scratch_name(identifier):
    if not ID_RE.fullmatch(identifier):
        raise ValueError('Invalid backup identity')
    return 'step76_r_' + identifier[:8] + '_' + identifier[-8:]


def manifest_verified(component, identifier):
    if not ID_RE.fullmatch(identifier):
        raise ValueError('Invalid backup identity')
    if ROOT.is_symlink() or not ROOT.is_dir():
        raise RuntimeError('Backup root missing or symbolic link')
    if ROOT.stat().st_uid != 0 or stat.S_IMODE(ROOT.stat().st_mode) != 0o700:
        raise RuntimeError('Expected root-owned 0700 backup root')
    directory = ROOT / identifier
    if directory.is_symlink() or not directory.is_dir():
        raise RuntimeError('Backup ID directory missing or symbolic link')
    if directory.stat().st_uid != 0 or stat.S_IMODE(directory.stat().st_mode) != 0o700:
        raise RuntimeError('Expected root-owned 0700 backup ID directory')
    filename = FILENAME[component]
    archive = directory / filename
    manifest = directory / (filename + '.manifest.json')
    for p in (archive, manifest):
        if p.is_symlink() or not p.is_file():
            raise RuntimeError('Dump or manifest missing / symbolic link')
        if p.stat().st_uid != 0 or stat.S_IMODE(p.stat().st_mode) != 0o600:
            raise RuntimeError('Dump or manifest needs root ownership and mode 0600')
    doc = json.loads(manifest.read_text(encoding='utf-8'))
    if (doc.get('backup_id') != identifier or doc.get('component') != component
            or doc.get('database') != DATABASE[component]
            or doc.get('archive_filename') != filename):
        raise RuntimeError('Backup manifest does not match selected source')
    dig = hashlib.sha256()
    size = 0
    with archive.open('rb') as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b''):
            dig.update(chunk)
            size += len(chunk)
    if size < 1024 or size != doc.get('bytes') or dig.hexdigest() != doc.get('sha256'):
        raise RuntimeError('Backup SHA-256 or size mismatch')
    if component == 'pg':
        with archive.open('rb') as fh:
            if fh.read(5) != b'PGDMP':
                raise RuntimeError('Unexpected PostgreSQL dump header')
    else:
        with archive.open('rb') as fh:
            if b'MySQL dump' not in fh.read(4096):
                raise RuntimeError('Unexpected MySQL dump header')
            fh.seek(max(size - 16384, 0))
            if b'-- Dump completed on ' not in fh.read():
                raise RuntimeError('MySQL dump completion marker not found')
    return archive, doc


def mysql_container():
    ids = run(['docker', 'ps', '-q', '--filter',
               'label=com.docker.compose.project=bigtop-openmetadata',
               '--filter', 'label=com.docker.compose.service=mysql']).splitlines()
    if len(ids) != 1:
        raise RuntimeError('Expected exactly one OpenMetadata MySQL container')
    cid = ids[0]
    source = run(['docker', 'exec', cid, 'sh', '-c',
                  'printf %s "$MYSQL_DATABASE"'])
    if source != DATABASE['mysql']:
        raise RuntimeError('OpenMetadata source database unexpectedly changed')
    return cid


def mysql_root(cid, sql):
    # Password remains inside the existing MySQL container environment.
    return run(['docker', 'exec', '-i', cid, 'sh', '-c',
                'MYSQL_PWD="$MYSQL_ROOT_PASSWORD" exec mysql '
                '--protocol=socket -uroot --batch --skip-column-names'],
               input_bytes=(sql + '\n').encode('utf-8'), timeout=90)


def mysql_as_scratch(cid, user, password, db, sql=None, archive=None):
    # Password is supplied as first stdin line, never shell argv or Ansible log.
    # The temporary account has privileges ONLY on this scratch database.
    command = ['docker', 'exec', '-i', cid, 'sh', '-c',
               'IFS= read -r STEP76_TEMP_PASSWORD; '
               'export MYSQL_PWD="$STEP76_TEMP_PASSWORD"; '
               'exec mysql --protocol=socket --batch --skip-column-names '
               '--user=' + user + ' --database=' + db]
    return pipe_to_process(command, password, archive=archive, sql=sql, timeout=900)


def pipe_to_process(argv, password=None, archive=None, sql=None, timeout=900):
    """Stream a dump without loading it in RAM. stdout/stderr are suppressed."""
    if (archive is None) == (sql is None):
        raise ValueError('Provide precisely one input source')
    try:
        p = subprocess.Popen(argv, stdin=subprocess.PIPE, stdout=subprocess.DEVNULL,
                             stderr=subprocess.DEVNULL)
    except OSError as e:
        raise RuntimeError('Restore executable unavailable: ' + argv[0]) from e
    try:
        if password is not None:
            p.stdin.write((password + '\n').encode('utf-8'))
        if archive is not None:
            with archive.open('rb') as fh:
                shutil.copyfileobj(fh, p.stdin, length=1024 * 1024)
        else:
            p.stdin.write((sql + '\n').encode('utf-8'))
        p.stdin.close()
        code = p.wait(timeout=timeout)
        if code:
            raise RuntimeError('Isolated restore returned nonzero (' + str(code) + ')')
    except (BrokenPipeError, subprocess.TimeoutExpired, OSError) as e:
        p.kill()
        p.wait()
        raise RuntimeError('Isolated restore stream failed') from e


def expected_pg_tables(archive):
    listing = run(['pg_restore', '--list', str(archive)], timeout=90)
    return sum(bool(re.search(r'\bTABLE\s', line)) and 'TABLE DATA' not in line
               for line in listing.splitlines() if not line.startswith(';'))


def expected_mysql_tables(archive):
    count = 0
    with archive.open('rb') as fh:
        for line in fh:
            if line.startswith(b'CREATE TABLE `'):
                count += 1
    return count


def pg_table_count(db):
    return int(pg_sql("SELECT count(*) FROM pg_catalog.pg_tables "
                      "WHERE schemaname NOT IN ('pg_catalog','information_schema')", db))


def mysql_table_count(cid, db):
    # MySQL dumps materialize views as temporary CREATE TABLE statements;
    # count both real tables and final views in the restored inventory.
    return int(mysql_root(cid, "SELECT COUNT(*) FROM information_schema.tables "
                          "WHERE table_schema='" + db + "';"))


def pg_sample_counts(db, tables=None):
    """Exact counts for up to five tables; never used to gate snapshot drift."""
    if tables is None:
        query = ("SELECT quote_ident(schemaname)||'.'||quote_ident(tablename) "
                 "FROM pg_catalog.pg_tables WHERE schemaname NOT IN "
                 "('pg_catalog','information_schema') "
                 "ORDER BY schemaname,tablename LIMIT 5")
        tables = pg_sql(query, db).splitlines()
    result = {}
    for name in tables:
        if name:
            result[name] = int(pg_sql('SELECT COUNT(*) FROM ' + name, db))
    return result


def mysql_sample_counts(cid, db, tables=None):
    """Exact sample counts on scratch and current source for drift diagnosis."""
    if tables is None:
        query = ("SELECT table_name FROM information_schema.tables "
                 "WHERE table_schema='" + db + "' AND table_type='BASE TABLE' "
                 "ORDER BY table_name LIMIT 5;")
        tables = mysql_root(cid, query).splitlines()
    result = {}
    for name in tables:
        if name:
            safe = name.replace('`', '``')
            sql = ('SELECT COUNT(*) FROM `' + db + '`.`' + safe + '`;')
            result[name] = int(mysql_root(cid, sql))
    return result


def preflight(component, identifier):
    archive, doc = manifest_verified(component, identifier)
    scratch = pg_scratch_name(identifier)
    evidence = archive.parent / EVIDENCE[component]
    if evidence.is_symlink():
        raise RuntimeError('Evidence cannot be symbolic link')
    if component == 'pg':
        original = pg_sql("SELECT datname FROM pg_database WHERE datname='iceberg_rest56'")
        if original != DATABASE['pg']:
            raise RuntimeError('Production source PostgreSQL catalog is missing')
        if pg_sql("SELECT 1 FROM pg_database WHERE datname='" + scratch + "'"):
            raise RuntimeError('Existing scratch PostgreSQL database; refusing to overwrite')
        if pg_sql("SELECT 1 FROM pg_roles WHERE rolname='" + scratch + "'"):
            raise RuntimeError('Existing scratch PostgreSQL role; refusing to overwrite')
        expected = expected_pg_tables(archive)
    else:
        cid = mysql_container()
        if mysql_root(cid, "SELECT SCHEMA_NAME FROM information_schema.schemata "
                           "WHERE SCHEMA_NAME='" + scratch + "';"):
            raise RuntimeError('Existing scratch MySQL database; refusing to overwrite')
        # Root cannot CREATE USER if an earlier cleanup left an account behind.
        if mysql_root(cid, "SELECT user FROM mysql.user WHERE user='" + scratch
                           + "' AND host='localhost';"):
            raise RuntimeError('Existing scratch MySQL user; refusing to overwrite')
        expected = expected_mysql_tables(archive)
    if expected < 1:
        raise RuntimeError('Backup contains no expected tables')
    if shutil.disk_usage(archive.parent).free < max(256 * 1024 * 1024, doc['bytes'] * 2):
        raise RuntimeError('Insufficient free disk space for restore exercise')
    return {'ok': True, 'backup_id': identifier, 'component': component,
            'expected_tables': expected, 'archive_bytes': doc['bytes'],
            'archive_sha256': doc['sha256'], 'evidence_exists': evidence.exists(),
            'scratch_db': scratch, 'scope': 'same_server_temporary_database_not_off_host_dr'}


def pg_restore_proof(identifier, archive, expected):
    scratch = pg_scratch_name(identifier)
    role_created = False
    db_created = False
    try:
        pg_sql('CREATE ROLE "' + scratch + '" NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE;')
        role_created = True
        pg_sql('CREATE DATABASE "' + scratch + '" OWNER "' + scratch
               + '" TEMPLATE template0;')
        db_created = True
        # --role causes SET ROLE before executing restore SQL. The role owns
        # only the scratch DB and cannot modify the source DB.
        cmd = ['runuser', '-u', 'postgres', '--', 'pg_restore',
               '--no-owner', '--no-privileges', '--exit-on-error',
               '--role=' + scratch, '--dbname=' + scratch]
        pipe_to_process(cmd, archive=archive)
        actual = pg_table_count(scratch)
        current = pg_table_count(DATABASE['pg'])
        if actual != expected:
            raise RuntimeError('PostgreSQL restored table count differs from archive')
        sample = pg_sample_counts(scratch)
        try:
            source_sample = pg_sample_counts(DATABASE['pg'], list(sample))
            match = sample == source_sample
        except RuntimeError:
            match = None  # Live source may have changed since backup.
        return {'tables_restored': actual, 'source_tables_current': current,
                'matches_live_schema_table_count': actual == current,
                'restored_sample_row_counts': sample,
                'matches_live_sample_row_counts': match}
    finally:
        if db_created:
            pg_sql('DROP DATABASE "' + scratch + '";')
        if role_created:
            pg_sql('DROP ROLE "' + scratch + '";')


def mysql_restore_proof(identifier, archive, expected):
    scratch = pg_scratch_name(identifier)
    cid = mysql_container()
    # This password is used for ONE ephemeral scratch-only database account.
    password = secrets.token_urlsafe(32)
    created_database = False
    created_user = False
    try:
        mysql_root(cid, 'CREATE DATABASE `' + scratch
                   + '` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;')
        created_database = True
        mysql_root(cid, "CREATE USER '" + scratch + "'@'localhost' IDENTIFIED BY '"
                   + password + "';")
        created_user = True
        mysql_root(cid, "GRANT ALL PRIVILEGES ON `" + scratch + "`.* TO '"
                   + scratch + "'@'localhost';")
        mysql_as_scratch(cid, scratch, password, scratch, archive=archive)
        actual = mysql_table_count(cid, scratch)
        current = mysql_table_count(cid, DATABASE['mysql'])
        if actual != expected:
            raise RuntimeError('MySQL restored table count differs from archive')
        sample = mysql_sample_counts(cid, scratch)
        try:
            source_sample = mysql_sample_counts(cid, DATABASE['mysql'], list(sample))
            match = sample == source_sample
        except RuntimeError:
            match = None  # Live source may have changed since backup.
        return {'tables_restored': actual, 'source_tables_current': current,
                'matches_live_schema_table_count': actual == current,
                'restored_sample_row_counts': sample,
                'matches_live_sample_row_counts': match}
    finally:
        # A restricted account cannot write to the production DB. Remove only
        # resources this invocation actually created.
        # Remove stored objects before dropping the account that may own
        # views/routines/triggers as their definer.
        if created_database:
            mysql_root(cid, 'DROP DATABASE `' + scratch + '`;')
        if created_user:
            mysql_root(cid, "DROP USER '" + scratch + "'@'localhost';")


def atomic_evidence(path, doc):
    temp = path.with_name(path.name + '.partial')
    fd = os.open(str(temp), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as fh:
            json.dump(doc, fh, sort_keys=True, indent=2)
            fh.write('\n')
            fh.flush()
            os.fsync(fh.fileno())
        os.link(temp, path)  # fail rather than overwrite an earlier proof
    finally:
        temp.unlink(missing_ok=True)


def restore(component, identifier):
    pf = preflight(component, identifier)
    if pf['evidence_exists']:
        raise RuntimeError('Restore proof already exists; refusing to repeat this backup')
    archive, doc = manifest_verified(component, identifier)
    if component == 'pg':
        metrics = pg_restore_proof(identifier, archive, pf['expected_tables'])
    else:
        metrics = mysql_restore_proof(identifier, archive, pf['expected_tables'])
    proof = {'ok': True, 'backup_id': identifier, 'component': component,
             'source_database': DATABASE[component], 'scratch_database': pf['scratch_db'],
             'archive_sha256': doc['sha256'], 'archive_bytes': doc['bytes'],
             'expected_dump_tables': pf['expected_tables'],
             'scratch_cleanup': 'completed', 'isolation': 'restricted_scratch_database_same_server',
             **metrics}
    atomic_evidence(archive.parent / EVIDENCE[component], proof)
    return proof


def verify(component, identifier):
    archive, doc = manifest_verified(component, identifier)
    proof_path = archive.parent / EVIDENCE[component]
    if proof_path.is_symlink() or not proof_path.is_file():
        raise RuntimeError('Restore proof missing')
    if proof_path.stat().st_uid != 0 or stat.S_IMODE(proof_path.stat().st_mode) != 0o600:
        raise RuntimeError('Restore proof requires root-owned 0600 file')
    proof = json.loads(proof_path.read_text(encoding='utf-8'))
    if (proof.get('backup_id') != identifier or proof.get('component') != component
            or proof.get('archive_sha256') != doc['sha256']
            or proof.get('scratch_cleanup') != 'completed'
            or proof.get('tables_restored') != proof.get('expected_dump_tables')
            or proof.get('tables_restored', 0) < 1):
        raise RuntimeError('Restore proof identity or data validation mismatch')
    scratch = pg_scratch_name(identifier)
    if component == 'pg':
        if pg_sql("SELECT 1 FROM pg_database WHERE datname='" + scratch + "'"):
            raise RuntimeError('Scratch PostgreSQL DB was not cleaned')
        if pg_sql("SELECT 1 FROM pg_roles WHERE rolname='" + scratch + "'"):
            raise RuntimeError('Scratch PostgreSQL role was not cleaned')
    else:
        cid = mysql_container()
        if mysql_root(cid, "SELECT SCHEMA_NAME FROM information_schema.schemata "
                           "WHERE SCHEMA_NAME='" + scratch + "';"):
            raise RuntimeError('Scratch MySQL DB was not cleaned')
        if mysql_root(cid, "SELECT user FROM mysql.user WHERE user='" + scratch
                           + "' AND host='localhost';"):
            raise RuntimeError('Scratch MySQL user was not cleaned')
    return {'ok': True, 'backup_id': identifier, 'component': component,
            'tables_restored': proof['tables_restored'], 'scratch_cleanup': 'verified',
            'archive_sha256': proof['archive_sha256']}


def main():
    if len(sys.argv) != 4 or sys.argv[1] not in ('preflight', 'restore', 'verify') \
            or sys.argv[2] not in DATABASE or not ID_RE.fullmatch(sys.argv[3]):
        print('Usage: step76_isolated_restore.py preflight|restore|verify pg|mysql ID',
              file=sys.stderr)
        return 2
    if os.geteuid() != 0:
        print('STEP76_C_ERROR: root access required for protected backups', file=sys.stderr)
        return 2
    mode, component, identifier = sys.argv[1:]
    try:
        if mode == 'preflight':
            report = preflight(component, identifier)
        elif mode == 'restore':
            report = restore(component, identifier)
        else:
            report = verify(component, identifier)
        print(json.dumps(report, ensure_ascii=False, sort_keys=True))
        return 0
    except (RuntimeError, OSError, ValueError, json.JSONDecodeError) as e:
        print('STEP76_C_ERROR: ' + str(e), file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())
