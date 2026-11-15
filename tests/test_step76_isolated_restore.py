"""Static and behavior tests: Step76C must never target a production DB."""
import hashlib
import importlib.util
import json
from pathlib import Path
import re
from unittest.mock import patch

import pytest

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / 'roles/olist_metadata76_restore/files/step76_isolated_restore.py'
spec = importlib.util.spec_from_file_location('step76_isolated_restore', SOURCE)
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)
ID = '20261115T084500Z-abcd1234'


def setup_backup(tmp_path, monkeypatch, component='mysql'):
    tmp_path.chmod(0o700)
    folder = tmp_path / ID
    folder.mkdir(mode=0o700)
    data = (b'-- MySQL dump\nCREATE TABLE `abc` (id INT);\n'
            b'-- Dump completed on 2026-11-15\n') if component == 'mysql' else (
            b'PGDMP' + b'123456789\n' * 200)
    data += b' ' * 1100
    if component == 'mysql':
        data += b'\n-- Dump completed on 2026-11-15\n'
    name = mod.FILENAME[component]
    dump = folder / name
    dump.write_bytes(data)
    dump.chmod(0o600)
    doc = {
        'backup_id': ID,
        'component': component,
        'database': mod.DATABASE[component],
        'archive_filename': name,
        'bytes': len(data),
        'sha256': hashlib.sha256(data).hexdigest(),
    }
    manifest = folder / (name + '.manifest.json')
    manifest.write_text(json.dumps(doc))
    manifest.chmod(0o600)
    monkeypatch.setattr(mod, 'ROOT', tmp_path)
    return dump, manifest


def test_scratch_cannot_equal_production():
    scratch = mod.pg_scratch_name(ID)
    assert re.fullmatch('[a-z_][a-z0-9_]*', scratch)
    assert scratch not in mod.DATABASE.values()
    for invalid in ['', 'iceberg_rest56', 'openmetadata_db', '20261115T084500Z-x;DROP']:
        with pytest.raises(ValueError):
            mod.pg_scratch_name(invalid)


def test_archive_checksum_validation(tmp_path, monkeypatch):
    dump, _ = setup_backup(tmp_path, monkeypatch)
    got, doc = mod.manifest_verified('mysql', ID)
    assert got == dump
    assert doc['archive_filename'] == mod.FILENAME['mysql']
    dump.write_bytes(dump.read_bytes() + b'BAD')
    with pytest.raises(RuntimeError, match='SHA-256'):
        mod.manifest_verified('mysql', ID)


def test_manifest_source_mismatch_is_rejected(tmp_path, monkeypatch):
    _, manifest = setup_backup(tmp_path, monkeypatch)
    doc = json.loads(manifest.read_text())
    doc['database'] = 'production'
    manifest.write_text(json.dumps(doc))
    with pytest.raises(RuntimeError, match='does not match'):
        mod.manifest_verified('mysql', ID)


def test_refuse_backup_symlink(tmp_path, monkeypatch):
    dump, _ = setup_backup(tmp_path, monkeypatch)
    dump.unlink()
    dump.symlink_to('/etc/hosts')
    with pytest.raises(RuntimeError, match='symbolic link'):
        mod.manifest_verified('mysql', ID)


def test_expected_mysql_table_count(tmp_path, monkeypatch):
    dump, _ = setup_backup(tmp_path, monkeypatch)
    assert mod.expected_mysql_tables(dump) == 1


def test_preflight_detects_existing_temp_db(tmp_path, monkeypatch):
    setup_backup(tmp_path, monkeypatch)
    monkeypatch.setattr(mod, 'mysql_container', lambda: 'container-1')
    monkeypatch.setattr(mod, 'mysql_root', lambda cid, sql: 'x' if 'information_schema.schemata' in sql else '')
    with pytest.raises(RuntimeError, match='Existing scratch MySQL database'):
        mod.preflight('mysql', ID)


def test_pg_restore_uses_restricted_role_and_scratch_db(tmp_path, monkeypatch):
    dump, _ = setup_backup(tmp_path, monkeypatch, 'pg')
    commands = []
    monkeypatch.setattr(mod, 'pg_sql', lambda sql, db='postgres': commands.append(('sql', sql, db)) or
                        ('1' if 'count(*)' in sql else ''))
    monkeypatch.setattr(mod, 'pg_table_count', lambda db: 1)
    monkeypatch.setattr(mod, 'pg_sample_counts', lambda db, tables=None: {'public.t': 1})
    monkeypatch.setattr(mod, 'pipe_to_process',
                        lambda cmd, **kw: commands.append(('restore', cmd, kw)))
    result = mod.pg_restore_proof(ID, dump, 1)
    scratch = mod.pg_scratch_name(ID)
    assert result['tables_restored'] == 1
    assert any(x[0] == 'restore' and '--role=' + scratch in x[1]
               and '--dbname=' + scratch in x[1] for x in commands)
    assert not any(x[0] == 'restore' and '--dbname=iceberg_rest56' in x[1]
                   for x in commands)
    assert any(x[0] == 'sql' and x[1] == 'DROP DATABASE "' + scratch + '";'
               for x in commands)


def test_mysql_restore_uses_scratch_account(tmp_path, monkeypatch):
    dump, _ = setup_backup(tmp_path, monkeypatch)
    commands = []
    monkeypatch.setattr(mod, 'mysql_container', lambda: 'container-1')
    monkeypatch.setattr(mod, 'mysql_root', lambda cid, sql: commands.append(('root', sql)) or '')
    monkeypatch.setattr(mod, 'mysql_as_scratch',
                        lambda cid, user, password, db, **kw: commands.append(('restore', user, db)))
    monkeypatch.setattr(mod, 'mysql_table_count', lambda cid, db: 1)
    monkeypatch.setattr(mod, 'mysql_sample_counts', lambda cid, db, tables=None: {'t': 1})
    result = mod.mysql_restore_proof(ID, dump, 1)
    scratch = mod.pg_scratch_name(ID)
    assert result['tables_restored'] == 1
    assert ('restore', scratch, scratch) in commands
    assert not any(x[0] == 'restore' and x[2] == 'openmetadata_db' for x in commands)
    assert any(x[0] == 'root' and x[1] == 'DROP DATABASE `' + scratch + '`;'
               for x in commands)
    assert any(x[0] == 'root' and x[1].startswith('DROP USER ') for x in commands)


def test_no_automatic_repeat_of_completed_proof(tmp_path, monkeypatch):
    dump, _ = setup_backup(tmp_path, monkeypatch)
    proof = dump.parent / mod.EVIDENCE['mysql']
    proof.write_text('{}')
    proof.chmod(0o600)
    monkeypatch.setattr(mod, 'preflight', lambda c, i: {'evidence_exists': True})
    with pytest.raises(RuntimeError, match='already exists'):
        mod.restore('mysql', ID)


def test_package_avoids_unsafe_targets():
    body = SOURCE.read_text(encoding='utf-8')
    assert 'DROP DATABASE `openmetadata_db`' not in body
    assert 'DROP DATABASE "iceberg_rest56"' not in body
    assert 'MYSQL_ROOT_PASSWORD' in body
    assert 'SCRATCH' not in body.upper() or 'scratch' in body
    assert (ROOT / 'scripts/run_step76_restore.sh').exists()
