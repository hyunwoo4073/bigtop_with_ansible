"""Offline unit tests: do not connect to Kafka, HDFS, Docker, or PostgreSQL."""
import importlib.util
import json
from pathlib import Path
import stat
import subprocess
from unittest.mock import patch

ROOT = Path(__file__).parents[1]
SCRIPT = ROOT / 'roles/olist_metadata76_backup/files/step76_logical_backup.py'
spec = importlib.util.spec_from_file_location('backup76', SCRIPT)
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)


def test_backup_ids_have_strict_shape():
    assert mod.ID_RE.fullmatch('20261115T084500Z-1234abcd')
    for value in ('../x', '2026', '20261115T084500Z-1234abcd/other', 'foo'):
        assert not mod.ID_RE.fullmatch(value)


def test_sources_fixed():
    assert mod.DATABASES == {'pg': 'iceberg_rest56', 'mysql': 'openmetadata_db'}
    assert mod.FILES['pg'].endswith('.pgcustom')


def test_bad_database_is_rejected_before_io(capsys):
    assert mod.main(['script', 'backup', 'pg', '20261115T084500Z-1234abcd', 'postgres']) == 2
    assert 'Invalid' in capsys.readouterr().err


def test_usage_error(capsys):
    assert mod.main(['script', 'verify', 'mysql']) == 2
    assert 'Usage' in capsys.readouterr().err


def test_sha_file(tmp_path):
    f = tmp_path / 'test.dat'
    f.write_bytes(b'abc' * 100)
    digest, length = mod.sha_file(f)
    assert length == 300 and len(digest) == 64


def test_pg_format_header_validation(tmp_path):
    f = tmp_path / 'x.pgcustom'
    f.write_bytes(b'PGDMP' + b'0' * 200)
    with patch.object(mod, 'run', return_value='1234; 0 0 TABLE public some_table owner'):
        assert mod.validate_dump('pg', f) == 'pg_restore_list_pass'
    f.write_bytes(b'notpg')
    try:
        mod.validate_dump('pg', f)
        raise AssertionError('missing PG header accepted')
    except RuntimeError:
        pass


def test_mysql_trailer_required(tmp_path):
    f = tmp_path / 'x.sql'
    f.write_bytes(b'-- MySQL dump\n' + b'-' * 100 + b'\n-- Dump completed on 2026-11-15\n')
    assert mod.validate_dump('mysql', f) == 'mysql_dump_header_footer_pass'
    f.write_bytes(b'-- MySQL dump\n' + b'x' * 100)
    try:
        mod.validate_dump('mysql', f)
        raise AssertionError('missing trailer accepted')
    except RuntimeError:
        pass


def test_directory_and_manifest_validation(tmp_path):
    root = tmp_path / 'backups'
    with patch.object(mod, 'ROOT', root):
        identifier = '20261115T084500Z-1234abcd'
        path = mod.safe_directory(identifier, create=True)
        assert path.is_dir() and stat.S_IMODE(path.stat().st_mode) == 0o700
        manifest = path / 'manifest.json'
        mod.atomic_json(manifest, {'hello': 'world'})
        assert json.loads(manifest.read_text())['hello'] == 'world'
        assert stat.S_IMODE(manifest.stat().st_mode) == 0o600


def test_mysql_command_never_embeds_password():
    with patch.object(mod, 'docker_mysql_id', return_value='example-container'):
        cmd = mod.command_for('mysql')
        assert cmd[:3] == ['docker', 'exec', 'example-container']
        assert 'MYSQL_PWD="$MYSQL_PASSWORD"' in cmd[-1]
        assert '--single-transaction' in cmd[-1]
        assert '--no-tablespaces' in cmd[-1]


def test_shell_script_valid():
    subprocess.run(['bash', '-n', str(ROOT / 'scripts/run_step76_backup.sh')], check=True)


def test_yaml_loads():
    import yaml
    for name in ['playbooks/76b-metadata-logical-backup.yml',
                 'roles/olist_metadata76_backup/tasks/main.yml',
                 'roles/olist_metadata76_backup/defaults/main.yml']:
        assert yaml.safe_load((ROOT / name).read_text()) is not None
