"""Pure local unit tests for the Step76 Phase D safe inventory implementation."""
import importlib.util
from pathlib import Path
import tempfile
from unittest import mock

MODULE = Path(__file__).resolve().parents[1] / 'roles/olist_metadata76_dr/files/step76_dr_probe.py'
spec = importlib.util.spec_from_file_location('step76_dr_probe', MODULE)
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)


def test_invalid_backup_id_rejected():
    try:
        mod.source('pg', '../../etc/passwd')
    except ValueError:
        pass
    else:
        raise AssertionError('path traversal accepted')


def test_unexpected_hdfs_path_rejected():
    try:
        mod.hdfs('/some/other/path')
    except ValueError:
        pass
    else:
        raise AssertionError('unexpected warehouse accepted')


def test_target_minimum_rejected():
    try:
        mod.target(500)
    except ValueError:
        pass
    else:
        raise AssertionError('too small free-space requirement accepted')


def test_source_checksum_and_identity():
    with tempfile.TemporaryDirectory() as root:
        backup_id = '20261115T000000Z-1234abcd'
        import hashlib
        import json
        base = Path(root)
        dumpdir = base / backup_id
        dumpdir.mkdir(mode=0o700)
        payload = dumpdir / 'iceberg_rest56.pgcustom'
        payload.write_bytes(b'PGDMP' + b'abc' * 100)
        payload.chmod(0o600)
        digest = hashlib.sha256(payload.read_bytes()).hexdigest()
        mf = dumpdir / 'iceberg_rest56.pgcustom.manifest.json'
        mf.write_text(json.dumps({'backup_id': backup_id, 'component': 'pg',
                                  'database': 'iceberg_rest56',
                                  'archive_filename': 'iceberg_rest56.pgcustom',
                                  'sha256': digest, 'bytes': payload.stat().st_size}))
        mf.chmod(0o600)
        with mock.patch.object(mod, 'BACKUP_ROOT', base), mock.patch.object(
                mod, 'check_private_directory'), mock.patch.object(mod, 'check_private_file'):
            report = mod.source('pg', backup_id)
        assert report['sha256_verified'] is True
        assert report['bytes'] == len(payload.read_bytes())


def test_es_inventory_snapshot_not_falsely_claimed():
    with mock.patch.object(mod, 'docker_service', return_value='f' * 12), mock.patch.object(
            mod, 'es_request', side_effect=[
                {'status': 'yellow'}, [{'index': 'some-index'}], {}, {'nodes': {'id': {'settings': {}}}}]):
        report = mod.elasticsearch()
    assert report['ok']
    assert not report['snapshot_ready']
    assert report['snapshot_repository_count'] == 0


def test_hdfs_missing_does_not_claim_protection():
    with mock.patch.object(mod, 'hadoop_cmd', side_effect=RuntimeError('test failed')):
        report = mod.hdfs('/data/olist/iceberg-rest')
    assert report['ok'] and not report['scope_confirmed']


def test_invalid_command_return():
    assert mod.main(['file', 'source', 'pg', 'bad']) == 1
    assert mod.main(['file', 'unknown', 'x']) == 1
