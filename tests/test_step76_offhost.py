"""Step76 D2 security, exact identity, checksum, and no-overwrite tests."""
import hashlib
import importlib.util
import json
from pathlib import Path
import os
import pytest

ROOT = Path(__file__).resolve().parents[1]
HELPER = ROOT / 'roles/olist_metadata76_offhost/files/step76_offhost_remote.py'
CONTROLLER = ROOT / 'scripts/step76d_offhost_copy.py'


def load(p, name):
    spec = importlib.util.spec_from_file_location(name, p)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


remote = load(HELPER, 'step76_remote')
runner = load(CONTROLLER, 'step76_runner')
BACKUP_ID = '20261115T082737Z-dc572b3b'


def test_strict_backup_ids():
    assert remote.must_id(BACKUP_ID) == BACKUP_ID
    for bad in ('../etc', '', '20261115_bad', '20261115T082737Z-zzzzzzzz'):
        with pytest.raises(ValueError):
            remote.must_id(bad)


def test_fixed_components_and_destinations():
    assert remote.must_component('pg') == 'pg'
    with pytest.raises(ValueError):
        remote.must_component('webconsole')
    with pytest.raises(ValueError):
        runner.ssh_argv('evil-host', 'x')
    with pytest.raises(ValueError):
        remote.payload_path(remote.TARGET_ROOT, BACKUP_ID, 'pg', '../../etc')


def test_metadata_root_format_and_sha(tmp_path, monkeypatch):
    monkeypatch.setattr(remote, 'SOURCE_ROOT', tmp_path)
    tmp_path.chmod(0o700)
    directory = tmp_path / BACKUP_ID
    directory.mkdir(mode=0o700)
    archive = directory / 'iceberg_rest56.pgcustom'
    archive.write_bytes(b'PGDMP' + b'a' * 100)
    archive.chmod(0o600)
    manifest = directory / 'iceberg_rest56.pgcustom.manifest.json'
    digest = hashlib.sha256(archive.read_bytes()).hexdigest()
    manifest.write_text(json.dumps({'backup_id': BACKUP_ID, 'component': 'pg',
                                    'database': 'iceberg_rest56',
                                    'archive_filename': archive.name,
                                    'sha256': digest, 'bytes': archive.stat().st_size}))
    manifest.chmod(0o600)
    report = remote.verify_one(tmp_path, BACKUP_ID, 'pg')
    assert report['archive_sha256'] == digest
    assert report['archive_bytes'] == 105
    archive.write_bytes(b'changed')
    with pytest.raises(RuntimeError, match='checksum'):
        remote.verify_one(tmp_path, BACKUP_ID, 'pg')


def test_symlink_rejected(tmp_path):
    file = tmp_path / 'data'
    file.write_bytes(b'hello')
    link = tmp_path / 'alias'
    link.symlink_to(file)
    with pytest.raises(RuntimeError, match='symbolic'):
        remote.private(link, False)


def test_controller_ssh_defaults():
    local = runner.ssh_argv('worker3', 'hello', 'source_check', BACKUP_ID, 'pg')
    assert local[:3] == ['sudo', '-n', 'python3']
    remote_cmd = runner.ssh_argv('metadata', 'hello', 'source_check', BACKUP_ID, 'mysql')
    assert '-o' in remote_cmd and 'StrictHostKeyChecking=yes' in remote_cmd
    assert remote_cmd[-2] == 'metadata'


def test_match_only_identical_source_dest():
    report = {'component': 'pg', 'archive_sha256': 'a'}
    a = {'pg': report, 'mysql': {'component': 'mysql', 'archive_sha256': 'b'}}
    assert runner.equal_snapshots(a, dict(a))
    assert not runner.equal_snapshots(a, {'pg': report, 'mysql': {}})


def test_copy_requires_optin(monkeypatch):
    monkeypatch.delenv('STEP76_ALLOW_OFFHOST_COPY', raising=False)
    monkeypatch.setattr(runner, 'read_sources', lambda *a: {'pg': {}, 'mysql': {}})
    monkeypatch.setattr(runner, 'run_json', lambda *a, **kw: {'disk_threshold_pass': True,
                                                               'backup_dir_exists': False})
    with pytest.raises(RuntimeError, match='STEP76_ALLOW_OFFHOST_COPY'):
        runner.orchestrate('copy', BACKUP_ID, 'worker1', 'helper')


def test_preflight_does_not_mutate(monkeypatch):
    monkeypatch.setattr(runner, 'read_sources', lambda *a: {'pg': {}, 'mysql': {}})
    monkeypatch.setattr(runner, 'run_json', lambda *a, **kw: {'disk_threshold_pass': True,
                                                               'backup_dir_exists': False,
                                                               'free_mib': 30000})
    result = runner.orchestrate('preflight', BACKUP_ID, 'worker1', 'helper')
    assert result['ready_for_copy'] and result['result'] == 'STEP76_OFFHOST_PREFLIGHT_PASS'


def test_offhost_stream_verify_and_seal(tmp_path, monkeypatch):
    import io
    import types
    remote_root = tmp_path / 'offhost'
    monkeypatch.setattr(remote, 'TARGET_ROOT', remote_root)
    # target root creation is always within a protected root-owned local test dir.
    report = remote.init_target(BACKUP_ID)
    assert report['target_initialized']
    for component, (database, filename) in remote.FILES.items():
        data = (b'PGDMP' if component == 'pg' else b'-- MySQL dump\n') + b'q' * 2048
        digest = hashlib.sha256(data).hexdigest()
        manifest = json.dumps({'backup_id': BACKUP_ID, 'component': component,
                               'database': database, 'archive_filename': filename,
                               'sha256': digest, 'bytes': len(data)}).encode()
        for kind, payload in [('archive', data), ('manifest', manifest)]:
            monkeypatch.setattr(remote.sys, 'stdin', types.SimpleNamespace(buffer=io.BytesIO(payload)))
            sha = hashlib.sha256(payload).hexdigest()
            result = remote.target_receive(BACKUP_ID, component, kind, sha, len(payload))
            assert result['bytes'] == len(payload)
            with pytest.raises(RuntimeError, match='refusing overwrite'):
                remote.target_receive(BACKUP_ID, component, kind, sha, len(payload))
    before = remote.target_verify(BACKUP_ID)
    assert before['sealed'] is False
    after = remote.seal(BACKUP_ID)
    assert after['sealed'] is True
    assert remote.target_verify(BACKUP_ID) == after
    with pytest.raises(RuntimeError, match='already sealed'):
        remote.seal(BACKUP_ID)
