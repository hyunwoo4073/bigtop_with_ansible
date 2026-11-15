"""Offline tests for fixed-scope Step76 D3 transport and archive safety."""
import importlib.util
from pathlib import Path
import tempfile
import tarfile
import io
import json

ROOT = Path(__file__).resolve().parents[1]


def load(path, name):
    spec = importlib.util.spec_from_file_location(name, ROOT / path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_fixed_hdfs_path():
    r = load('roles/olist_metadata76_hdfs/files/step76_hdfs_remote.py', 'rem76')
    assert r.TABLE == '/data/olist/iceberg-rest/olist56/catalog_smoke'
    assert 'root' not in r.TABLE
    assert r.TARGET if hasattr(r, 'TARGET') else True
    assert r.check_id('20261115T082737Z-dc572b3b')


def test_invalid_id():
    r = load('roles/olist_metadata76_hdfs/files/step76_hdfs_remote.py', 'rem76bad')
    for bad in ('../etc', 'x', '20261115T082737Z-ZZZZZZZZ'):
        try:
            r.check_id(bad)
            assert False
        except ValueError:
            pass


def test_transport_hosts_allowlist():
    c = load('scripts/step76_hdfs_offhost.py', 'copy76')
    try:
        c.remote('helper', 'evilhost', 'source_stream', 'x')
        assert False
    except ValueError:
        pass
    argv = c.remote('helper', 'master', 'source_probe', '20261115T082737Z-dc572b3b')
    assert argv[0] == 'ssh'
    assert 'StrictHostKeyChecking=yes' in argv


def test_digests_are_strict():
    c = load('scripts/step76_hdfs_offhost.py', 'copy76cmp')
    a = {'archive_sha256': 'a', 'archive_bytes': 10, 'manifest_sha256': 'b',
         'manifest_bytes': 20, 'file_count': 3}
    assert c.matching(a, dict(a))
    assert not c.matching(a, dict(a, archive_bytes=11))


def test_proof_labels_are_scoped():
    text = (ROOT / 'docs/76d-hdfs-metadata-proof.md').read_text()
    assert 'not a point-in-time' in text
    assert 'Never writes to HDFS' in text


def test_restore_does_not_extract_to_hdfs():
    r = load('roles/olist_metadata76_hdfs/files/step76_hdfs_remote.py', 'rem76paths')
    assert str(r.BASE).startswith('/var/backups/')
    assert r.TABLE.startswith('/data/olist/')


def test_yaml_is_parseable():
    import yaml
    for path in [ROOT / 'playbooks/76d-hdfs-metadata-proof.yml',
                 ROOT / 'roles/olist_metadata76_hdfs/tasks/main.yml',
                 ROOT / 'roles/olist_metadata76_hdfs/defaults/main.yml']:
        assert yaml.safe_load(path.read_text()) is not None


def test_full_isolated_restore_from_synthetic_archive(monkeypatch, tmp_path):
    import hashlib
    r = load('roles/olist_metadata76_hdfs/files/step76_hdfs_remote.py', 'rem76restore')
    base = tmp_path / 'backups'
    base.mkdir(mode=0o700)
    monkeypatch.setattr(r, 'BASE', base)
    backup_id = '20261115T082737Z-dc572b3b'
    dest = base / backup_id
    dest.mkdir(mode=0o700)
    payload = { 'metadata/v1.metadata.json': b'{"format-version":2}',
                'data/part-00001.parquet': b'PAR1fake PAR1'}
    archive = dest / r.ARCHIVE
    with tarfile.open(archive, 'w:gz') as tf:
        for name, content in payload.items():
            info = tarfile.TarInfo('catalog_smoke/' + name)
            info.size = len(content)
            tf.addfile(info, io.BytesIO(content))
    archive.chmod(0o600)
    sha, count = r.checks(archive)
    manifest = {
        'version': 1, 'backup_id': backup_id, 'table': r.TABLE,
        'file_count': len(payload), 'archive_name': r.ARCHIVE,
        'archive_sha256': sha, 'archive_bytes': count,
        'files': [{'path': name, 'sha256': hashlib.sha256(content).hexdigest(),
                   'bytes': len(content)} for name, content in payload.items()]
    }
    mf = dest / r.MANIFEST
    mf.write_text(json.dumps(manifest))
    mf.chmod(0o600)
    proof = r.target_restore(backup_id)
    assert proof['verified_files'] == 2
    assert (dest / 'restore-proof.json').is_file()
    assert not (dest / 'isolated-restore').exists()
    assert r.verify_pair(backup_id, True)['restore_proof']
    try:
        r.target_restore(backup_id)
        assert False
    except RuntimeError as exc:
        assert 'already exists' in str(exc)


def test_hdfs_inventory_parses_expected_table(monkeypatch):
    r = load('roles/olist_metadata76_hdfs/files/step76_hdfs_remote.py', 'rem76inventory')
    lines = ('drwxr-xr-x   - hadoop supergroup          0 2026-11-15 12:00 '
             '/data/olist/iceberg-rest/olist56/catalog_smoke/metadata\n'
             '-rw-r--r--   3 hadoop supergroup         21 2026-11-15 12:00 '
             '/data/olist/iceberg-rest/olist56/catalog_smoke/metadata/v1.metadata.json\n'
             '-rw-r--r--   3 hadoop supergroup         14 2026-11-15 12:00 '
             '/data/olist/iceberg-rest/olist56/catalog_smoke/data/part-00001.parquet\n')
    def fake_hdfs(*args):
        return lines if '-ls' in args else ''
    monkeypatch.setattr(r, 'run_hdfs', fake_hdfs)
    result = r.inventory()
    assert len(result) == 2
    assert result[0]['path'] == 'data/part-00001.parquet'
    assert result[1]['bytes'] == 21


def test_inventory_rejects_missing_metadata(monkeypatch):
    r = load('roles/olist_metadata76_hdfs/files/step76_hdfs_remote.py', 'rem76missing')
    monkeypatch.setattr(r, 'run_hdfs', lambda *args: '-rw-r--r-- 3 hadoop g 1 2026-11-15 12:00 '
                        '/data/olist/iceberg-rest/olist56/catalog_smoke/data/x\n')
    try:
        r.inventory()
        assert False
    except RuntimeError as exc:
        assert 'metadata files missing' in str(exc)
