#!/usr/bin/env python3
"""Read-only Phase D inventory of protected dumps, HDFS scope, ES snapshots, and a DR target.

No secrets, dump contents, Docker environment, or Elasticsearch repository settings
are printed. Do not use online Docker-volume file copies as Elasticsearch backups.
"""
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import stat
import subprocess
import sys

BACKUP_ROOT = Path('/var/backups/bigtop-step76')
ID_RE = re.compile(r'^[0-9]{8}T[0-9]{6}Z-[a-f0-9]{8}$')
FILES = {'pg': ('iceberg_rest56', 'iceberg_rest56.pgcustom'),
         'mysql': ('openmetadata_db', 'openmetadata_db.sql')}


def call(argv, timeout=35, limit=8 * 1024 * 1024):
    try:
        p = subprocess.run(argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                           timeout=timeout, check=False)
    except (OSError, subprocess.TimeoutExpired) as e:
        raise RuntimeError('command unavailable or timeout: ' + argv[0]) from e
    if p.returncode:
        raise RuntimeError('command failed: ' + argv[0] + ' (status=' + str(p.returncode) + ')')
    if len(p.stdout) > limit:
        raise RuntimeError('command output exceeded inventory limit')
    return p.stdout.decode('utf-8', errors='replace').strip()


def check_private_directory(path):
    if path.is_symlink() or not path.is_dir():
        raise RuntimeError('protected backup directory absent or a symbolic link')
    s = path.stat()
    if s.st_uid != 0 or stat.S_IMODE(s.st_mode) != 0o700:
        raise RuntimeError('protected backup directory is not root-owned mode 0700')


def check_private_file(path):
    if path.is_symlink() or not path.is_file():
        raise RuntimeError('protected backup file absent or a symbolic link')
    s = path.stat()
    if s.st_uid != 0 or stat.S_IMODE(s.st_mode) != 0o600:
        raise RuntimeError('protected backup file is not root-owned mode 0600')


def sha256(path):
    digest = hashlib.sha256()
    total = 0
    with path.open('rb') as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b''):
            digest.update(chunk)
            total += len(chunk)
    return digest.hexdigest(), total


def source(component, backup_id):
    if component not in FILES or not ID_RE.fullmatch(backup_id):
        raise ValueError('invalid source/backup ID')
    database, filename = FILES[component]
    directory = BACKUP_ROOT / backup_id
    check_private_directory(BACKUP_ROOT)
    check_private_directory(directory)
    payload = directory / filename
    manifest = directory / (filename + '.manifest.json')
    check_private_file(payload)
    check_private_file(manifest)
    meta = json.loads(manifest.read_text(encoding='utf-8'))
    if (meta.get('backup_id') != backup_id or meta.get('component') != component
            or meta.get('database') != database or meta.get('archive_filename') != filename):
        raise RuntimeError('backup manifest identity mismatch')
    digest, size = sha256(payload)
    if digest != meta.get('sha256') or size != meta.get('bytes'):
        raise RuntimeError('backup manifest checksum mismatch')
    return {'ok': True, 'component': component, 'backup_id': backup_id,
            'database': database, 'bytes': size, 'sha256_verified': True,
            'storage': 'same_node_protected_dump_not_offhost'}


def docker_service(service):
    result = call(['docker', 'ps', '-q',
                   '--filter', 'label=com.docker.compose.project=bigtop-openmetadata',
                   '--filter', 'label=com.docker.compose.service=' + service])
    ids = result.splitlines()
    if len(ids) != 1 or not re.fullmatch(r'[a-f0-9]{12,64}', ids[0]):
        raise RuntimeError('expected one running OpenMetadata ' + service + ' container')
    return ids[0]


def es_request(cid, endpoint):
    raw = call(['docker', 'exec', cid, 'curl', '-fsS', '--max-time', '10',
                'http://127.0.0.1:9200' + endpoint], timeout=20)
    return json.loads(raw)


def elasticsearch():
    cid = docker_service('elasticsearch')
    health = es_request(cid, '/_cluster/health')
    indices = es_request(cid, '/_cat/indices?format=json&h=index,health,docs.count')
    repo = es_request(cid, '/_snapshot')
    nodes = es_request(cid, '/_nodes/settings?filter_path=nodes.*.settings.path.repo')
    configured_paths = []
    for node in nodes.get('nodes', {}).values():
        configured_paths.extend(node.get('settings', {}).get('path', {}).get('repo', []))
    repos = [{'name': name, 'type': value.get('type', 'unknown')}
             for name, value in repo.items()]
    es_status = health.get('status', 'unknown')
    return {'ok': True, 'component': 'openmetadata_elasticsearch',
            'cluster_health': es_status, 'index_count': len(indices),
            'snapshot_repository_count': len(repos), 'snapshot_repositories': repos,
            'filesystem_repo_paths_configured': len(configured_paths) > 0,
            'snapshot_ready': bool(repos) and es_status in ('green', 'yellow'),
            'note': 'No snapshot taken; live volume copies are not a valid online Elasticsearch backup.'}


def hadoop_cmd(*args, timeout=50):
    return call(['runuser', '-u', 'hadoop', '--', 'env',
                 'HADOOP_CONF_DIR=/etc/hadoop/conf', 'hdfs', 'dfs', *args], timeout=timeout)


def hdfs(warehouse):
    if warehouse != '/data/olist/iceberg-rest':
        raise ValueError('unexpected warehouse location')
    try:
        hadoop_cmd('-test', '-d', warehouse)
    except RuntimeError:
        return {'ok': True, 'component': 'iceberg_hdfs', 'warehouse': warehouse,
                'warehouse_exists': False, 'metadata_paths_found': 0,
                'scope_confirmed': False,
                'note': 'Catalog warehouse unavailable: confirm actual location before protection.'}
    usage = hadoop_cmd('-du', '-s', warehouse, timeout=60)
    parts = usage.split()
    if len(parts) < 3:
        raise RuntimeError('unexpected HDFS usage output')
    try:
        listing = hadoop_cmd('-find', warehouse, '-name', 'metadata', timeout=90)
        paths = sorted(set(line.strip() for line in listing.splitlines()
                           if line.strip().rstrip('/').endswith('/metadata')))
        discovery_ok = True
    except RuntimeError:
        paths = []
        discovery_ok = False
    return {'ok': True, 'component': 'iceberg_hdfs', 'warehouse': warehouse,
            'warehouse_exists': True, 'warehouse_logical_bytes': int(parts[0]),
            'metadata_paths_found': len(paths), 'metadata_path_samples': paths[:12],
            'metadata_discovery_ok': discovery_ok,
            'scope_confirmed': discovery_ok and bool(paths),
            'note': 'Metadata paths are discovery only; metadata JSON, manifests, and data must be backed up consistently.'}


def target(min_free_mib):
    if min_free_mib < 1024:
        raise ValueError('insufficient target free space limit')
    usage = shutil.disk_usage('/var')
    free_mib = usage.free // 1048576
    return {'ok': True, 'component': 'offhost_candidate',
            'free_mib': free_mib, 'required_free_mib': min_free_mib,
            'tools': {command: shutil.which(command) is not None
                      for command in ('tar', 'sha256sum', 'rsync', 'ssh')},
            'disk_threshold_pass': free_mib >= min_free_mib,
            'note': 'Read-only candidate check; no directory created and no backup copied.'}


def main(argv):
    if len(argv) != 4 and len(argv) != 3:
        print('Usage: step76_dr_probe.py source pg|mysql ID | elasticsearch inspect | hdfs PATH | target MIB',
              file=sys.stderr)
        return 2
    try:
        if argv[1] == 'source' and len(argv) == 4:
            output = source(argv[2], argv[3])
        elif argv[1] == 'elasticsearch' and len(argv) == 3 and argv[2] == 'inspect':
            output = elasticsearch()
        elif argv[1] == 'hdfs' and len(argv) == 3:
            output = hdfs(argv[2])
        elif argv[1] == 'target' and len(argv) == 3:
            output = target(int(argv[2]))
        else:
            raise ValueError('unsupported read-only mode')
        print(json.dumps(output, sort_keys=True, ensure_ascii=False))
        return 0
    except (RuntimeError, ValueError, OSError, json.JSONDecodeError) as e:
        print(json.dumps({'ok': False, 'mode': argv[1] if len(argv) > 1 else '?',
                          'error': str(e)}, ensure_ascii=False))
        return 1


if __name__ == '__main__':
    sys.exit(main(sys.argv))
