#!/usr/bin/env python3
"""Root-only, fixed-scope helper invoked through SSH from worker3.

Copies only an existing verified Step76 backup set, never databases or services.
No secret material is printed. All backups are protected mode 0700/0600.
"""
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import stat
import sys

SOURCE_ROOT = Path('/var/backups/bigtop-step76')
TARGET_ROOT = Path('/var/backups/bigtop-step76-offhost')
ID_RE = re.compile(r'^[0-9]{8}T[0-9]{6}Z-[a-f0-9]{8}$')
FILES = {'pg': ('iceberg_rest56', 'iceberg_rest56.pgcustom'),
         'mysql': ('openmetadata_db', 'openmetadata_db.sql')}
MIN_FREE_BYTES = 2 * 1024 * 1024 * 1024


def must_id(backup_id):
    if not ID_RE.fullmatch(backup_id):
        raise ValueError('invalid backup id')
    return backup_id


def must_component(component):
    if component not in FILES:
        raise ValueError('invalid component')
    return component


def private(path, is_dir):
    if path.is_symlink() or not (path.is_dir() if is_dir else path.is_file()):
        raise RuntimeError('missing protected path or symbolic link: ' + path.name)
    info = path.stat()
    if info.st_uid != 0 or stat.S_IMODE(info.st_mode) != (0o700 if is_dir else 0o600):
        raise RuntimeError('unsafe ownership or mode: ' + path.name)


def checksum(path):
    h = hashlib.sha256()
    n = 0
    with path.open('rb') as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b''):
            h.update(chunk)
            n += len(chunk)
    return h.hexdigest(), n


def paths(root, backup_id, component):
    must_id(backup_id)
    must_component(component)
    directory = root / backup_id
    filename = FILES[component][1]
    return directory, directory / filename, directory / (filename + '.manifest.json')


def verify_one(root, backup_id, component):
    directory, archive, manifest = paths(root, backup_id, component)
    private(root, True)
    private(directory, True)
    private(archive, False)
    private(manifest, False)
    data = json.loads(manifest.read_text(encoding='utf-8'))
    if (data.get('backup_id') != backup_id or
            data.get('component') != component or
            data.get('database') != FILES[component][0] or
            data.get('archive_filename') != archive.name):
        raise RuntimeError('manifest identity mismatch')
    digest, size = checksum(archive)
    if data.get('sha256') != digest or data.get('bytes') != size:
        raise RuntimeError('archive checksum/size mismatch')
    manifest_sha, manifest_bytes = checksum(manifest)
    return {'component': component, 'archive_sha256': digest, 'archive_bytes': size,
            'manifest_sha256': manifest_sha, 'manifest_bytes': manifest_bytes,
            'database': FILES[component][0]}


def payload_path(root, backup_id, component, kind):
    if kind not in ('archive', 'manifest'):
        raise ValueError('invalid kind')
    _, archive, manifest = paths(root, backup_id, component)
    return archive if kind == 'archive' else manifest


def source_stream(backup_id, component, kind):
    verify_one(SOURCE_ROOT, backup_id, component)
    path = payload_path(SOURCE_ROOT, backup_id, component, kind)
    with path.open('rb') as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b''):
            sys.stdout.buffer.write(chunk)
    sys.stdout.buffer.flush()


def target_probe(backup_id):
    must_id(backup_id)
    free = shutil.disk_usage('/var').free
    directory = TARGET_ROOT / backup_id
    return {'ok': True, 'free_mib': free // 1048576,
            'disk_threshold_pass': free >= MIN_FREE_BYTES,
            'backup_dir_exists': directory.exists() or directory.is_symlink(),
            'target_sealed': (directory / 'offhost-proof.json').exists()}


def create_private_dir(path):
    if path.is_symlink():
        raise RuntimeError('refuse symbolic link directory')
    if not path.exists():
        path.mkdir(mode=0o700)
    private(path, True)


def init_target(backup_id):
    must_id(backup_id)
    if shutil.disk_usage('/var').free < MIN_FREE_BYTES:
        raise RuntimeError('target needs at least 2048 MiB available')
    # /var/backups is a pre-existing system location. We only create our own root.
    if not TARGET_ROOT.parent.is_dir() or TARGET_ROOT.parent.is_symlink():
        raise RuntimeError('backup parent path invalid')
    create_private_dir(TARGET_ROOT)
    directory = TARGET_ROOT / backup_id
    if directory.exists() or directory.is_symlink():
        raise RuntimeError('backup ID already exists on target: refusing overwrite')
    directory.mkdir(mode=0o700)
    private(directory, True)
    return {'ok': True, 'target_initialized': True}


def target_receive(backup_id, component, kind, expected_sha, expected_size):
    must_id(backup_id)
    must_component(component)
    if not re.fullmatch(r'[a-f0-9]{64}', expected_sha):
        raise ValueError('invalid expected digest')
    expected_size = int(expected_size)
    if not 1 <= expected_size <= 5 * 1024 ** 3:
        raise ValueError('invalid expected length')
    directory = TARGET_ROOT / backup_id
    private(TARGET_ROOT, True)
    private(directory, True)
    final = payload_path(TARGET_ROOT, backup_id, component, kind)
    partial = final.with_name(final.name + '.partial')
    if final.exists() or final.is_symlink() or partial.exists() or partial.is_symlink():
        raise RuntimeError('target file exists; refusing overwrite')
    fd = os.open(str(partial), os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW, 0o600)
    digest = hashlib.sha256()
    size = 0
    try:
        with os.fdopen(fd, 'wb') as f:
            for chunk in iter(lambda: sys.stdin.buffer.read(1024 * 1024), b''):
                size += len(chunk)
                if size > expected_size:
                    raise RuntimeError('stream exceeds expected size')
                digest.update(chunk)
                f.write(chunk)
            f.flush()
            os.fsync(f.fileno())
        if size != expected_size or digest.hexdigest() != expected_sha:
            raise RuntimeError('stream digest or length mismatch')
        # Atomic create-if-absent. os.replace would risk overwriting a concurrent file.
        os.link(partial, final, follow_symlinks=False)
        partial.unlink()
        dirfd = os.open(str(directory), os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(dirfd)
        finally:
            os.close(dirfd)
        return {'ok': True, 'component': component, 'kind': kind, 'bytes': size}
    finally:
        partial.unlink(missing_ok=True)


def target_verify(backup_id):
    result = {component: verify_one(TARGET_ROOT, backup_id, component)
              for component in ('pg', 'mysql')}
    proof = TARGET_ROOT / backup_id / 'offhost-proof.json'
    if proof.exists():
        private(proof, False)
        data = json.loads(proof.read_text(encoding='utf-8'))
        if data.get('backup_id') != backup_id or data.get('components') != result:
            raise RuntimeError('offhost proof mismatch')
        sealed = True
    else:
        sealed = False
    return {'ok': True, 'backup_id': backup_id, 'components': result, 'sealed': sealed}


def seal(backup_id):
    data = target_verify(backup_id)
    if data['sealed']:
        raise RuntimeError('offhost backup already sealed')
    directory = TARGET_ROOT / backup_id
    proof = directory / 'offhost-proof.json'
    doc = {'backup_id': backup_id, 'components': data['components'],
           'sealed_utc': dt.datetime.now(dt.timezone.utc).isoformat(),
           'scope': 'logical_postgresql_mysql_only_no_hdfs_elasticsearch'}
    fd = os.open(proof, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, 'w', encoding='utf-8') as f:
        json.dump(doc, f, sort_keys=True, indent=2)
        f.write('\n')
        f.flush()
        os.fsync(f.fileno())
    return target_verify(backup_id)


def main(argv):
    os.umask(0o077)
    if os.geteuid() != 0:
        raise RuntimeError('root required; use noninteractive sudo')
    if len(argv) < 3:
        raise ValueError('expected action and backup ID')
    action, backup_id, *args = argv[1:]
    must_id(backup_id)
    if action == 'source_check' and len(args) == 1:
        result = verify_one(SOURCE_ROOT, backup_id, args[0])
    elif action == 'source_stream' and len(args) == 2:
        source_stream(backup_id, *args)
        return 0
    elif action == 'target_probe' and not args:
        result = target_probe(backup_id)
    elif action == 'target_init' and not args:
        result = init_target(backup_id)
    elif action == 'target_receive' and len(args) == 4:
        result = target_receive(backup_id, *args)
    elif action == 'target_verify' and not args:
        result = target_verify(backup_id)
    elif action == 'target_seal' and not args:
        result = seal(backup_id)
    else:
        raise ValueError('unknown mode/arguments')
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == '__main__':
    try:
        sys.exit(main(sys.argv))
    except (ValueError, RuntimeError, OSError, json.JSONDecodeError, KeyError) as exc:
        # Never include SQL or credentials in error messages.
        print('STEP76_OFFHOST_REMOTE_ERROR: ' + str(exc), file=sys.stderr)
        sys.exit(1)
