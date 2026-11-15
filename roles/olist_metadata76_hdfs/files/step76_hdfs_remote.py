#!/usr/bin/env python3
"""Root-invoked, fixed-scope HDFS table archive and isolated file restore helper.

D3 proof covers the *one* immutable lab smoke table. It never writes to HDFS.
"""
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import pwd
import re
import shutil
import stat
import subprocess
import sys
import tarfile
import tempfile

ID_RE = re.compile(r'^[0-9]{8}T[0-9]{6}Z-[a-f0-9]{8}$')
TABLE = '/data/olist/iceberg-rest/olist56/catalog_smoke'
TABLE_NAME = 'catalog_smoke'
BASE = Path('/var/backups/bigtop-step76-hdfs')
ARCHIVE = 'catalog_smoke.tar.gz'
MANIFEST = 'catalog_smoke.manifest.json'
MIN_FREE = 512 * 1024 * 1024
MAX_ARCHIVE = 1024 * 1024 * 1024


def check_id(value):
    if not ID_RE.fullmatch(value):
        raise ValueError('invalid backup identity')
    return value


def checks(path):
    sha = hashlib.sha256()
    n = 0
    with path.open('rb') as f:
        for block in iter(lambda: f.read(1024 * 1024), b''):
            sha.update(block)
            n += len(block)
    return sha.hexdigest(), n


def protected(path, directory=False):
    if path.is_symlink() or not (path.is_dir() if directory else path.is_file()):
        raise RuntimeError('protected path not found: ' + path.name)
    s = path.stat()
    if s.st_uid != 0 or stat.S_IMODE(s.st_mode) != (0o700 if directory else 0o600):
        raise RuntimeError('protected path has unexpected owner/mode: ' + path.name)


def root_path(backup_id):
    return BASE / check_id(backup_id)


def run_hdfs(*args):
    result = subprocess.run(['runuser', '-u', 'hadoop', '--', 'env',
                             'HADOOP_CONF_DIR=/etc/hadoop/conf', 'hdfs', 'dfs', *args],
                            capture_output=True, text=True, timeout=180, check=False)
    if result.returncode:
        raise RuntimeError('HDFS access failed; review Hadoop service and user permissions: '
                           + result.stderr.strip()[-350:])
    return result.stdout


def inventory():
    run_hdfs('-test', '-d', TABLE + '/metadata')
    listing = run_hdfs('-ls', '-R', TABLE)
    files = []
    for line in listing.splitlines():
        if not line or line.startswith('Found '):
            continue
        parts = line.split(maxsplit=7)
        if len(parts) != 8:
            raise RuntimeError('unexpected HDFS directory listing')
        mode, _, _, _, size, date, tm, path = parts
        if not (path == TABLE or path.startswith(TABLE + '/')):
            raise RuntimeError('HDFS listing escaped table scope')
        if mode.startswith('l'):
            raise RuntimeError('HDFS symbolic link unsupported')
        if mode.startswith('-'):
            files.append({'path': path[len(TABLE):].lstrip('/'),
                          'bytes': int(size), 'date': date, 'time': tm})
        elif not mode.startswith('d'):
            raise RuntimeError('unexpected HDFS file type')
    if not files or not any(x['path'].startswith('metadata/') for x in files):
        raise RuntimeError('table metadata files missing')
    return sorted(files, key=lambda x: x['path'])


def preflight(backup_id):
    files = inventory()
    free = shutil.disk_usage('/var').free
    size = sum(f['bytes'] for f in files)
    dest = root_path(backup_id)
    return {'ok': True, 'backup_id': backup_id, 'table': TABLE,
            'hdfs_file_count': len(files), 'logical_bytes': size,
            'source_free_mib': free // (1024 * 1024),
            'ready_for_capture': free >= MIN_FREE and size <= MAX_ARCHIVE
                                 and not (dest.exists() or dest.is_symlink()),
            'capture_exists': dest.exists() or dest.is_symlink()}


def create_dir(path):
    if path.exists() or path.is_symlink():
        protected(path, True)
    else:
        path.mkdir(mode=0o700)
        protected(path, True)


def source_capture(backup_id):
    report = preflight(backup_id)
    if not report['ready_for_capture']:
        raise RuntimeError('capture blocked: target exists, insufficient free disk or excessive table size')
    if not BASE.parent.is_dir() or BASE.parent.is_symlink():
        raise RuntimeError('invalid backup parent')
    create_dir(BASE)
    dest = root_path(backup_id)
    dest.mkdir(mode=0o700)
    parent = Path(tempfile.mkdtemp(prefix='step76-hdfs-', dir='/tmp'))
    try:
        # Hadoop CLI runs as hadoop only inside its own isolated temporary directory.
        hadoop_uid = pwd.getpwnam('hadoop').pw_uid
        hadoop_gid = pwd.getpwnam('hadoop').pw_gid
        os.chown(parent, hadoop_uid, hadoop_gid)
        os.chmod(parent, 0o700)
        before = inventory()
        run_hdfs('-get', TABLE, str(parent))
        after = inventory()
        if before != after:
            raise RuntimeError('HDFS table changed while archiving; do not use capture')
        copied = parent / TABLE_NAME
        if not copied.is_dir() or not (copied / 'metadata').is_dir():
            raise RuntimeError('HDFS copied table incomplete')
        local = []
        for f in before:
            file = copied / f['path']
            if file.is_symlink() or not file.is_file() or file.stat().st_size != f['bytes']:
                raise RuntimeError('HDFS copied file mismatch: ' + f['path'])
            sha, size = checks(file)
            local.append({'path': f['path'], 'sha256': sha, 'bytes': size})
        arc = dest / ARCHIVE
        # Archive only the HDFS-inventoried files. Hadoop's local FS may also
        # create local checksum sidecars, which must never enter this snapshot.
        with tarfile.open(arc, 'w:gz', format=tarfile.PAX_FORMAT) as tf:
            for entry in local:
                file = copied / entry['path']
                info = tarfile.TarInfo(TABLE_NAME + '/' + entry['path'])
                info.size = entry['bytes']
                info.mode = 0o600
                info.mtime = 0
                with file.open('rb') as source_file:
                    tf.addfile(info, source_file)
        os.chown(arc, 0, 0)
        os.chmod(arc, 0o600)
        sha, size = checks(arc)
        if size > MAX_ARCHIVE:
            raise RuntimeError('archive exceeds 1GiB ceiling')
        manifest = {'version': 1, 'backup_id': backup_id, 'table': TABLE,
                    'file_count': len(local), 'files': local, 'archive_name': ARCHIVE,
                    'archive_sha256': sha, 'archive_bytes': size,
                    'consistency': 'HDFS pre/post inventory equality only; not an atomic HDFS snapshot'}
        mf = dest / MANIFEST
        mf.write_text(json.dumps(manifest, sort_keys=True, indent=2) + '\n', encoding='utf-8')
        os.chmod(mf, 0o600)
        return {'ok': True, 'backup_id': backup_id, 'table': TABLE,
                'file_count': len(local), 'archive_sha256': sha, 'archive_bytes': size}
    finally:
        shutil.rmtree(parent, ignore_errors=True)


def verify_pair(backup_id, on_target):
    protected(BASE, True)
    dest = root_path(backup_id)
    protected(dest, True)
    arc, mf = dest / ARCHIVE, dest / MANIFEST
    protected(arc)
    protected(mf)
    m = json.loads(mf.read_text(encoding='utf-8'))
    sha, size = checks(arc)
    if (m.get('backup_id') != backup_id or m.get('table') != TABLE or
            m.get('archive_name') != ARCHIVE or m.get('archive_sha256') != sha or
            m.get('archive_bytes') != size or m.get('file_count') != len(m.get('files', [])) or
            not m.get('files')):
        raise RuntimeError('archive/manifest identity or checksum mismatch')
    proof = dest / 'restore-proof.json'
    return {'ok': True, 'backup_id': backup_id, 'table': TABLE,
            'archive_sha256': sha, 'archive_bytes': size,
            'manifest_sha256': checks(mf)[0], 'manifest_bytes': mf.stat().st_size,
            'file_count': m['file_count'], 'restore_proof': proof.exists() if on_target else False}


def source_stream(backup_id, kind):
    verify_pair(backup_id, False)
    path = root_path(backup_id) / {'archive': ARCHIVE, 'manifest': MANIFEST}[kind]
    with path.open('rb') as f:
        while True:
            b = f.read(1024 * 1024)
            if not b:
                break
            sys.stdout.buffer.write(b)
    sys.stdout.buffer.flush()


def target_probe(backup_id):
    dest = root_path(backup_id)
    free = shutil.disk_usage('/var').free
    return {'ok': True, 'target_free_mib': free // (1024 * 1024),
            'ready_for_copy': free >= 2 * 1024 ** 3 and not (dest.exists() or dest.is_symlink()),
            'target_exists': dest.exists() or dest.is_symlink()}


def target_init(backup_id):
    if not target_probe(backup_id)['ready_for_copy']:
        raise RuntimeError('target exists or disk too small; never overwrite')
    create_dir(BASE)
    dest = root_path(backup_id)
    dest.mkdir(mode=0o700)
    protected(dest, True)
    return {'ok': True}


def target_receive(backup_id, kind, expected_sha, expected_bytes):
    dest = root_path(backup_id)
    protected(BASE, True)
    protected(dest, True)
    expected_bytes = int(expected_bytes)
    if kind not in ('archive', 'manifest') or not re.fullmatch('[a-f0-9]{64}', expected_sha):
        raise ValueError('invalid stream identity')
    if not 1 <= expected_bytes <= MAX_ARCHIVE:
        raise ValueError('invalid stream length')
    final = dest / {'archive': ARCHIVE, 'manifest': MANIFEST}[kind]
    partial = Path(str(final) + '.partial')
    if any(x.exists() or x.is_symlink() for x in (final, partial)):
        raise RuntimeError('target file already exists')
    fd = os.open(partial, os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW, 0o600)
    sha = hashlib.sha256()
    n = 0
    try:
        with os.fdopen(fd, 'wb') as f:
            for block in iter(lambda: sys.stdin.buffer.read(1024 * 1024), b''):
                n += len(block)
                if n > expected_bytes:
                    raise RuntimeError('stream exceeded expected bytes')
                sha.update(block)
                f.write(block)
            f.flush()
            os.fsync(f.fileno())
        if n != expected_bytes or sha.hexdigest() != expected_sha:
            raise RuntimeError('offhost checksum mismatch')
        os.link(partial, final, follow_symlinks=False)
        partial.unlink()
        return {'ok': True, 'bytes': n, 'sha256': sha.hexdigest()}
    finally:
        partial.unlink(missing_ok=True)


def target_restore(backup_id):
    info = verify_pair(backup_id, True)
    dest = root_path(backup_id)
    proof_path = dest / 'restore-proof.json'
    if proof_path.exists() or proof_path.is_symlink():
        raise RuntimeError('restore proof already exists; do not repeat')
    manifest = json.loads((dest / MANIFEST).read_text(encoding='utf-8'))
    # The scratch directory only exists on worker1 and never maps to a live HDFS path.
    scratch = dest / 'isolated-restore'
    if scratch.exists() or scratch.is_symlink():
        raise RuntimeError('scratch directory already exists; inspect before retry')
    scratch.mkdir(mode=0o700)
    with tarfile.open(dest / ARCHIVE, 'r:gz') as tf:
        members = tf.getmembers()
        for m in members:
            path = PurePosixPath(m.name)
            if (path.is_absolute() or '..' in path.parts or not path.parts or
                    path.parts[0] != TABLE_NAME or not (m.isdir() or m.isfile())):
                raise RuntimeError('archive contains unsafe member')
        for m in members:
            rel = PurePosixPath(m.name)
            target = scratch.joinpath(*rel.parts)
            if m.isdir():
                target.mkdir(parents=True, exist_ok=True, mode=0o700)
            else:
                target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
                source = tf.extractfile(m)
                if source is None:
                    raise RuntimeError('unreadable archive file')
                with target.open('xb') as out:
                    shutil.copyfileobj(source, out, 1024 * 1024)
                target.chmod(0o600)
    live = scratch / TABLE_NAME
    expected = {x['path']: (x['sha256'], x['bytes']) for x in manifest['files']}
    actual = {}
    for path in live.rglob('*'):
        if path.is_symlink():
            raise RuntimeError('restored symlink unsupported')
        if path.is_file():
            actual[path.relative_to(live).as_posix()] = checks(path)
    if {k: (v[0], v[1]) for k, v in actual.items()} != expected:
        raise RuntimeError('restore proof failed: file hashes or file set differ')
    # Proof remains, scratch files cleaned; neither HDFS nor production DB is touched.
    proof = {'ok': True, 'backup_id': backup_id, 'verified_files': len(expected),
             'restore_mode': 'isolated_worker1_filesystem_only',
             'archive_sha256': info['archive_sha256']}
    shutil.rmtree(scratch)
    proof_path.write_text(json.dumps(proof, sort_keys=True, indent=2) + '\n', encoding='utf-8')
    os.chmod(proof_path, 0o600)
    return proof


def main(argv):
    if len(argv) < 3:
        raise ValueError('required: mode backup_id [kind sha size]')
    mode, backup_id = argv[1:3]
    check_id(backup_id)
    if mode == 'source_probe' and len(argv) == 3:
        result = preflight(backup_id)
    elif mode == 'source_capture' and len(argv) == 3:
        result = source_capture(backup_id)
    elif mode == 'source_check' and len(argv) == 3:
        result = verify_pair(backup_id, False)
    elif mode == 'target_probe' and len(argv) == 3:
        result = target_probe(backup_id)
    elif mode == 'target_init' and len(argv) == 3:
        result = target_init(backup_id)
    elif mode == 'target_receive' and len(argv) == 6:
        result = target_receive(backup_id, argv[3], argv[4], argv[5])
    elif mode == 'source_stream' and len(argv) == 4 and argv[3] in ('archive', 'manifest'):
        source_stream(backup_id, argv[3])
        return
    elif mode == 'target_check' and len(argv) == 3:
        result = verify_pair(backup_id, True)
    elif mode == 'target_restore' and len(argv) == 3:
        result = target_restore(backup_id)
    elif mode == 'target_proof' and len(argv) == 3:
        result = verify_pair(backup_id, True)
        pf = root_path(backup_id) / 'restore-proof.json'
        protected(pf)
        proof = json.loads(pf.read_text(encoding='utf-8'))
        if (proof.get('backup_id') != backup_id or proof.get('archive_sha256') != result['archive_sha256']
                or not proof.get('ok') or proof.get('verified_files') != result['file_count']):
            raise RuntimeError('restore proof is inconsistent')
        result['restore_verified'] = True
    else:
        raise ValueError('unsupported operation or arguments')
    print(json.dumps(result, sort_keys=True))


if __name__ == '__main__':
    try:
        main(sys.argv)
    except Exception as exc:
        print('STEP76_HDFS_REMOTE_ERROR: ' + str(exc), file=sys.stderr)
        sys.exit(1)
