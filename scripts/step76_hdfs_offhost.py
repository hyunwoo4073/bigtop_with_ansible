#!/usr/bin/env python3
"""Orchestrate isolated Step76 D3 HDFS smoke-table backup and off-host restore."""
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import sys

ID_RE = re.compile(r'^[0-9]{8}T[0-9]{6}Z-[a-f0-9]{8}$')
SOURCE = 'master'
TARGET = 'worker1'
MAX_HELPER = 32768


def remote(helper, host, *args):
    if host not in (SOURCE, TARGET):
        raise ValueError('unapproved SSH target')
    command = shlex.join(['sudo', '-n', 'python3', '-c', helper, *map(str, args)])
    return ['ssh', '-T', '-o', 'BatchMode=yes', '-o', 'StrictHostKeyChecking=yes',
            '-o', 'ConnectTimeout=10', host, command]


def run_json(helper, host, *args):
    p = subprocess.run(remote(helper, host, *args), stdout=subprocess.PIPE,
                       stderr=subprocess.PIPE, timeout=300, check=False)
    if p.returncode != 0:
        raise RuntimeError(host + ' action failed: ' + p.stderr.decode('utf-8', 'replace')[-350:])
    try:
        return json.loads(p.stdout)
    except (ValueError, UnicodeError) as exc:
        raise RuntimeError(host + ' returned invalid JSON') from exc


def copy_file(helper, backup_id, kind, expected_hash, expected_bytes):
    reader = subprocess.Popen(remote(helper, SOURCE, 'source_stream', backup_id, kind),
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    writer = subprocess.Popen(remote(helper, TARGET, 'target_receive', backup_id,
                                     kind, expected_hash, expected_bytes),
                              stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    copied = 0
    try:
        while True:
            block = reader.stdout.read(256 * 1024)
            if not block:
                break
            writer.stdin.write(block)
            copied += len(block)
        writer.stdin.close()
        read_rc = reader.wait(timeout=300)
        write_rc = writer.wait(timeout=300)
        if read_rc != 0 or write_rc != 0 or copied != expected_bytes:
            raise RuntimeError('copy failed for ' + kind + ': master=' +
                               reader.stderr.read().decode('utf-8', 'replace')[-200:] +
                               ' worker1=' + writer.stderr.read().decode('utf-8', 'replace')[-200:])
        reply = json.loads(writer.stdout.read())
        if not reply.get('ok') or reply.get('bytes') != expected_bytes:
            raise RuntimeError('worker1 reported incomplete stream')
    finally:
        if reader.poll() is None:
            reader.kill()
        if writer.poll() is None:
            writer.kill()
        for stream in (reader.stdout, reader.stderr, writer.stdout, writer.stderr):
            if stream:
                stream.close()


def matching(a, b):
    return all(a[x] == b[x] for x in ('archive_sha256', 'archive_bytes',
                                       'manifest_sha256', 'manifest_bytes', 'file_count'))


def execute(mode, backup_id, helper):
    if mode == 'preflight':
        source = run_json(helper, SOURCE, 'source_probe', backup_id)
        dest = run_json(helper, TARGET, 'target_probe', backup_id)
        return {'result': 'STEP76_HDFS_PREFLIGHT_PASS', 'source': source, 'target': dest}
    if mode == 'capture':
        if os.environ.get('STEP76_ALLOW_HDFS_CAPTURE') != 'YES':
            raise RuntimeError('capture requires STEP76_ALLOW_HDFS_CAPTURE=YES')
        data = run_json(helper, SOURCE, 'source_capture', backup_id)
        return {'result': 'STEP76_HDFS_CAPTURE_PASS', **data}
    if mode == 'copy':
        if os.environ.get('STEP76_ALLOW_HDFS_COPY') != 'YES':
            raise RuntimeError('copy requires STEP76_ALLOW_HDFS_COPY=YES')
        origin = run_json(helper, SOURCE, 'source_check', backup_id)
        target = run_json(helper, TARGET, 'target_probe', backup_id)
        if not target['ready_for_copy']:
            raise RuntimeError('worker1 target exists or is short of disk; refusing overwrite')
        run_json(helper, TARGET, 'target_init', backup_id)
        for kind in ('archive', 'manifest'):
            copy_file(helper, backup_id, kind,
                      origin[kind + '_sha256'], origin[kind + '_bytes'])
        dest = run_json(helper, TARGET, 'target_check', backup_id)
        if not matching(origin, dest):
            raise RuntimeError('source and off-host archive digests differ')
        return {'result': 'STEP76_HDFS_COPY_PASS', 'backup_id': backup_id,
                'source_and_target_checksums_match': True}
    if mode == 'restore':
        if os.environ.get('STEP76_ALLOW_HDFS_RESTORE') != 'YES':
            raise RuntimeError('restore requires STEP76_ALLOW_HDFS_RESTORE=YES')
        origin = run_json(helper, SOURCE, 'source_check', backup_id)
        dest = run_json(helper, TARGET, 'target_check', backup_id)
        if not matching(origin, dest):
            raise RuntimeError('backup pair mismatch')
        result = run_json(helper, TARGET, 'target_restore', backup_id)
        return {'result': 'STEP76_HDFS_ISOLATED_RESTORE_PASS', **result}
    if mode == 'verify':
        origin = run_json(helper, SOURCE, 'source_check', backup_id)
        dest = run_json(helper, TARGET, 'target_proof', backup_id)
        if not matching(origin, dest) or not dest.get('restore_verified'):
            raise RuntimeError('backup hash or isolated restore proof failed')
        return {'result': 'STEP76_HDFS_VERIFY_PASS', 'backup_id': backup_id,
                'table': origin['table'], 'verified_files': origin['file_count']}
    raise ValueError('unsupported mode')


def main(argv):
    if len(argv) != 4:
        raise ValueError('usage: step76_hdfs_offhost.py MODE BACKUP_ID HELPER_PATH')
    _, mode, backup_id, helper_path = argv
    if mode not in ('preflight', 'capture', 'copy', 'restore', 'verify') or not ID_RE.fullmatch(backup_id):
        raise ValueError('invalid mode or backup ID')
    path = Path(helper_path)
    if path.is_symlink() or not path.is_file() or path.stat().st_size > MAX_HELPER:
        raise ValueError('invalid remote helper')
    helper = path.read_text(encoding='utf-8')
    print(json.dumps(execute(mode, backup_id, helper), indent=2, sort_keys=True))


if __name__ == '__main__':
    try:
        main(sys.argv)
    except Exception as exc:
        print('STEP76_HDFS_ERROR: ' + str(exc), file=sys.stderr)
        sys.exit(1)
