#!/usr/bin/env python3
"""Stream two verified protected logical backup sets to worker1/2 via SSH.

No backups or DB credentials are written to the orchestration host's filesystem.
SSH runs a fixed root-only helper using sudo -n; all commands are allowlisted.
"""
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import sys

ID_RE = re.compile(r'^[0-9]{8}T[0-9]{6}Z-[a-f0-9]{8}$')
HOSTS = {'pg': 'worker3', 'mysql': 'metadata'}
TARGETS = ('worker1', 'worker2')
MAX_HELPER_BYTES = 32768


def ssh_argv(host, helper, *args):
    if host not in ('worker1', 'worker2', 'worker3', 'metadata'):
        raise ValueError('unapproved SSH host')
    sudo_argv = ['sudo', '-n', 'python3', '-c', helper, *map(str, args)]
    # The controller is worker3. Direct local sudo avoids an unnecessary SSH
    # round trip to itself and does not require a worker3 self-host key.
    if host == 'worker3':
        return sudo_argv
    remote_cmd = shlex.join(sudo_argv)
    return ['ssh', '-T', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=10',
            '-o', 'StrictHostKeyChecking=yes', host, remote_cmd]


def run_json(host, helper, *args, timeout=90):
    p = subprocess.run(ssh_argv(host, helper, *args), stdout=subprocess.PIPE,
                       stderr=subprocess.PIPE, timeout=timeout, check=False)
    if p.returncode:
        raise RuntimeError('SSH/root helper failed on ' + host +
                           '; inspect SSH host keys, sudo -n permission, and backup paths. ' +
                           p.stderr.decode('utf-8', 'replace')[:500])
    try:
        return json.loads(p.stdout)
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise RuntimeError('unexpected helper response from ' + host) from exc


def stream_one(helper, source_host, target_host, backup_id, component, kind, sha, size):
    """Backups travel as bytes via worker3 RAM/SSH only; never written to disk locally."""
    reader = subprocess.Popen(ssh_argv(source_host, helper, 'source_stream', backup_id,
                                       component, kind), stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE)
    writer = subprocess.Popen(ssh_argv(target_host, helper, 'target_receive', backup_id,
                                       component, kind, sha, size), stdin=subprocess.PIPE,
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    copied = 0
    try:
        while True:
            block = reader.stdout.read(256 * 1024)
            if not block:
                break
            writer.stdin.write(block)
            copied += len(block)
        writer.stdin.close()
        reader_rc = reader.wait(timeout=90)
        writer_rc = writer.wait(timeout=90)
        if reader_rc or writer_rc or copied != size:
            raise RuntimeError('encrypted SSH stream failed: ' + component + '/' + kind +
                               ' source_rc=' + str(reader_rc) + ' target_rc=' + str(writer_rc) +
                               ' source_error=' + reader.stderr.read().decode('utf-8', 'replace')[:250] +
                               ' target_error=' + writer.stderr.read().decode('utf-8', 'replace')[:250])
        response = json.loads(writer.stdout.read())
        if not response.get('ok') or response.get('bytes') != size:
            raise RuntimeError('target did not confirm stream length')
    finally:
        if reader.poll() is None:
            reader.kill()
        if writer.poll() is None:
            writer.kill()
        if reader.stdout:
            reader.stdout.close()
        if reader.stderr:
            reader.stderr.close()
        if writer.stdout:
            writer.stdout.close()
        if writer.stderr:
            writer.stderr.close()


def read_sources(helper, backup_id):
    return {key: run_json(host, helper, 'source_check', backup_id, key)
            for key, host in HOSTS.items()}


def equal_snapshots(a, b):
    return all(a[k] == b[k] for k in ('pg', 'mysql'))


def orchestrate(mode, backup_id, target, helper):
    sources = read_sources(helper, backup_id)
    probe = run_json(target, helper, 'target_probe', backup_id)
    if mode == 'preflight':
        if not probe.get('disk_threshold_pass'):
            raise RuntimeError('worker target has less than 2 GiB free')
        return {'result': 'STEP76_OFFHOST_PREFLIGHT_PASS', 'backup_id': backup_id,
                'target': target, 'source_checksums_verified': True,
                'target_free_mib': probe['free_mib'],
                'target_backup_exists': probe['backup_dir_exists'],
                'ready_for_copy': not probe['backup_dir_exists']}
    if mode == 'copy':
        if os.environ.get('STEP76_ALLOW_OFFHOST_COPY') != 'YES':
            raise RuntimeError('copy requires STEP76_ALLOW_OFFHOST_COPY=YES')
        if not probe.get('disk_threshold_pass'):
            raise RuntimeError('worker target has less than 2 GiB free')
        if probe.get('backup_dir_exists'):
            raise RuntimeError('target backup ID already exists; run verify or inspect failed partial transfer')
        run_json(target, helper, 'target_init', backup_id)
        for component, source_host in HOSTS.items():
            item = sources[component]
            for kind in ('archive', 'manifest'):
                stream_one(helper, source_host, target, backup_id, component, kind,
                           item[kind + '_sha256'], item[kind + '_bytes'])
        checked = run_json(target, helper, 'target_verify', backup_id)
        if not equal_snapshots(sources, checked['components']):
            raise RuntimeError('target checksum differs from source')
        sealed = run_json(target, helper, 'target_seal', backup_id)
        if not sealed.get('sealed') or not equal_snapshots(sources, sealed['components']):
            raise RuntimeError('offhost verification/seal failed')
        return {'result': 'STEP76_OFFHOST_COPY_PASS', 'backup_id': backup_id,
                'target': target, 'components_verified': ['pg', 'mysql'],
                'source_sha256_match': True, 'sealed': True}
    if mode == 'verify':
        checked = run_json(target, helper, 'target_verify', backup_id)
        if not checked.get('sealed') or not equal_snapshots(sources, checked['components']):
            raise RuntimeError('target is unsealed or differs from source')
        return {'result': 'STEP76_OFFHOST_VERIFY_PASS', 'backup_id': backup_id,
                'target': target, 'components_verified': ['pg', 'mysql'],
                'source_sha256_match': True, 'sealed': True}
    raise ValueError('invalid mode')


def main(args):
    if len(args) != 5:
        raise ValueError('Usage: step76d_offhost_copy.py preflight|copy|verify ID worker1|worker2 PATH_TO_HELPER')
    mode, backup_id, target, helper_path = args[1:]
    if mode not in ('preflight', 'copy', 'verify') or not ID_RE.fullmatch(backup_id) or target not in TARGETS:
        raise ValueError('invalid action, backup identity or worker target')
    path = Path(helper_path)
    if not path.is_file() or path.is_symlink() or path.stat().st_size > MAX_HELPER_BYTES:
        raise RuntimeError('remote helper missing or invalid')
    helper = path.read_text(encoding='utf-8')
    print(json.dumps(orchestrate(mode, backup_id, target, helper), sort_keys=True))
    return 0


if __name__ == '__main__':
    try:
        sys.exit(main(sys.argv))
    except (OSError, ValueError, RuntimeError, subprocess.TimeoutExpired) as exc:
        print('STEP76_OFFHOST_ERROR: ' + str(exc), file=sys.stderr)
        sys.exit(1)
