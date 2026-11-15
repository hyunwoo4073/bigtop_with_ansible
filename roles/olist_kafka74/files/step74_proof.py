#!/usr/bin/env python3
"""Step74 guarded live proof of the existing Step66 recovery policy.

Default inspect mode is read-only. Fault injection requires BOTH exact app ID and
an explicit confirmation, and never touches HDFS/Iceberg/Kafka contents.
Run as the Step65 hadoop user on master only.
"""
from __future__ import annotations

import argparse
import fcntl
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time

APP_NAME = 'olist-iceberg-kafka65-continuous'
OWNER = 'hadoop'
CHECKPOINT = 'hdfs://master:9000/data/olist/iceberg-checkpoints/step65/v1'
ROOT = Path('/var/lib/bigtop-olist-step65')
CONTROL = Path('/opt/bigtop-iceberg/step65/step65_yarn_control.py')
WATCHDOG = Path('/opt/bigtop-iceberg/step65/step66_watchdog.py')
LOCK = ROOT / 'control.lock'
DESIRED = ROOT / 'desired66.json'
RECOVERY = ROOT / 'recovery66.json'
EVIDENCE = ROOT / 'step74-proof.json'
TIMER = 'bigtop-olist-step66-watchdog.timer'
ACTIVE_STATES = {'NEW', 'NEW_SAVING', 'SUBMITTED', 'ACCEPTED', 'RUNNING'}
PATTERN = re.compile(r'^\s*(application_\d+_\d+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\b')
ID_PATTERN = re.compile(r'^application_\d+_\d+$')


class GuardError(RuntimeError):
    pass


def command(*args, timeout=45, check=True):
    env = os.environ.copy()
    env['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
    env['SPARK_CONF_DIR'] = '/etc/spark/conf'
    try:
        proc = subprocess.run(args, capture_output=True, text=True, timeout=timeout, env=env, check=False)
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise GuardError('Command unavailable/timed out: ' + args[0]) from exc
    if check and proc.returncode:
        raise GuardError('Command failed: ' + ' '.join(args[:3]) + ' (rc=' + str(proc.returncode) + ') ' + proc.stderr[-300:])
    return proc


def yarn_apps():
    out = command('yarn', 'application', '-list', '-appStates', 'NEW,NEW_SAVING,SUBMITTED,ACCEPTED,RUNNING').stdout
    if 'Total number of applications' not in out or 'Application-Id' not in out:
        raise GuardError('Unexpected YARN report; refusing to infer empty cluster')
    apps = []
    for line in out.splitlines():
        match = PATTERN.match(line)
        if match:
            app_id, name, app_type, owner, queue, state = match.groups()
            if state in ACTIVE_STATES:
                apps.append(dict(app_id=app_id, name=name, type=app_type,
                                 owner=owner, queue=queue, state=state))
    return apps


def exact_step65(apps):
    found = [a for a in apps if a['name'] == APP_NAME and a['owner'] == OWNER]
    if len(found) > 1:
        raise GuardError('Multiple active Step65 applications: refusing to proceed')
    return found[0] if found else None


def load_json(path, fallback=None):
    if not path.exists():
        if fallback is None:
            raise GuardError('Missing required state file: ' + str(path))
        return fallback
    data = json.loads(path.read_text(encoding='utf-8'))
    if not isinstance(data, dict):
        raise GuardError('Invalid JSON object in ' + str(path))
    return data


def desired_state():
    value = load_json(DESIRED, {'desired': 'stopped'}).get('desired')
    if value not in ('running', 'stopped'):
        raise GuardError('Invalid desired66 state')
    return value


def recovery_state():
    data = load_json(RECOVERY, {'attempts': [], 'total_attempts': 0})
    if not isinstance(data.get('total_attempts'), int) or not isinstance(data.get('attempts'), list):
        raise GuardError('Invalid recovery state')
    return data


def timer_state():
    active = command('systemctl', 'is-active', TIMER, check=False).stdout.strip()
    enabled = command('systemctl', 'is-enabled', TIMER, check=False).stdout.strip()
    return active, enabled


def checkpoint_exists():
    return command('hdfs', 'dfs', '-test', '-d', CHECKPOINT, timeout=45, check=False).returncode == 0


def base_state():
    if not CONTROL.is_file() or not WATCHDOG.is_file():
        raise GuardError('Step66 controller or watchdog not installed; inspect Step66 apply first')
    ctl = CONTROL.read_text(encoding='utf-8')
    if 'DESIRED_FILE' not in ctl or 'RECOVERY_MAX_ATTEMPTS' not in ctl or 'do_recover(' not in ctl:
        raise GuardError('Installed Step65 controller lacks expected Step66 recovery safeguards')
    apps = yarn_apps()
    current = exact_step65(apps)
    desired = desired_state()
    recovery = recovery_state()
    active, enabled = timer_state()
    checkpoint = checkpoint_exists()
    now = int(time.time())
    recent = [t for t in recovery['attempts'] if isinstance(t, int) and 0 <= now - t < 3600]
    safe = (current is not None and current['state'] == 'RUNNING' and
            len(apps) == 1 and desired == 'running' and active == 'active' and
            enabled in ('enabled', 'enabled-runtime') and checkpoint and len(recent) < 3)
    return dict(app=current, active_applications=apps, desired=desired,
                timer_active=active, timer_enabled=enabled, checkpoint_present=checkpoint,
                attempts_in_last_hour=len(recent), total_attempts=recovery['total_attempts'],
                ready_for_fault=safe)


def atomic_create_json(path, record):
    # x mode deliberately refuses to overwrite a previous trial or incomplete evidence.
    with path.open('x', encoding='utf-8') as handle:
        os.chmod(path, 0o600)
        json.dump(record, handle, sort_keys=True)
        handle.write('\n')
        handle.flush()
        os.fsync(handle.fileno())


def atomic_update_json(path, record):
    tmp = path.with_name(path.name + '.tmp.' + str(os.getpid()))
    try:
        with tmp.open('x', encoding='utf-8') as handle:
            os.chmod(tmp, 0o600)
            json.dump(record, handle, sort_keys=True)
            handle.write('\n')
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, path)
    finally:
        if tmp.exists():
            tmp.unlink()


def inspect():
    value = base_state()
    value['evidence_exists'] = EVIDENCE.exists()
    value['mode'] = 'read_only'
    print(json.dumps(value, indent=2, sort_keys=True))
    print('STEP74_INSPECT_PASS')


def fault(expected_app_id, confirmation):
    if confirmation != 'YES' or not ID_PATTERN.fullmatch(expected_app_id or ''):
        raise GuardError('Requires exact --expected-app-id and --confirm YES')
    with LOCK.open('a+', encoding='utf-8') as handle:
        fcntl.flock(handle, fcntl.LOCK_EX)
        value = base_state()
        if EVIDENCE.exists():
            raise GuardError('Step74 evidence already exists; refuse repeated fault injection')
        if not value['ready_for_fault'] or value['app']['app_id'] != expected_app_id:
            raise GuardError('Preconditions changed, timer disabled, or app ID mismatch')
        record = dict(expected_app_id=expected_app_id, checkpoint=CHECKPOINT,
                      original_attempts=value['total_attempts'],
                      original_recent_attempts=value['attempts_in_last_hour'],
                      created_at=int(time.time()), phase='prepared')
        atomic_create_json(EVIDENCE, record)
        # Under Step65 controller lock, exact ID observed running + current desired=running.
        # This intentionally BYPASSES Step65 'stop' which would disarm recovery.
        command('yarn', 'application', '-kill', expected_app_id, timeout=90)
        record['phase'] = 'fault_sent'
        record['fault_sent_at'] = int(time.time())
        atomic_update_json(EVIDENCE, record)
    print('STEP74_FAULT_INJECTED: ' + expected_app_id)
    print('STEP74_EXPECTED: timer eventually creates a new Step65 YARN app; checkpoint untouched')


def verify_once():
    evidence = load_json(EVIDENCE)
    if evidence.get('phase') != 'fault_sent':
        raise GuardError('Fault evidence not complete; do not assume fault injected')
    value = base_state()
    app = value['app']
    new_id = app['app_id'] if app else None
    before = evidence['original_attempts']
    attempted = value['total_attempts']
    if attempted > before + 1:
        raise GuardError('More than one recovery attempt recorded; investigate duplicate/retry behavior')
    if value['desired'] != 'running' or value['timer_active'] != 'active':
        raise GuardError('Watchdog is disarmed or timer stopped during recovery')
    if not value['checkpoint_present']:
        raise GuardError('Original HDFS checkpoint directory not accessible')
    if app and app['app_id'] == evidence['expected_app_id']:
        return False, value, 'Old app still active'
    if len(value['active_applications']) > 1:
        raise GuardError('Unexpected concurrent YARN apps in single-slot lab')
    if attempted == before + 1 and app and app['state'] == 'RUNNING' and new_id != evidence['expected_app_id']:
        return True, value, 'Recovered new Step65 app running'
    return False, value, 'Waiting for Step66 watchdog recovery'


def verify(wait_seconds, poll_seconds):
    deadline = time.monotonic() + wait_seconds
    while True:
        passed, value, detail = verify_once()
        print('STEP74_RECOVERY_CHECK: ' + detail + ' app=' + str(value['app']) +
              ' attempts=' + str(value['total_attempts']), flush=True)
        if passed:
            print('STEP74_WATCHDOG_RECOVERY_PASS')
            print('STEP74_SINGLE_ACTIVE_APP_PASS')
            print('STEP74_CHECKPOINT_PATH_PRESERVED_PASS')
            return
        if time.monotonic() >= deadline:
            raise GuardError('Recovery not verified; inspect Step66 journal/YARN; do not re-inject fault')
        time.sleep(min(poll_seconds, max(0.1, deadline - time.monotonic())))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', choices=['inspect', 'fault', 'verify'])
    parser.add_argument('--expected-app-id', default='')
    parser.add_argument('--confirm', default='NO')
    parser.add_argument('--wait-seconds', type=int, default=540)
    parser.add_argument('--poll-seconds', type=int, default=20)
    opts = parser.parse_args()
    if opts.wait_seconds < 0 or opts.wait_seconds > 900 or opts.poll_seconds < 1:
        raise GuardError('Invalid polling parameters')
    if os.geteuid() == 0 or os.environ.get('USER') not in (None, OWNER):
        raise GuardError('Refuse to run as root or as a different user; use hadoop')
    if opts.mode == 'inspect':
        inspect()
    elif opts.mode == 'fault':
        fault(opts.expected_app_id, opts.confirm)
    elif opts.mode == 'verify':
        verify(opts.wait_seconds, opts.poll_seconds)


if __name__ == '__main__':
    try:
        main()
    except (GuardError, ValueError, OSError, KeyError, TypeError, json.JSONDecodeError) as exc:
        print('STEP74_PROOF_FAILED: ' + str(exc), file=sys.stderr)
        sys.exit(1)
