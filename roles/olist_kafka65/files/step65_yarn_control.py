#!/usr/bin/env python3
"""Strict YARN lifecycle for ONLY Step65 continuous CDC application.

One active application maximum; no broad regex app kills and no checkpoint changes.
"""
from __future__ import annotations

import argparse
import json
import os
import fcntl
import re
import subprocess
import sys
import time
from pathlib import Path

NAME = 'olist-iceberg-kafka65-continuous'
OWNER = 'hadoop'
APP = '/opt/bigtop-iceberg/step65/olist_kafka65.py'
ICEBERG = '/opt/bigtop-iceberg/iceberg-spark-runtime-3.5_2.12-1.10.1.jar'
JAR_DIR = '/opt/bigtop-iceberg/kafka64'
JARS = [
    'spark-sql-kafka-0-10_2.12-3.5.6.jar',
    'spark-token-provider-kafka-0-10_2.12-3.5.6.jar',
    'kafka-clients-3.4.1.jar',
    'commons-pool2-2.11.1.jar',
    'jsr305-3.0.0.jar',
]
LOCK = '/var/lib/bigtop-olist-step65/control.lock'
ACTIVE = {'NEW', 'NEW_SAVING', 'SUBMITTED', 'ACCEPTED', 'RUNNING'}
DESIRED_FILE = '/var/lib/bigtop-olist-step65/desired66.json'
RECOVERY_FILE = '/var/lib/bigtop-olist-step65/recovery66.json'
RECOVERY_GRACE_SECONDS = 120
RECOVERY_COOLDOWN_SECONDS = 120
RECOVERY_WINDOW_SECONDS = 3600
RECOVERY_MAX_ATTEMPTS = 3


class StateError(RuntimeError):
    pass


def execute(*cmd, timeout=90):
    if cmd and cmd[0] in ('yarn', 'spark-submit'):
        env_bin = os.environ.get('STEP66_YARN_BIN' if cmd[0] == 'yarn' else 'STEP66_SPARK_BIN')
        if env_bin:
            if not os.path.isabs(env_bin) or not Path(env_bin).is_file():
                raise StateError('Invalid configured Step66 executable: ' + env_bin)
            cmd = (env_bin,) + cmd[1:]
    p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    if p.returncode:
        raise StateError('Command failed: ' + ' '.join(cmd[:3]) + ' ' +
                         p.stderr[-600:])
    return p.stdout


def active_apps(strict=False):
    # Exact application-name and exact owner match; no substring/grep kill.
    output = execute('yarn', 'application', '-list', '-appStates', 'ALL')
    if strict and not ('Application-Id' in output or 'Total number of applications' in output):
        raise StateError('YARN list returned unrecognized output; refuse recovery')
    matches = []
    for line in output.splitlines():
        match = re.match(r'^\s*(application_\d+_\d+)\s+(\S+)\s+(\S+)\s+(\S+)\s+'
                         r'(\S+)\s+(\S+)\s+', line)
        if not match:
            continue
        app_id, name, app_type, user, queue, state = match.groups()
        if name == NAME and user == OWNER and state.upper() in ACTIVE:
            matches.append((app_id, state.upper()))
    if len(matches) > 1:
        raise StateError('Duplicate active Step65 YARN apps; refuse automatic action: '
                         + str(matches))
    return matches


def spark_command(bootstrap):
    jars = [ICEBERG] + [JAR_DIR + '/' + name for name in JARS]
    for path in [APP] + jars:
        if not Path(path).is_file():
            raise StateError('Missing staged Step65 dependency: ' + path)
    return [
        'spark-submit', '--master', 'yarn', '--deploy-mode', 'cluster',
        '--name', NAME, '--queue', 'batch',
        '--driver-memory', '768m', '--executor-memory', '768m',
        '--executor-cores', '1', '--num-executors', '1',
        '--jars', ','.join(jars),
        '--conf', 'spark.yarn.submit.waitAppCompletion=false',
        '--conf', 'spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions',
        '--conf', 'spark.sql.catalog.shared=org.apache.iceberg.spark.SparkCatalog',
        '--conf', 'spark.sql.catalog.shared.type=rest',
        '--conf', 'spark.sql.catalog.shared.uri=http://192.158.2.103:9001/iceberg/',
        '--conf', 'spark.sql.shuffle.partitions=4',
        '--conf', 'spark.sql.session.timeZone=UTC',
        '--conf', 'spark.pyspark.python=/usr/bin/python3',
        APP, '--mode', 'stream', '--bootstrap', bootstrap,
    ]


def atomic_json(path, record):
    target = Path(path)
    tmp = target.with_name(target.name + '.tmp.' + str(os.getpid()))
    try:
        with tmp.open('x', encoding='utf-8') as fp:
            json.dump(record, fp, ensure_ascii=True, sort_keys=True)
            fp.write('\n')
        os.replace(tmp, target)
    finally:
        if tmp.exists():
            tmp.unlink()


def read_json(path, default):
    p = Path(path)
    if not p.exists():
        return dict(default)
    data = json.loads(p.read_text(encoding='utf-8'))
    if not isinstance(data, dict):
        raise StateError('Invalid state file: ' + path)
    return data


def desired_running():
    state = read_json(DESIRED_FILE, {'desired': 'stopped'})
    if state.get('desired') not in {'stopped', 'running'}:
        raise StateError('Invalid desired state; fail closed')
    return state['desired'] == 'running'


def set_desired(running):
    atomic_json(DESIRED_FILE, {'desired': 'running' if running else 'stopped',
                               'updated_at': int(time.time())})


def recovery_state():
    data = read_json(RECOVERY_FILE, {'attempts': [], 'total_attempts': 0,
                                      'missing_since': 0, 'last_attempt_at': 0})
    for key in ('attempts', 'total_attempts', 'missing_since', 'last_attempt_at'):
        if key not in data:
            raise StateError('Recovery state missing ' + key)
    if not isinstance(data['attempts'], list):
        raise StateError('Invalid recovery history')
    return data


def print_recovery(result):
    print('STEP66_RECOVERY_RESULT: ' + json.dumps(result, sort_keys=True), flush=True)


def do_recover(bootstrap, now=None):
    # This function must run UNDER THE SAME LOCK as manual 65 start/stop.
    now = int(time.time() if now is None else now)
    result = {'desired_running': int(desired_running()), 'running': 0,
              'active': 0, 'restarted': 0, 'blocked': 0,
              'attempts_window': 0, 'total_attempts': 0,
              'state': 'unarmed'}
    history = recovery_state()
    result['attempts_window'] = len([t for t in history['attempts']
                                     if now - int(t) < RECOVERY_WINDOW_SECONDS])
    result['total_attempts'] = int(history['total_attempts'])
    if not result['desired_running']:
        return result
    apps = active_apps(strict=True)
    if apps:
        result['active'] = 1
        result['running'] = int(apps[0][1] == 'RUNNING')
        result['state'] = apps[0][1].lower()
        if history['missing_since']:
            history['missing_since'] = 0
            atomic_json(RECOVERY_FILE, history)
        return result
    if not history['missing_since']:
        history['missing_since'] = now
        atomic_json(RECOVERY_FILE, history)
        result['state'] = 'missing_grace'
        return result
    if now - int(history['missing_since']) < RECOVERY_GRACE_SECONDS:
        result['state'] = 'missing_grace'
        return result
    recent = [int(t) for t in history['attempts']
              if 0 <= now - int(t) < RECOVERY_WINDOW_SECONDS]
    result['attempts_window'] = len(recent)
    if len(recent) >= RECOVERY_MAX_ATTEMPTS:
        result['state'] = 'rate_limited'
        result['blocked'] = 1
        return result
    if history['last_attempt_at'] and now - int(history['last_attempt_at']) < RECOVERY_COOLDOWN_SECONDS:
        result['state'] = 'cooldown'
        return result
    history['attempts'] = recent + [now]
    history['last_attempt_at'] = now
    history['total_attempts'] = int(history['total_attempts']) + 1
    # Persist attempt BEFORE spark-submit to cap repeated submissions on crash.
    atomic_json(RECOVERY_FILE, history)
    result['attempts_window'] = len(history['attempts'])
    result['total_attempts'] = history['total_attempts']
    result['state'] = 'submit_attempt'
    try:
        execute(*spark_command(bootstrap), timeout=240)
        for _ in range(24):
            apps = active_apps(strict=True)
            if apps:
                history['missing_since'] = 0
                atomic_json(RECOVERY_FILE, history)
                result['active'] = 1
                result['running'] = int(apps[0][1] == 'RUNNING')
                result['restarted'] = 1
                result['state'] = 'restarted_' + apps[0][1].lower()
                return result
            time.sleep(5)
        raise StateError('Spark submit finished but no exact Step65 app observed')
    except (OSError, subprocess.TimeoutExpired, StateError) as exc:
        # Fail closed; retain attempt in rolling history to avoid restart storms.
        raise StateError('Recovery submission failed (counted against limit): ' + str(exc)) from exc


def run(action, bootstrap):
    lock = Path(LOCK)
    if not lock.parent.is_dir():
        raise StateError('Missing Step65 lock directory; run apply first')
    with lock.open('a+') as handle:
        fcntl.flock(handle, fcntl.LOCK_EX)
        if action == 'recover':
            print_recovery(do_recover(bootstrap))
            return
        if action in {'stop', 'disarm'}:
            # User intent must be recorded even during ResourceManager outages.
            set_desired(False)
        if action == 'disarm':
            print('STEP66_AUTORESTART_DISARMED: streaming app not killed')
            return
        running = active_apps()
        if action == 'status':
            if running:
                print('STEP65_YARN_RUNNING: ' + running[0][0] + ' ' + running[0][1])
            else:
                print('STEP65_YARN_NOT_RUNNING')
            return
        if action == 'arm':
            if not running or running[0][1] != 'RUNNING':
                raise StateError('Step65 must be RUNNING to enable watchdog')
            set_desired(True)
            print('STEP66_AUTORESTART_ARMED: ' + running[0][0])
            return
        if action == 'start':
            if running:
                set_desired(True)
                print('STEP65_YARN_ALREADY_RUNNING: ' + running[0][0])
                return
            print('STEP65_YARN_SUBMITTING', flush=True)
            output = execute(*spark_command(bootstrap), timeout=240)
            print(output[-1800:], flush=True)
            for _ in range(24):
                running = active_apps()
                if running:
                    set_desired(True)
                    print('STEP65_YARN_START_PASS: ' + running[0][0], flush=True)
                    return
                time.sleep(5)
            raise StateError('Submit returned but Step65 app not active. Inspect YARN history')
        if action == 'stop':
            # Desired state was recorded before contacting YARN.
            if not running:
                print('STEP65_YARN_ALREADY_STOPPED')
                return
            target = running[0][0]
            execute('yarn', 'application', '-kill', target)
            for _ in range(18):
                if not active_apps():
                    print('STEP65_YARN_STOP_PASS: ' + target, flush=True)
                    print('STEP65_CHECKPOINT_PRESERVED', flush=True)
                    return
                time.sleep(5)
            raise StateError('YARN kill sent but app remains active; inspect ' + target)
        raise StateError('Unsupported action')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['start', 'status', 'stop', 'arm', 'disarm', 'recover'])
    parser.add_argument('--bootstrap',
                        default='192.158.2.101:9092,192.158.2.102:9092,192.158.2.103:9092')
    args = parser.parse_args()
    run(args.action, args.bootstrap)


if __name__ == '__main__':
    try:
        main()
    except (OSError, ValueError, subprocess.TimeoutExpired, StateError) as exc:
        print('STEP65_YARN_CONTROL_FAILED: ' + str(exc), file=sys.stderr)
        sys.exit(1)
