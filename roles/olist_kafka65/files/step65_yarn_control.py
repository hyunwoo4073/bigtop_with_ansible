#!/usr/bin/env python3
"""Strict YARN lifecycle for ONLY Step65 continuous CDC application.

One active application maximum; no broad regex app kills and no checkpoint changes.
"""
from __future__ import annotations

import argparse
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


class StateError(RuntimeError):
    pass


def execute(*cmd, timeout=90):
    p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    if p.returncode:
        raise StateError('Command failed: ' + ' '.join(cmd[:3]) + ' ' +
                         p.stderr[-600:])
    return p.stdout


def active_apps():
    # Exact application-name and exact owner match; no substring/grep kill.
    output = execute('yarn', 'application', '-list', '-appStates', 'ALL')
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


def run(action, bootstrap):
    lock = Path(LOCK)
    if not lock.parent.is_dir():
        raise StateError('Missing Step65 lock directory; run apply first')
    with lock.open('a+') as handle:
        fcntl.flock(handle, fcntl.LOCK_EX)
        running = active_apps()
        if action == 'status':
            if running:
                print('STEP65_YARN_RUNNING: ' + running[0][0] + ' ' + running[0][1])
            else:
                print('STEP65_YARN_NOT_RUNNING')
            return
        if action == 'start':
            if running:
                print('STEP65_YARN_ALREADY_RUNNING: ' + running[0][0])
                return
            print('STEP65_YARN_SUBMITTING', flush=True)
            output = execute(*spark_command(bootstrap), timeout=240)
            print(output[-1800:], flush=True)
            for _ in range(24):
                running = active_apps()
                if running:
                    print('STEP65_YARN_START_PASS: ' + running[0][0], flush=True)
                    return
                time.sleep(5)
            raise StateError('Submit returned but Step65 app not active. Inspect YARN history')
        if action == 'stop':
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
    parser.add_argument('action', choices=['start', 'status', 'stop'])
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
