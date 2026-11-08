#!/usr/bin/env python3
"""Root metrics wrapper. Lifecycle commands run as hadoop, never as root.

Node exporter textfile is updated atomically even when YARN is unreachable.
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

CONTROLLER = '/opt/bigtop-iceberg/step65/step65_yarn_control.py'
USER = 'hadoop'
PREFIX = 'bigtop_olist_step66_'


def textfile(values, timestamp):
    data = [
        '# HELP bigtop_olist_step66_watchdog_success Last watchdog poll succeeded.\n',
        '# TYPE bigtop_olist_step66_watchdog_success gauge\n',
    ]
    for field, value in values.items():
        metric = PREFIX + field
        if field != 'watchdog_success':
            data.append('# TYPE ' + metric + ' gauge\n')
        data.append(metric + ' ' + str(int(value)) + '\n')
    data.append(PREFIX + 'last_run_timestamp_seconds ' + str(int(timestamp)) + '\n')
    return ''.join(data)


def write_atomic(path, content):
    target = Path(path)
    if not target.parent.is_dir():
        raise RuntimeError('Missing Node Exporter textfile dir: ' + str(target.parent))
    fd, temp = tempfile.mkstemp(prefix='.step66-', suffix='.prom.tmp', dir=target.parent)
    try:
        with os.fdopen(fd, 'w', encoding='ascii') as output:
            output.write(content)
        os.chmod(temp, 0o644)
        os.replace(temp, path)
    finally:
        if os.path.exists(temp):
            os.unlink(temp)


def collect(yarn_bin, spark_bin):
    if os.geteuid() != 0:
        raise RuntimeError('Watchdog wrapper must run as root to write metrics and drop user privileges')
    runuser = shutil.which('runuser')
    if not runuser:
        raise RuntimeError('runuser binary is absent')
    env = dict(os.environ)
    env['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
    env['SPARK_CONF_DIR'] = '/etc/spark/conf'
    env['PYSPARK_PYTHON'] = '/usr/bin/python3'
    env['STEP66_YARN_BIN'] = yarn_bin
    env['STEP66_SPARK_BIN'] = spark_bin
    command = [runuser, '-u', USER, '--', '/usr/bin/python3', CONTROLLER, 'recover']
    proc = subprocess.run(command, capture_output=True, text=True,
                          timeout=330, env=env, check=False)
    if proc.returncode:
        raise RuntimeError('Step65 recovery failed: ' + proc.stderr[-700:])
    lines = [line for line in proc.stdout.splitlines()
             if line.startswith('STEP66_RECOVERY_RESULT: ')]
    if len(lines) != 1:
        raise RuntimeError('Missing exact Step66 recovery JSON; refusal to infer success')
    data = json.loads(lines[0].split(': ', 1)[1])
    expected = {'desired_running', 'running', 'active', 'restarted', 'blocked',
                'attempts_window', 'total_attempts'}
    if not expected.issubset(data):
        raise RuntimeError('Missing recovery metrics: ' + str(expected - data.keys()))
    print(lines[0], flush=True)
    return data


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--metrics-file', required=True)
    parser.add_argument('--yarn-bin', required=True)
    parser.add_argument('--spark-bin', required=True)
    args = parser.parse_args()
    now = int(time.time())
    values = {'watchdog_success': 0, 'desired_running': 0,
              'running': 0, 'active': 0, 'restarted': 0,
              'blocked': 0, 'attempts_window': 0, 'total_attempts': 0}
    error = None
    try:
        report = collect(args.yarn_bin, args.spark_bin)
        values.update({key: report[key] for key in values if key in report})
        values['watchdog_success'] = 1
    except (ValueError, KeyError, OSError, RuntimeError, subprocess.TimeoutExpired) as exc:
        error = exc
    write_atomic(args.metrics_file, textfile(values, now))
    if error:
        print('STEP66_WATCHDOG_FAILED: ' + str(error), file=sys.stderr, flush=True)
        return 1
    print('STEP66_WATCHDOG_METRICS_PASS', flush=True)
    return 0


if __name__ == '__main__':
    sys.exit(main())
