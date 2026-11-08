#!/usr/bin/env python3
"""Step67 read-only Kafka latest offsets / committed Spark checkpoint metrics.

Runs as a short-lived systemd oneshot on worker3 (Kafka) and master (HDFS).
Never submits a Spark app, changes a checkpoint, or reads a Kafka payload.
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
import time

KAFKA_BIN = '/opt/bigtop-kafka-kraft64/kafka/bin/kafka-get-offsets.sh'
JAVA17 = '/opt/bigtop-kafka-kraft64/jdk17'
DEFAULT_TOPIC = 'bigtop_olist_step65_cdc_v1'
DEFAULT_BOOTSTRAP = '192.158.2.101:9092,192.158.2.102:9092,192.158.2.103:9092'
DEFAULT_CHECKPOINT = 'hdfs://master:9000/data/olist/iceberg-checkpoints/step65/v1'
METRIC = 'bigtop_olist_step67_'


class CollectionError(RuntimeError):
    pass


def require(condition, reason):
    if not condition:
        raise CollectionError(reason)


def call(argv, *, timeout=35, env=None):
    result = subprocess.run(argv, text=True, capture_output=True,
                            timeout=timeout, check=False, env=env)
    if result.returncode:
        raise CollectionError('command failed: ' + str(argv[0]) + ' ' +
                              result.stderr[-450:])
    return result.stdout


def kafka_latest(topic, bootstrap):
    require(Path(JAVA17 + '/bin/java').is_file(), 'dedicated Java17 missing')
    require(Path(KAFKA_BIN).is_file(), 'Kafka get-offsets CLI missing')
    env = os.environ.copy()
    env['JAVA_HOME'] = JAVA17
    env['PATH'] = JAVA17 + '/bin:' + env.get('PATH', '')
    env['KAFKA_HEAP_OPTS'] = '-Xms64m -Xmx192m'
    output = call([KAFKA_BIN, '--bootstrap-server', bootstrap,
                   '--topic', topic, '--time', '-1'], timeout=45, env=env)
    # Step65 is intentionally a single-partition topic; do not silently aggregate
    # a changed topology or parse unrelated stdout/warnings as offsets.
    values = []
    for line in output.splitlines():
        line = line.strip()
        if not line:
            continue
        match = re.fullmatch(re.escape(topic) + r':(\d+):(\d+)', line)
        require(match is not None, 'unrecognized Kafka offset line: ' + line)
        values.append((int(match.group(1)), int(match.group(2))))
    require(len(values) == 1 and values[0][0] == 0,
            'Step65 topic must have exactly partition 0')
    return values[0][1]


def hdfs_call(hdfs_bin, *args):
    runuser = shutil.which('runuser')
    require(runuser is not None, 'runuser binary missing')
    env = os.environ.copy()
    env.update({'HADOOP_CONF_DIR': '/etc/hadoop/conf',
                'SPARK_CONF_DIR': '/etc/spark/conf'})
    return call([runuser, '-u', 'hadoop', '--', hdfs_bin, 'dfs', *args], env=env)


def parse_commit_ids(listing):
    batches = []
    for line in listing.splitlines():
        columns = line.split()
        if len(columns) < 8 or not columns[0].startswith('-'):
            continue
        name = columns[-1].rsplit('/', 1)[-1]
        if re.fullmatch(r'\d+', name):
            batches.append(int(name))
    require(batches, 'No committed Spark batch found')
    return max(batches)


def parse_committed_offset(content, topic):
    lines = [line.strip() for line in content.splitlines() if line.strip()]
    require(len(lines) >= 3 and lines[0].startswith('v'),
            'Unexpected Spark offset metadata layout')
    # Spark offsets/<committed-batch-id>: v1, metadata JSON, Kafka offset JSON.
    payload = json.loads(lines[-1])
    require(isinstance(payload, dict) and isinstance(payload.get(topic), dict),
            'Kafka topic absent from Spark commit-matched offset file')
    partitions = payload[topic]
    require(set(partitions) == {'0'}, 'Unexpected Kafka partition layout in checkpoint')
    offset = partitions['0']
    require(isinstance(offset, int) and not isinstance(offset, bool) and offset >= 0,
            'Invalid committed Kafka offset')
    return offset


def spark_committed(topic, checkpoint, hdfs_bin):
    batch = parse_commit_ids(hdfs_call(hdfs_bin, '-ls', checkpoint + '/commits'))
    content = hdfs_call(hdfs_bin, '-cat', checkpoint + '/offsets/' + str(batch))
    offset = parse_committed_offset(content, topic)
    mtime = hdfs_call(hdfs_bin, '-stat', '%Y', checkpoint + '/commits/' + str(batch)).strip()
    require(re.fullmatch(r'\d+', mtime) is not None, 'Invalid HDFS commit mtime')
    mtime_seconds = int(mtime) / 1000.0
    require(0 < mtime_seconds < time.time() + 600, 'Invalid/future HDFS commit timestamp')
    return batch, offset, mtime_seconds


def labels(topic, partition=0):
    require(re.fullmatch(r'[A-Za-z0-9._-]+', topic) is not None, 'Unsafe topic metric label')
    return '{topic="' + topic + '",partition="' + str(partition) + '"}'


def format_metrics(source, topic, started, values):
    common = '{source="' + source + '"}'
    out = [f'{METRIC}collector_success{common} ' + ('1' if values else '0'),
           f'{METRIC}last_run_timestamp_seconds{common} {started}']
    if values:
        key = labels(topic)
        if source == 'kafka':
            out.append(f'{METRIC}kafka_end_offset{key} {values["offset"]}')
        else:
            out.extend([f'{METRIC}spark_committed_offset{key} {values["offset"]}',
                        f'{METRIC}spark_committed_batch_id {values["batch"]}',
                        f'{METRIC}spark_last_commit_timestamp_seconds {values["mtime"]:.3f}'])
    return '\n'.join(out) + '\n'


def write_atomic(filename, content):
    path = Path(filename)
    require(path.parent.is_dir(), 'Missing Node Exporter textfile directory')
    fd, tmp = tempfile.mkstemp(prefix='.step67-', suffix='.prom.tmp', dir=path.parent)
    try:
        with os.fdopen(fd, 'w', encoding='ascii') as output:
            output.write(content)
        os.chmod(tmp, 0o644)
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', choices=['kafka', 'spark'], required=True)
    parser.add_argument('--metrics-file', required=True)
    parser.add_argument('--topic', default=DEFAULT_TOPIC)
    parser.add_argument('--bootstrap', default=DEFAULT_BOOTSTRAP)
    parser.add_argument('--checkpoint', default=DEFAULT_CHECKPOINT)
    parser.add_argument('--hdfs-bin', default='/usr/bin/hdfs')
    args = parser.parse_args(argv)
    now = int(time.time())
    data = None
    error = None
    try:
        if args.source == 'kafka':
            data = {'offset': kafka_latest(args.topic, args.bootstrap)}
        else:
            batch, offset, mtime = spark_committed(args.topic, args.checkpoint, args.hdfs_bin)
            data = {'batch': batch, 'offset': offset, 'mtime': mtime}
    except (CollectionError, OSError, ValueError, TypeError, KeyError,
            json.JSONDecodeError, subprocess.TimeoutExpired) as exc:
        error = exc
    # Replace previous good offset metrics with success=0 on failed reads,
    # preventing apparently fresh/stale-but-successful offsets from being joined.
    write_atomic(args.metrics_file, format_metrics(args.source, args.topic, now, data))
    if error:
        print('STEP67_COLLECT_FAILED: ' + str(error), file=sys.stderr)
        return 1
    print('STEP67_' + args.source.upper() + '_METRICS_PASS: ' + str(data))
    return 0


if __name__ == '__main__':
    sys.exit(main())
