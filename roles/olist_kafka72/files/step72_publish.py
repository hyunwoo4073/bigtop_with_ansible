#!/usr/bin/env python3
"""Step72 one-shot 9-message publisher with exact partition-offset safety guards."""
import argparse
import json
import re
import subprocess
import sys
from pathlib import Path
from step72_cdc import TOPIC, GuardError, fixture_events, require


def checked(*argv, input_text=None):
    p = subprocess.run(argv, input=input_text, text=True, capture_output=True, timeout=120)
    require(p.returncode == 0,
            f'Kafka CLI failed: {Path(argv[0]).name}: {p.stderr[-600:]}')
    return p.stdout


def kafka_end(kafka_dir, bootstrap):
    output = checked(kafka_dir + '/kafka-get-offsets.sh',
                     '--bootstrap-server', bootstrap, '--topic', TOPIC)
    values = []
    for line in output.splitlines():
        line = line.strip()
        match = re.fullmatch(r'([^:]+):(\d+):(\d+)', line)
        if match:
            values.append(match)
    require(len(values) == 1 and values[0].group(1) == TOPIC and
            values[0].group(2) == '0',
            'Step72 requires exactly one partition and recognized Kafka end offsets')
    return int(values[0].group(3))


def publish(mode, kafka_dir, bootstrap):
    expected = 0
    before = kafka_end(kafka_dir, bootstrap)
    require(before == expected,
            f'{mode} guard: expected Kafka end={expected}, observed {before}; no changes made')
    events = fixture_events()
    require(len(events) == 9, 'Step72 fixture must contain nine events')
    payload = ''.join(json.dumps(e, sort_keys=True, separators=(',', ':')) + '\n'
                      for e in events)
    checked(kafka_dir + '/kafka-console-producer.sh',
            '--bootstrap-server', bootstrap, '--topic', TOPIC,
            '--producer-property', 'acks=all',
            '--producer-property', 'enable.idempotence=true',
            input_text=payload)
    after = kafka_end(kafka_dir, bootstrap)
    require(after == before + 9,
            f'Expected exactly 9 published messages; offsets before={before} after={after}')
    print(f'STEP72_{mode.upper()}_PASS: offsets={before}->{after} messages=9', flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--mode', choices=('publish',), required=True)
    parser.add_argument('--bootstrap', required=True)
    parser.add_argument('--kafka-bin', default='/opt/bigtop-kafka-kraft64/kafka/bin')
    args = parser.parse_args()
    publish(args.mode, args.kafka_bin, args.bootstrap)


if __name__ == '__main__':
    try:
        main()
    except (GuardError, OSError, ValueError, subprocess.TimeoutExpired) as exc:
        print('STEP72_PUBLISH_FAILED: ' + str(exc), file=sys.stderr)
        sys.exit(1)
