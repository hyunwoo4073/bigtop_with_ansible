#!/usr/bin/env python3
"""Offline SHA256 validation of Step64 pinned Spark/Kafka jars."""
from __future__ import annotations
import hashlib
import json
import sys
from pathlib import Path

EXPECTED = {
    'spark-sql-kafka-0-10_2.12-3.5.6.jar',
    'spark-token-provider-kafka-0-10_2.12-3.5.6.jar',
    'kafka-clients-3.4.1.jar',
    'commons-pool2-2.11.1.jar',
    'jsr305-3.0.0.jar',
}


def verify(folder):
    p = Path(folder)
    obj = json.loads((p / 'manifest.json').read_text())
    if set(obj) != EXPECTED:
        raise ValueError('Incomplete/wrong Step64 dependency manifest')
    for name, record in obj.items():
        file = p / name
        content = file.read_bytes()
        if hashlib.sha256(content).hexdigest() != record['sha256'] or len(content) != record['size']:
            raise ValueError('JAR SHA256 mismatch: ' + name)
    print('STEP64_KAFKA_JARS_VERIFIED', flush=True)


if __name__ == '__main__':
    try:
        verify(sys.argv[1] if len(sys.argv) > 1 else 'vendor/kafka64')
    except (ValueError, OSError, KeyError, TypeError) as exc:
        print('STEP64_KAFKA_JARS_FAIL: ' + str(exc), file=sys.stderr)
        sys.exit(1)
