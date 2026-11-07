#!/usr/bin/env python3
"""Fetch pinned Spark 3.5.6 Structured Streaming Kafka artifacts for offline YARN deployment.

Downloads from Maven Central HTTPS, validates publisher SHA1 sidecars and stores
local SHA256 manifest for subsequent offline verification. SHA1 sidecar is not a
signature; use a trusted internal repository and approved artifacts in production.
"""
from __future__ import annotations
import hashlib
import json
import os
import sys
import tempfile
from pathlib import Path
from urllib.request import urlopen

ROOT = Path(__file__).resolve().parents[1]
DEST = ROOT / 'vendor' / 'kafka64'
BASE = os.environ.get('STEP64_MAVEN_BASE', 'https://repo.maven.apache.org/maven2').rstrip('/')
ARTIFACTS = (
    ('org/apache/spark', 'spark-sql-kafka-0-10_2.12', '3.5.6'),
    ('org/apache/spark', 'spark-token-provider-kafka-0-10_2.12', '3.5.6'),
    ('org/apache/kafka', 'kafka-clients', '3.4.1'),
    ('org/apache/commons', 'commons-pool2', '2.11.1'),
    ('com/google/code/findbugs', 'jsr305', '3.0.0'),
)


def sha(data, alg='sha256'):
    return hashlib.new(alg, data).hexdigest()


def artifact_urls():
    for group, artifact, version in ARTIFACTS:
        filename = f'{artifact}-{version}.jar'
        yield filename, f'{BASE}/{group}/{artifact}/{version}/{filename}'


def fetch(url):
    if not url.startswith('https://'):
        raise RuntimeError('Maven mirror must use HTTPS')
    with urlopen(url, timeout=75) as resp:
        return resp.read()


def run():
    DEST.mkdir(parents=True, exist_ok=True)
    manifest = {}
    for filename, url in artifact_urls():
        expected_sha1 = fetch(url + '.sha1').decode('ascii').strip().split()[0].lower()
        if len(expected_sha1) != 40:
            raise RuntimeError('Invalid Maven SHA1 sidecar: ' + filename)
        destination = DEST / filename
        content = destination.read_bytes() if destination.exists() else fetch(url)
        if sha(content, 'sha1') != expected_sha1:
            raise RuntimeError('Maven SHA1 mismatch for ' + filename)
        if not destination.exists():
            with tempfile.NamedTemporaryFile(dir=DEST, delete=False) as fp:
                fp.write(content)
                temporary = Path(fp.name)
            temporary.replace(destination)
        manifest[filename] = {'sha256': sha(content), 'sha1': expected_sha1,
                              'source_url': url, 'size': len(content)}
        print(f'VERIFIED: {filename} ({len(content):,} bytes)', flush=True)
    (DEST / 'manifest.json').write_text(json.dumps(manifest, indent=2, sort_keys=True) + '\n')
    print('STEP64_KAFKA_JARS_READY: ' + str(DEST), flush=True)


if __name__ == '__main__':
    try:
        run()
    except Exception as exc:
        print('STEP64_KAFKA_ASSET_ERROR: ' + str(exc), file=sys.stderr)
        sys.exit(1)
