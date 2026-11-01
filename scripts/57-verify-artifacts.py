#!/usr/bin/env python3
"""Offline verification of pinned Step 57 binaries before privileged installation."""
import json
from pathlib import Path
import re
import sys
from importlib.machinery import SourceFileLoader

TRINO_SHA256 = '4b2d05f2e52fcc156b2f137cabc886353e5dde782d12bf71e290ffc13463b81a'


def validate(directory, sha256_file=None):
    if sha256_file is None:
        import hashlib
        def sha256_file(path):
            h = hashlib.sha256()
            with open(path, 'rb') as src:
                for chunk in iter(lambda: src.read(1024 * 1024), b''):
                    h.update(chunk)
            return h.hexdigest()
    path = Path(directory)
    manifest = json.loads((path / 'manifest.json').read_text(encoding='utf-8'))
    assert manifest['trino_version'] == '484', 'Wrong Trino version'
    assert manifest['trino_archive_sha256'] == TRINO_SHA256, 'Incorrect pinned Trino SHA256'
    assert manifest['jdk_major'] == 25, 'Expected Temurin Java 25'
    assert re.fullmatch(r'[a-f0-9]{64}', manifest['jdk_archive_sha256']), 'Invalid JDK checksum'
    assert (path / 'trino-server-484.tar.gz').is_file(), 'Trino tarball missing'
    assert (path / 'temurin25-jdk-linux-x64.tar.gz').is_file(), 'JDK tarball missing'
    assert sha256_file(path / 'trino-server-484.tar.gz') == TRINO_SHA256, 'Trino checksum differs'
    assert sha256_file(path / 'temurin25-jdk-linux-x64.tar.gz') == manifest['jdk_archive_sha256'], 'JDK checksum differs'
    return manifest


def main():
    directory = sys.argv[1] if len(sys.argv) > 1 else 'vendor/trino57'
    try:
        manifest = validate(directory)
        print(f'STEP57_ARTIFACTS_VERIFIED: Trino {manifest["trino_version"]}, Temurin {manifest["jdk_version"]}')
    except (OSError, ValueError, KeyError, AssertionError) as err:
        print(f'STEP57_ARTIFACTS_INVALID: {err}', file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()
