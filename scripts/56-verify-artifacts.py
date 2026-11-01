#!/usr/bin/env python3
"""Verify artifact sha256 sidecars and pinned Apache Gravitino release."""
import hashlib
import sys
from pathlib import Path

PIN = {'gravitino-iceberg-rest-server-1.3.1-bin.tar.gz': '46524669213374b60243e7fd4b75709cbeb45ce78fd0d88fd1ac797d928ee8ba'}
FILES = ('gravitino-iceberg-rest-server-1.3.1-bin.tar.gz', 'postgresql-42.7.11.jar', 'temurin17-jdk-linux-x64.tar.gz')


def digest(path):
    h = hashlib.sha256()
    with path.open('rb') as f:
        for c in iter(lambda: f.read(1024 * 1024), b''):
            h.update(c)
    return h.hexdigest()


def validate(directory):
    for name in FILES:
        file = directory / name
        if not file.is_file() or file.stat().st_size < 100000:
            raise ValueError(f'archive missing/too small: {file}')
        expected = PIN.get(name)
        if expected is None:
            sidecar = directory / (name + '.sha256')
            data = sidecar.read_text().strip().split()
            if len(data) != 2 or data[1].lstrip('*') != name:
                raise ValueError(f'invalid checksum sidecar: {sidecar}')
            expected = data[0]
        actual = digest(file)
        if actual.lower() != expected.lower():
            raise ValueError(f'sha256 mismatch for {name}')
    return True


if __name__ == '__main__':
    try:
        validate(Path(sys.argv[1]))
        print('STEP56_ARTIFACTS_VERIFIED')
    except (ValueError, OSError, IndexError) as e:
        raise SystemExit(str(e))
