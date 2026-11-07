#!/usr/bin/env python3
"""Create and persist ONE KRaft cluster UUID (22-char URL-safe Kafka form)."""
import base64
import os
import re
import sys
import uuid
from pathlib import Path


def create_or_read(path):
    p = Path(path)
    if p.is_symlink():
        raise RuntimeError('Symlink cluster-id file forbidden')
    try:
        fd = os.open(str(p), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
    except FileExistsError:
        value = p.read_text(encoding='ascii').strip()
        created = False
    else:
        value = base64.urlsafe_b64encode(uuid.uuid4().bytes).decode('ascii').rstrip('=')
        with os.fdopen(fd, 'w', encoding='ascii') as f:
            f.write(value + '\n')
        created = True
    if not re.fullmatch(r'[A-Za-z0-9_-]{22}', value):
        raise RuntimeError('Invalid stored Kafka cluster ID; refusing reformat')
    return value, created


if __name__ == '__main__':
    try:
        result, created = create_or_read(sys.argv[1])
        print('STEP64A_CLUSTER_ID_CREATED' if created else 'STEP64A_CLUSTER_ID_REUSED', result)
    except Exception as exc:
        print('ERROR:', exc, file=sys.stderr)
        raise SystemExit(1)
