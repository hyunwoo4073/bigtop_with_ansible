#!/usr/bin/env python3
"""Securely store the ingestion-bot JWT on worker3, without shell history."""
import argparse
import getpass
import os
from pathlib import Path
import tempfile

DEFAULT_FILE = '/etc/bigtop-governance/pg-catalog/bot.token'


def save_token(path: Path, token: str, replace: bool = False):
    if not token or len(token) < 30 or len(token.split('.')) != 3 or any(c.isspace() for c in token):
        raise ValueError('Expected a nonempty JWT with three dot-separated segments')
    if path.is_symlink():
        raise RuntimeError('Refusing symlink token destination')
    if path.exists() and not replace:
        raise FileExistsError('Existing token preserved; use --replace only for intentional rotation')
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.parent.stat().st_mode & 0o077:
        raise PermissionError('Token directory must not be accessible to group or others')
    fd, tmp = tempfile.mkstemp(prefix='.bot-token-', dir=path.parent)
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, 'w', encoding='ascii') as f:
            f.write(token + '\n')
            f.flush()
            os.fsync(f.fileno())
        if replace:
            os.replace(tmp, path)
        else:
            os.link(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--path', default=DEFAULT_FILE)
    ap.add_argument('--replace', action='store_true')
    args = ap.parse_args()
    if os.geteuid() != 0:
        raise SystemExit('Run with sudo on worker3')
    token = getpass.getpass('Paste OpenMetadata ingestion-bot JWT (hidden): ').strip()
    save_token(Path(args.path), token, args.replace)
    print('Bot token stored securely on worker3; no token printed')


if __name__ == '__main__':
    main()
