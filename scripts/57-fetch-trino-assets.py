#!/usr/bin/env python3
"""Download only Trino 484 + Temurin 25 for offline Ansible; verify SHA-256."""
import hashlib
import json
import os
from pathlib import Path
import re
import sys
import urllib.request

TRINO_SHA256 = '4b2d05f2e52fcc156b2f137cabc886353e5dde782d12bf71e290ffc13463b81a'
TRINO_URL = 'https://github.com/trinodb/trino/releases/download/484/trino-server-484.tar.gz'
JDK_API = 'https://api.adoptium.net/v3/assets/latest/25/hotspot?architecture=x64&image_type=jdk&os=linux&vendor=eclipse'


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, 'rb') as stream:
        for chunk in iter(lambda: stream.read(2 * 1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()


def request(url):
    req = urllib.request.Request(url, headers={'User-Agent': 'bigtop-trino57-offline-fetch/1.0', 'Accept': 'application/json'})
    return urllib.request.urlopen(req, timeout=90)


def jdk_asset(metadata):
    if not isinstance(metadata, list):
        raise ValueError('Unexpected Adoptium API payload')
    for record in metadata:
        binary = record.get('binary', {})
        package = binary.get('package', {})
        checksum = package.get('checksum', '')
        url = package.get('link', '')
        version = record.get('version', {}).get('semver', '')
        if (re.fullmatch('[a-fA-F0-9]{64}', checksum)
                and url.startswith('https://')
                and (version.startswith('25.0.') or version.startswith('25+'))):
            if version.startswith('25.0.') and int(version.split('.')[2].split('+')[0]) < 1:
                continue
            return {'url': url, 'sha256': checksum.lower(), 'version': version}
    raise RuntimeError('No supported Temurin Java 25.0.1+ x64 JDK asset available')


def download_verified(path, url, expected):
    if path.exists():
        found = sha256_file(path)
        if found == expected:
            print(f'EXISTS VERIFIED: {path}', flush=True)
            return
        raise RuntimeError(f'Existing file SHA256 mismatch: {path}; inspect it before retrying')
    temp = path.with_suffix(path.suffix + '.part')
    try:
        with request(url) as src, open(temp, 'wb') as dst:
            h = hashlib.sha256()
            while True:
                chunk = src.read(2 * 1024 * 1024)
                if not chunk:
                    break
                h.update(chunk)
                dst.write(chunk)
        if h.hexdigest() != expected:
            raise RuntimeError(f'Unexpected checksum for {path.name}; expected {expected}, got {h.hexdigest()}')
        os.replace(temp, path)
        print(f'DOWNLOADED VERIFIED: {path}', flush=True)
    finally:
        temp.unlink(missing_ok=True)


def main():
    directory = Path(sys.argv[1] if len(sys.argv) > 1 else 'vendor/trino57').resolve()
    directory.mkdir(parents=True, exist_ok=True)
    print('Fetching Trino 484 and dedicated Java 25; large files, requires internet + disk space.', flush=True)
    # Download Trino first; pin digest to the 484 Homebrew formula published checksum.
    download_verified(directory / 'trino-server-484.tar.gz', TRINO_URL, TRINO_SHA256)
    with request(JDK_API) as response:
        asset = jdk_asset(json.load(response))
    download_verified(directory / 'temurin25-jdk-linux-x64.tar.gz', asset['url'], asset['sha256'])
    manifest = {
        'trino_version': '484',
        'trino_archive_sha256': TRINO_SHA256,
        'jdk_major': 25,
        'jdk_version': asset['version'],
        'jdk_archive_sha256': asset['sha256'],
        'jdk_source_url': asset['url'],
    }
    (directory / 'manifest.json').write_text(json.dumps(manifest, indent=2, sort_keys=True) + '\n', encoding='utf-8')
    print(f'STEP57_FETCH_COMPLETE: {directory}', flush=True)


if __name__ == '__main__':
    try:
        main()
    except Exception as exc:
        print(f'STEP57_FETCH_FAILED: {type(exc).__name__}: {exc}', file=sys.stderr)
        sys.exit(1)
