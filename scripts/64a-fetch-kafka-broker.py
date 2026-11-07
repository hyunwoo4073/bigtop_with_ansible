#!/usr/bin/env python3
"""Download Kafka 4.2.2 server tarball with official Apache SHA-512 verification.

Writes only to vendor/kafka-broker64. Run on Internet-connected host, then copy
vendor/ to air-gapped controller. JDK17 is reused from Step 56.
"""
from __future__ import annotations
import hashlib
import re
import sys
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DEST = ROOT / 'vendor' / 'kafka-broker64'
NAME = 'kafka_2.13-4.2.2.tgz'
BASE = 'https://downloads.apache.org/kafka/4.2.2/'
# Verified against the Apache Kafka 4.2.2 official release checksum.
PINNED_SHA512 = (
    'ACB9961F7EC72F5FF1544AFDFBCCA39030F2C9CA0D80E8466CF961BD83AE2030'
    'BAFC28D2577F3F1A099B39C06E6252B8886C1EAF6477F2A2ACA8BE8B68C0B25E'
).lower()


def read_text(url):
    req = urllib.request.Request(url, headers={'User-Agent': 'bigtop-kafka-kraft64/1'})
    with urllib.request.urlopen(req, timeout=90) as response:
        return response.read().decode('ascii')


def parse_checksum(text):
    """Parse both Apache multi-line SHA-512 and GNU sha512sum formats.

    Apache publishes `filename: AAAAAAAA BBBBBBBB ...` split over several
    lines. The format is validated *before* whitespace is removed, preventing
    arbitrary text from being mistaken for a checksum.
    """
    value = text.strip()
    apache_prefix = NAME + ':'
    if value.startswith(apache_prefix):
        tokens = value[len(apache_prefix):].split()
        if not tokens or not all(re.fullmatch(r'[0-9a-fA-F]{8}', t) for t in tokens):
            raise ValueError('Invalid Apache Kafka SHA-512 checksum format')
        candidate = ''.join(tokens)
    else:
        match = re.fullmatch(
            r'([0-9a-fA-F]{128})(?:[ \t]+\*?' + re.escape(NAME) + r')?', value
        )
        if match is None:
            raise ValueError('Invalid Kafka SHA-512 checksum format')
        candidate = match.group(1)
    if re.fullmatch(r'[0-9a-fA-F]{128}', candidate) is None:
        raise ValueError('Invalid official Kafka SHA-512 checksum length')
    return candidate.lower()


def digest_file(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha512').hexdigest()


def fetch():
    DEST.mkdir(parents=True, exist_ok=True)
    sidecar = DEST / (NAME + '.sha512')
    if sidecar.exists():
        expected = parse_checksum(sidecar.read_text(encoding='ascii'))
    else:
        data = read_text(BASE + NAME + '.sha512')
        expected = parse_checksum(data)
        sidecar.write_text(data, encoding='ascii')
    if expected != PINNED_SHA512:
        raise RuntimeError('Kafka SHA-512 sidecar differs from pinned Apache 4.2.2 checksum')
    archive = DEST / NAME
    if archive.exists():
        if digest_file(archive) != expected:
            raise RuntimeError('Local Kafka tarball checksum differs. Investigate, do not overwrite.')
        print('EXISTS VERIFIED:', archive)
        return
    part = archive.with_suffix(archive.suffix + '.part')
    req = urllib.request.Request(BASE + NAME, headers={'User-Agent': 'bigtop-kafka-kraft64/1'})
    try:
        with urllib.request.urlopen(req, timeout=90) as src, part.open('wb') as out:
            while True:
                chunk = src.read(1024 * 1024)
                if not chunk:
                    break
                out.write(chunk)
        if digest_file(part) != expected:
            raise RuntimeError('Kafka tarball SHA-512 mismatch; download rejected.')
        part.replace(archive)
        print('DOWNLOADED VERIFIED:', archive)
    finally:
        part.unlink(missing_ok=True)


if __name__ == '__main__':
    try:
        fetch()
        print('STEP64A_KAFKA_ARCHIVE_VERIFIED')
    except Exception as exc:
        print('ERROR:', exc, file=sys.stderr)
        raise SystemExit(1)
