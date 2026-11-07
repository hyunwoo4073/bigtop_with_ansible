#!/usr/bin/env python3
"""Fail-closed offline checksum inspection of Kafka 4.2.2 and Step56 Temurin17."""
import hashlib
import re
import sys
from pathlib import Path

KAFKA_NAME = 'kafka_2.13-4.2.2.tgz'
PINNED_KAFKA_SHA512 = (
    'ACB9961F7EC72F5FF1544AFDFBCCA39030F2C9CA0D80E8466CF961BD83AE2030'
    'BAFC28D2577F3F1A099B39C06E6252B8886C1EAF6477F2A2ACA8BE8B68C0B25E'
).lower()


def digest(path, algorithm):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, algorithm).hexdigest()


def checksum(text, chars):
    value = text.strip()
    if chars == 128 and value.startswith(KAFKA_NAME + ':'):
        # Apache's official .sha512 is wrapped into 8-hex-digit groups.
        tokens = value[len(KAFKA_NAME) + 1:].split()
        if not tokens or not all(re.fullmatch(r'[0-9a-fA-F]{8}', x) for x in tokens):
            raise ValueError('Invalid Apache Kafka SHA-512 checksum format')
        candidate = ''.join(tokens)
    else:
        # GNU sha*sum output, or one bare checksum (Temurin JDK17 sidecar).
        pattern = rf'([0-9a-fA-F]{{{chars}}})(?:[ \t]+\*?[^\r\n]+)?'
        match = re.fullmatch(pattern, value)
        if match is None:
            raise ValueError('Invalid checksum file')
        candidate = match.group(1)
    if re.fullmatch(rf'[0-9a-fA-F]{{{chars}}}', candidate) is None:
        raise ValueError('Invalid checksum file')
    return candidate.lower()


def verify(asset, jdk, jdk_sha):
    asset = Path(asset)
    kafka = asset / 'kafka_2.13-4.2.2.tgz'
    kafka_sha = asset / (kafka.name + '.sha512')
    jdk = Path(jdk)
    jdk_sha = Path(jdk_sha)
    for entry in (kafka, kafka_sha, jdk, jdk_sha):
        if not entry.is_file() or entry.is_symlink():
            raise RuntimeError(f'Missing or unsafe artifact: {entry}')
    expected_kafka = checksum(kafka_sha.read_text(encoding='ascii'), 128)
    expected_jdk = checksum(jdk_sha.read_text(encoding='ascii'), 64)
    if expected_kafka != PINNED_KAFKA_SHA512:
        raise RuntimeError('Kafka SHA-512 sidecar differs from pinned Apache 4.2.2 checksum')
    if digest(kafka, 'sha512') != expected_kafka:
        raise RuntimeError('Kafka SHA-512 does not match')
    if digest(jdk, 'sha256') != expected_jdk:
        raise RuntimeError('Temurin17 SHA-256 does not match')
    print('STEP64A_KAFKA_JDK17_ASSETS_VERIFIED')


if __name__ == '__main__':
    try:
        verify(*sys.argv[1:])
    except Exception as exc:
        print('ERROR:', exc, file=sys.stderr)
        raise SystemExit(1)
