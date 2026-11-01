#!/usr/bin/env python3
"""Fetch pinned Gravitino tar + Apache PostgreSQL JDBC jar + Temurin JDK17 into ignored vendor/."""
import hashlib
import json
import sys
import urllib.request
from pathlib import Path

HERE = Path(__file__).resolve().parents[1] / 'vendor' / 'iceberg-rest'
GRAVITINO = 'gravitino-iceberg-rest-server-1.3.1-bin.tar.gz'
GRAVITINO_SHA256 = '46524669213374b60243e7fd4b75709cbeb45ce78fd0d88fd1ac797d928ee8ba'
PG = 'postgresql-42.7.11.jar'
# Pinned checksum for PostgreSQL JDBC 42.7.11. Maven Central may not provide
# a .jar.sha256 sidecar; downloading that URL previously returned HTTP 404.
# Verify the downloaded JAR against this independently recorded SHA-256.
PG_SHA256 = '1981b31d3993c58702783c1cddf10a34e48c1f413d70ff1cb6def0a143484647'
JDK = 'temurin17-jdk-linux-x64.tar.gz'


def read_url(url):
    req = urllib.request.Request(url, headers={'User-Agent': 'bigtop-iceberg-rest-setup/56'})
    with urllib.request.urlopen(req, timeout=60) as resp:
        return resp.read()


def fetch_file(url, target, sha256):
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        with target.open('rb') as already:
            actual = hashlib.file_digest(already, 'sha256').hexdigest()
        if actual == sha256:
            print('EXISTS VERIFIED:', target)
            return
        raise RuntimeError(f'Existing file checksum differs; inspect it manually: {target}')
    tmp = target.with_suffix(target.suffix + '.part')
    h = hashlib.sha256()
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'bigtop-iceberg-rest-setup/56'})
        with urllib.request.urlopen(req, timeout=60) as response, tmp.open('wb') as dst:
            while True:
                block = response.read(1024 * 1024)
                if not block: break
                h.update(block); dst.write(block)
        if h.hexdigest() != sha256:
            raise RuntimeError('SHA256 mismatch downloading ' + str(target))
        tmp.replace(target)
        print('DOWNLOADED VERIFIED:', target)
    except Exception:
        tmp.unlink(missing_ok=True)
        raise


def main():
    HERE.mkdir(parents=True, exist_ok=True)
    print('Downloading ~100 MB Gravitino, PostgreSQL driver, and portable Temurin JDK17.')
    fetch_file('https://github.com/apache/gravitino/releases/download/v1.3.1/' + GRAVITINO,
               HERE / GRAVITINO, GRAVITINO_SHA256)
    base = 'https://repo.maven.apache.org/maven2/org/postgresql/postgresql/42.7.11/'
    fetch_file(base + PG, HERE / PG, PG_SHA256)
    (HERE / (PG + '.sha256')).write_text(f'{PG_SHA256}  {PG}\n')
    api = ('https://api.adoptium.net/v3/assets/latest/17/hotspot?'
           'architecture=x64&image_type=jdk&os=linux&vendor=eclipse')
    assets = json.loads(read_url(api))
    asset = next(x for x in assets if x.get('binary', {}).get('package', {}).get('link'))
    package = asset['binary']['package']
    fetch_file(package['link'], HERE / JDK, package['checksum'])
    (HERE / (JDK + '.sha256')).write_text(f'{package["checksum"]}  {JDK}\n')
    print('STEP56_FETCH_COMPLETE:', HERE)


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print('Download failed:', e, file=sys.stderr)
        raise SystemExit(1)
