#!/usr/bin/env python3
"""Read-only Trino HTTP protocol smoke validating data originally written by Spark."""
import argparse
import json
import sys
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import Request, urlopen

QUERIES = (
    ('row', 'SELECT id, source FROM iceberg_rest.olist56.catalog_smoke ORDER BY id', [[56, 'spark']]),
    ('count', 'SELECT count(*) FROM iceberg_rest.olist56.catalog_smoke', [[1]]),
)


def json_request(url, *, sql=None, timeout=20):
    payload = sql.encode('utf-8') if sql is not None else None
    headers = {'X-Trino-User': 'step57_smoke', 'Accept': 'application/json'}
    if payload is not None:
        headers['Content-Type'] = 'text/plain; charset=utf-8'
    req = Request(url, data=payload, headers=headers, method='POST' if payload is not None else 'GET')
    with urlopen(req, timeout=timeout) as response:
        return json.load(response)


def execute_sql(base_url, sql, deadline_seconds=180):
    base = base_url.rstrip('/')
    result = json_request(base + '/v1/statement', sql=sql)
    rows = []
    deadline = time.monotonic() + deadline_seconds
    while True:
        if result.get('error'):
            err = result['error']
            raise RuntimeError(f"Trino query failed: {err.get('errorCode')} {err.get('message')} [{err.get('errorName')}]")
        rows.extend(result.get('data', []))
        next_url = result.get('nextUri')
        if not next_url:
            break
        if time.monotonic() > deadline:
            raise TimeoutError('Trino query exceeded smoke deadline')
        if urlsplit(next_url).netloc != urlsplit(base).netloc:
            raise RuntimeError(f'Unexpected Trino nextUri host: {next_url}')
        time.sleep(.15)
        result = json_request(next_url)
    return rows


def main():
    parser = argparse.ArgumentParser(description='Validate Trino reads Spark-created Iceberg smoke data')
    parser.add_argument('--url', default='http://192.158.2.104:8080')
    args = parser.parse_args()
    info = json_request(args.url.rstrip('/') + '/v1/info')
    version = info.get('nodeVersion', {}).get('version')
    if version != '484':
        raise RuntimeError(f'Expected Trino 484, got {version!r}')
    print('STEP57_TRINO_INFO_OK: version=484', flush=True)
    for label, sql, expected in QUERIES:
        found = execute_sql(args.url, sql)
        if found != expected:
            raise RuntimeError(f'Unexpected {label} result: {found!r}, expected {expected!r}')
        print(f'STEP57_ICEBERG_{label.upper()}_PASS: {found}', flush=True)
    print('STEP57_TRINO_SPARK_CROSS_QUERY_PASS', flush=True)


if __name__ == '__main__':
    try:
        main()
    except (OSError, KeyError, ValueError, RuntimeError, TimeoutError) as exc:
        print(f'STEP57_SMOKE_FAIL: {type(exc).__name__}: {exc}', file=sys.stderr)
        sys.exit(1)
