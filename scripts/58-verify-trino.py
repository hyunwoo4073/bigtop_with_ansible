#!/usr/bin/env python3
"""Step 58 independent read-only Trino count checks against Spark-generated Iceberg proof."""
import argparse
import json
import sys
import time
from pathlib import Path
from urllib.parse import urlsplit
from urllib.request import Request, urlopen

CATALOG_NAMESPACE = "iceberg_rest.olist58_v3"
EXPECTED_TABLES = (
    "silver_orders", "silver_order_items", "silver_customers", "silver_products",
    "silver_order_item_fact", "quality_quarantine_rows", "gold_daily_category_sales",
)


def request_json(url, *, sql=None, timeout=30):
    payload = sql.encode('utf-8') if sql is not None else None
    headers = {'X-Trino-User': 'step58_verifier', 'Accept': 'application/json'}
    if payload is not None:
        headers['Content-Type'] = 'text/plain; charset=utf-8'
    request = Request(url, headers=headers, data=payload, method='POST' if sql is not None else 'GET')
    with urlopen(request, timeout=timeout) as result:
        return json.load(result)


def execute_sql(base, sql, *, deadline_seconds=300):
    base = base.rstrip('/')
    result = request_json(base + '/v1/statement', sql=sql)
    rows = []
    deadline = time.monotonic() + deadline_seconds
    while True:
        if result.get('error'):
            error = result['error']
            raise RuntimeError(f"Trino SQL failed: {error.get('errorName')}: {error.get('message')}")
        rows.extend(result.get('data', []))
        next_url = result.get('nextUri')
        if not next_url:
            return rows
        if time.monotonic() > deadline:
            raise TimeoutError(f'Trino SQL deadline exceeded: {sql}')
        if urlsplit(next_url).netloc != urlsplit(base).netloc:
            raise RuntimeError(f'Unexpected Trino nextUri host: {next_url}')
        time.sleep(.15)
        result = request_json(next_url)


def validate_proof(obj):
    if not isinstance(obj, dict) or obj.get('status') != 'PASS':
        raise ValueError('Missing passing Spark proof')
    if obj.get('schema') != 'bigtop.olist.iceberg-materialization.v1':
        raise ValueError('Unexpected Step58 proof schema')
    if obj.get('source_version') != 'v3' or obj.get('namespace') != 'olist58_v3':
        raise ValueError('Wrong source or target namespace')
    if obj.get('parity') != 'bidirectional_exceptAll_exact_multiset':
        raise ValueError('Spark exact parity proof required')
    counts = obj.get('tables')
    if not isinstance(counts, dict) or set(counts) != set(EXPECTED_TABLES):
        raise ValueError('Unexpected table inventory in Step58 proof')
    for table in EXPECTED_TABLES:
        if type(counts[table]) is not int or counts[table] < 0:
            raise ValueError('Invalid row count in Step58 proof')
    if counts['gold_daily_category_sales'] == 0:
        raise ValueError('Expected nonempty Olist Gold mart')
    return counts


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--url', default='http://192.158.2.104:8080')
    parser.add_argument('--proof', default='artifacts/58-iceberg-v3-proof.json')
    args = parser.parse_args()
    if not args.url.startswith('http://') and not args.url.startswith('https://'):
        raise ValueError('Trino URL must be HTTP or HTTPS')
    counts = validate_proof(json.loads(Path(args.proof).read_text(encoding='utf-8')))
    info = request_json(args.url.rstrip('/') + '/v1/info')
    if info.get('nodeVersion', {}).get('version') != '484' or info.get('state') != 'ACTIVE':
        raise RuntimeError('Expected active Trino 484')
    print('STEP58_TRINO_READY: 484', flush=True)
    for table in EXPECTED_TABLES:
        sql = f'SELECT count(*) FROM {CATALOG_NAMESPACE}.{table}'
        rows = execute_sql(args.url, sql)
        if rows != [[counts[table]]]:
            raise RuntimeError(f'Row mismatch {table}: Trino {rows!r}, Spark {counts[table]}')
        print(f'STEP58_TRINO_TABLE_PASS: {table} rows={counts[table]}', flush=True)
    # Separate SQL operator test, beyond simply listing catalog table names or doing counts.
    rows = execute_sql(args.url,
        f'SELECT SUM(sold_units) FROM {CATALOG_NAMESPACE}.gold_daily_category_sales')
    if len(rows) != 1 or len(rows[0]) != 1 or not isinstance(rows[0][0], int) or rows[0][0] <= 0:
        raise RuntimeError(f'Gold SUM(sold_units) returned invalid result: {rows!r}')
    print(f'STEP58_TRINO_GOLD_AGG_PASS: sold_units={rows[0][0]}', flush=True)
    print('STEP58_SPARK_TRINO_SILVER_GOLD_CROSS_QUERY_PASS', flush=True)


if __name__ == '__main__':
    try:
        main()
    except (OSError, ValueError, RuntimeError, TimeoutError, KeyError) as exc:
        print(f'STEP58_TRINO_VERIFY_FAIL: {exc}', file=sys.stderr, flush=True)
        sys.exit(1)
