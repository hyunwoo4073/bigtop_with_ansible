#!/usr/bin/env python3
"""Read-only Trino 484 cross-engine verification of Step62 isolated Iceberg maintenance.

Inspect mode only checks protected Gold. Lab mode additionally checks the immutable
Step62 lab proof and exact Trino row count, aggregates, file count and main ref.
"""
import argparse
import json
import sys
import time
from pathlib import Path
from urllib.parse import urlsplit
from urllib.request import Request, urlopen

GOLD = 'iceberg_rest.olist59_v3.gold_daily_category_sales'
LAB = 'iceberg_rest.olist62_ops.maintenance_smoke'


def require(ok, msg):
    if not ok:
        raise ValueError(msg)


def request_json(url, sql=None):
    data = sql.encode('utf-8') if sql is not None else None
    headers = {'X-Trino-User': 'step62_verifier', 'Accept': 'application/json'}
    if data is not None:
        headers['Content-Type'] = 'text/plain; charset=utf-8'
    with urlopen(Request(url, data=data, headers=headers,
                         method='POST' if data is not None else 'GET'), timeout=40) as result:
        return json.load(result)


def query(base, sql):
    base = base.rstrip('/')
    state = request_json(base + '/v1/statement', sql=sql)
    deadline = time.monotonic() + 300
    rows = []
    while True:
        if state.get('error'):
            error = state['error']
            raise RuntimeError(f"Trino error {error.get('errorName')}: {error.get('message')}")
        rows += state.get('data', [])
        nxt = state.get('nextUri')
        if not nxt:
            return rows
        require(urlsplit(nxt).netloc == urlsplit(base).netloc,
                'Unexpected Trino nextUri target')
        require(time.monotonic() <= deadline, 'Trino SQL took too long')
        time.sleep(.15)
        state = request_json(nxt)


def validate_lab(obj):
    require(obj.get('schema') == 'bigtop.olist.iceberg-maintenance-lab.v1' and
            obj.get('status') == 'PASS' and
            obj.get('table') == 'shared.olist62_ops.maintenance_smoke' and
            obj.get('rows') == 2048 and
            type(obj.get('before_files')) is int and
            type(obj.get('after_files')) is int and
            0 < obj['after_files'] < obj['before_files'] and
            obj.get('main_snapshot') == obj.get('compacted_snapshot'),
            'Incorrect Step62 proof')
    return obj


def validate_publish(obj):
    require(obj.get('schema') == 'bigtop.olist.iceberg-wap-publish.v1' and
            obj.get('status') == 'PASS' and
            obj.get('target_table') == 'shared.olist59_v3.gold_daily_category_sales' and
            type(obj.get('published_rows')) is int and obj['published_rows'] > 1 and
            str(obj.get('main_snapshot_id', '')).isdigit(),
            'Incorrect Step59 publication proof')
    return obj


def verify_gold(url, pub):
    rows = query(url, f'SELECT count(*) FROM {GOLD}')
    require(rows == [[pub['published_rows']]], 'Published Gold row count mismatch')
    r = query(url, f'SELECT snapshot_id FROM iceberg_rest.olist59_v3."gold_daily_category_sales$refs" WHERE name = \'main\'')
    require(len(r) == 1 and str(r[0][0]) == str(pub['main_snapshot_id']),
            'Gold main reference changed since publication')
    print(f"STEP62_GOLD_PROTECTED_PASS: rows={pub['published_rows']} snapshot={pub['main_snapshot_id']}", flush=True)


def verify_lab(url, proof):
    validate_lab(proof)
    rows = query(url, f'SELECT count(*), min(id), max(id), sum(amount) FROM {LAB}')
    expected_sum = sum((i * 13) % 10000 for i in range(2048))
    require(rows == [[2048, 0, 2047, expected_sum]],
            'Lab rows or aggregate mismatch after Spark compaction+rollback')
    files = query(url, 'SELECT count(*) FROM iceberg_rest.olist62_ops."maintenance_smoke$files" WHERE content=0')
    require(files == [[proof['after_files']]], 'Trino data-file count differs from Spark')
    refs = query(url, "SELECT snapshot_id FROM iceberg_rest.olist62_ops.\"maintenance_smoke$refs\" WHERE name='main'")
    require(len(refs) == 1 and str(refs[0][0]) == str(proof['main_snapshot']),
            'Trino main snapshot differs from rollback proof')
    print(f"STEP62_TRINO_LAB_PASS: rows=2048 files={proof['after_files']}", flush=True)
    print('STEP62_SPARK_TRINO_MAINTENANCE_CROSS_QUERY_PASS', flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--mode', choices=('inspect', 'lab'), default='lab')
    parser.add_argument('--url', default='http://192.158.2.104:8080')
    parser.add_argument('--gold-proof', default='artifacts/59-olist-wap-publish.json')
    parser.add_argument('--lab-proof', default='artifacts/62-iceberg-maintenance-lab.json')
    args = parser.parse_args()
    parsed = urlsplit(args.url)
    require(parsed.scheme in ('http', 'https') and bool(parsed.netloc) and
            parsed.path in ('', '/'), 'Invalid Trino URL')
    info = request_json(args.url.rstrip('/') + '/v1/info')
    require(info.get('state') == 'ACTIVE' and
            info.get('nodeVersion', {}).get('version') == '484',
            'Expected healthy Trino 484')
    pub = validate_publish(json.loads(Path(args.gold_proof).read_text(encoding='utf-8')))
    verify_gold(args.url, pub)
    if args.mode == 'lab':
        proof = validate_lab(json.loads(Path(args.lab_proof).read_text(encoding='utf-8')))
        require(proof['protected_gold_snapshot'] == str(pub['main_snapshot_id']),
                'Step62 proof does not bind to approved Gold main')
        verify_lab(args.url, proof)
    else:
        print('STEP62_TRINO_READ_ONLY_INSPECT_PASS', flush=True)


if __name__ == '__main__':
    try:
        main()
    except (OSError, ValueError, RuntimeError, TimeoutError, TypeError, KeyError) as exc:
        print('STEP62_TRINO_VERIFY_FAIL: ' + str(exc), file=sys.stderr, flush=True)
        sys.exit(1)
