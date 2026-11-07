#!/usr/bin/env python3
"""Trino 484 cross-engine validation of isolated Step63 incremental CDC MERGE lab."""
from __future__ import annotations

import argparse
import importlib.util
import json
import sys
import time
from pathlib import Path
from urllib.parse import urlsplit
from urllib.request import Request, urlopen

TARGET = 'iceberg_rest.olist63_cdc.fact_change_lab'
SOURCE = 'iceberg_rest.olist58_v3.silver_order_item_fact'
GOLD = 'iceberg_rest.olist59_v3.gold_daily_category_sales'

# Share identical canonical decimal/row digest semantics with Spark's standalone script.
UTIL = Path(__file__).resolve().parents[1] / 'roles/olist_iceberg_incremental63/files/olist_iceberg_incremental63.py'


def require(condition, msg):
    if not condition:
        raise ValueError(msg)


def load_canonical():
    spec = importlib.util.spec_from_file_location('step63_canonical', str(UTIL))
    require(spec is not None and spec.loader is not None, 'Missing Step63 Spark canonical module')
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def fetch(url, sql=None):
    data = sql.encode('utf-8') if sql is not None else None
    headers = {'X-Trino-User': 'step63_verifier', 'Accept': 'application/json'}
    if data is not None:
        headers['Content-Type'] = 'text/plain; charset=utf-8'
    with urlopen(Request(url, data=data, headers=headers,
                         method='POST' if data is not None else 'GET'), timeout=40) as response:
        return json.load(response)


def query(base, sql):
    state = fetch(base.rstrip('/') + '/v1/statement', sql)
    deadline = time.monotonic() + 300
    all_rows = []
    while True:
        if state.get('error'):
            error = state['error']
            raise RuntimeError(f"Trino {error.get('errorName')}: {error.get('message')}")
        all_rows.extend(state.get('data', []))
        next_uri = state.get('nextUri')
        if not next_uri:
            return all_rows
        require(urlsplit(next_uri).netloc == urlsplit(base).netloc,
                'Unexpected Trino nextUri target')
        require(time.monotonic() <= deadline, 'Trino query timeout')
        time.sleep(0.15)
        state = fetch(next_uri)


def validate_proofs(seed, applied):
    c = load_canonical()
    c.validate_proof(seed, 'seed')
    c.validate_proof(applied, 'apply')
    require(applied.get('seed_snapshot') == seed['main_snapshot'] and
            applied.get('source_snapshot') == seed['source_snapshot'] and
            applied.get('gold_snapshot') == seed['gold_snapshot'] and
            applied.get('duplicate_replay_skipped') is True and
            applied.get('late_events_skipped') is True,
            'Step63 applied proof does not match the immutable seed')
    return c


def one_ref(url, table, name='main'):
    catalog, namespace, short = table.split('.')
    refs_table = f'{catalog}.{namespace}."{short}$refs"'
    rows = query(url, f"SELECT snapshot_id FROM {refs_table} WHERE name='{name}'")
    require(len(rows) == 1, f'Missing unique {name} ref: {table}')
    return str(rows[0][0])


def verify(url, seed, applied):
    canonical = validate_proofs(seed, applied)
    info = fetch(url.rstrip('/') + '/v1/info')
    require(info.get('state') == 'ACTIVE' and
            info.get('nodeVersion', {}).get('version') == '484',
            'Expected healthy Trino 484')
    require(one_ref(url, GOLD) == seed['gold_snapshot'], 'Protected Gold snapshot drift')
    require(one_ref(url, SOURCE) == seed['source_snapshot'], 'Step58 fact snapshot drift')
    require(one_ref(url, TARGET) == applied['main_snapshot'], 'CDC table main drift')
    rows = query(url, f'''SELECT order_id, order_item_id, price, freight_value,
                                item_total, last_event_seq, is_deleted
                         FROM {TARGET}
                         ORDER BY order_id, order_item_id''')
    require(len(rows) == applied['rows'], 'Trino CDC row count mismatch')
    digest = canonical.normalized_digest(rows)
    require(digest == applied['digest_sha256'], 'Trino rows differ from Spark canonical digest')
    require(sum(1 for r in rows if bool(r[6])) == applied['deleted_rows'],
            'Trino tombstone count mismatch')
    require(sum(1 for r in rows if not bool(r[6])) == applied['live_rows'],
            'Trino live CDC count mismatch')
    print(f"STEP63_TRINO_CDC_ROWS_PASS: total={len(rows)} live={applied['live_rows']} "
          f"tombstones={applied['deleted_rows']}", flush=True)
    print('STEP63_SPARK_TRINO_INCREMENTAL_CROSS_QUERY_PASS', flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--url', default='http://192.158.2.104:8080')
    parser.add_argument('--seed-proof', default='artifacts/63-iceberg-cdc-seed.json')
    parser.add_argument('--apply-proof', default='artifacts/63-iceberg-cdc-apply.json')
    args = parser.parse_args()
    parsed = urlsplit(args.url)
    require(parsed.scheme in ('http', 'https') and bool(parsed.netloc)
            and parsed.path in ('', '/'), 'Invalid Trino base URL')
    seed = json.loads(Path(args.seed_proof).read_text(encoding='utf-8'))
    applied = json.loads(Path(args.apply_proof).read_text(encoding='utf-8'))
    verify(args.url, seed, applied)


if __name__ == '__main__':
    try:
        main()
    except (OSError, RuntimeError, ValueError, TypeError, KeyError) as exc:
        print('STEP63_TRINO_VERIFY_FAIL: ' + str(exc), file=sys.stderr, flush=True)
        sys.exit(1)
