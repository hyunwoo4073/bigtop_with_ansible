#!/usr/bin/env python3
"""Read-only Trino 484 data/snapshot verification of the Step64 Kafka CDC lab."""
from __future__ import annotations
import argparse
import importlib.util
import json
import sys
import time
from pathlib import Path
from urllib.parse import urlsplit
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]
CORE = ROOT / 'roles/olist_iceberg_kafka64/files/olist_iceberg_kafka64.py'
TARGET = 'iceberg_rest.olist64_kafka.fact_cdc_stream_v1'
SOURCE = 'iceberg_rest.olist63_cdc.fact_change_lab'
GOLD = 'iceberg_rest.olist59_v3.gold_daily_category_sales'


def require(ok, msg):
    if not ok:
        raise ValueError(msg)


def canonical():
    spec = importlib.util.spec_from_file_location('step64_core', CORE)
    require(spec is not None and spec.loader is not None, 'Missing canonical Step64 module')
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def fetch(url, sql=None):
    headers = {'X-Trino-User': 'step64_verifier', 'Accept': 'application/json'}
    if sql is not None:
        headers['Content-Type'] = 'text/plain; charset=utf-8'
    with urlopen(Request(url, data=sql.encode() if sql is not None else None,
                         headers=headers, method='POST' if sql is not None else 'GET'),
                 timeout=40) as resp:
        return json.load(resp)


def query(base, sql):
    next_state = fetch(base.rstrip('/') + '/v1/statement', sql)
    deadline = time.monotonic() + 300
    rows = []
    while True:
        if next_state.get('error'):
            e = next_state['error']
            raise RuntimeError(f"Trino {e.get('errorName')}: {e.get('message')}")
        rows.extend(next_state.get('data', []))
        next_uri = next_state.get('nextUri')
        if not next_uri:
            return rows
        require(urlsplit(next_uri).netloc == urlsplit(base).netloc,
                'Unexpected Trino redirect destination')
        require(time.monotonic() <= deadline, 'Trino query timeout')
        time.sleep(0.15)
        next_state = fetch(next_uri)


def one_ref(url, table):
    catalog, namespace, short = table.split('.')
    refs = query(url, f'''SELECT snapshot_id FROM {catalog}.{namespace}."{short}$refs"
                            WHERE name='main' ''')
    require(len(refs) == 1, 'Missing/ambiguous Iceberg main Snapshot')
    return str(refs[0][0])


def rows(url, table):
    return query(url, f'''SELECT order_id, order_item_id, price, freight_value,
                              item_total, last_event_seq, is_deleted
                         FROM {table} ORDER BY order_id, order_item_id''')


def validate_proofs(seed, produced, ingested, c):
    require(seed.get('schema') == 'bigtop.olist.kafka-cdc-seed.v1' and
            seed.get('status') == 'PASS' and seed.get('table') == c.TARGET and
            seed.get('rows') == 516, 'Invalid seed proof')
    c.check_produce_proof(produced, {'source_snapshot': seed['source_snapshot']})
    require(produced.get('events_digest_sha256') and
            len(produced['events_digest_sha256']) == 64, 'Missing producer digest')
    require(ingested.get('schema') == 'bigtop.olist.kafka-cdc-ingest.v1' and
            ingested.get('status') == 'PASS' and ingested.get('table') == c.TARGET and
            ingested.get('rows') == 520 and ingested.get('live_rows') == 512 and
            ingested.get('tombstones') == 8 and
            ingested.get('checkpoint') == c.CHECKPOINT and
            ingested.get('seed_snapshot') == seed['main_snapshot'] and
            ingested.get('source_snapshot') == seed['source_snapshot'] and
            ingested.get('gold_snapshot') == seed['gold_snapshot'],
            'Invalid Kafka ingest proof')


def verify(url, seed, produced, ingested):
    c = canonical()
    validate_proofs(seed, produced, ingested, c)
    info = fetch(url.rstrip('/') + '/v1/info')
    require(info.get('state') == 'ACTIVE' and
            info.get('nodeVersion', {}).get('version') == '484',
            'Trino 484 is not healthy')
    require(one_ref(url, GOLD) == seed['gold_snapshot'], 'Step59 Gold main drift')
    require(one_ref(url, SOURCE) == seed['source_snapshot'], 'Step63 source main drift')
    require(one_ref(url, TARGET) == ingested['main_snapshot'],
            'Step64 CDC target Snapshot changed after verified ingest')
    baseline = rows(url, SOURCE)
    require(len(baseline) == 516 and c.normalized_digest(baseline) == seed['digest_sha256'],
            'Trino Step63 source does not match immutable baseline')
    require(c.events_digest(c.event_payloads(baseline)) == produced['events_digest_sha256'],
            'Kafka producer events differ from deterministic Step63 baseline')
    expected = c.normalized_digest(c.final_rows(baseline))
    actual = rows(url, TARGET)
    require(len(actual) == 520 and
            c.normalized_digest(actual) == expected == ingested['digest_sha256'],
            'Trino 520 CDC rows do not match full Spark/expected data')
    require(sum(bool(r[6]) for r in actual) == 8 and
            sum(not bool(r[6]) for r in actual) == 512,
            'Trino CDC tombstone/live counts are incorrect')
    print('STEP64_TRINO_KAFKA_CDC_ROWS_PASS: total=520 live=512 tombstones=8', flush=True)
    print('STEP64_SPARK_KAFKA_ICEBERG_TRINO_PASS', flush=True)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--url', default='http://192.158.2.104:8080')
    p.add_argument('--seed-proof', default='artifacts/64-kafka-cdc-seed.json')
    p.add_argument('--produce-proof', default='artifacts/64-kafka-cdc-produce.json')
    p.add_argument('--ingest-proof', default='artifacts/64-kafka-cdc-ingest.json')
    args = p.parse_args()
    parsed = urlsplit(args.url)
    require(parsed.scheme in ('http', 'https') and bool(parsed.netloc)
            and parsed.path in ('', '/'), 'Invalid Trino URL')
    seed = json.loads(Path(args.seed_proof).read_text(encoding='utf-8'))
    produced = json.loads(Path(args.produce_proof).read_text(encoding='utf-8'))
    ingested = json.loads(Path(args.ingest_proof).read_text(encoding='utf-8'))
    verify(args.url, seed, produced, ingested)


if __name__ == '__main__':
    try:
        main()
    except (OSError, ValueError, RuntimeError, TypeError, KeyError) as exc:
        print('STEP64_TRINO_VERIFY_FAIL: ' + str(exc), file=sys.stderr, flush=True)
        sys.exit(1)
