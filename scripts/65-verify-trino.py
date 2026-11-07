#!/usr/bin/env python3
"""Cross-engine read-only Trino 484 validation of Step65 CDC and snapshots."""
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
CORE = ROOT / 'roles/olist_kafka65/files/olist_kafka65.py'
SOURCE = 'iceberg_rest.olist64_kafka.fact_cdc_stream_v1'
TARGET = 'iceberg_rest.olist65_stream.fact_cdc_running_v1'


def require(ok, reason):
    if not ok:
        raise ValueError(reason)


def read_json(path):
    return json.loads(Path(path).read_text())


def core():
    spec = importlib.util.spec_from_file_location('step65_trino_core', CORE)
    require(spec and spec.loader, 'Missing Step65 core module')
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def fetch(url, sql=None):
    headers = {'X-Trino-User': 'step65_verifier', 'Accept': 'application/json'}
    if sql is not None:
        headers['Content-Type'] = 'text/plain; charset=utf-8'
    with urlopen(Request(url, data=sql.encode() if sql is not None else None,
                         headers=headers, method='POST' if sql is not None else 'GET'),
                 timeout=40) as resp:
        return json.load(resp)


def query(base, sql):
    state = fetch(base.rstrip('/') + '/v1/statement', sql)
    result = []
    deadline = time.monotonic() + 300
    while True:
        if state.get('error'):
            error = state['error']
            raise RuntimeError(f"Trino {error.get('errorName')}: {error.get('message')}")
        result.extend(state.get('data', []))
        nxt = state.get('nextUri')
        if not nxt:
            return result
        require(urlsplit(nxt).netloc == urlsplit(base).netloc,
                'Untrusted Trino nextUri')
        require(time.monotonic() <= deadline, 'Trino query timeout')
        time.sleep(0.15)
        state = fetch(nxt)


def snapshot(url, table):
    catalog, schema, name = table.split('.')
    values = query(url,
        f'''SELECT snapshot_id FROM {catalog}.{schema}."{name}$refs"
            WHERE name='main' ''')
    require(len(values) == 1, 'Missing or ambiguous Iceberg main ref')
    return str(values[0][0])


def table_rows(url, table):
    return query(url, f'''SELECT order_id, order_item_id, price, freight_value,
        item_total, last_event_seq, is_deleted FROM {table}
        ORDER BY order_id, order_item_id''')


def verify(url, seed, produce, applied):
    c = core()
    info = fetch(url.rstrip('/') + '/v1/info')
    require(info.get('state') == 'ACTIVE' and
            info.get('nodeVersion', {}).get('version') == '484',
            'Trino 484 not active')
    require(seed.get('status') == 'PASS' and
            seed.get('schema') == 'bigtop.olist.kafka-stream-seed.v1' and
            seed.get('table') == c.TARGET and seed.get('rows') == 520,
            'Invalid Step65 seed proof')
    require(produce.get('status') == 'PASS' and
            produce.get('schema') == 'bigtop.olist.kafka-stream-produce.v1' and
            produce.get('topic') == c.TOPIC and produce.get('messages') == 12,
            'Invalid Step65 produce proof')
    require(applied.get('status') == 'PASS' and
            applied.get('schema') == 'bigtop.olist.kafka-stream-apply.v1' and
            applied.get('table') == c.TARGET and applied.get('rows') == 521 and
            applied.get('live_rows') == 512 and applied.get('tombstones') == 9 and
            applied.get('checkpoint') == c.CHECKPOINT,
            'Invalid Step65 CDC apply proof')
    require(snapshot(url, SOURCE) == str(seed['source_snapshot']),
            'Step64 protected source snapshot drift')
    require(snapshot(url, TARGET) == str(applied['main_snapshot']),
            'Step65 snapshot changed after verified ingest')
    source = table_rows(url, SOURCE)
    require(len(source) == 520 and c.digest(source) == seed['source_digest'],
            'Step64 protected source content drift')
    result = table_rows(url, TARGET)
    expected = c.digest(c.expected_after(source))
    require(len(result) == 521 and c.digest(result) == expected == applied['digest_sha256'],
            'Trino Step65 CDC row digest mismatch')
    require(sum(bool(row[6]) for row in result) == 9 and
            sum(not bool(row[6]) for row in result) == 512,
            'Trino Step65 live/tombstone mismatch')
    print('STEP65_TRINO_KAFKA_CDC_ROWS_PASS: total=521 live=512 tombstones=9')
    print('STEP65_SPARK_KAFKA_ICEBERG_TRINO_PASS')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--url', default='http://192.158.2.104:8080')
    parser.add_argument('--seed-proof', default='artifacts/65-kafka-cdc-seed.json')
    parser.add_argument('--produce-proof', default='artifacts/65-kafka-cdc-produce.json')
    parser.add_argument('--apply-proof', default='artifacts/65-kafka-cdc-apply.json')
    args = parser.parse_args()
    host = urlsplit(args.url)
    require(host.scheme in ('http', 'https') and bool(host.netloc)
            and host.path in ('', '/'), 'Invalid Trino base URL')
    verify(args.url, read_json(args.seed_proof),
           read_json(args.produce_proof), read_json(args.apply_proof))


if __name__ == '__main__':
    try:
        main()
    except (OSError, ValueError, TypeError, KeyError, RuntimeError) as exc:
        print('STEP65_TRINO_VERIFY_FAILED: ' + str(exc), file=sys.stderr)
        sys.exit(1)
