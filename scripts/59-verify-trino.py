#!/usr/bin/env python3
"""Read-only independent Trino checks for Step59 Gold staged/published main."""
import argparse
import hashlib
import json
import sys
import time
from pathlib import Path
from urllib.parse import urlsplit
from urllib.request import Request, urlopen

SOURCE = 'iceberg_rest.olist58_v3.gold_daily_category_sales'
TARGET = 'iceberg_rest.olist59_v3.gold_daily_category_sales'


def request_json(url, sql=None):
    data = sql.encode('utf-8') if sql is not None else None
    headers = {'X-Trino-User': 'step59_verifier', 'Accept': 'application/json'}
    if sql is not None:
        headers['Content-Type'] = 'text/plain; charset=utf-8'
    with urlopen(Request(url, data=data, headers=headers,
                         method='POST' if sql is not None else 'GET'), timeout=40) as stream:
        return json.load(stream)


def sql_rows(base, sql):
    base = base.rstrip('/')
    obj = request_json(base + '/v1/statement', sql)
    rows = []
    deadline = time.monotonic() + 300
    while True:
        if 'error' in obj:
            err = obj['error']
            raise RuntimeError('Trino query failed: ' +
                               str(err.get('errorName')) + ': ' + str(err.get('message')))
        rows.extend(obj.get('data', []))
        next_url = obj.get('nextUri')
        if not next_url:
            return rows
        if time.monotonic() > deadline:
            raise TimeoutError('Trino query deadline exceeded: ' + sql)
        require(urlsplit(next_url).netloc == urlsplit(base).netloc,
                'Untrusted Trino nextUri host')
        time.sleep(.15)
        obj = request_json(next_url)


def require(ok, message):
    if not ok:
        raise ValueError(message)


def validate_proofs(stage, publish=None, stage_bytes=None):
    require(stage.get('schema') == 'bigtop.olist.iceberg-wap-stage.v1' and
            stage.get('status') == 'PASS' and stage.get('target_table') ==
            'shared.olist59_v3.gold_daily_category_sales' and
            stage.get('quality_checks_total') == 29 and
            stage.get('quality_checks_failed') == 0 and
            len(stage.get('checks', [])) == 29 and
            all(c.get('status') == 'PASS' for c in stage['checks']),
            'Invalid Gold WAP staged quality proof')
    count = stage.get('expected_gold_count')
    require(type(count) is int and count > 1, 'Invalid expected Gold count')
    if publish is not None:
        require(publish.get('schema') == 'bigtop.olist.iceberg-wap-publish.v1' and
                publish.get('status') == 'PASS' and
                publish.get('target_table') == stage['target_table'] and
                publish.get('main_snapshot_id') == stage.get('audit_snapshot_id') and
                publish.get('published_rows') == count and
                publish.get('stage_report_sha256') == hashlib.sha256(stage_bytes).hexdigest(),
                'Invalid Gold WAP publication evidence or immutable stage hash')
    return count


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--url', default='http://192.158.2.104:8080')
    parser.add_argument('--mode', choices=('stage', 'publish'), required=True)
    parser.add_argument('--stage-proof', default='artifacts/59-olist-wap-stage.json')
    parser.add_argument('--publish-proof', default='artifacts/59-olist-wap-publish.json')
    args = parser.parse_args()
    require(args.url.startswith(('http://', 'https://')), 'Invalid Trino base URL')
    stage_bytes = Path(args.stage_proof).read_bytes()
    stage = json.loads(stage_bytes)
    publish = json.loads(Path(args.publish_proof).read_bytes()) if args.mode == 'publish' else None
    count = validate_proofs(stage, publish, stage_bytes)
    info = request_json(args.url.rstrip('/') + '/v1/info')
    require(info.get('nodeVersion', {}).get('version') == '484' and
            info.get('state') == 'ACTIVE', 'Expected healthy Trino 484')
    source_stats = sql_rows(args.url,
                            f'SELECT count(*), sum(sold_units), sum(gross_item_total) FROM {SOURCE}')
    require(len(source_stats) == 1 and source_stats[0][0] == count,
            '58 Gold source count changed')
    target_stats = sql_rows(args.url,
                            f'SELECT count(*), sum(sold_units), sum(gross_item_total) FROM {TARGET}')
    if args.mode == 'stage':
        require(target_stats[0][0] == 1, 'Gold main was changed before publication')
        print('STEP59_TRINO_STAGE_MAIN_PROTECTED_PASS: main_rows=1 '
              f'audit_candidate_expected_rows={count}', flush=True)
    else:
        require(source_stats == target_stats, 'Trino published Gold stats mismatch')
        print(f'STEP59_TRINO_GOLD_PUBLISHED_PASS: rows={count}', flush=True)
        print('STEP59_TRINO_SPARK_WAP_CROSS_QUERY_PASS', flush=True)


if __name__ == '__main__':
    try:
        main()
    except (ValueError, RuntimeError, OSError, TimeoutError, KeyError, TypeError) as exc:
        print('STEP59_TRINO_VERIFY_FAILED: ' + str(exc), file=sys.stderr)
        sys.exit(1)
