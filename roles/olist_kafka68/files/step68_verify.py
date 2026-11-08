#!/usr/bin/env python3
"""Read-only Step68 validation against the existing Prometheus API."""
import argparse
import json
import sys
import urllib.error
import urllib.parse
import urllib.request

RAW_LAG = (
    'max by (topic, partition) (bigtop_olist_step67_kafka_end_offset) '
    '- on (topic, partition) '
    'max by (topic, partition) (bigtop_olist_step67_spark_committed_offset)'
)


def read_api(base, path, params=None):
    query = '?' + urllib.parse.urlencode(params) if params else ''
    req = urllib.request.Request(base.rstrip('/') + path + query)
    with urllib.request.urlopen(req, timeout=15) as response:
        result = json.load(response)
    if result.get('status') != 'success':
        raise ValueError(f'Prometheus API failed: {path}: {result}')
    return result['data']


def instant(base, expr):
    payload = read_api(base, '/api/v1/query', {'query': expr})
    if payload.get('resultType') != 'vector':
        raise ValueError(f'unexpected query resultType: {expr}')
    return payload.get('result', [])


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--url', default='http://127.0.0.1:9090')
    parser.add_argument('--stale-seconds', type=int, default=240)
    args = parser.parse_args(argv)
    errors = []
    warnings = []
    rules = read_api(args.url, '/api/v1/rules')
    groups = [g for g in rules.get('groups', []) if g.get('name') == 'bigtop-olist-step68']
    if len(groups) != 1:
        errors.append(f'Expected 1 Step68 rule group, got {len(groups)}')
    else:
        print('STEP68_RULE_GROUP_LOADED: bigtop-olist-step68')
        failing_rules = [r.get('name') for r in groups[0].get('rules', [])
                         if r.get('health') == 'err']
        if failing_rules:
            errors.append('Rule evaluation errors: ' + ', '.join(failing_rules))

    success = instant(args.url, 'bigtop_olist_step67_collector_success')
    ages = instant(args.url, 'time() - bigtop_olist_step67_last_run_timestamp_seconds')
    statuses = {v.get('metric', {}).get('source'): float(v['value'][1]) for v in success}
    freshness = {v.get('metric', {}).get('source'): float(v['value'][1]) for v in ages}
    for source in ('spark', 'kafka'):
        if statuses.get(source) != 1.0:
            errors.append(f'{source} collector missing/failed: {statuses.get(source)}')
        if source not in freshness or freshness[source] > args.stale_seconds or freshness[source] < -60:
            errors.append(f'{source} stale/missing collector timestamp: {freshness.get(source)}')
        print(f'STEP68_COLLECTOR_{source.upper()}: success={statuses.get(source)}, age_seconds={freshness.get(source)}')

    lag = instant(args.url, RAW_LAG)
    if not lag:
        errors.append('Raw Kafka minus Spark committed offset query returned no partitions')
    for record in lag:
        labels = record.get('metric', {})
        value = float(record['value'][1])
        print('STEP68_RAW_OFFSET_LAG: topic={topic} partition={partition} lag={lag}'.format(
            topic=labels.get('topic'), partition=labels.get('partition'), lag=value))
        if value < 0:
            errors.append(f'Negative offset lag; inspect stream and Kafka offsets: {labels}={value}')

    recorded = instant(args.url, 'bigtop_olist_step68_offset_lag')
    if not recorded:
        warnings.append('Recording-rule sample not available yet (first evaluation interval may be pending).')
    else:
        print(f'STEP68_RECORDING_SAMPLES: {len(recorded)}')

    for item in warnings:
        print('STEP68_WARN:', item)
    if errors:
        for item in errors:
            print('STEP68_FAIL:', item, file=sys.stderr)
        return 1
    print('STEP68_VERIFY_PASS: rules loaded, collectors healthy and live raw lag returned')
    return 0


if __name__ == '__main__':
    try:
        sys.exit(main())
    except (ValueError, urllib.error.URLError, KeyError, IndexError) as exc:
        print(f'STEP68_FAIL: {exc}', file=sys.stderr)
        sys.exit(1)
