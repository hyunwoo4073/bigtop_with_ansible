#!/usr/bin/env python3
"""Step69: observational evidence for a Kafka -> Spark Structured Streaming recovery.

Uses only Prometheus HTTP GET and local evidence files. This program does not
launch Spark, write Kafka, mutate HDFS, or change any Iceberg tables.
"""
import argparse
import datetime
import json
import os
import re
import sys
import urllib.error
import urllib.parse
import urllib.request


def query(base, expression):
    qs = urllib.parse.urlencode({'query': expression})
    url = base.rstrip('/') + '/api/v1/query?' + qs
    with urllib.request.urlopen(url, timeout=15) as response:
        result = json.load(response)
    if result.get('status') != 'success' or result.get('data', {}).get('resultType') != 'vector':
        raise ValueError('Bad Prometheus query response: ' + expression)
    return result['data'].get('result', [])


def read_metric(base, expr, expected_label=None):
    result = query(base, expr)
    if expected_label is not None:
        result = [r for r in result if r.get('metric', {}).get('topic') == expected_label]
    return result


def numeric(raw, kind):
    value = float(raw)
    if kind == 'offset':
        if value < 0 or not value.is_integer():
            raise ValueError('Noninteger/negative offset ' + str(value))
        return int(value)
    return value


def single_number(rows, description, kind='float'):
    if len(rows) != 1:
        raise ValueError(f'Expected one {description} sample, found {len(rows)}')
    return numeric(rows[0]['value'][1], kind)


def partition_values(rows, topic, description):
    values = {}
    for row in rows:
        labels = row.get('metric', {})
        if labels.get('topic') != topic:
            continue
        partition = labels.get('partition')
        if partition is None or partition in values:
            raise ValueError(f'Duplicate or missing partition in {description}: {labels}')
        values[partition] = numeric(row['value'][1], 'offset')
    if not values:
        raise ValueError(f'No {description} observations for topic {topic}')
    return values


def capture_snapshot(base, topic, stale_seconds):
    collector_rows = read_metric(base, 'max by (source) (bigtop_olist_step67_collector_success)')
    age_rows = read_metric(base, 'time() - max by (source) (bigtop_olist_step67_last_run_timestamp_seconds)')
    collector = {}
    ages = {}
    for item in collector_rows:
        src = item.get('metric', {}).get('source')
        collector[src] = numeric(item['value'][1], 'float')
    for item in age_rows:
        src = item.get('metric', {}).get('source')
        ages[src] = numeric(item['value'][1], 'float')
    for src in ('kafka', 'spark'):
        if collector.get(src) != 1 or src not in ages or not (-60 <= ages[src] <= stale_seconds):
            raise ValueError(f'Collector {src} missing/failed/stale: success={collector.get(src)}, age={ages.get(src)}')

    kafka_expr = 'max by (topic, partition) (bigtop_olist_step67_kafka_end_offset)'
    spark_expr = 'max by (topic, partition) (bigtop_olist_step67_spark_committed_offset)'
    kafka = partition_values(read_metric(base, kafka_expr, topic), topic, 'Kafka end offset')
    spark = partition_values(read_metric(base, spark_expr, topic), topic, 'Spark committed offset')
    if set(kafka) != set(spark):
        raise ValueError(f'Kafka/Spark partition mismatch: Kafka={sorted(kafka)}, Spark={sorted(spark)}')
    partitions = {}
    for partition in sorted(kafka, key=lambda s: int(s) if s.isdecimal() else s):
        lag = kafka[partition] - spark[partition]
        if lag < 0:
            raise ValueError(f'Negative lag for partition={partition}: {lag}')
        partitions[partition] = {'kafka_end': kafka[partition], 'spark_committed': spark[partition], 'lag': lag}

    batch = single_number(read_metric(base, 'max(bigtop_olist_step67_spark_committed_batch_id)'), 'Spark batch id', 'offset')
    commit_time = single_number(read_metric(base, 'max(bigtop_olist_step67_spark_last_commit_timestamp_seconds)'), 'Spark last commit time')
    return {
        'schema': 1,
        'captured_at_utc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
        'topic': topic,
        'collector': {src: {'success': collector[src], 'age_seconds': ages[src]} for src in ('kafka', 'spark')},
        'spark_batch_id': batch,
        'spark_last_commit_timestamp_seconds': commit_time,
        'partitions': partitions,
    }


def read_json(path):
    with open(path, 'r', encoding='utf-8') as stream:
        return json.load(stream)


def checkpoint_batches(text):
    # HDFS `dfs -ls` output contains full path of each numeric offsets/commits filename.
    batches = set()
    for line in text.splitlines():
        match = re.search(r'/(\d+)\s*$', line)
        if match:
            batches.add(int(match.group(1)))
    return batches


def compare(before, after, chk_before, chk_after, require_input, require_drained):
    errors = []
    warnings = []
    if before['topic'] != after['topic']:
        errors.append('Different Kafka topic across captures')
    if before['partitions'].keys() != after['partitions'].keys():
        errors.append('Partition list changed; cannot compare safely')

    kafka_advanced = False
    spark_advanced = False
    for partition in sorted(set(before['partitions']) & set(after['partitions'])):
        b = before['partitions'][partition]
        a = after['partitions'][partition]
        for key in ('kafka_end', 'spark_committed'):
            if a[key] < b[key]:
                errors.append(f'partition={partition} {key} regressed {b[key]} -> {a[key]}')
        if a['lag'] < 0:
            errors.append(f'partition={partition} negative final lag {a["lag"]}')
        if a['kafka_end'] > b['kafka_end']:
            kafka_advanced = True
        if a['spark_committed'] > b['spark_committed']:
            spark_advanced = True
        if require_drained and a['lag'] != 0:
            errors.append(f'partition={partition} has remaining observed lag={a["lag"]}')
        print(f'STEP69_PARTITION: {partition} kafka={b["kafka_end"]}->{a["kafka_end"]} '
              f'spark={b["spark_committed"]}->{a["spark_committed"]} lag={a["lag"]}')
    if after['spark_batch_id'] < before['spark_batch_id']:
        errors.append('Spark batch id regressed')
    if require_input and not kafka_advanced:
        errors.append('Expected new Kafka input, but no Kafka end offsets advanced')
    if kafka_advanced and not spark_advanced:
        warnings.append('Kafka input grew but Spark committed offset has not advanced yet')
    if not kafka_advanced:
        warnings.append('No new input observed; this is a baseline stability check, NOT replay proof')

    if chk_before['checkpoint'] != chk_after['checkpoint']:
        errors.append('Different checkpoint path across captures')
    for kind in ('offsets', 'commits'):
        b_ids = checkpoint_batches(chk_before[kind + '_ls'])
        a_ids = checkpoint_batches(chk_after[kind + '_ls'])
        if not b_ids or not a_ids:
            errors.append(f'Missing numeric HDFS {kind} files in captured listing')
            continue
        if max(a_ids) < max(b_ids):
            errors.append(f'HDFS {kind} latest batch regressed: {max(b_ids)}->{max(a_ids)}')
        # Files can be compacted/retained; compare highest index, not full file list.
        print(f'STEP69_HDFS_{kind.upper()}: max_batch={max(b_ids)}->{max(a_ids)}')
    a_offsets = checkpoint_batches(chk_after['offsets_ls'])
    a_commits = checkpoint_batches(chk_after['commits_ls'])
    if a_offsets and a_commits and max(a_commits) > max(a_offsets):
        errors.append('HDFS commits latest batch > offsets latest batch')

    if errors:
        for error in errors:
            print('STEP69_COMPARE_FAIL:', error, file=sys.stderr)
        return 1
    for warning in warnings:
        print('STEP69_NOTE:', warning)
    print('STEP69_COMPARE_PASS: no observed progress or checkpoint regression; replay/sink equivalence still requires separate controlled evidence')
    return 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    subs = parser.add_subparsers(dest='mode', required=True)
    for name in ('check', 'capture'):
        sub = subs.add_parser(name)
        sub.add_argument('--url', default='http://127.0.0.1:9090')
        sub.add_argument('--topic', required=True)
        sub.add_argument('--stale-seconds', type=int, default=240)
        if name == 'capture':
            sub.add_argument('--output', required=True)
    comparison = subs.add_parser('compare')
    comparison.add_argument('--before', required=True)
    comparison.add_argument('--after', required=True)
    comparison.add_argument('--checkpoint-before', required=True)
    comparison.add_argument('--checkpoint-after', required=True)
    comparison.add_argument('--require-input-advance', choices=('true', 'false'), default='false')
    comparison.add_argument('--require-drained', choices=('true', 'false'), default='false')
    args = parser.parse_args()
    if args.mode in ('check', 'capture'):
        snapshot = capture_snapshot(args.url, args.topic, args.stale_seconds)
        print(f'STEP69_PROGRESS_PASS: topic={args.topic} batch={snapshot["spark_batch_id"]} '
              f'partitions={len(snapshot["partitions"])}')
        if args.mode == 'capture':
            # O_EXCL avoids accidental replacement of an earlier evidence set.
            fd = os.open(args.output, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(fd, 'w', encoding='utf-8') as stream:
                json.dump(snapshot, stream, ensure_ascii=False, sort_keys=True, indent=2)
                stream.write('\n')
            print('STEP69_CAPTURE_PASS: ' + args.output)
        return 0
    return compare(read_json(args.before), read_json(args.after),
                   read_json(args.checkpoint_before), read_json(args.checkpoint_after),
                   args.require_input_advance == 'true', args.require_drained == 'true')


if __name__ == '__main__':
    try:
        sys.exit(main())
    except (ValueError, KeyError, TypeError, OSError, urllib.error.URLError, IndexError) as exc:
        print('STEP69_FAILED: ' + str(exc), file=sys.stderr)
        sys.exit(1)
