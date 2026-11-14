#!/usr/bin/env python3
"""Step71 independent end-to-end CDC lab: isolated Kafka -> Spark -> Iceberg.

Never touches Step64/65 or published Gold. Modes: init, run, verify, baseline, verify_replay.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path

TOPIC = 'bigtop_olist_step71_cdc_v1'
TARGET = 'shared.olist71_cdc.fact_cdc_consistency_v1'
NAMESPACE = 'shared.olist71_cdc'
CHECKPOINT = 'hdfs://master:9000/data/olist/iceberg-checkpoints/step71/v1'
BASELINE = '/data/olist/iceberg-validation/step71/v1/_BASELINE_SNAPSHOT.json'
RUN_ID = 'olist-step71-v1'
KEYS = ('step71-order-A', 'step71-order-B', 'step71-order-C')


class GuardError(RuntimeError):
    pass


def require(value, message):
    if not value:
        raise GuardError(message)


def money(number):
    val = Decimal(str(number)).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
    require(val.is_finite() and val >= 0, 'Amount must be nonnegative and finite')
    return str(val)


def event(order_id, seq, op, price, freight):
    price, freight = money(price), money(freight)
    return {'run_id': RUN_ID, 'order_id': order_id, 'order_item_id': 1,
            'event_seq': seq, 'op': op, 'price': price,
            'freight_value': freight, 'item_total': money(Decimal(price) + Decimal(freight))}


def fixture_events():
    """9 events across I/U/D, duplicate and late arrivals. Replay is byte-identical."""
    a1 = event(KEYS[0], 1, 'I', '10', '2')
    a3 = event(KEYS[0], 3, 'U', '15', '2')
    b1 = event(KEYS[1], 1, 'I', '20', '1')
    return [a1, a3, event(KEYS[0], 2, 'U', '12', '2'), dict(a3),
            b1, event(KEYS[1], 2, 'D', '20', '1'), dict(b1),
            event(KEYS[2], 2, 'I', '30', '3'), event(KEYS[2], 1, 'U', '28', '3')]


def validated(e):
    require(isinstance(e, dict) and e.get('run_id') == RUN_ID, 'Invalid Step71 run_id')
    require(e.get('order_id') in KEYS and int(e.get('order_item_id', 0)) == 1,
            'Unexpected key outside isolated fixture')
    require(e.get('op') in ('I', 'U', 'D'), 'Invalid CDC operation')
    seq = int(e['event_seq'])
    require(seq > 0, 'Invalid CDC event sequence')
    p, f, t = money(e['price']), money(e['freight_value']), money(e['item_total'])
    require(Decimal(p) + Decimal(f) == Decimal(t), 'Invalid price + freight')
    return (str(e['order_id']), seq, p, f, t, e['op'] == 'D')


def fold(events):
    """Key+sequence conflicts fail; latest sequence wins, independent of arrival order."""
    latest, signatures = {}, {}
    for raw in events:
        name, seq, p, f, t, deleted = validated(raw)
        signature = (p, f, t, deleted)
        signature_key = (name, seq)
        require(signature_key not in signatures or signatures[signature_key] == signature,
                'Conflicting event with identical key and sequence')
        signatures[signature_key] = signature
        if name not in latest or seq > latest[name][1]:
            latest[name] = (name, seq, p, f, t, deleted)
    return latest


def normalized(rows):
    return sorted((str(r[0]), int(r[1]), money(r[2]), money(r[3]),
                   money(r[4]), bool(r[5])) for r in rows)


def digest(rows):
    return hashlib.sha256(json.dumps(normalized(rows), separators=(',', ':')).encode()).hexdigest()


def expected():
    return normalized(fold(fixture_events()).values())


def actual(spark):
    return normalized([(r['order_id'], r['last_event_seq'], r['price'],
                        r['freight_value'], r['item_total'], r['is_deleted'])
                       for r in spark.table(TARGET).select('order_id', 'last_event_seq',
                           'price', 'freight_value', 'item_total', 'is_deleted').collect()])


def snapshot(spark):
    refs = spark.sql(f'SELECT name, snapshot_id FROM {TARGET}.refs').collect()
    matches = [str(r[1]) for r in refs if str(r[0]) == 'main']
    require(len(matches) == 1, 'Expected precisely one main Iceberg reference')
    return matches[0]


def owned(spark):
    require(spark.catalog.tableExists(TARGET), 'Step71 target table is missing; run apply')
    props = {str(r[0]): str(r[1]) for r in spark.sql(f'SHOW TBLPROPERTIES {TARGET}').collect()}
    require(props.get('bigtop.olist.step') == '71' and
            props.get('bigtop.olist.scope') == 'isolated-cdc-consistency',
            'Refusing to modify a table without Step71 ownership markers')


def init(spark):
    spark.sql(f'CREATE NAMESPACE IF NOT EXISTS {NAMESPACE}')
    if spark.catalog.tableExists(TARGET):
        owned(spark)
        print('STEP71_TABLE_ALREADY_OWNED', flush=True)
        return
    spark.sql(f'''CREATE TABLE {TARGET} (
        order_id STRING, order_item_id INT, price DECIMAL(18,2),
        freight_value DECIMAL(18,2), item_total DECIMAL(18,2),
        last_event_seq BIGINT, is_deleted BOOLEAN
    ) USING iceberg TBLPROPERTIES (
        'bigtop.olist.step'='71',
        'bigtop.olist.scope'='isolated-cdc-consistency'
    )''')
    owned(spark)
    require(len(actual(spark)) == 0, 'New Step71 table is not empty')
    print('STEP71_TABLE_INIT_PASS: isolated empty target', flush=True)


def microbatch(batch, batch_id):
    from pyspark.sql import functions as F
    from pyspark.sql.types import (StructType, StructField, StringType,
                                   IntegerType, DecimalType, LongType, BooleanType)
    spark = batch.sparkSession
    records = batch.select(F.col('value').cast('string').alias('raw'),
                           'topic', 'partition').limit(101).collect()
    require(len(records) <= 100, 'Step71 microbatch size cap 100 exceeded')
    if not records:
        return
    require(all(r['topic'] == TOPIC and int(r['partition']) == 0 for r in records),
            'Step71 only permits topic partition 0')
    try:
        events = [json.loads(r['raw']) for r in records]
    except (ValueError, TypeError) as exc:
        raise GuardError('Invalid JSON in Step71 Kafka') from exc
    latest = fold(events)
    owned(spark)
    saved = {r[0]: r for r in actual(spark)}
    pending = []
    for name in sorted(latest):
        e = latest[name]
        prev = saved.get(name)
        if prev is not None and e[1] == prev[1]:
            require((e[2], e[3], e[4], e[5]) == prev[2:],
                    'Conflicting persisted same-sequence event')
        if prev is None or e[1] > prev[1]:
            pending.append(e)
    if not pending:
        print(f'STEP71_MICROBATCH_NOOP: batch={batch_id} records={len(records)}', flush=True)
        return
    schema = StructType([
        StructField('order_id', StringType(), False),
        StructField('order_item_id', IntegerType(), False),
        StructField('price', DecimalType(18, 2), False),
        StructField('freight_value', DecimalType(18, 2), False),
        StructField('item_total', DecimalType(18, 2), False),
        StructField('event_seq', LongType(), False),
        StructField('is_deleted', BooleanType(), False)])
    values = [(e[0], 1, Decimal(e[2]), Decimal(e[3]), Decimal(e[4]), e[1], e[5])
              for e in pending]
    spark.createDataFrame(values, schema).createOrReplaceTempView('step71_pending')
    spark.sql(f'''MERGE INTO {TARGET} t USING step71_pending s
        ON t.order_id=s.order_id AND t.order_item_id=s.order_item_id
        WHEN MATCHED AND s.event_seq > t.last_event_seq THEN UPDATE SET
          t.price=s.price, t.freight_value=s.freight_value,
          t.item_total=s.item_total, t.last_event_seq=s.event_seq,
          t.is_deleted=s.is_deleted
        WHEN NOT MATCHED THEN INSERT (
          order_id, order_item_id, price, freight_value, item_total,
          last_event_seq, is_deleted
        ) VALUES (
          s.order_id, s.order_item_id, s.price, s.freight_value,
          s.item_total, s.event_seq, s.is_deleted
        )''').collect()
    print(f'STEP71_MICROBATCH_MERGED: batch={batch_id} pending={len(pending)}', flush=True)


def stream(spark, bootstrap):
    owned(spark)
    source = (spark.readStream.format('kafka')
              .option('kafka.bootstrap.servers', bootstrap)
              .option('subscribe', TOPIC)
              .option('startingOffsets', 'earliest')
              .option('failOnDataLoss', 'true')
              .option('maxOffsetsPerTrigger', '100').load())
    query = (source.writeStream.foreachBatch(microbatch)
             .option('checkpointLocation', CHECKPOINT)
             .trigger(availableNow=True).start())
    print('STEP71_STREAM_STARTED: availableNow, persistent isolated checkpoint', flush=True)
    query.awaitTermination()
    require(query.exception() is None, 'Step71 Spark stream failed')
    print('STEP71_STREAM_RUN_PASS', flush=True)


def hdfs(*args):
    result = subprocess.run(['hdfs', 'dfs', *args], capture_output=True, text=True)
    require(result.returncode == 0, 'HDFS operation failed: ' + result.stderr[-500:])
    return result.stdout


def hdfs_exists(path):
    result = subprocess.run(['hdfs', 'dfs', '-test', '-e', path], capture_output=True, text=True)
    require(result.returncode in (0, 1), 'HDFS path check error: ' + result.stderr[-300:])
    return result.returncode == 0


def committed_end():
    text = hdfs('-ls', CHECKPOINT + '/commits')
    ids = []
    for line in text.splitlines():
        last = line.split()[-1].rsplit('/', 1)[-1] if line.split() else ''
        if last.isdigit():
            ids.append(int(last))
    require(ids, 'Step71 stream has no committed batch')
    raw = hdfs('-cat', CHECKPOINT + '/offsets/' + str(max(ids)))
    lines = raw.strip().splitlines()
    require(len(lines) >= 2, 'Invalid Spark checkpoint offsets file')
    metadata = json.loads(lines[-1])
    require(TOPIC in metadata and '0' in metadata[TOPIC],
            'Step71 topic partition 0 absent from checkpoint')
    return int(metadata[TOPIC]['0'])


def kafka_end(spark, bootstrap):
    from pyspark.sql import functions as F
    data = (spark.read.format('kafka')
            .option('kafka.bootstrap.servers', bootstrap)
            .option('subscribe', TOPIC)
            .option('startingOffsets', 'earliest')
            .option('endingOffsets', 'latest').load())
    offsets = data.groupBy('partition').agg(F.max('offset').alias('last')).collect()
    require(len(offsets) == 1 and int(offsets[0]['partition']) == 0,
            'Expected one nonempty Step71 Kafka partition')
    return int(offsets[0]['last']) + 1


def verify(spark, bootstrap, phase):
    owned(spark)
    end = kafka_end(spark, bootstrap)
    expected_end = 18 if phase == 'replay' else 9
    require(end == expected_end, f'Expected Kafka end {expected_end}; observed {end}')
    committed = committed_end()
    require(committed >= end, f'Checkpoint lag: committed={committed} kafka={end}')
    observed = actual(spark)
    require(observed == expected(), f'CDC content mismatch: observed={observed!r}')
    snap = snapshot(spark)
    result = {'phase': phase, 'table': TARGET, 'topic': TOPIC,
              'rows': len(observed), 'live': sum(not r[5] for r in observed),
              'tombstones': sum(r[5] for r in observed),
              'kafka_end': end, 'committed_end': committed,
              'snapshot': snap, 'sha256': digest(observed)}
    require((result['rows'], result['live'], result['tombstones']) == (3, 2, 1),
            'Unexpected CDC row counts')
    if phase == 'replay':
        require(hdfs_exists(BASELINE), 'Missing immutable initial baseline; refuse replay verification')
        saved = json.loads(hdfs('-cat', BASELINE))
        require(saved.get('phase') == 'initial' and
                saved.get('snapshot') == snap and saved.get('sha256') == result['sha256'] and
                saved.get('kafka_end') == 9, 'Replay modified Iceberg snapshot or contents')
    print('STEP71_VERIFY_PASS: ' + json.dumps(result, sort_keys=True), flush=True)
    return result


def save_baseline(result):
    require(not hdfs_exists(BASELINE), 'Baseline already exists: immutable proof may not be overwritten')
    proof = dict(result)
    proof['time_utc'] = datetime.now(timezone.utc).isoformat()
    root = BASELINE.rsplit('/', 1)[0]
    hdfs('-mkdir', '-p', root)
    with tempfile.TemporaryDirectory(prefix='step71-proof-') as td:
        file = Path(td) / 'baseline.json'
        file.write_text(json.dumps(proof, indent=2, sort_keys=True) + '\n')
        tmp = root + '/.tmp-' + os.urandom(8).hex() + '.json'
        hdfs('-put', str(file), tmp)
        require(not hdfs_exists(BASELINE), 'Concurrent baseline creation detected')
        hdfs('-mv', tmp, BASELINE)
    print('STEP71_BASELINE_PASS: ' + BASELINE, flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--mode', choices=('init', 'run', 'verify', 'baseline', 'verify_replay'), required=True)
    parser.add_argument('--bootstrap', default='192.158.2.101:9092,192.158.2.102:9092,192.158.2.103:9092')
    args = parser.parse_args()
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName('olist-step71-' + args.mode).getOrCreate()
    try:
        if args.mode == 'init':
            init(spark)
        elif args.mode == 'run':
            stream(spark, args.bootstrap)
        else:
            result = verify(spark, args.bootstrap, 'replay' if args.mode == 'verify_replay' else 'initial')
            if args.mode == 'baseline':
                save_baseline(result)
    finally:
        spark.stop()


if __name__ == '__main__':
    try:
        main()
    except (GuardError, OSError, ValueError, KeyError, subprocess.TimeoutExpired) as exc:
        print('STEP71_FAILED: ' + str(exc), file=sys.stderr)
        sys.exit(1)
