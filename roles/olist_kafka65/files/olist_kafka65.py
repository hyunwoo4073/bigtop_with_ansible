#!/usr/bin/env python3
"""Step65: isolated persistent Kafka -> Structured Streaming -> Iceberg CDC lab.

Owns ONLY shared.olist65_stream.fact_cdc_running_v1. Never changes Step64/Gold.
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

SOURCE = 'shared.olist64_kafka.fact_cdc_stream_v1'
TARGET = 'shared.olist65_stream.fact_cdc_running_v1'
NAMESPACE = 'shared.olist65_stream'
TOPIC = 'bigtop_olist_step65_cdc_v1'
RUN_ID = 'olist-step65-v1'
STEP64_PROOF = '/data/olist/iceberg-validation/step64/v1/_INGEST_REPORT.json'
ROOT = '/data/olist/iceberg-validation/step65/v1'
SEED_PROOF = ROOT + '/_SEED_REPORT.json'
PRODUCE_PROOF = ROOT + '/_PRODUCE_REPORT.json'
APPLY_PROOF = ROOT + '/_APPLY_REPORT.json'
CHECKPOINT = 'hdfs://master:9000/data/olist/iceberg-checkpoints/step65/v1'
APP_NAME = 'olist-iceberg-kafka65-continuous'


class GuardError(RuntimeError):
    pass


def require(condition, message):
    if not condition:
        raise GuardError(message)


def money(value):
    return str(Decimal(str(value)).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP))


def normalized(row):
    require(len(row) == 7, 'Unexpected CDC target schema')
    return (str(row[0]), int(row[1]), money(row[2]), money(row[3]),
            money(row[4]), int(row[5]), bool(row[6]))


def digest(rows):
    data = sorted((normalized(r) for r in rows), key=lambda r: (r[0], r[1]))
    require(len(data) == len({r[:2] for r in data}), 'Duplicate CDC primary key')
    return hashlib.sha256(json.dumps(data, separators=(',', ':')).encode()).hexdigest()


def rows(spark, table):
    return [tuple(r) for r in spark.table(table).select(
        'order_id', 'order_item_id', 'price', 'freight_value',
        'item_total', 'last_event_seq', 'is_deleted').collect()]


def snapshot(spark, table):
    matches = [r for r in spark.sql(f'SELECT name, snapshot_id FROM {table}.refs').collect()
               if str(r[0]) == 'main']
    require(len(matches) == 1, 'Missing main snapshot: ' + table)
    return str(matches[0][1])


def hdfs(*args):
    p = subprocess.run(['hdfs', 'dfs', *args], capture_output=True, check=False)
    require(p.returncode == 0, 'HDFS command failed: ' + ' '.join(args[:2]) + ' '
            + p.stderr.decode('utf-8', 'replace')[-350:])
    return p.stdout


def exists(path):
    p = subprocess.run(['hdfs', 'dfs', '-test', '-e', path], capture_output=True)
    require(p.returncode in (0, 1) and not p.stderr,
            'HDFS path check failed: ' + path)
    return p.returncode == 0


def read_proof(path):
    proof = json.loads(hdfs('-cat', path).decode('utf-8'))
    require(isinstance(proof, dict) and proof.get('status') == 'PASS',
            'Invalid proof: ' + path)
    return proof


def write_proof(path, proof):
    require(path in (SEED_PROOF, PRODUCE_PROOF, APPLY_PROOF), 'Unowned proof path')
    require(not exists(path), 'Immutable proof already exists: ' + path)
    hdfs('-mkdir', '-p', ROOT)
    with tempfile.TemporaryDirectory(prefix='olist65-') as directory:
        local = Path(directory) / 'proof.json'
        local.write_text(json.dumps(proof, sort_keys=True, indent=2) + '\n')
        stage = ROOT + '/.tmp-' + os.urandom(12).hex() + '.json'
        hdfs('-put', str(local), stage)
        require(not exists(path), 'Concurrent proof creation')
        hdfs('-mv', stage, path)


def ensure_source(spark):
    proof = read_proof(STEP64_PROOF)
    require(proof.get('schema') == 'bigtop.olist.kafka-cdc-ingest.v1'
            and proof.get('table') == SOURCE
            and proof.get('rows') == 520
            and proof.get('live_rows') == 512
            and proof.get('tombstones') == 8,
            'Step64 verified source proof missing or invalid')
    source_rows = rows(spark, SOURCE)
    require(len(source_rows) == 520 and digest(source_rows) == proof['digest_sha256']
            and snapshot(spark, SOURCE) == str(proof['main_snapshot']),
            'Step64 protected source drift; refuse Step65 change')
    return source_rows, proof


def ensure_owned(spark):
    require(spark.catalog.tableExists(TARGET), 'Step65 target not seeded')
    props = {str(r[0]): str(r[1]) for r in
             spark.sql(f'SHOW TBLPROPERTIES {TARGET}').collect()}
    require(props.get('bigtop.olist.step') == '65' and
            props.get('bigtop.olist.scope') == 'isolated-continuous-kafka-cdc',
            'Refusing to mutate an unowned table')


def ensure_seed(spark, source_proof):
    proof = read_proof(SEED_PROOF)
    require(proof.get('schema') == 'bigtop.olist.kafka-stream-seed.v1'
            and proof.get('table') == TARGET
            and proof.get('rows') == 520
            and proof.get('source_snapshot') == source_proof['main_snapshot']
            and proof.get('source_digest') == source_proof['digest_sha256'],
            'Step65 seed evidence mismatch')
    ensure_owned(spark)
    return proof


def seed(spark, source_rows, source_proof):
    if exists(SEED_PROOF):
        ensure_seed(spark, source_proof)
        print('STEP65_ALREADY_SEEDED: immutable proof valid', flush=True)
        return
    require(not spark.catalog.tableExists(TARGET),
            'Step65 table exists without seed proof; manual reconciliation needed')
    spark.sql(f'CREATE NAMESPACE IF NOT EXISTS {NAMESPACE}')
    spark.sql(f'''CREATE TABLE {TARGET} USING iceberg
        TBLPROPERTIES ('bigtop.olist.step'='65',
                       'bigtop.olist.scope'='isolated-continuous-kafka-cdc')
        AS SELECT order_id, order_item_id, price, freight_value, item_total,
                  last_event_seq, is_deleted FROM {SOURCE}''')
    ensure_owned(spark)
    require(digest(rows(spark, TARGET)) == digest(source_rows), 'Seed content drift')
    write_proof(SEED_PROOF, {'status': 'PASS',
        'schema': 'bigtop.olist.kafka-stream-seed.v1', 'table': TARGET,
        'rows': 520, 'digest_sha256': digest(source_rows),
        'main_snapshot': snapshot(spark, TARGET),
        'source_snapshot': source_proof['main_snapshot'],
        'source_digest': source_proof['digest_sha256'],
        'time_utc': datetime.now(timezone.utc).isoformat()})
    print('STEP65_SEED_PASS: rows=520', flush=True)


def validate_event(e):
    require(isinstance(e, dict) and e.get('run_id') == RUN_ID,
            'Unexpected Kafka event/run_id')
    require(e.get('op') in ('I', 'U', 'D'), 'Invalid CDC op')
    key = (str(e['order_id']), int(e['order_item_id']))
    seq = int(e['event_seq'])
    require(seq > 0, 'Invalid event sequence')
    p, f, t = money(e['price']), money(e['freight_value']), money(e['item_total'])
    require(Decimal(p) + Decimal(f) == Decimal(t), 'Invalid CDC amount')
    return key, seq, (e['op'], p, f, t)


def newest(events):
    latest, signatures = {}, {}
    for e in events:
        key, seq, signature = validate_event(e)
        require((key, seq) not in signatures or signatures[key, seq] == signature,
                'Conflicting same key+sequence event')
        signatures[key, seq] = signature
        if key not in latest or seq > latest[key]['event_seq']:
            latest[key] = e
    return [latest[key] for key in sorted(latest)]


def events_for(source_rows):
    living = sorted((normalized(r) for r in source_rows if not r[6]),
                    key=lambda r: (r[0], r[1]))
    require(len(living) == 512, 'Baseline live count mismatch')
    events = []
    for i in range(4):
        r = living[i % 3]
        is_insert = i == 3
        order_id = 'step65_stream_order_001' if is_insert else r[0]
        item_id = 1 if is_insert else r[1]
        p = Decimal(r[2]) + (Decimal('3.00') if i < 2 else Decimal('0'))
        f = Decimal(r[3])
        event = {'run_id': RUN_ID, 'order_id': order_id,
                 'order_item_id': item_id, 'price': money(p),
                 'freight_value': money(f), 'item_total': money(p + f),
                 'event_seq': 400, 'op': ('I' if is_insert else 'D' if i == 2 else 'U')}
        events.extend([dict(event), dict(event), {**event, 'event_seq': 350}])
    require(len(events) == 12 and len(newest(events)) == 4, 'Invalid event fixture')
    return events


def expected_after(source_rows):
    state = {(r[0], r[1]): r for r in (normalized(x) for x in source_rows)}
    for event in newest(events_for(source_rows)):
        key, seq, _ = validate_event(event)
        prev = state.get(key)
        if prev is None or seq > prev[5]:
            state[key] = (key[0], key[1], money(event['price']),
                          money(event['freight_value']), money(event['item_total']),
                          seq, event['op'] == 'D')
    output = list(state.values())
    require(len(output) == 521 and sum(r[6] for r in output) == 9 and
            sum(not r[6] for r in output) == 512,
            'Expected 521 / 512 live / 9 tombstones')
    return output


def produce(spark, source_rows, source_proof, bootstrap, replay=False):
    ensure_seed(spark, source_proof)
    if replay:
        require(exists(APPLY_PROOF), 'Replay requires verified first apply')
        verify(spark, source_rows, source_proof, bootstrap, save=False)
    elif exists(PRODUCE_PROOF):
        proof = read_proof(PRODUCE_PROOF)
        require(proof.get('topic') == TOPIC and proof.get('messages') == 12,
                'Invalid existing Step65 produce proof')
        print('STEP65_PRODUCE_ALREADY_VERIFIED: skip duplicate publish', flush=True)
        return
    elif exists(APPLY_PROOF):
        raise GuardError('Unexpected apply proof without produce proof')
    else:
        require(snapshot(spark, TARGET) == read_proof(SEED_PROOF)['main_snapshot'],
                'Step65 target changed before first publish')
    from pyspark.sql import functions as F
    from pyspark.sql.types import StructType, StructField, StringType
    data = [(e['order_id'] + ':' + str(e['order_item_id']),
             json.dumps(e, sort_keys=True, separators=(',', ':')))
            for e in events_for(source_rows)]
    schema = StructType([StructField('key', StringType(), False),
                         StructField('value', StringType(), False)])
    df = spark.createDataFrame(data, schema)
    df.select(F.col('key').cast('binary'), F.col('value').cast('binary')) \
        .write.format('kafka').option('kafka.bootstrap.servers', bootstrap) \
        .option('topic', TOPIC).option('kafka.acks', 'all').save()
    if not replay:
        write_proof(PRODUCE_PROOF, {'status': 'PASS',
            'schema': 'bigtop.olist.kafka-stream-produce.v1',
            'topic': TOPIC, 'messages': 12, 'run_id': RUN_ID,
            'source_snapshot': source_proof['main_snapshot'],
            'time_utc': datetime.now(timezone.utc).isoformat()})
    print('STEP65_KAFKA_REPLAY_SENT' if replay else
          'STEP65_KAFKA_PRODUCE_PASS: messages=12', flush=True)


def microbatch(batch_df, batch_id):
    from pyspark.sql import functions as F
    from pyspark.sql.types import (StructType, StructField, StringType,
                                   IntegerType, DecimalType, LongType)
    spark = batch_df.sparkSession
    batch_rows = batch_df.select(F.col('value').cast('string').alias('value'),
                                 'topic', 'partition', 'offset').limit(501).collect()
    require(len(batch_rows) <= 500, 'Step65 max 500 messages per batch')
    if not batch_rows:
        return
    events = []
    for r in batch_rows:
        require(r['topic'] == TOPIC, 'Non-Step65 Kafka topic in stream')
        try:
            events.append(json.loads(r['value']))
        except (ValueError, TypeError) as exc:
            raise GuardError('Malformed Kafka event') from exc
    updates = newest(events)
    ensure_owned(spark)
    current = {(r[0], r[1]): r for r in (normalized(x) for x in rows(spark, TARGET))}
    pending = []
    for event in updates:
        key, seq, _ = validate_event(event)
        old = current.get(key)
        if old and seq == old[5]:
            expected = (money(event['price']), money(event['freight_value']),
                        money(event['item_total']), event['op'] == 'D')
            require(old[2:5] + (old[6],) == expected,
                    'Conflicting persisted state at identical sequence')
        if old is None or seq > old[5]:
            pending.append(event)
    if not pending:
        print(f'STEP65_MICROBATCH_NOOP: batch={batch_id} events={len(events)}', flush=True)
        return
    schema = StructType([
        StructField('order_id', StringType(), False),
        StructField('order_item_id', IntegerType(), False),
        StructField('price', DecimalType(18, 2), False),
        StructField('freight_value', DecimalType(18, 2), False),
        StructField('item_total', DecimalType(18, 2), False),
        StructField('event_seq', LongType(), False),
        StructField('op', StringType(), False)])
    data = [(e['order_id'], int(e['order_item_id']), Decimal(money(e['price'])),
             Decimal(money(e['freight_value'])), Decimal(money(e['item_total'])),
             int(e['event_seq']), e['op']) for e in pending]
    spark.createDataFrame(data, schema).createOrReplaceTempView('step65_pending_events')
    spark.sql(f'''MERGE INTO {TARGET} t USING step65_pending_events s
        ON t.order_id=s.order_id AND t.order_item_id=s.order_item_id
        WHEN MATCHED AND s.event_seq > t.last_event_seq THEN UPDATE SET
          t.price=s.price, t.freight_value=s.freight_value,
          t.item_total=s.item_total, t.last_event_seq=s.event_seq,
          t.is_deleted=(s.op='D')
        WHEN NOT MATCHED THEN INSERT (
          order_id, order_item_id, price, freight_value, item_total,
          last_event_seq, is_deleted
        ) VALUES (
          s.order_id, s.order_item_id, s.price, s.freight_value,
          s.item_total, s.event_seq, (s.op='D')
        )''').collect()
    print(f'STEP65_MICROBATCH_MERGED: batch={batch_id} pending={len(pending)}', flush=True)


def stream(spark, source_proof, bootstrap):
    ensure_seed(spark, source_proof)
    require(exists(PRODUCE_PROOF) or not exists(APPLY_PROOF),
            'Inconsistent proof state')
    df = (spark.readStream.format('kafka')
          .option('kafka.bootstrap.servers', bootstrap)
          .option('subscribe', TOPIC)
          .option('startingOffsets', 'earliest')
          .option('maxOffsetsPerTrigger', '500')
          .option('failOnDataLoss', 'true').load())
    query = (df.writeStream.foreachBatch(microbatch)
             .option('checkpointLocation', CHECKPOINT)
             .trigger(processingTime='10 seconds').start())
    print(f'STEP65_STREAM_READY: checkpoint={CHECKPOINT}', flush=True)
    query.awaitTermination()
    require(query.exception() is None, 'Streaming query failed')



def verify_checkpoint_caught_up(spark, bootstrap):
    """Require the committed Spark checkpoint to cover the Kafka log end offset.

    In Step65 the dedicated topic has ONE partition and only small synthetic data.
    Committed batch IDs, not merely staged offset logs, confirm foreachBatch finished.
    """
    from pyspark.sql import functions as F
    df = (spark.read.format('kafka')
          .option('kafka.bootstrap.servers', bootstrap)
          .option('subscribe', TOPIC)
          .option('startingOffsets', 'earliest')
          .option('endingOffsets', 'latest')
          .option('failOnDataLoss', 'true').load())
    kafka_offsets = df.groupBy('partition').agg(F.max('offset').alias('last')).collect()
    require(len(kafka_offsets) == 1 and int(kafka_offsets[0]['partition']) == 0,
            'Expected one nonempty Step65 Kafka partition')
    kafka_end = int(kafka_offsets[0]['last']) + 1
    listing = hdfs('-ls', CHECKPOINT + '/commits').decode('utf-8')
    commits = []
    for line in listing.splitlines():
        name = line.split()[-1].rsplit('/', 1)[-1] if line.split() else ''
        if name.isdecimal():
            commits.append(int(name))
    require(bool(commits), 'No committed Spark Structured Streaming batch')
    batch_id = max(commits)
    offsets_raw = hdfs('-cat', CHECKPOINT + '/offsets/' + str(batch_id))
    offset_lines = offsets_raw.decode('utf-8').strip().splitlines()
    require(len(offset_lines) >= 2, 'Invalid Spark checkpoint offset format')
    offset_json = json.loads(offset_lines[-1])
    require(TOPIC in offset_json and '0' in offset_json[TOPIC],
            'Step65 topic/partition absent from checkpoint offset log')
    committed_end = int(offset_json[TOPIC]['0'])
    require(committed_end >= kafka_end,
            f'Kafka consumer not caught up: committed={committed_end} latest={kafka_end}')
    print(f'STEP65_KAFKA_CHECKPOINT_PASS: committed={committed_end} kafka_end={kafka_end}',
          flush=True)


def verify(spark, source_rows, source_proof, bootstrap, save=True):
    ensure_seed(spark, source_proof)
    evidence = read_proof(PRODUCE_PROOF)
    require(evidence.get('topic') == TOPIC and evidence.get('messages') == 12,
            'Step65 Kafka producer proof mismatch')
    actual = rows(spark, TARGET)
    expected_digest = digest(expected_after(source_rows))
    require(digest(actual) == expected_digest,
            'Step65 CDC not fully applied yet (or target drift)')
    verify_checkpoint_caught_up(spark, bootstrap)
    snap = snapshot(spark, TARGET)
    if exists(APPLY_PROOF):
        original = read_proof(APPLY_PROOF)
        require(original.get('table') == TARGET and
                original.get('digest_sha256') == expected_digest and
                str(original.get('main_snapshot')) == snap,
                'Replay changed the Iceberg snapshot or target digest')
    elif save:
        write_proof(APPLY_PROOF, {'status': 'PASS',
            'schema': 'bigtop.olist.kafka-stream-apply.v1',
            'table': TARGET, 'topic': TOPIC, 'rows': 521,
            'live_rows': 512, 'tombstones': 9,
            'digest_sha256': expected_digest, 'main_snapshot': snap,
            'checkpoint': CHECKPOINT,
            'source_snapshot': source_proof['main_snapshot'],
            'time_utc': datetime.now(timezone.utc).isoformat()})
    else:
        raise GuardError('Expected first Step65 verified apply proof')
    print(f'STEP65_ICEBERG_VERIFY_PASS: rows=521 live=512 tombstones=9 snapshot={snap}',
          flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--mode', choices=['seed', 'produce', 'stream', 'replay', 'verify'],
                        required=True)
    parser.add_argument('--bootstrap', default='')
    args = parser.parse_args()
    if args.mode in ('produce', 'stream', 'replay', 'verify'):
        require(re.fullmatch(r'[A-Za-z0-9._:-]+(,[A-Za-z0-9._:-]+)*', args.bootstrap),
                'Invalid Kafka bootstrap')
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName(APP_NAME if args.mode == 'stream' else
                                         'olist-kafka65-' + args.mode).getOrCreate()
    try:
        source_rows, proof = ensure_source(spark)
        if args.mode == 'seed':
            seed(spark, source_rows, proof)
        elif args.mode in ('produce', 'replay'):
            produce(spark, source_rows, proof, args.bootstrap,
                    replay=args.mode == 'replay')
        elif args.mode == 'stream':
            stream(spark, proof, args.bootstrap)
        else:
            verify(spark, source_rows, proof, args.bootstrap)
    finally:
        spark.stop()


if __name__ == '__main__':
    try:
        main()
    except (GuardError, ValueError, TypeError, KeyError, OSError) as exc:
        print('STEP65_FAILED: ' + str(exc), file=sys.stderr, flush=True)
        sys.exit(1)
