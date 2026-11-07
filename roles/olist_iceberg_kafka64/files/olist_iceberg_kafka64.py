#!/usr/bin/env python3
"""Step64 isolated Kafka -> Spark Structured Streaming -> Iceberg CDC MERGE lab.

Only `shared.olist64_kafka.fact_cdc_stream_v1` may be mutated. The Kafka source
is a dedicated, explicitly created topic. Each ingest runs AvailableNow; no
continuous service, production-grade CDC connector, or Gold publish is claimed.
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

SOURCE = 'shared.olist63_cdc.fact_change_lab'
GOLD = 'shared.olist59_v3.gold_daily_category_sales'
TARGET = 'shared.olist64_kafka.fact_cdc_stream_v1'
NS = 'shared.olist64_kafka'
TOPIC = 'bigtop_olist_step64_cdc_v1'
RUN_ID = 'olist-step64-v1'
CHECKPOINT = 'hdfs://master:9000/data/olist/iceberg-checkpoints/step64/v1'
PROOF_ROOT = '/data/olist/iceberg-validation/step64/v1'
SEED_PROOF = PROOF_ROOT + '/_SEED_REPORT.json'
PRODUCE_PROOF = PROOF_ROOT + '/_PRODUCE_REPORT.json'
INGEST_PROOF = PROOF_ROOT + '/_INGEST_REPORT.json'
STEP63_APPLY = '/data/olist/iceberg-validation/step63/v1/_APPLY_REPORT.json'
STEP59_PUBLISH = '/data/olist/iceberg-validation/step59/v3/_PUBLISH_REPORT.json'
SEQUENCE = 300
EARLY_SEQUENCE = 250
UPDATES = 8
DELETES = 4
INSERTS = 4
TOTAL_MESSAGES = (UPDATES + DELETES + INSERTS) * 3
BASE_ROWS = 516
FINAL_ROWS = 520


class Step64Error(RuntimeError):
    pass


def require(condition, message):
    if not condition:
        raise Step64Error(message)


def validate_bootstrap(value):
    require(bool(value) and all(re.fullmatch(r'[A-Za-z0-9._-]+:\d{1,5}', h)
                                for h in value.split(',')),
            'Specify real Kafka bootstrap host:port[,host:port] (no default brokers)')
    return value


def money(value):
    require(value is not None, 'Null amount')
    return str(Decimal(str(value)).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP))


def normalized(row):
    require(len(row) == 7, 'Expected seven CDC target fields')
    return [str(row[0]), int(row[1]), money(row[2]), money(row[3]),
            money(row[4]), int(row[5]), bool(row[6])]


def normalized_digest(rows):
    vals = [normalized(r) for r in rows]
    vals.sort(key=lambda x: (x[0], x[1]))
    require(len(vals) == len({(r[0], r[1]) for r in vals}),
            'Duplicate order/item key in target')
    raw = json.dumps(vals, separators=(',', ':'), ensure_ascii=False)
    return hashlib.sha256(raw.encode('utf-8')).hexdigest()


def read_rows(spark, table):
    return [tuple(r) for r in spark.table(table)
            .select('order_id', 'order_item_id', 'price', 'freight_value',
                    'item_total', 'last_event_seq', 'is_deleted').collect()]


def latest_events(events):
    """Maximum event sequence per PK; conflicting same-seq payload is fatal."""
    latest = {}
    signatures = {}
    for e in events:
        require(e.get('run_id') == RUN_ID, 'Foreign run id in dedicated Kafka topic')
        require(e.get('op') in ('I', 'U', 'D'), 'Unexpected CDC op')
        key = (str(e['order_id']), int(e['order_item_id']))
        seq = int(e['event_seq'])
        require(seq > 0, 'Invalid sequence')
        p, f, total = money(e['price']), money(e['freight_value']), money(e['item_total'])
        require(Decimal(p) + Decimal(f) == Decimal(total), 'Invalid item_total')
        signature = (e['op'], p, f, total)
        sig_key = (key, seq)
        require(sig_key not in signatures or signatures[sig_key] == signature,
                'Conflicting same key and sequence payload')
        signatures[sig_key] = signature
        if key not in latest or seq > int(latest[key]['event_seq']):
            latest[key] = e
    return [latest[k] for k in sorted(latest)]


def event_payloads(seed_rows):
    """Olist-derived 48 Kafka events: 8 U, 4 D, 4 I; 16 keys x 3."""
    rows = [normalized(r) for r in seed_rows]
    require(len(rows) == BASE_ROWS, 'Step63 baseline must contain 516 rows')
    living = [r for r in rows if not r[6]]
    require(len(living) == 512, 'Step63 baseline live row count mismatch')
    events = []
    for i, r in enumerate(living[:UPDATES + DELETES]):
        p = Decimal(r[2]) + (Decimal('2.00') if i < UPDATES else Decimal('0'))
        f = Decimal(r[3])
        event = {'run_id': RUN_ID, 'order_id': r[0], 'order_item_id': r[1],
                 'price': money(p), 'freight_value': money(f),
                 'item_total': money(p + f), 'event_seq': SEQUENCE,
                 'op': 'U' if i < UPDATES else 'D'}
        events.extend([dict(event), dict(event), {**event, 'event_seq': EARLY_SEQUENCE}])
    for i in range(INSERTS):
        r = living[i]
        event = {'run_id': RUN_ID, 'order_id': f'step64_kafka_order_{i:03d}',
                 'order_item_id': 1, 'price': r[2], 'freight_value': r[3],
                 'item_total': r[4], 'event_seq': SEQUENCE, 'op': 'I'}
        events.extend([dict(event), dict(event), {**event, 'event_seq': EARLY_SEQUENCE}])
    require(len(events) == TOTAL_MESSAGES and len(latest_events(events)) == 16,
            'Step64 event generation bug')
    return events


def events_digest(events):
    records = [dict(e) for e in events]
    records.sort(key=lambda e: (e['order_id'], int(e['order_item_id']),
                                int(e['event_seq']), e['op']))
    return hashlib.sha256(json.dumps(records, sort_keys=True,
                                     separators=(',', ':')).encode()).hexdigest()


def final_rows(seed_rows):
    state = {(r[0], r[1]): r for r in (normalized(x) for x in seed_rows)}
    for e in latest_events(event_payloads(seed_rows)):
        key = (e['order_id'], int(e['order_item_id']))
        old = state.get(key)
        if old is None or int(e['event_seq']) > old[5]:
            state[key] = [key[0], key[1], money(e['price']), money(e['freight_value']),
                          money(e['item_total']), int(e['event_seq']), e['op'] == 'D']
    result = list(state.values())
    require(len(result) == FINAL_ROWS and
            sum(bool(r[6]) for r in result) == 8 and
            sum(not bool(r[6]) for r in result) == 512,
            'Expected 520 rows (512 live, 8 tombstones)')
    return result


def hdfs(*args):
    run = subprocess.run(['hdfs', 'dfs', *args], capture_output=True, check=False)
    require(run.returncode == 0, f'HDFS {args[0]}: '
            + run.stderr.decode('utf-8', 'replace')[-450:])
    return run.stdout


def exists(path):
    r = subprocess.run(['hdfs', 'dfs', '-test', '-e', path],
                       capture_output=True, check=False)
    if r.returncode == 0:
        return True
    require(r.returncode == 1 and not r.stderr, f'HDFS access error: {path}')
    return False


def read_json(path):
    obj = json.loads(hdfs('-cat', path))
    require(isinstance(obj, dict), f'Invalid JSON proof {path}')
    return obj


def write_proof(path, data):
    require(path in (SEED_PROOF, PRODUCE_PROOF, INGEST_PROOF), 'Unexpected proof path')
    require(not exists(path), f'Immutable evidence already exists: {path}')
    hdfs('-mkdir', '-p', PROOF_ROOT)
    with tempfile.TemporaryDirectory(prefix='step64-') as folder:
        p = Path(folder) / 'proof.json'
        p.write_text(json.dumps(data, sort_keys=True, indent=2) + '\n', encoding='utf-8')
        staged = PROOF_ROOT + '/.tmp-' + os.urandom(12).hex() + '.json'
        hdfs('-put', str(p), staged)
        require(not exists(path), 'Concurrent immutable proof creation detected')
        hdfs('-mv', staged, path)


def refs(spark, table):
    return {str(r['name']): str(r['snapshot_id']) for r in
            spark.sql(f'SELECT name, snapshot_id FROM {table}.refs').collect()}


def table_props(spark, table):
    return {str(r[0]): str(r[1]) for r in spark.sql(f'SHOW TBLPROPERTIES {table}').collect()}


def ensure_guard(spark):
    previous = read_json(STEP63_APPLY)
    pub = read_json(STEP59_PUBLISH)
    require(previous.get('status') == 'PASS' and
            previous.get('schema') == 'bigtop.olist.iceberg-cdc-apply.v1' and
            previous.get('table') == SOURCE and previous.get('rows') == BASE_ROWS and
            previous.get('live_rows') == 512 and previous.get('deleted_rows') == 4,
            'Step63 approved CDC evidence invalid')
    require(pub.get('status') == 'PASS' and pub.get('target_table') == GOLD,
            'Step59 Gold publication evidence invalid')
    require(refs(spark, SOURCE).get('main') == previous['main_snapshot'] and
            refs(spark, GOLD).get('main') == str(pub['main_snapshot_id']),
            'Step63 source / Step59 Gold main snapshot has drifted')
    seed_rows = read_rows(spark, SOURCE)
    require(normalized_digest(seed_rows) == previous['digest_sha256'],
            'Step63 source row content drift')
    return seed_rows, {'source_snapshot': previous['main_snapshot'],
                       'gold_snapshot': str(pub['main_snapshot_id']),
                       'source_digest': previous['digest_sha256']}


def ensure_owned(spark):
    require(spark.catalog.tableExists(TARGET), 'Step64 table absent')
    props = table_props(spark, TARGET)
    require(props.get('bigtop.olist.step') == '64' and
            props.get('bigtop.olist.scope') == 'kafka-cdc-lab',
            'Refuse to touch a non-Step64 Iceberg table')


def seed(spark, seed_rows, guard):
    if exists(SEED_PROOF):
        proof = read_json(SEED_PROOF)
        check_seed_proof(proof, guard)
        ensure_owned(spark)
        if exists(INGEST_PROOF):
            print('STEP64_ALREADY_SEEDED: ingestion proof exists', flush=True)
            return
        require(refs(spark, TARGET).get('main') == proof['main_snapshot'] and
                normalized_digest(read_rows(spark, TARGET)) == proof['digest_sha256'],
                'Unapproved Step64 baseline drift')
        print('STEP64_ALREADY_SEEDED: unchanged', flush=True)
        return
    require(not spark.catalog.tableExists(TARGET),
            'Step64 target exists without immutable seed proof; manual reconciliation required')
    spark.sql(f'CREATE NAMESPACE IF NOT EXISTS {NS}')
    spark.sql(f'''CREATE TABLE {TARGET}
        USING iceberg
        TBLPROPERTIES ('bigtop.olist.step'='64', 'bigtop.olist.scope'='kafka-cdc-lab')
        AS SELECT order_id, order_item_id, price, freight_value, item_total,
                  last_event_seq, is_deleted FROM {SOURCE}''')
    ensure_owned(spark)
    require(normalized_digest(read_rows(spark, TARGET)) == normalized_digest(seed_rows),
            'Step64 baseline copy differs from immutable Step63 source')
    snapshot = refs(spark, TARGET)['main']
    proof = {'schema': 'bigtop.olist.kafka-cdc-seed.v1', 'status': 'PASS',
             'table': TARGET, 'rows': BASE_ROWS, 'digest_sha256': normalized_digest(seed_rows),
             'main_snapshot': snapshot, **guard, 'time_utc': datetime.now(timezone.utc).isoformat()}
    write_proof(SEED_PROOF, proof)
    print(f'STEP64_SEED_PASS: rows=516 snapshot={snapshot}', flush=True)


def check_seed_proof(proof, guard):
    require(proof.get('schema') == 'bigtop.olist.kafka-cdc-seed.v1' and
            proof.get('status') == 'PASS' and proof.get('table') == TARGET and
            proof.get('rows') == BASE_ROWS and
            proof.get('source_snapshot') == guard['source_snapshot'] and
            proof.get('source_digest') == guard['source_digest'] and
            proof.get('gold_snapshot') == guard['gold_snapshot'], 'Invalid Step64 seed proof')


def check_produce_proof(proof, guard):
    require(proof.get('schema') == 'bigtop.olist.kafka-cdc-produce.v1' and
            proof.get('status') == 'PASS' and proof.get('topic') == TOPIC and
            proof.get('run_id') == RUN_ID and proof.get('messages') == TOTAL_MESSAGES and
            proof.get('source_snapshot') == guard['source_snapshot'],
            'Invalid Step64 Kafka publish proof')


def verify_ingested(spark, seed_rows, guard):
    proof = read_json(INGEST_PROOF)
    require(proof.get('schema') == 'bigtop.olist.kafka-cdc-ingest.v1' and
            proof.get('status') == 'PASS' and proof.get('table') == TARGET and
            proof.get('rows') == FINAL_ROWS and proof.get('tombstones') == 8 and
            proof.get('live_rows') == 512 and
            proof.get('source_snapshot') == guard['source_snapshot'] and
            proof.get('gold_snapshot') == guard['gold_snapshot'], 'Invalid Step64 ingest evidence')
    ensure_owned(spark)
    actual = read_rows(spark, TARGET)
    digest = normalized_digest(actual)
    require(digest == normalized_digest(final_rows(seed_rows)) == proof['digest_sha256'],
            'Step64 CDC rows diverge from expected post-image')
    require(refs(spark, TARGET).get('main') == proof['main_snapshot'],
            'Step64 main snapshot changed after verified ingest')
    print('STEP64_INGEST_VERIFY_PASS: 520 rows / 512 live / 8 tombstones', flush=True)
    return proof


def produce(spark, seed_rows, guard, bootstrap, replay=False):
    check_seed_proof(read_json(SEED_PROOF), guard)
    ensure_owned(spark)
    if replay:
        require(exists(INGEST_PROOF), 'Replay requires successful first ingest')
        verify_ingested(spark, seed_rows, guard)
    elif exists(PRODUCE_PROOF):
        check_produce_proof(read_json(PRODUCE_PROOF), guard)
        print('STEP64_PRODUCE_ALREADY_VERIFIED: no new Kafka messages', flush=True)
        return
    elif exists(INGEST_PROOF):
        raise Step64Error('Ingest proof exists but producer proof missing')
    else:
        require(refs(spark, TARGET).get('main') == read_json(SEED_PROOF)['main_snapshot'],
                'Target was modified before Kafka publish')
    events = event_payloads(seed_rows)
    from pyspark.sql.types import StructField, StructType, StringType
    from pyspark.sql import functions as F
    kafka_rows = [(e['order_id'] + ':' + str(e['order_item_id']),
                   json.dumps(e, separators=(',', ':'), sort_keys=True)) for e in events]
    schema = StructType([StructField('key', StringType(), False),
                         StructField('value', StringType(), False)])
    df = spark.createDataFrame(kafka_rows, schema)
    # Kafka broker ack confirms writes; failure after this before proof is intentionally fail-closed.
    df.select(F.col('key').cast('binary'), F.col('value').cast('binary')) \
      .write.format('kafka').option('kafka.bootstrap.servers', bootstrap) \
      .option('topic', TOPIC).option('kafka.acks', 'all').save()
    if not replay:
        write_proof(PRODUCE_PROOF, {'schema': 'bigtop.olist.kafka-cdc-produce.v1',
                                   'status': 'PASS', 'topic': TOPIC, 'run_id': RUN_ID,
                                   'messages': TOTAL_MESSAGES,
                                   'events_digest_sha256': events_digest(events),
                                   'source_snapshot': guard['source_snapshot'],
                                   'time_utc': datetime.now(timezone.utc).isoformat()})
    print('STEP64_KAFKA_REPLAY_SENT' if replay else
          f'STEP64_KAFKA_PRODUCE_PASS: topic={TOPIC} messages=48', flush=True)


def apply_microbatch(batch_df, batch_id):
    # Use the microbatch's driver-side session; never capture SparkContext in the closure.
    spark = batch_df.sparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.types import (StructType, StructField, StringType, LongType,
                                   IntegerType, DecimalType)
    rows = batch_df.select(F.col('value').cast('string').alias('value'),
                           'topic', 'partition', 'offset').limit(1001).collect()
    require(len(rows) <= 1000, 'Lab safety limit: max 1000 Kafka events per batch')
    if not rows:
        return
    events = []
    for record in rows:
        require(record['topic'] == TOPIC, 'Unexpected Kafka topic')
        try:
            event = json.loads(record['value'])
        except (ValueError, TypeError) as error:
            raise Step64Error('Invalid Kafka JSON event') from error
        require(isinstance(event, dict), 'Kafka value must be JSON object')
        events.append(event)
    # In a dedicated lab topic, all events MUST belong to this run. No silent filtering.
    candidates = latest_events(events)
    ensure_owned(spark)
    current = {(r[0], r[1]): r for r in (normalized(x) for x in read_rows(spark, TARGET))}
    pending = []
    for e in candidates:
        key = (str(e['order_id']), int(e['order_item_id']))
        state = current.get(key)
        seq = int(e['event_seq'])
        if state and seq == state[5]:
            expected = (money(e['price']), money(e['freight_value']),
                        money(e['item_total']), e['op'] == 'D')
            require(tuple(state[2:5]) + (state[6],) == expected,
                    'Conflicting replay at same sequence in persisted target')
        if state is None or seq > state[5]:
            pending.append(e)
    if not pending:
        print(f'STEP64_MICROBATCH_NOOP: id={batch_id} seen={len(rows)}', flush=True)
        return
    schema = StructType([
        StructField('order_id', StringType(), False),
        StructField('order_item_id', IntegerType(), False),
        StructField('price', DecimalType(18, 2), False),
        StructField('freight_value', DecimalType(18, 2), False),
        StructField('item_total', DecimalType(18, 2), False),
        StructField('event_seq', LongType(), False),
        StructField('op', StringType(), False),
    ])
    data = [(e['order_id'], int(e['order_item_id']), Decimal(money(e['price'])),
             Decimal(money(e['freight_value'])), Decimal(money(e['item_total'])),
             int(e['event_seq']), e['op']) for e in pending]
    spark.createDataFrame(data, schema).createOrReplaceTempView('step64_pending_kafka')
    # Sequence is checked inside MERGE too, to make retry an idempotent state transition.
    spark.sql(f'''MERGE INTO {TARGET} t USING step64_pending_kafka s
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
    print(f'STEP64_MICROBATCH_MERGED: id={batch_id} pending={len(pending)}', flush=True)


def ingest(spark, seed_rows, guard, bootstrap):
    check_seed_proof(read_json(SEED_PROOF), guard)
    check_produce_proof(read_json(PRODUCE_PROOF), guard)
    ensure_owned(spark)
    previous = read_json(INGEST_PROOF) if exists(INGEST_PROOF) else None
    if previous:
        verify_ingested(spark, seed_rows, guard)
    from pyspark.sql import functions as F
    stream = (spark.readStream.format('kafka')
              .option('kafka.bootstrap.servers', bootstrap)
              .option('subscribe', TOPIC)
              .option('startingOffsets', 'earliest')
              .option('failOnDataLoss', 'true')
              .load())
    q = (stream.writeStream.foreachBatch(
             lambda batch, batch_id: apply_microbatch(batch, batch_id))
         .option('checkpointLocation', CHECKPOINT)
         .trigger(availableNow=True).start())
    q.awaitTermination()
    require(q.exception() is None, 'Streaming query failed')
    expected = normalized_digest(final_rows(seed_rows))
    actual = read_rows(spark, TARGET)
    require(normalized_digest(actual) == expected,
            'Kafka events were not fully or correctly applied; inspect offsets/retention')
    snap = refs(spark, TARGET)['main']
    if previous:
        require(snap == previous['main_snapshot'],
                'Kafka replay changed Snapshot: upsert is not idempotent')
        verify_ingested(spark, seed_rows, guard)
        print('STEP64_KAFKA_REPLAY_IDEMPOTENT_PASS', flush=True)
        return
    require(snap != read_json(SEED_PROOF)['main_snapshot'],
            'No CDC write Snapshot created; Kafka topic may have been empty')
    write_proof(INGEST_PROOF, {'schema': 'bigtop.olist.kafka-cdc-ingest.v1',
                               'status': 'PASS', 'table': TARGET, 'topic': TOPIC,
                               'run_id': RUN_ID, 'checkpoint': CHECKPOINT,
                               'rows': FINAL_ROWS, 'live_rows': 512,
                               'tombstones': 8, 'digest_sha256': expected,
                               'main_snapshot': snap, 'seed_snapshot': read_json(SEED_PROOF)['main_snapshot'],
                               **guard, 'time_utc': datetime.now(timezone.utc).isoformat()})
    verify_ingested(spark, seed_rows, guard)
    print('STEP64_KAFKA_ICEBERG_INGEST_PASS: 48 Kafka events to 520 Iceberg rows', flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--mode', choices=('seed', 'produce', 'ingest', 'replay', 'verify'), required=True)
    parser.add_argument('--bootstrap', default='')
    args = parser.parse_args()
    if args.mode in ('produce', 'ingest', 'replay'):
        validate_bootstrap(args.bootstrap)
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName('olist-iceberg-kafka64-' + args.mode).getOrCreate()
    try:
        seed_rows, guard = ensure_guard(spark)
        if args.mode == 'seed':
            seed(spark, seed_rows, guard)
        elif args.mode == 'produce':
            produce(spark, seed_rows, guard, args.bootstrap)
        elif args.mode == 'replay':
            produce(spark, seed_rows, guard, args.bootstrap, replay=True)
        elif args.mode == 'ingest':
            ingest(spark, seed_rows, guard, args.bootstrap)
        else:
            check_seed_proof(read_json(SEED_PROOF), guard)
            verify_ingested(spark, seed_rows, guard)
    finally:
        spark.stop()


if __name__ == '__main__':
    try:
        main()
    except (Step64Error, OSError, ValueError, TypeError, KeyError) as err:
        print('STEP64_KAFKA_CDC_FAIL: ' + str(err), file=sys.stderr, flush=True)
        sys.exit(1)
