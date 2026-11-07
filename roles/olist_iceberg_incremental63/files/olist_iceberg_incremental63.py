#!/usr/bin/env python3
"""Step63: Olist-derived, isolated incremental CDC MERGE lab (Spark 3.5 / Iceberg 1.10.1).

Modes: seed | apply | verify.  No writes to Step58/59 or historical HDFS proofs.
Synthetic CDC events model upserts, tombstones, late delivery and duplicates; NOT a
real Olist CDC connector or a multi-table publish workflow.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path

SOURCE = 'shared.olist58_v3.silver_order_item_fact'
PROTECTED = 'shared.olist59_v3.gold_daily_category_sales'
TARGET = 'shared.olist63_cdc.fact_change_lab'
NAMESPACE = 'shared.olist63_cdc'
PROOF_ROOT = '/data/olist/iceberg-validation/step63/v1'
SEED_PROOF = PROOF_ROOT + '/_SEED_REPORT.json'
APPLY_PROOF = PROOF_ROOT + '/_APPLY_REPORT.json'
PUBLISH59 = '/data/olist/iceberg-validation/step59/v3/_PUBLISH_REPORT.json'
STAGE59 = '/data/olist/iceberg-validation/step59/v3/_STAGE_REPORT.json'
SOURCE58_PROOF = '/data/olist/iceberg-validation/step58/v3/_SUCCESS'
ROWS = 512
UPDATES = 8
DELETES = 4
INSERTS = 4
EVENT_SEQ = 200


class Step63Error(RuntimeError):
    pass


def require(ok, message):
    if not ok:
        raise Step63Error(message)


def hdfs(*args):
    proc = subprocess.run(['hdfs', 'dfs', *args], capture_output=True, check=False)
    require(proc.returncode == 0, 'HDFS ' + str(args[0]) + ': ' +
            proc.stderr.decode('utf-8', 'replace')[-500:])
    return proc.stdout


def hdfs_exists(path):
    proc = subprocess.run(['hdfs', 'dfs', '-test', '-e', path],
                          capture_output=True, check=False)
    if proc.returncode == 0:
        return True
    if proc.returncode == 1 and not proc.stderr:
        return False
    raise Step63Error('Cannot stat HDFS path: ' + path + ': ' +
                      proc.stderr.decode(errors='replace')[-250:])


def read_json(path):
    raw = hdfs('-cat', path)
    obj = json.loads(raw)
    require(isinstance(obj, dict), 'Expected JSON object: ' + path)
    return obj, raw


def write_immutable_proof(path, report):
    require(path in (SEED_PROOF, APPLY_PROOF), 'Proof path is not permitted')
    require(not hdfs_exists(path), 'Immutable proof already exists: ' + path)
    hdfs('-mkdir', '-p', PROOF_ROOT)
    with tempfile.TemporaryDirectory(prefix='step63-') as dirname:
        local = Path(dirname) / 'evidence.json'
        local.write_text(json.dumps(report, sort_keys=True, indent=2,
                                    ensure_ascii=False) + '\n', encoding='utf-8')
        stage = PROOF_ROOT + '/.tmp-' + os.urandom(12).hex() + '.json'
        hdfs('-put', str(local), stage)
        require(not hdfs_exists(path), 'Concurrent Step63 proof publication')
        hdfs('-mv', stage, path)


def refs(spark, table):
    return {str(row['name']): str(row['snapshot_id']) for row in
            spark.sql(f'SELECT name, snapshot_id FROM {table}.refs').collect()}


def ensure_protected(spark):
    stage, raw = read_json(STAGE59)
    pub, _ = read_json(PUBLISH59)
    require(stage.get('status') == 'PASS' and stage.get('target_table') == PROTECTED
            and stage.get('quality_checks_total') == 29
            and stage.get('quality_checks_failed') == 0
            and len(stage.get('checks', [])) == 29
            and all(isinstance(c, dict) and c.get('status') == 'PASS'
                    for c in stage['checks']), 'Approved Step59 quality evidence missing')
    require(pub.get('schema') == 'bigtop.olist.iceberg-wap-publish.v1'
            and pub.get('status') == 'PASS'
            and pub.get('target_table') == PROTECTED
            and pub.get('stage_report_sha256') == hashlib.sha256(raw).hexdigest()
            and pub.get('main_snapshot_id') == stage.get('audit_snapshot_id')
            and type(pub.get('published_rows')) is int
            and pub['published_rows'] == stage.get('expected_gold_count'),
            'Step59 Gold publication evidence invalid')
    require(refs(spark, PROTECTED).get('main') == str(pub['main_snapshot_id']),
            'Protected Gold main was changed; refuse to run Step63')
    require(spark.table(PROTECTED).count() == pub['published_rows'],
            'Protected Gold row count drift')
    require(hdfs_exists(SOURCE58_PROOF), 'Step58 source parity proof missing')
    source_main = refs(spark, SOURCE).get('main')
    require(source_main is not None, 'Missing Step58 source main ref')
    return {'gold_snapshot': str(pub['main_snapshot_id']),
            'gold_rows': pub['published_rows'], 'source_snapshot': source_main}


def money(value):
    if value is None:
        raise Step63Error('Null amount not permitted')
    return str(Decimal(str(value)).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP))


def normalized(row):
    """Canonical row identity used by Spark and Trino; avoids float serialization."""
    require(len(row) == 7, 'Expected exactly seven target columns')
    return [str(row[0]), int(row[1]), money(row[2]), money(row[3]),
            money(row[4]), int(row[5]), bool(row[6])]


def normalized_digest(rows):
    encoded = [normalized(row) for row in rows]
    encoded.sort(key=lambda x: (x[0], x[1]))
    require(len(encoded) == len({(x[0], x[1]) for x in encoded}),
            'Duplicate primary keys in CDC target')
    raw = json.dumps(encoded, ensure_ascii=False, separators=(',', ':'))
    return hashlib.sha256(raw.encode('utf-8')).hexdigest()


def read_target_rows(spark):
    return [tuple(row) for row in spark.table(TARGET)
            .select('order_id', 'order_item_id', 'price', 'freight_value',
                    'item_total', 'last_event_seq', 'is_deleted').collect()]


def sample_source(spark):
    from pyspark.sql import functions as F
    df = (spark.table(SOURCE)
          .select(F.col('order_id').cast('string').alias('order_id'),
                  F.col('order_item_id').cast('bigint').alias('order_item_id'),
                  F.col('price').cast('decimal(18,2)').alias('price'),
                  F.col('freight_value').cast('decimal(18,2)').alias('freight_value'))
          .orderBy('order_id', 'order_item_id').limit(ROWS))
    records = []
    for r in df.collect():
        oid, iid, price, freight = r
        require(oid is not None and iid is not None and price is not None and
                freight is not None, 'Null source fact field')
        records.append((str(oid), int(iid), price, freight,
                        price + freight, 0, False))
    require(len(records) == ROWS, 'Need 512 ordered Olist fact rows')
    normalized_digest(records)  # duplicate-key check
    return records


def event_payload(event):
    return (event['op'], money(event['price']), money(event['freight_value']),
            money(event['item_total']))


def unique_latest_events(events):
    """Highest sequence wins; same key+sequence with differing data is invalid."""
    max_events = {}
    by_sequence = {}
    for event in events:
        require(event['op'] in ('I', 'U', 'D'), 'Unsupported CDC operation')
        key = (str(event['order_id']), int(event['order_item_id']))
        seq = int(event['event_seq'])
        sig = event_payload(event)
        old_sig = by_sequence.setdefault((key, seq), sig)
        require(old_sig == sig, 'Conflicting CDC events for same key and sequence')
        if key not in max_events or seq > max_events[key]['event_seq']:
            max_events[key] = event
    return [max_events[key] for key in sorted(max_events)]


def make_events(seed):
    """16 distinct keys, duplicated and delivered out of order (48 raw events)."""
    require(len(seed) == ROWS, 'Expected deterministic baseline size')
    events = []
    for index, r in enumerate(seed[:UPDATES + DELETES]):
        oid, iid, price, freight, _, _, _ = normalized(r)
        change = index < UPDATES
        p = Decimal(price) + (Decimal('1.00') if change else Decimal('0.00'))
        f = Decimal(freight)
        event = {'order_id': oid, 'order_item_id': iid,
                 'price': p, 'freight_value': f, 'item_total': p + f,
                 'event_seq': EVENT_SEQ, 'op': 'U' if change else 'D'}
        events += [event.copy(), event.copy(), {**event, 'event_seq': 100}]
    for index in range(INSERTS):
        r = normalized(seed[index])
        p, f = Decimal(r[2]), Decimal(r[3])
        event = {'order_id': 'step63_synthetic_order_' + str(index).zfill(3),
                 'order_item_id': 1, 'price': p, 'freight_value': f,
                 'item_total': p + f, 'event_seq': EVENT_SEQ, 'op': 'I'}
        events += [event.copy(), {**event, 'event_seq': 100}, event.copy()]
    require(len(events) == 3 * (UPDATES + DELETES + INSERTS), 'CDC event count mismatch')
    return events


def desired_rows(seed):
    baseline = {tuple(normalized(r)[:2]): normalized(r) for r in seed}
    for event in unique_latest_events(make_events(seed)):
        key = (event['order_id'], event['order_item_id'])
        baseline[key] = [key[0], key[1], money(event['price']),
                         money(event['freight_value']), money(event['item_total']),
                         int(event['event_seq']), event['op'] == 'D']
    return list(baseline.values())


def table_properties(spark):
    return {str(row[0]): str(row[1]) for row in
            spark.sql(f'SHOW TBLPROPERTIES {TARGET}').collect()}


def validate_target(spark):
    require(spark.catalog.tableExists(TARGET), 'Missing isolated CDC lab table')
    props = table_properties(spark)
    require(props.get('bigtop.olist.step') == '63'
            and props.get('bigtop.olist.scope') == 'synthetic-cdc-lab',
            'Target ownership properties do not match Step63')


def validate_proof(proof, kind):
    require(isinstance(proof, dict) and
            proof.get('schema') == 'bigtop.olist.iceberg-cdc-' + kind + '.v1' and
            proof.get('status') == 'PASS' and proof.get('table') == TARGET and
            type(proof.get('rows')) is int and
            isinstance(proof.get('digest_sha256'), str) and
            len(proof['digest_sha256']) == 64 and
            all(isinstance(proof.get(k), str) and proof[k].isdigit()
                for k in ('main_snapshot', 'gold_snapshot', 'source_snapshot')),
            'Invalid immutable Step63 ' + kind + ' proof')
    if kind == 'seed':
        require(proof['rows'] == ROWS, 'Seed row count invalid')
    else:
        require(proof['rows'] == ROWS + INSERTS and
                proof.get('live_rows') == ROWS and
                proof.get('deleted_rows') == DELETES and
                proof.get('updates') == UPDATES and
                proof.get('inserts') == INSERTS and
                proof.get('unique_events') == UPDATES + DELETES + INSERTS and
                proof.get('raw_events') == 3 * (UPDATES + DELETES + INSERTS),
                'CDC merge proof counts invalid')
    return proof


def check_same_baseline(seed, seed_proof, guard):
    validate_proof(seed_proof, 'seed')
    require(guard['gold_snapshot'] == seed_proof['gold_snapshot'] and
            guard['source_snapshot'] == seed_proof['source_snapshot'],
            'Protected or source snapshot drift since seed')
    require(normalized_digest(seed) == seed_proof['digest_sha256'],
            'Original Olist source sample changed')


def seed(spark, guard):
    baseline = sample_source(spark)
    if hdfs_exists(SEED_PROOF):
        proof, _ = read_json(SEED_PROOF)
        check_same_baseline(baseline, proof, guard)
        validate_target(spark)
        require(refs(spark, TARGET).get('main') == proof['main_snapshot'] and
                normalized_digest(read_target_rows(spark)) == proof['digest_sha256'],
                'Previously seeded lab changed; refuse rerun')
        print('STEP63_SEED_ALREADY_VERIFIED: no writes', flush=True)
        return
    require(not spark.catalog.tableExists(TARGET),
            'Table exists without seed proof; inspect and recover manually')
    spark.sql(f'CREATE NAMESPACE IF NOT EXISTS {NAMESPACE}')
    from pyspark.sql.types import (StructType, StructField, StringType, LongType,
                                   DecimalType, BooleanType)
    schema = StructType([
        StructField('order_id', StringType(), False),
        StructField('order_item_id', LongType(), False),
        StructField('price', DecimalType(18, 2), False),
        StructField('freight_value', DecimalType(18, 2), False),
        StructField('item_total', DecimalType(18, 2), False),
        StructField('last_event_seq', LongType(), False),
        StructField('is_deleted', BooleanType(), False),
    ])
    spark.createDataFrame(baseline, schema).coalesce(2).writeTo(TARGET).using('iceberg') \
        .tableProperty('format-version', '2') \
        .tableProperty('bigtop.olist.step', '63') \
        .tableProperty('bigtop.olist.scope', 'synthetic-cdc-lab').create()
    validate_target(spark)
    require(normalized_digest(read_target_rows(spark)) == normalized_digest(baseline),
            'Seed row parity failed')
    proof = {'schema': 'bigtop.olist.iceberg-cdc-seed.v1', 'status': 'PASS',
             'table': TARGET, 'rows': ROWS,
             'digest_sha256': normalized_digest(baseline),
             'main_snapshot': refs(spark, TARGET)['main'],
             'source_snapshot': guard['source_snapshot'],
             'gold_snapshot': guard['gold_snapshot'],
             'created_utc': datetime.now(timezone.utc).isoformat()}
    write_immutable_proof(SEED_PROOF, proof)
    print('STEP63_CDC_SEED_PASS: rows=512', flush=True)


def merge_events(spark, events):
    """Merge only unseen higher-sequence events, preserving deletion tombstones."""
    latest = unique_latest_events(events)
    from pyspark.sql import functions as F
    from pyspark.sql.types import (StructType, StructField, StringType,
                                   LongType, DecimalType)
    schema = StructType([
        StructField('order_id', StringType(), False),
        StructField('order_item_id', LongType(), False),
        StructField('price', DecimalType(18, 2), False),
        StructField('freight_value', DecimalType(18, 2), False),
        StructField('item_total', DecimalType(18, 2), False),
        StructField('event_seq', LongType(), False),
        StructField('op', StringType(), False),
    ])
    values = [(e['order_id'], int(e['order_item_id']),
               Decimal(money(e['price'])), Decimal(money(e['freight_value'])),
               Decimal(money(e['item_total'])), int(e['event_seq']), e['op'])
              for e in latest]
    df = spark.createDataFrame(values, schema)
    old = spark.table(TARGET).select('order_id', 'order_item_id', 'last_event_seq')
    fresh = (df.alias('s').join(old.alias('t'),
              (F.col('s.order_id') == F.col('t.order_id')) &
              (F.col('s.order_item_id') == F.col('t.order_item_id')), 'left')
             .where(F.col('t.last_event_seq').isNull() |
                    (F.col('s.event_seq') > F.col('t.last_event_seq')))
             .select('s.*'))
    pending = fresh.count()
    if not pending:
        print('STEP63_CDC_DUPLICATE_OR_LATE_SKIP: no new sequences', flush=True)
        return 0
    fresh.createOrReplaceTempView('step63_pending_cdc')
    # No branch-publish in this isolated lab; only one Step63-owned Iceberg table.
    spark.sql(f'''MERGE INTO {TARGET} t
        USING step63_pending_cdc s
        ON t.order_id = s.order_id AND t.order_item_id = s.order_item_id
        WHEN MATCHED THEN UPDATE SET
          t.price = s.price,
          t.freight_value = s.freight_value,
          t.item_total = s.item_total,
          t.last_event_seq = s.event_seq,
          t.is_deleted = (s.op = 'D')
        WHEN NOT MATCHED THEN INSERT (
          order_id, order_item_id, price, freight_value, item_total,
          last_event_seq, is_deleted
        ) VALUES (
          s.order_id, s.order_item_id, s.price, s.freight_value,
          s.item_total, s.event_seq, (s.op = 'D')
        )''').collect()
    return pending


def verify(spark, guard):
    seed_proof, _ = read_json(SEED_PROOF)
    proof, _ = read_json(APPLY_PROOF)
    baseline = sample_source(spark)
    check_same_baseline(baseline, seed_proof, guard)
    validate_proof(proof, 'apply')
    validate_target(spark)
    require(proof['gold_snapshot'] == guard['gold_snapshot']
            and proof['source_snapshot'] == guard['source_snapshot']
            and proof.get('seed_snapshot') == seed_proof['main_snapshot'],
            'Step63 proof no longer references the protected snapshots')
    require(refs(spark, TARGET).get('main') == proof['main_snapshot'],
            'CDC lab main snapshot drift since proof')
    actual = read_target_rows(spark)
    expected = desired_rows(baseline)
    require(normalized_digest(actual) == normalized_digest(expected) ==
            proof['digest_sha256'], 'CDC lab differs from independently calculated events')
    require(len(actual) == ROWS + INSERTS and
            sum(not x[6] for x in actual) == ROWS and
            sum(bool(x[6]) for x in actual) == DELETES,
            'CDC tombstone/live row counts invalid')
    print('STEP63_CDC_VERIFY_PASS: rows=516 live=512 tombstones=4', flush=True)
    return proof


def apply(spark, guard):
    baseline = sample_source(spark)
    seed_proof, _ = read_json(SEED_PROOF)
    check_same_baseline(baseline, seed_proof, guard)
    validate_target(spark)
    if hdfs_exists(APPLY_PROOF):
        verify(spark, guard)
        print('STEP63_CDC_APPLY_ALREADY_VERIFIED: no writes', flush=True)
        return
    require(refs(spark, TARGET).get('main') == seed_proof['main_snapshot'] and
            normalized_digest(read_target_rows(spark)) == seed_proof['digest_sha256'],
            'Unproven partial apply state — manual inspection required')
    events = make_events(baseline)
    expected = desired_rows(baseline)
    require(len(unique_latest_events(events)) == UPDATES + DELETES + INSERTS,
            'CDC dedup count mismatch')
    changed = merge_events(spark, events)
    require(changed == UPDATES + DELETES + INSERTS,
            'Unexpected number of fresh CDC keys')
    data = read_target_rows(spark)
    require(normalized_digest(data) == normalized_digest(expected),
            'CDC MERGE differs from independently computed post-image')
    after_merge_snapshot = refs(spark, TARGET)['main']
    replayed = merge_events(spark, events)
    require(replayed == 0 and refs(spark, TARGET)['main'] == after_merge_snapshot,
            'Duplicate CDC retry was not a no-op')
    late = [{**e, 'event_seq': 150} for e in unique_latest_events(events)]
    skipped = merge_events(spark, late)
    require(skipped == 0 and refs(spark, TARGET)['main'] == after_merge_snapshot,
            'Out-of-order CDC event changed current state')
    require(normalized_digest(read_target_rows(spark)) == normalized_digest(expected),
            'Late/duplicate events changed canonical row content')
    protected_after = ensure_protected(spark)
    require(protected_after == guard, 'Protected Gold/source mutated during CDC lab')
    proof = {'schema': 'bigtop.olist.iceberg-cdc-apply.v1',
             'status': 'PASS', 'table': TARGET,
             'rows': ROWS + INSERTS, 'live_rows': ROWS,
             'deleted_rows': DELETES, 'updates': UPDATES, 'inserts': INSERTS,
             'raw_events': len(events), 'unique_events': len(unique_latest_events(events)),
             'digest_sha256': normalized_digest(expected),
             'main_snapshot': after_merge_snapshot,
             'seed_snapshot': seed_proof['main_snapshot'],
             'source_snapshot': guard['source_snapshot'],
             'gold_snapshot': guard['gold_snapshot'],
             'duplicate_replay_skipped': True, 'late_events_skipped': True,
             'created_utc': datetime.now(timezone.utc).isoformat()}
    write_immutable_proof(APPLY_PROOF, proof)
    verify(spark, guard)
    print('STEP63_CDC_APPLY_PASS: unique=16 raw=48; replay=SKIP late=SKIP', flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--mode', required=True, choices=('seed', 'apply', 'verify'))
    args = parser.parse_args()
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName('olist-iceberg-step63-' + args.mode).getOrCreate()
    try:
        guard = ensure_protected(spark)
        if args.mode == 'seed':
            seed(spark, guard)
        elif args.mode == 'apply':
            apply(spark, guard)
        else:
            verify(spark, guard)
    finally:
        spark.stop()


if __name__ == '__main__':
    try:
        main()
    except (Step63Error, OSError, ValueError, TypeError, KeyError) as e:
        print('STEP63_CDC_FAIL: ' + str(e), file=sys.stderr, flush=True)
        sys.exit(1)
