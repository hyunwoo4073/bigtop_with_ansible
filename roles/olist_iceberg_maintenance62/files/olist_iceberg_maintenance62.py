#!/usr/bin/env python3
"""Step 62. Read-only production audit and isolated Iceberg maintenance experiments.

Modes:
  inspect: read-only 58/59 snapshot, data file and WAP evidence inspection.
  lab: write ONLY shared.olist62_ops.maintenance_smoke; compact, inject, rollback.
  verify: read-only verification of immutable lab proof and protected Gold.
  expire-lab: explicitly expire ONLY the known, rolled-back fault snapshot in lab.

Step 58 and 59 tables are NEVER altered by this script. No orphan cleanup.
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
from pathlib import Path

CATALOG = 'shared'
PROTECTED = 'shared.olist59_v3.gold_daily_category_sales'
TABLE = 'shared.olist62_ops.maintenance_smoke'
TABLE_SHORT = 'olist62_ops.maintenance_smoke'
NS = 'shared.olist62_ops'
PROOF_ROOT = '/data/olist/iceberg-validation/step62/v1'
LAB_PROOF = PROOF_ROOT + '/_LAB_REPORT.json'
EXPIRE_PROOF = PROOF_ROOT + '/_EXPIRE_REPORT.json'
STAGE59 = '/data/olist/iceberg-validation/step59/v3/_STAGE_REPORT.json'
PUBLISH59 = '/data/olist/iceberg-validation/step59/v3/_PUBLISH_REPORT.json'
SOURCE58 = 'shared.olist58_v3.gold_daily_category_sales'
SOURCE_TABLES = (
    'silver_orders', 'silver_order_items', 'silver_customers', 'silver_products',
    'silver_order_item_fact', 'quality_quarantine_rows', 'gold_daily_category_sales',
)
ROWS = 2048


class GuardError(RuntimeError):
    pass


def require(ok, message):
    if not ok:
        raise GuardError(message)


def require_isolated_table(table):
    require(table == TABLE and table.startswith('shared.olist62_ops.'),
            'Only shared.olist62_ops.maintenance_smoke is mutable by Step62')


def hdfs(*args):
    result = subprocess.run(['hdfs', 'dfs', *args], check=False,
                            capture_output=True)
    if result.returncode:
        raise GuardError('HDFS operation ' + args[0] + ' failed: ' +
                         result.stderr.decode('utf-8', 'replace')[-350:])
    return result.stdout


def exists(path):
    result = subprocess.run(['hdfs', 'dfs', '-test', '-e', path],
                            check=False, capture_output=True)
    if result.returncode == 0:
        return True
    if result.returncode == 1 and not result.stderr:
        return False
    raise GuardError('Unable to determine HDFS path existence: ' + path)


def read_json(path):
    data = hdfs('-cat', path)
    obj = json.loads(data)
    require(isinstance(obj, dict), 'JSON object expected: ' + path)
    return obj, data


def atomic_proof(path, payload):
    """Create immutable proof via temporary HDFS path and rename; never overwrite."""
    require(path in (LAB_PROOF, EXPIRE_PROOF), 'Unexpected proof path')
    require(not exists(path), 'Refusing to overwrite existing proof: ' + path)
    hdfs('-mkdir', '-p', PROOF_ROOT)
    with tempfile.TemporaryDirectory(prefix='step62-') as d:
        local = Path(d) / 'proof.json'
        local.write_text(json.dumps(payload, indent=2, sort_keys=True,
                                    ensure_ascii=False) + '\n', encoding='utf-8')
        tmp = PROOF_ROOT + '/.tmp-' + os.urandom(12).hex() + '.json'
        hdfs('-put', str(local), tmp)
        require(not exists(path), 'Concurrent Step62 proof detected')
        hdfs('-mv', tmp, path)


def refs(spark, table):
    return {str(x['name']): str(x['snapshot_id']) for x in
            spark.sql(f'SELECT name, snapshot_id FROM {table}.refs').collect()}


def snapshots(spark, table):
    return {str(x['snapshot_id']) for x in
            spark.sql(f'SELECT snapshot_id FROM {table}.snapshots').collect()}


def files(spark, table):
    x = spark.sql(f'''SELECT count(*) AS n, coalesce(sum(file_size_in_bytes), 0) AS bytes
                     FROM {table}.files WHERE content = 0''').first()
    return {'data_files': int(x['n']), 'data_bytes': int(x['bytes'])}


def expected_frame(spark):
    return spark.range(0, ROWS).selectExpr(
        'cast(id as bigint) AS id',
        'cast((id * 13) % 10000 as bigint) AS amount',
        "'step62' AS source",
    )


def verify_exact_rows(spark):
    expected = expected_frame(spark)
    actual = spark.table(TABLE).select('id', 'amount', 'source')
    require(actual.count() == ROWS, f'Isolated lab row count must be {ROWS}')
    require(expected.exceptAll(actual).limit(1).count() == 0,
            'Lab table is missing expected rows or values')
    require(actual.exceptAll(expected).limit(1).count() == 0,
            'Lab table has extra or duplicate rows')
    return ROWS


def check_release(spark):
    """Fail closed on protected WAP main snapshot drift before every operation."""
    stage, raw = read_json(STAGE59)
    publish, _ = read_json(PUBLISH59)
    require(stage.get('status') == 'PASS' and stage.get('target_table') == PROTECTED
            and stage.get('quality_checks_total') == 29
            and stage.get('quality_checks_failed') == 0
            and len(stage.get('checks', [])) == 29
            and all(c.get('status') == 'PASS' for c in stage['checks']),
            'Step59 quality stage is not approved')
    require(publish.get('schema') == 'bigtop.olist.iceberg-wap-publish.v1' and
            publish.get('status') == 'PASS' and
            publish.get('target_table') == PROTECTED and
            publish.get('stage_report_sha256') == hashlib.sha256(raw).hexdigest() and
            publish.get('main_snapshot_id') == stage.get('audit_snapshot_id') and
            publish.get('published_rows') == stage.get('expected_gold_count'),
            'Published Gold proof mismatch')
    current = refs(spark, PROTECTED)
    require(current.get('main') == str(publish['main_snapshot_id']) and
            current.get('audit_v3') == current.get('main'),
            'Protected Gold main/audit snapshot drift — do not run maintenance')
    actual = spark.table(PROTECTED).count()
    require(actual == publish['published_rows'], 'Protected Gold row count changed')
    return {'snapshot_id': str(publish['main_snapshot_id']), 'rows': actual}


def inspect(spark):
    guard = check_release(spark)
    report = {'status': 'PASS', 'protected_gold': guard, 'tables': {}}
    for table_name in SOURCE_TABLES:
        full = 'shared.olist58_v3.' + table_name
        report['tables'][full] = {
            **files(spark, full), 'snapshots': len(snapshots(spark, full)),
        }
    report['tables'][PROTECTED] = {
        **files(spark, PROTECTED), 'snapshots': len(snapshots(spark, PROTECTED)),
    }
    print('STEP62_READ_ONLY_INSPECT_PASS: ' + json.dumps(report, sort_keys=True), flush=True)
    return report


def validate_lab_proof(p):
    require(p.get('schema') == 'bigtop.olist.iceberg-maintenance-lab.v1' and
            p.get('status') == 'PASS' and p.get('table') == TABLE and
            p.get('rows') == ROWS and
            isinstance(p.get('before_files'), int) and p['before_files'] >= 2 and
            isinstance(p.get('after_files'), int) and
            0 < p['after_files'] < p['before_files'] and
            all(isinstance(p.get(k), str) and p[k].isdigit()
                for k in ('compacted_snapshot', 'fault_snapshot', 'main_snapshot')) and
            p['main_snapshot'] == p['compacted_snapshot'] and
            p['fault_snapshot'] != p['main_snapshot'] and
            isinstance(p.get('protected_gold_snapshot'), str),
            'Invalid isolated maintenance lab proof')
    return p


def verify(spark):
    guard = check_release(spark)
    p, _ = read_json(LAB_PROOF)
    validate_lab_proof(p)
    require(spark.catalog.tableExists(TABLE), 'Missing isolated Step62 lab table')
    prop = {str(x[0]): str(x[1]) for x in
            spark.sql(f'SHOW TBLPROPERTIES {TABLE}').collect()}
    require(prop.get('bigtop.olist.step') == '62', 'Unexpected table ownership')
    require(guard['snapshot_id'] == p['protected_gold_snapshot'],
            'Protected Gold snapshot changed after maintenance lab')
    require(refs(spark, TABLE).get('main') == p['main_snapshot'],
            'Isolated lab main moved since rollback')
    verify_exact_rows(spark)
    require(files(spark, TABLE)['data_files'] == p['after_files'],
            'Isolated lab data file count changed')
    print('STEP62_LAB_VERIFY_PASS: ' + json.dumps({
        'rows': ROWS, 'data_files': p['after_files'],
        'main_snapshot': p['main_snapshot'],
        'gold_snapshot_unchanged': guard['snapshot_id']}, sort_keys=True), flush=True)
    return p


def lab(spark):
    require_isolated_table(TABLE)
    guard = check_release(spark)
    if exists(LAB_PROOF):
        verify(spark)
        print('STEP62_LAB_ALREADY_VERIFIED: no writes', flush=True)
        return
    require(not spark.catalog.tableExists(TABLE),
            'Lab table exists without immutable proof. Inspect it manually; never overwrite')
    spark.sql(f'CREATE NAMESPACE IF NOT EXISTS {NS}')
    spark.sql(f'''CREATE TABLE {TABLE} (
        id BIGINT, amount BIGINT, source STRING
    ) USING iceberg TBLPROPERTIES (
        'format-version'='2', 'bigtop.olist.step'='62'
    )''')
    expected_frame(spark).repartition(8).writeTo(TABLE).append()
    require(verify_exact_rows(spark) == ROWS, 'Initial lab ingest failed parity')
    before = files(spark, TABLE)
    require(before['data_files'] >= 2, 'Need >=2 data files to demonstrate compaction')
    print('STEP62_SMALL_FILES_BEFORE: ' + json.dumps(before), flush=True)
    spark.sql(f'''CALL {CATALOG}.system.rewrite_data_files(
      table => '{TABLE_SHORT}',
      options => map('rewrite-all', 'true', 'min-input-files', '2',
                     'target-file-size-bytes', '1048576')
    )''').collect()
    after = files(spark, TABLE)
    require(0 < after['data_files'] < before['data_files'],
            'Compaction did not reduce file count on isolated table')
    verify_exact_rows(spark)
    compacted = refs(spark, TABLE)['main']
    spark.sql(f"INSERT INTO {TABLE} VALUES (-1, -1, 'fault-injection')")
    fault = refs(spark, TABLE)['main']
    require(fault != compacted and spark.table(TABLE).count() == ROWS + 1,
            'Fault-injection append not visible')
    spark.sql(f"CALL {CATALOG}.system.rollback_to_snapshot('{TABLE_SHORT}', {int(compacted)})")
    spark.catalog.refreshTable(TABLE)
    require(refs(spark, TABLE)['main'] == compacted,
            'Rollback did not restore original snapshot')
    verify_exact_rows(spark)
    guard_after = check_release(spark)
    require(guard_after == guard, 'Protected Gold changed during isolated maintenance')
    payload = {
        'schema': 'bigtop.olist.iceberg-maintenance-lab.v1',
        'status': 'PASS', 'table': TABLE, 'rows': ROWS,
        'before_files': before['data_files'], 'after_files': after['data_files'],
        'before_bytes': before['data_bytes'], 'after_bytes': after['data_bytes'],
        'compacted_snapshot': compacted, 'fault_snapshot': fault,
        'main_snapshot': refs(spark, TABLE)['main'],
        'protected_gold_snapshot': guard['snapshot_id'],
        'gold_rows': guard['rows'],
        'tested': ['data_files_compaction', 'exact_multiset_parity',
                   'fault_injection_append', 'rollback_to_snapshot',
                   'protected_gold_unchanged'],
        'recorded_at_utc': datetime.now(timezone.utc).isoformat(),
    }
    atomic_proof(LAB_PROOF, payload)
    print('STEP62_LAB_PASS: ' + json.dumps(payload, sort_keys=True), flush=True)


def expire_lab(spark):
    """Expire precisely the injected, rolled-back snapshot; never a live ref."""
    require_isolated_table(TABLE)
    p = verify(spark)
    if exists(EXPIRE_PROOF):
        old, _ = read_json(EXPIRE_PROOF)
        require(old.get('status') == 'PASS' and old.get('table') == TABLE and
                old.get('expired_snapshot') == p['fault_snapshot'],
                'Invalid expire-lab proof')
        require(p['fault_snapshot'] not in snapshots(spark, TABLE),
                'Expired lab fault snapshot returned unexpectedly')
        print('STEP62_EXPIRE_ALREADY_VERIFIED: no writes', flush=True)
        return
    require(p['fault_snapshot'] != p['main_snapshot'], 'Cannot expire current lab snapshot')
    current = refs(spark, TABLE)
    require(p['fault_snapshot'] not in current.values(),
            'Cannot expire a snapshot referenced by any active branch or tag')
    require(p['fault_snapshot'] in snapshots(spark, TABLE),
            'Target lab snapshot is already absent, but no expiration proof exists')
    # IDs are generated by Spark, checked to be decimal in validate_lab_proof.
    # Expire only this one abandoned snapshot; do not touch protected tables.
    result = spark.sql(f"""CALL {CATALOG}.system.expire_snapshots(
        table => '{TABLE_SHORT}',
        snapshot_ids => array(cast({int(p['fault_snapshot'])} AS bigint))
    )""").collect()
    spark.catalog.refreshTable(TABLE)
    require(p['fault_snapshot'] not in snapshots(spark, TABLE),
            'Abandoned fault snapshot still present after expiration')
    verify(spark)
    payload = {
        'schema': 'bigtop.olist.iceberg-maintenance-expire.v1',
        'status': 'PASS', 'table': TABLE,
        'expired_snapshot': p['fault_snapshot'],
        'current_snapshot': p['main_snapshot'],
        'operation': 'expire_only_abandoned_fault_snapshot',
        'result': [r.asDict(recursive=True) for r in result],
        'recorded_at_utc': datetime.now(timezone.utc).isoformat(),
    }
    atomic_proof(EXPIRE_PROOF, payload)
    print('STEP62_EXPIRE_LAB_PASS: only abandoned lab snapshot expired', flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--mode', choices=('inspect', 'lab', 'verify', 'expire-lab'), required=True)
    parser.add_argument('--confirm-expire-sandbox', action='store_true')
    args = parser.parse_args()
    require(args.mode != 'expire-lab' or args.confirm_expire_sandbox,
            'expire-lab requires explicit --confirm-expire-sandbox')
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName('olist-iceberg-maintenance62-' + args.mode).getOrCreate()
    try:
        if args.mode == 'inspect':
            inspect(spark)
        elif args.mode == 'lab':
            lab(spark)
        elif args.mode == 'verify':
            verify(spark)
        else:
            expire_lab(spark)
    finally:
        spark.stop()


if __name__ == '__main__':
    try:
        main()
    except (GuardError, ValueError, RuntimeError, OSError, KeyError, TypeError) as exc:
        print('STEP62_FAIL: ' + str(exc), file=sys.stderr, flush=True)
        sys.exit(1)
