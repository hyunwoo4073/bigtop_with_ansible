#!/usr/bin/env python3
"""Step54: isolated Iceberg 1.10.1 Spark SQL/branches exercise on YARN.

Runs only against two dedicated step54 tables in lab.olist. No Step45-53
source tables or approved releases are changed. Publish is explicitly gated.
"""
import argparse
import json
from datetime import datetime, timezone

CATALOG = 'lab'
NAMESPACE = 'olist'
PROOF_ROOT = 'hdfs://master:9000/data/olist/iceberg-validation/step54'
BRANCH = 'audit_step54'
MUTATIONS = 'iceberg_mutations_step54'
WAP = 'iceberg_wap_step54'
BASELINE = [(1, 'published', 100)]
CANDIDATE = [(1, 'published', 100), (2, 'candidate', 200)]


def guard_args(args):
    if args.catalog != CATALOG:
        raise ValueError('Only existing isolated lab catalog is permitted')
    if args.mode not in ('stage', 'publish'):
        raise ValueError('Invalid mode; choose stage or publish')
    if args.proof_root != PROOF_ROOT:
        raise ValueError('Proof path must stay in isolated step54 validation root')


def assert_rows(actual, expected, label):
    if actual != expected:
        raise RuntimeError('%s mismatch: expected=%r actual=%r' % (label, expected, actual))


def mutation_rows(spark, name):
    return [(int(x.id), x.label, int(x.amount)) for x in
            spark.sql('SELECT id, label, amount FROM %s ORDER BY id' % name).collect()]


def wap_rows(spark, name):
    return [(int(x.id), x.state, int(x.amount)) for x in
            spark.sql('SELECT id, state, amount FROM %s ORDER BY id' % name).collect()]


def read_refs(spark, table):
    return {r.name: int(r.snapshot_id) for r in
            spark.sql('SELECT name, snapshot_id FROM %s.refs' % table).collect()}


def snapshot_ids(spark, table):
    return [(int(r.snapshot_id), None if r.parent_id is None else int(r.parent_id))
            for r in spark.sql('SELECT snapshot_id, parent_id FROM %s.snapshots' % table).collect()]


def check_quality(main, candidate):
    # Deliberately visible/simple gate for the lab. Real 47/52 QA remains separate.
    assert_rows(main, BASELINE, 'pre-publication main')
    assert_rows(candidate, CANDIDATE, 'audit candidate')
    if len({x[0] for x in candidate}) != len(candidate) or any(x[2] <= 0 for x in candidate):
        raise RuntimeError('Candidate has duplicate IDs or non-positive amount')
    if sum(x[2] for x in candidate) != 300:
        raise RuntimeError('Candidate amount checksum failed')


def ensure_mutation_demo(spark, table):
    spark.sql('''CREATE TABLE IF NOT EXISTS %s
                 (id BIGINT, label STRING, amount BIGINT)
                 USING iceberg TBLPROPERTIES ('format-version'='2')''' % table)
    rows = mutation_rows(spark, table)
    if rows == []:
        spark.sql("INSERT INTO %s VALUES (1, 'one', 100), (2, 'two', 200)" % table)
        rows = mutation_rows(spark, table)
    if any(r[0] not in (1, 2, 3) for r in rows):
        raise RuntimeError('Unexpected row in mutation lab table')
    if 2 in [r[0] for r in rows]:
        spark.sql('DELETE FROM %s WHERE id = 2' % table)
    r1 = [r for r in mutation_rows(spark, table) if r[0] == 1]
    if len(r1) != 1 or r1[0][2] not in (100, 110, 120):
        raise RuntimeError('Unexpected id=1 state in mutation lab')
    if r1[0][2] == 100:
        spark.sql('UPDATE %s SET amount = 110 WHERE id = 1' % table)
    rows = mutation_rows(spark, table)
    if rows not in ([(1, 'one', 110)], [(1, 'one', 120), (3, 'three', 300)]):
        raise RuntimeError('Unexpected state before MERGE: ' + str(rows))
    if rows == [(1, 'one', 110)]:
        spark.sql('''MERGE INTO %s t USING
                     (SELECT CAST(1 AS BIGINT) id, 'one' label, CAST(120 AS BIGINT) amount
                      UNION ALL SELECT CAST(3 AS BIGINT), 'three', CAST(300 AS BIGINT)) s
                     ON t.id = s.id
                     WHEN MATCHED THEN UPDATE SET t.label=s.label, t.amount=s.amount
                     WHEN NOT MATCHED THEN INSERT (id, label, amount)
                       VALUES (s.id, s.label, s.amount)''' % table)
    assert_rows(mutation_rows(spark, table),
                [(1, 'one', 120), (3, 'three', 300)], 'post-merge rows')
    columns = [f.name for f in spark.table(table).schema.fields]
    if 'audit_note' not in columns:
        spark.sql('ALTER TABLE %s ADD COLUMNS (audit_note STRING)' % table)
    notes = [(int(r.id), r.audit_note) for r in
             spark.sql('SELECT id, audit_note FROM %s ORDER BY id' % table).collect()]
    if notes == [(1, None), (3, None)]:
        spark.sql("UPDATE %s SET audit_note='verified' WHERE id=1" % table)
    notes = [(int(r.id), r.audit_note) for r in
             spark.sql('SELECT id, audit_note FROM %s ORDER BY id' % table).collect()]
    assert_rows(notes, [(1, 'verified'), (3, None)], 'schema evolution results')
    snaps = snapshot_ids(spark, table)
    roots = [s[0] for s in snaps if s[1] is None]
    if len(roots) != 1 or len(snaps) < 4:
        raise RuntimeError('Expected original snapshot and row-level mutation snapshots')
    initial = spark.read.option('snapshot-id', str(roots[0])).table(table)
    initial_rows = [(int(x.id), x.label, int(x.amount)) for x in
                    initial.select('id', 'label', 'amount').orderBy('id').collect()]
    assert_rows(initial_rows, [(1, 'one', 100), (2, 'two', 200)], 'time travel to baseline')
    return {'snapshot_count': len(snaps), 'first_snapshot_id': str(roots[0]),
            'baseline_rows': len(initial_rows), 'current_rows': 2,
            'schema_added_column': 'audit_note'}


def ensure_wap(spark, table, create):
    if create:
        spark.sql('''CREATE TABLE IF NOT EXISTS %s
                     (id BIGINT, state STRING, amount BIGINT)
                     USING iceberg TBLPROPERTIES ('format-version'='2', 'write.wap.enabled'='true')''' % table)
    elif not spark.catalog.tableExists(table):
        raise RuntimeError('Run stage before publish; WAP lab table is absent')
    rows = wap_rows(spark, table)
    if create and rows == []:
        spark.sql("INSERT INTO %s VALUES (1, 'published', 100)" % table)
        rows = wap_rows(spark, table)
    if rows not in (BASELINE, CANDIDATE):
        raise RuntimeError('Unexpected WAP main table content: ' + str(rows))
    refs = read_refs(spark, table)
    if create and BRANCH not in refs:
        spark.sql('ALTER TABLE %s CREATE BRANCH IF NOT EXISTS %s' % (table, BRANCH))
        refs = read_refs(spark, table)
    if BRANCH not in refs:
        raise RuntimeError('Run stage first; audit branch is absent')
    return refs


def stage(spark, catalog, proof_root):
    mutations = catalog + '.olist.' + MUTATIONS
    wap = catalog + '.olist.' + WAP
    mutation_info = ensure_mutation_demo(spark, mutations)
    ensure_wap(spark, wap, create=True)
    branch_table = wap + '.branch_' + BRANCH
    branch_rows = wap_rows(spark, branch_table)
    if branch_rows == BASELINE:
        # Only the named branch is updated. main is never modified here.
        spark.sql("INSERT INTO %s VALUES (2, 'candidate', 200)" % branch_table)
    branch_rows = wap_rows(spark, branch_table)
    main_rows = wap_rows(spark, wap)
    if main_rows == BASELINE:
        check_quality(main_rows, branch_rows)
        status = 'STAGED'
    elif main_rows == CANDIDATE:
        assert_rows(branch_rows, CANDIDATE, 'post-publish audit branch')
        status = 'ALREADY_PUBLISHED'
    else:
        raise RuntimeError('Unexpected WAP main state')
    refs = read_refs(spark, wap)
    proof = {'status': status, 'mode': 'stage', 'mutation_demo': mutation_info,
             'main_snapshot_id': str(refs['main']),
             'audit_snapshot_id': str(refs[BRANCH]),
             'main_rows': len(main_rows), 'audit_rows': len(branch_rows)}
    write_proof(spark, proof_root + '/stage', proof)
    return proof


def publish(spark, catalog, proof_root):
    wap = catalog + '.olist.' + WAP
    refs = ensure_wap(spark, wap, create=False)
    # Exact stage artifact binds the quality decision to a specific snapshot.
    proof_rows = spark.read.text(proof_root + '/stage').collect()
    if len(proof_rows) != 1:
        raise RuntimeError('Missing valid stage evidence; stage must finish first')
    stage_proof = json.loads(proof_rows[0].value)
    if stage_proof.get('mode') != 'stage' or stage_proof.get('status') not in ('STAGED', 'ALREADY_PUBLISHED'):
        raise RuntimeError('Stage evidence is not a successful audit')
    if str(refs[BRANCH]) != stage_proof.get('audit_snapshot_id'):
        raise RuntimeError('Audit branch changed since stage; reject publication')
    current = wap_rows(spark, wap)
    candidate = wap_rows(spark, wap + '.branch_' + BRANCH)
    if current == BASELINE:
        if str(refs['main']) != stage_proof.get('main_snapshot_id'):
            raise RuntimeError('Main branch changed since stage; reject publication')
        check_quality(current, candidate)
        spark.sql("CALL %s.system.fast_forward('olist.%s', 'main', '%s')" %
                  (catalog, WAP, BRANCH))
        spark.catalog.refreshTable(wap)
        result = 'PUBLISHED'
    elif current == CANDIDATE and refs['main'] == refs[BRANCH]:
        # Repeated publish is an idempotent no-op.
        assert_rows(candidate, CANDIDATE, 'already-published audit branch')
        result = 'ALREADY_PUBLISHED'
    else:
        raise RuntimeError('Main changed independently; publication forbidden')
    assert_rows(wap_rows(spark, wap), CANDIDATE, 'published main rows')
    latest = read_refs(spark, wap)
    if latest['main'] != latest[BRANCH]:
        raise RuntimeError('Fast-forward did not set main to audit head')
    proof = {'status': 'PASS', 'mode': 'publish', 'result': result,
             'main_snapshot_id': str(latest['main']),
             'audit_snapshot_id': str(latest[BRANCH]),
             'published_rows': 2, 'published_amount_sum': 300}
    write_proof(spark, proof_root + '/publish', proof)
    return proof


def write_proof(spark, path, obj):
    obj = dict(obj)
    obj['recorded_at_utc'] = datetime.now(timezone.utc).isoformat()
    spark.createDataFrame([(json.dumps(obj, sort_keys=True),)], ['value']).coalesce(1).write.mode('overwrite').text(path)


def run(args):
    guard_args(args)
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName('bigtop-iceberg-step54-' + args.mode).getOrCreate()
    try:
        if args.mode == 'stage':
            spark.sql('CREATE NAMESPACE IF NOT EXISTS %s.%s' % (args.catalog, NAMESPACE))
            result = stage(spark, args.catalog, args.proof_root)
        else:
            result = publish(spark, args.catalog, args.proof_root)
        print('ICEBERG STEP54 %s %s' % (args.mode.upper(), json.dumps(result, sort_keys=True)), flush=True)
    finally:
        spark.stop()


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--catalog', default=CATALOG)
    p.add_argument('--mode', choices=['stage', 'publish'], required=True)
    p.add_argument('--proof-root', default=PROOF_ROOT)
    run(p.parse_args())


if __name__ == '__main__':
    main()
