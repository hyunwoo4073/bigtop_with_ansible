#!/usr/bin/env python3
"""Step 59. Gold-only Iceberg WAP release, bound to approved Olist v3 / Step 58.

A deliberately isolated release namespace contains one Gold table. The staged
candidate is a complete copy of Step 58's approved Gold table, not a new Raw ETL.
Sources are read-only. Stage never fast-forwards main. Publish requires a
snapshot-bound immutable PASS report and explicitly fast-forwards this one table.
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
from pathlib import Path

CATALOG = 'shared'
SOURCE_NS = 'olist58_v3'
TARGET_NS = 'olist59_v3'
GOLD = 'gold_daily_category_sales'
BRANCH = 'audit_v3'
SRC_GOLD = f'{CATALOG}.{SOURCE_NS}.{GOLD}'
TARGET = f'{CATALOG}.{TARGET_NS}.{GOLD}'
BRANCH_TABLE = f'{TARGET}.branch_{BRANCH}'
PROOF_ROOT = '/data/olist/iceberg-validation/step59/v3'
STEP58_PROOF = '/data/olist/iceberg-validation/step58/v3/part-*'
SOURCE_MANIFEST = '/data/olist/warehouse/v3/_ETL_MANIFEST.json'
SOURCE_APPROVAL = '/data/olist/published/v3/_APPROVAL.json'
SOURCE_TABLES = (
    ('curated/orders', 'silver_orders'),
    ('curated/order_items', 'silver_order_items'),
    ('curated/customers', 'silver_customers'),
    ('curated/products', 'silver_products'),
    ('curated/order_item_fact', 'silver_order_item_fact'),
    ('quarantine/rows', 'quality_quarantine_rows'),
    ('mart/daily_category_sales', GOLD),
)
ORPHAN_REASONS = ('missing_order', 'missing_customer', 'missing_product')
SOURCE_NAMES = ('orders', 'order_items', 'customers', 'products')
MART_COLS = (
    'purchase_date', 'product_category', 'customer_state', 'orders', 'sold_units',
    'gross_item_revenue', 'total_freight', 'gross_item_total',
)
CHECK_COUNT = 29
TABLES = tuple(name for name, _ in SOURCE_TABLES)


def rate(numerator, denominator):
    return float(numerator) / denominator if denominator else 0.0


class GateError(RuntimeError):
    pass


class QualityError(GateError):
    pass


def require(ok, msg):
    if not ok:
        raise GateError(msg)


def hdfs(*args, input_bytes=None):
    p = subprocess.run(['hdfs', 'dfs', *args], input=input_bytes, capture_output=True,
                       check=False)
    if p.returncode:
        raise GateError(f'HDFS {args[0]} failed: {p.stderr.decode(errors="replace")[-450:]}')
    return p.stdout


def exists(path):
    p = subprocess.run(['hdfs', 'dfs', '-test', '-e', path], capture_output=True, check=False)
    if p.returncode == 0:
        return True
    if p.returncode == 1 and b'Permission denied' not in p.stderr:
        return False
    raise GateError('HDFS path check failed: ' + path)


def read_json(path):
    try:
        raw = hdfs('-cat', path)
        value = json.loads(raw)
    except (UnicodeDecodeError, ValueError) as exc:
        raise GateError('Invalid JSON at ' + path) from exc
    require(isinstance(value, dict), 'Expected a JSON object at ' + path)
    return value, raw


def json_sha(raw):
    return hashlib.sha256(raw).hexdigest()


def source_evidence():
    """Bind 59 to the immutable 46/47/52 evidence and successful 58 proof."""
    manifest, mraw = read_json(SOURCE_MANIFEST)
    approval, araw = read_json(SOURCE_APPROVAL)
    report_path = approval.get('quality_report_path')
    require(isinstance(report_path, str) and
            re.fullmatch(r'/data/olist/quality/v3/runs/[0-9a-f]{32}/_QUALITY_REPORT.json', report_path),
            'Approval has unexpected report path')
    report, rraw = read_json(report_path)
    proof58, _ = read_json(STEP58_PROOF)
    digest = json_sha(mraw)
    report_digest = json_sha(rraw)
    require(manifest.get('pipeline') == 'olist_spark_batch_etl' and
            manifest.get('raw_version') == 'v1', 'Unexpected ETL manifest provenance')
    require(manifest.get('tables') == [name for name, _ in SOURCE_TABLES],
            'Step 46 source inventory mismatch')
    metrics = manifest.get('metrics', {})
    require(metrics.get('etl_version') == 'v3', 'Only approved Olist v3 allowed')
    require(approval.get('schema_version') == 1 and
            approval.get('kind') == 'olist_quality_gated_approval' and
            approval.get('warehouse_version') == 'v3' and
            approval.get('etl_manifest_sha256') == digest and
            approval.get('quality_report_sha256') == report_digest,
            'Step 52 approval does not match source manifest and quality report')
    checks = report.get('checks', [])
    require(report.get('status') == 'PASS' and report.get('warehouse_version') == 'v3'
            and report.get('etl_manifest_sha256') == digest and
            isinstance(checks, list) and len(checks) >= CHECK_COUNT and
            all(isinstance(c, dict) and c.get('status') == 'PASS' for c in checks),
            'Step 47 original quality evidence is not PASS')
    counts = metrics.get('output_rows', {})
    require(isinstance(counts, dict) and all(type(counts.get(k)) is int and
            counts[k] >= 0 for k, _ in SOURCE_TABLES), 'Invalid source manifest counts')
    require(proof58.get('schema') == 'bigtop.olist.iceberg-materialization.v1' and
            proof58.get('status') == 'PASS' and
            proof58.get('source_version') == 'v3' and
            proof58.get('namespace') == SOURCE_NS and
            proof58.get('parity') == 'bidirectional_exceptAll_exact_multiset' and
            proof58.get('etl_manifest_sha256') == digest and
            proof58.get('quality_report_sha256') == report_digest and
            proof58.get('tables') == {target: counts[src] for src, target in SOURCE_TABLES},
            'Step 58 exact parity proof is absent, invalid or does not match v3')
    require(counts['mart/daily_category_sales'] > 1, 'Gold release needs more than 1 row')
    return {'manifest': manifest, 'manifest_sha256': digest,
            'approval_sha256': json_sha(araw),
            'quality_report_sha256': report_digest,
            'step58_proof_sha256': json_sha(hdfs('-cat', STEP58_PROOF)),
            'expected_gold_count': counts['mart/daily_category_sales']}


def validate_stage_report(report, evidence):
    require(isinstance(report, dict) and
            report.get('schema') == 'bigtop.olist.iceberg-wap-stage.v1' and
            report.get('status') == 'PASS' and
            report.get('source_namespace') == SOURCE_NS and
            report.get('target_table') == TARGET and
            report.get('branch') == BRANCH and
            report.get('quality_checks_total') == CHECK_COUNT and
            report.get('quality_checks_failed') == 0 and
            isinstance(report.get('checks'), list) and
            len(report['checks']) == CHECK_COUNT and
            all(c.get('status') == 'PASS' for c in report['checks']) and
            report.get('expected_gold_count') == evidence['expected_gold_count'] and
            report.get('manifest_sha256') == evidence['manifest_sha256'] and
            report.get('approval_sha256') == evidence['approval_sha256'] and
            report.get('step58_proof_sha256') == evidence['step58_proof_sha256'] and
            report.get('quality_report_sha256') == evidence['quality_report_sha256'],
            'Invalid/stale or failing immutable Stage 59 quality report')
    snapshot_ids = report.get('source_snapshot_ids')
    require(isinstance(snapshot_ids, dict) and
            set(snapshot_ids) == {target for _, target in SOURCE_TABLES} and
            all(isinstance(v, str) and v.isdigit() for v in snapshot_ids.values()),
            'Source snapshot inventory missing')
    for k in ('main_snapshot_before', 'audit_snapshot_id'):
        require(isinstance(report.get(k), str) and report[k].isdigit(),
                'Invalid stage snapshot ID: ' + k)
    require(report['main_snapshot_before'] != report['audit_snapshot_id'],
            'Stage must represent an unpublished candidate')


def atomic_report(path, report):
    """Create immutable single JSON report; no -f/overwrite, temp + rename on HDFS."""
    require(not exists(path), f'Proof already exists: {path}')
    parent = path.rsplit('/', 1)[0]
    hdfs('-mkdir', '-p', parent)
    with tempfile.TemporaryDirectory(prefix='step59-') as local:
        temp_path = Path(local) / 'report.json'
        temp_path.write_text(json.dumps(report, sort_keys=True, ensure_ascii=False,
                                       indent=2) + '\n', encoding='utf-8')
        staging = parent + '/.tmp-' + os.urandom(12).hex() + '.json'
        hdfs('-put', str(temp_path), staging)
        try:
            require(not exists(path), f'Concurrent report publication at {path}')
            hdfs('-mv', staging, path)
        except Exception:
            # Do not delete staging automatically: retain partial evidence for review.
            raise


def refs(spark, table):
    return {str(r['name']): str(r['snapshot_id']) for r in
            spark.sql(f'SELECT name, snapshot_id FROM {table}.refs').collect()}


def source_refs(spark):
    result = {}
    for _, short in SOURCE_TABLES:
        item = refs(spark, f'{CATALOG}.{SOURCE_NS}.{short}')
        require('main' in item, f'Missing Step58 source main ref: {short}')
        result[short] = item['main']
    return result


def exact_parity(left, right):
    return left.exceptAll(right).limit(1).count() == 0 and right.exceptAll(left).limit(1).count() == 0


class CheckBook:
    def __init__(self):
        self.checks = []

    def add(self, name, passed, observed, expected, operator='eq'):
        self.checks.append({'id': name, 'status': 'PASS' if passed else 'FAIL',
                            'observed': observed, 'expected': expected,
                            'operator': operator})

    def equal(self, name, observed, expected):
        self.add(name, observed == expected, observed, expected)

    def maximum(self, name, observed, expected):
        self.add(name, observed <= expected, observed, expected, 'lte')

    @property
    def failed(self):
        return sum(c['status'] == 'FAIL' for c in self.checks)

    @property
    def status(self):
        return 'PASS' if self.failed == 0 else 'FAIL'


# The inspect_iceberg implementation below is derived from the checks in Step 47.
# It intentionally runs the same 29 rule IDs against 6 source Iceberg main tables
# and the *staged Gold branch*, never against the published Gold main.
def inspect_iceberg(spark, manifest: dict, maximum_quarantine_rate: float,
                    maximum_orphans: int, checks: CheckBook) -> dict:
    """Collect bounded aggregate statistics; no customer/order IDs in the report."""
    from pyspark.sql import functions as F

    frames = {name: spark.table(f"{CATALOG}.{SOURCE_NS}.{short}")
              for name, short in SOURCE_TABLES}
    frames["mart/daily_category_sales"] = spark.table(BRANCH_TABLE)
    counts = {name: frames[name].count() for name in TABLES}
    for name, _ in SOURCE_TABLES:
        checks.equal("manifest_rows_" + name.replace("/", "_"),
                     counts[name], manifest["metrics"]["output_rows"][name])

    curated_keys = {
        "curated/orders": ("order_id",),
        "curated/order_items": ("order_id", "item_no"),
        "curated/customers": ("customer_id",),
        "curated/products": ("product_id",),
        "curated/order_item_fact": ("order_id", "order_item_id"),
    }
    for name, keys in curated_keys.items():
        duplicates = (frames[name].groupBy(*keys).count()
                      .where(F.col("count") > 1).count())
        checks.equal("unique_" + name.replace("/", "_"), duplicates, 0)

    fact = frames["curated/order_item_fact"]
    nulls = fact.where(
        F.col("order_id").isNull() | F.col("order_item_id").isNull() |
        F.col("purchase_date").isNull() | F.col("product_id").isNull() |
        F.col("price").isNull() | F.col("freight_value").isNull() |
        F.col("item_total").isNull()
    ).count()
    checks.equal("fact_required_columns_not_null", nulls, 0)
    invalid_amounts = fact.where(
        (F.col("price") < 0) | (F.col("freight_value") < 0) |
        (F.col("item_total") != F.col("price") + F.col("freight_value"))
    ).count()
    checks.equal("fact_amounts_reconciled", invalid_amounts, 0)

    # Independent referential integrity check of the published fact.
    fact_keys = fact.select("order_id", "order_item_id")
    orphan_facts = (fact_keys.join(
        frames["curated/order_items"].select(
            "order_id", F.col("item_no").alias("order_item_id")),
        ["order_id", "order_item_id"], "left_anti").count())
    checks.equal("fact_items_exist_in_curated", orphan_facts, 0)
    orphan_orders = fact.select("order_id").join(
        frames["curated/orders"].select("order_id"), "order_id", "left_anti").count()
    checks.equal("fact_orders_exist_in_curated", orphan_orders, 0)
    customer_ref = (fact.select("order_id", "customer_unique_id").alias("f")
                    .join(frames["curated/orders"].select(
                        "order_id", "customer_id").alias("o"),
                        F.col("f.order_id") == F.col("o.order_id"), "left")
                    .join(frames["curated/customers"].select(
                        "customer_id", "customer_unique_id").alias("c"),
                        F.col("o.customer_id") == F.col("c.customer_id"), "left"))
    inconsistent_customers = customer_ref.where(
        F.col("c.customer_id").isNull() |
        (~F.col("f.customer_unique_id").eqNullSafe(F.col("c.customer_unique_id")))
    ).count()
    checks.equal("fact_customer_relationship_valid", inconsistent_customers, 0)
    orphan_products = fact.select("product_id").join(
        frames["curated/products"].select("product_id"), "product_id", "left_anti").count()
    checks.equal("fact_products_exist_in_curated", orphan_products, 0)

    quarantine = frames["quarantine/rows"]
    reason_stats = quarantine.groupBy("dataset", "reason").count().collect()
    breakdown: dict[str, dict[str, int]] = {}
    for row in reason_stats:
        ds, reason, count = row["dataset"], row["reason"], int(row["count"])
        if ds is None or reason is None:
            raise QualityError("Quarantine contains null dataset/reason")
        breakdown.setdefault(ds, {})[reason] = count
    orphan_count = sum(breakdown.get("order_items", {}).get(r, 0) for r in ORPHAN_REASONS)
    checks.equal("orphan_count_matches_manifest", orphan_count,
                 manifest["metrics"]["orphan_order_items"])
    checks.maximum("orphan_rows_threshold", orphan_count, maximum_orphans)
    item_rows = counts["curated/order_items"]
    checks.equal("valid_items_partitioned_to_fact_or_orphans", counts["curated/order_item_fact"] + orphan_count, item_rows)

    for ds in SOURCE_NAMES:
        valid = counts["curated/" + ds]
        invalid = sum(c for reason, c in breakdown.get(ds, {}).items()
                      if ds != "order_items" or reason not in ORPHAN_REASONS)
        checks.equal("raw_conservation_" + ds, valid + invalid,
                     manifest["metrics"]["source_rows"][ds])

    raw_rows = sum(manifest["metrics"]["source_rows"].values())
    invalid_source_rows = counts["quarantine/rows"] - orphan_count
    quarantine_rate = rate(invalid_source_rows, raw_rows)
    checks.maximum("quarantine_rate_threshold", quarantine_rate, maximum_quarantine_rate)

    # Recompute the entire Mart independently, compare rows and monetary sums.
    delivered = fact.where(F.col("order_status") == "delivered")
    expected_mart = (delivered
                     .groupBy("purchase_date", "product_category", "customer_state")
                     .agg(F.countDistinct("order_id").alias("orders"),
                          F.count(F.lit(1)).alias("sold_units"),
                          F.sum("price").alias("gross_item_revenue"),
                          F.sum("freight_value").alias("total_freight"),
                          F.sum("item_total").alias("gross_item_total"))
                     .select(*MART_COLS))
    actual_mart = frames["mart/daily_category_sales"].select(*MART_COLS)
    missing_mart = expected_mart.exceptAll(actual_mart).count()
    extra_mart = actual_mart.exceptAll(expected_mart).count()
    checks.equal("mart_missing_or_changed_rows", missing_mart, 0)
    checks.equal("mart_extra_or_changed_rows", extra_mart, 0)
    delivered_count = delivered.count()
    mart_sold_units = actual_mart.agg(F.coalesce(F.sum("sold_units"), F.lit(0))).first()[0]
    checks.equal("mart_units_match_delivered_fact", int(mart_sold_units), delivered_count)

    return {
        "fact_rows": counts["curated/order_item_fact"],
        "mart_rows": counts["mart/daily_category_sales"],
        "quarantine_rows": counts["quarantine/rows"],
        "invalid_source_rows": invalid_source_rows,
        "quarantine_rate": quarantine_rate,
        "orphan_order_items": orphan_count,
        "delivered_fact_rows": delivered_count,
        "input_rows": raw_rows,
        "output_rows": counts,
        "quarantine_breakdown": breakdown,
    }



def schema_signature(frame):
    return [(field.name, field.dataType.simpleString()) for field in frame.schema]


def inspect_release(spark, evidence, source_snapshot_ids):
    source_gold = spark.table(SRC_GOLD)
    staged_gold = spark.table(BRANCH_TABLE)
    count = staged_gold.count()
    checks = CheckBook()
    checks.equal('iceberg_candidate_gold_rows', count, evidence['expected_gold_count'])
    # Step 47's original 29 rules remain separately recognizable.
    legacy = CheckBook()
    observations = inspect_iceberg(spark, evidence['manifest'], 0.05, 0, legacy)
    require(len(legacy.checks) == CHECK_COUNT, 'Expected exactly 29 Step47-derived checks')
    # Cross engine/parity checks are gates as well, not just monitoring.
    parity = exact_parity(source_gold, staged_gold)
    require(parity, 'Gold candidate has mismatched rows, duplicates, or values')
    require(schema_signature(source_gold) == schema_signature(staged_gold),
            'Gold candidate schema changed')
    require(source_refs(spark) == source_snapshot_ids,
            'Step58 source snapshots moved while auditing')
    require(checks.failed == 0, 'Step59 candidate row count does not match expected')
    return legacy, observations, count


def ensure_target(spark, evidence):
    source = spark.table(SRC_GOLD)
    if not spark.catalog.tableExists(TARGET):
        spark.sql(f'CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{TARGET_NS}')
        # Iceberg cannot branch a table without an initial snapshot. Seed this
        # *isolated technical* main table with exactly 1 real source row. It is
        # NOT the published consumer table until explicit publish succeeds.
        seed = source.orderBy('purchase_date', 'product_category', 'customer_state').limit(1)
        (seed.writeTo(TARGET).using('iceberg')
         .tableProperty('format-version', '2')
         .tableProperty('write.wap.enabled', 'true')
         .tableProperty('bigtop.olist.step', '59')
         .tableProperty('bigtop.olist.source-manifest-sha256', evidence['manifest_sha256'])
         .tableProperty('bigtop.olist.release-status', 'experimental')
         .create())
        print('STEP59_GOLD_SEED_CREATED', flush=True)
    props = {str(r[0]): str(r[1]) for r in
             spark.sql(f'SHOW TBLPROPERTIES {TARGET}').collect()}
    require(props.get('bigtop.olist.step') == '59' and
            props.get('bigtop.olist.source-manifest-sha256') == evidence['manifest_sha256'],
            'Refusing to use unexpected pre-existing Gold release table')
    require(schema_signature(source) == schema_signature(spark.table(TARGET)),
            'Target main schema mismatch')
    current = spark.table(TARGET)
    main_count = current.count()
    require(main_count in (1, evidence['expected_gold_count']),
            'Unexpected main count; do not overwrite existing release')
    if main_count == 1:
        require(current.exceptAll(source).limit(1).count() == 0,
                'Seed does not originate from approved source Gold')
    elif main_count == evidence['expected_gold_count']:
        require(exact_parity(source, current), 'Published main differs from source Gold')
    return main_count


def stage(spark):
    evidence = source_evidence()
    source_snaps = source_refs(spark)
    prior = None
    if exists(PROOF_ROOT + '/_STAGE_REPORT.json'):
        prior, _ = read_json(PROOF_ROOT + '/_STAGE_REPORT.json')
        validate_stage_report(prior, evidence)
    main_count = ensure_target(spark, evidence)
    before = refs(spark, TARGET)
    require('main' in before, 'Target main has no snapshot')
    if prior is None and BRANCH not in before:
        require(main_count == 1, 'Cannot create a new branch after publication')
        spark.sql(f'ALTER TABLE {TARGET} CREATE BRANCH IF NOT EXISTS {BRANCH}')
        before = refs(spark, TARGET)
    require(BRANCH in before, 'Missing audit branch')
    if prior is None:
        require(main_count == 1, 'Expected isolated one-row main before staging')
        if before['main'] == before[BRANCH]:
            source = spark.table(SRC_GOLD)
            main = spark.table(TARGET)
            remainder = source.exceptAll(main)
            require(remainder.count() == evidence['expected_gold_count'] - 1,
                    'Staging remainder differs from expected source Gold')
            remainder.writeTo(BRANCH_TABLE).append()
            print('STEP59_WAP_BRANCH_APPEND_COMPLETE', flush=True)
        after = refs(spark, TARGET)
        require(after['main'] == before['main'], 'Stage accidentally moved main')
        require(after[BRANCH] != after['main'], 'Stage did not create a distinct audit snapshot')
        report = {'schema': 'bigtop.olist.iceberg-wap-stage.v1',
                  'source_namespace': SOURCE_NS, 'target_table': TARGET,
                  'branch': BRANCH, 'source_snapshot_ids': source_snaps,
                  'main_snapshot_before': after['main'],
                  'audit_snapshot_id': after[BRANCH],
                  'expected_gold_count': evidence['expected_gold_count'],
                  'manifest_sha256': evidence['manifest_sha256'],
                  'approval_sha256': evidence['approval_sha256'],
                  'quality_report_sha256': evidence['quality_report_sha256'],
                  'step58_proof_sha256': evidence['step58_proof_sha256'],
                  'checked_at_utc': datetime.now(timezone.utc).isoformat()}
    else:
        report = prior
        after = before
        require(after[BRANCH] == prior['audit_snapshot_id'] and
                after['main'] in (prior['main_snapshot_before'], prior['audit_snapshot_id']),
                'Existing staged snapshot or main changed unexpectedly')
        require(source_snaps == prior['source_snapshot_ids'], 'Source Snapshot changed after approval')
    legacy, observations, gold_count = inspect_release(spark, evidence, source_snaps)
    if prior is None:
        report.update({'status': legacy.status,
                       'quality_checks_total': len(legacy.checks),
                       'quality_checks_failed': legacy.failed,
                       'checks': legacy.checks,
                       'candidate_gold_count': gold_count,
                       'observations': observations,
                       'quality_scope': 'Step47 29 rules on Step58 Silver+Quarantine and staged Step59 Gold',
                       'publication_scope': 'One Gold table only; no atomic seven-table release'})
        # Persist FAIL too. Publish explicitly refuses anything but PASS.
        atomic_report(PROOF_ROOT + '/_STAGE_REPORT.json', report)
    validate_stage_report(report, evidence)
    print('STEP59_WAP_STAGED_PASS: 29 quality checks, Gold parity, immutable stage proof; '
          'no Gold main fast_forward', flush=True)


def publish(spark):
    evidence = source_evidence()
    stage_path = PROOF_ROOT + '/_STAGE_REPORT.json'
    require(exists(stage_path), 'Missing stage proof; run mode=stage first')
    report, stage_raw = read_json(stage_path)
    validate_stage_report(report, evidence)
    require(spark.catalog.tableExists(TARGET), 'Missing release table')
    current_source_snaps = source_refs(spark)
    require(current_source_snaps == report['source_snapshot_ids'],
            'Step58 source snapshots changed since Stage 59 audit')
    current = refs(spark, TARGET)
    require(current.get(BRANCH) == report['audit_snapshot_id'],
            'Audit branch advanced since stage quality approval')
    require(current.get('main') in (report['main_snapshot_before'], report['audit_snapshot_id']),
            'Main branch advanced independently; publication blocked')
    if current['main'] == report['main_snapshot_before']:
        require(spark.table(TARGET).count() == 1,
                'Main is not the original one-row technical seed')
    _, _, gold_count = inspect_release(spark, evidence, report['source_snapshot_ids'])
    require(gold_count == report['expected_gold_count'], 'Staged row count drift')
    already = current['main'] == current[BRANCH]
    if not already:
        spark.sql(f"CALL {CATALOG}.system.fast_forward('{TARGET_NS}.{GOLD}', 'main', '{BRANCH}')")
        spark.catalog.refreshTable(TARGET)
    end = refs(spark, TARGET)
    require(end.get('main') == report['audit_snapshot_id'] and
            end.get(BRANCH) == report['audit_snapshot_id'],
            'fast_forward did not publish the exact audited snapshot')
    require(spark.table(TARGET).count() == report['expected_gold_count'] and
            exact_parity(spark.table(TARGET), spark.table(SRC_GOLD)),
            'Published main differs from audited source Gold')
    publication = {
        'schema': 'bigtop.olist.iceberg-wap-publish.v1', 'status': 'PASS',
        'target_table': TARGET, 'branch': BRANCH,
        'main_snapshot_id': end['main'],
        'stage_report_sha256': json_sha(stage_raw),
        'published_rows': report['expected_gold_count'],
        'published_at_utc': datetime.now(timezone.utc).isoformat(),
        'scope': 'One Gold table. No atomic Silver+Gold multi-table transaction.',
    }
    target = PROOF_ROOT + '/_PUBLISH_REPORT.json'
    if exists(target):
        previous, _ = read_json(target)
        for key in ('schema', 'status', 'target_table', 'branch', 'main_snapshot_id',
                    'stage_report_sha256', 'published_rows', 'scope'):
            require(previous.get(key) == publication.get(key),
                    'Existing publication proof differs: ' + key)
    else:
        atomic_report(target, publication)
    print('STEP59_WAP_PUBLISHED_PASS: Gold main now points to exact audited Iceberg '
          f'snapshot {end["main"]}; rows={report["expected_gold_count"]}', flush=True)


def run(mode):
    require(mode in ('stage', 'publish'), 'Unrecognized Step59 mode')
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName('olist-iceberg-wap59-' + mode).getOrCreate()
    try:
        if mode == 'stage':
            stage(spark)
        else:
            publish(spark)
    finally:
        spark.stop()


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--mode', choices=('stage', 'publish'), required=True)
    args = p.parse_args()
    try:
        run(args.mode)
    except Exception as exc:  # Fail closed, with actionable message in YARN driver logs.
        print(f'STEP59_WAP_FAILED: {type(exc).__name__}: {exc}', file=sys.stderr, flush=True)
        raise


if __name__ == '__main__':
    main()
