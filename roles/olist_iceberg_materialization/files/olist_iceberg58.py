#!/usr/bin/env python3
"""Step 58: immutable, verified materialization of approved Olist v3 Parquet to REST Iceberg.

Intentionally does not re-run Raw ETL or implement WAP promotion. Source Parquet is
read-only; Iceberg tables are version-scoped, with fail-closed provenance and exact
multiset parity checks. Partial failures can be resumed without replacing tables.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
import sys
from dataclasses import dataclass

CATALOG = "shared"
NAMESPACE = "olist58_v3"
WAREHOUSE_ROOT = "/data/olist/warehouse"
APPROVAL_ROOT = "/data/olist/published"
PROOF_ROOT = "/data/olist/iceberg-validation/step58"
SOURCE_VERSION = "v3"
MAPPING = (
    ("curated/orders", "silver_orders"),
    ("curated/order_items", "silver_order_items"),
    ("curated/customers", "silver_customers"),
    ("curated/products", "silver_products"),
    ("curated/order_item_fact", "silver_order_item_fact"),
    ("quarantine/rows", "quality_quarantine_rows"),
    ("mart/daily_category_sales", "gold_daily_category_sales"),
)
TABLES = tuple(src for src, _ in MAPPING)
PROPERTIES = {
    "bigtop.olist.step": "58",
    "bigtop.olist.source-version": SOURCE_VERSION,
}
HEX64 = re.compile(r"[a-f0-9]{64}\Z")


class MaterializationError(RuntimeError):
    pass


def require(condition: bool, msg: str) -> None:
    if not condition:
        raise MaterializationError(msg)


def hdfs_read(path: str) -> bytes:
    result = subprocess.run(["hdfs", "dfs", "-cat", path], capture_output=True, check=False)
    require(result.returncode == 0, f"HDFS read failed: {path}: {result.stderr.decode(errors='replace')[-600:]}")
    return result.stdout


def hdfs_file_exists(path: str) -> bool:
    result = subprocess.run(["hdfs", "dfs", "-test", "-e", path], capture_output=True, check=False)
    if result.returncode == 0:
        return True
    if result.returncode == 1 and b"Permission denied" not in result.stderr:
        return False
    raise MaterializationError(f"HDFS existence check failed: {path}: {result.stderr[-300:]!r}")


def read_object(raw: bytes, name: str) -> dict:
    try:
        value = json.loads(raw)
    except (UnicodeDecodeError, ValueError) as exc:
        raise MaterializationError(f"Invalid {name} JSON") from exc
    require(isinstance(value, dict), f"{name} must be JSON object")
    return value


@dataclass(frozen=True)
class Provenance:
    version: str
    manifest_sha256: str
    quality_report_sha256: str
    expected_counts: dict[str, int]


def validate_provenance(manifest_bytes: bytes, approval_bytes: bytes, report_bytes: bytes) -> Provenance:
    manifest = read_object(manifest_bytes, "Step 46 ETL manifest")
    approval = read_object(approval_bytes, "Step 52 approval")
    report = read_object(report_bytes, "Step 47 quality report")
    digest = hashlib.sha256(manifest_bytes).hexdigest()
    report_digest = hashlib.sha256(report_bytes).hexdigest()
    require(manifest.get("pipeline") == "olist_spark_batch_etl", "Unexpected source ETL pipeline")
    require(manifest.get("raw_version") == "v1", "Source must derive from immutable Raw v1")
    require(manifest.get("tables") == list(TABLES), "Source table inventory mismatch")
    metrics = manifest.get("metrics")
    require(isinstance(metrics, dict) and metrics.get("etl_version") == SOURCE_VERSION,
            "Source manifest version does not match v3")
    counts = metrics.get("output_rows")
    require(isinstance(counts, dict), "Missing ETL output_rows")
    for source in TABLES:
        value = counts.get(source)
        require(type(value) is int and value >= 0, f"Invalid source row count: {source}")
    require(approval.get("schema_version") == 1 and approval.get("kind") == "olist_quality_gated_approval",
            "Source has no valid Step 52 approval")
    require(approval.get("warehouse_version") == SOURCE_VERSION, "Approval version mismatch")
    require(approval.get("etl_manifest_sha256") == digest, "Approval doesn't match source manifest")
    require(approval.get("quality_report_sha256") == report_digest,
            "Approved quality report SHA256 mismatch")
    require(report.get("status") == "PASS", "Approved quality report is not PASS")
    require(report.get("warehouse_version") == SOURCE_VERSION, "Quality report version mismatch")
    require(report.get("etl_manifest_sha256") == digest, "Quality report manifest mismatch")
    checks = report.get("checks")
    require(isinstance(checks, list) and len(checks) >= 29 and
            all(isinstance(c, dict) and c.get("status") == "PASS" for c in checks),
            "Quality report has missing/failed checks")
    return Provenance(SOURCE_VERSION, digest, report_digest,
                      {key: counts[key] for key in TABLES})


def read_provenance() -> Provenance:
    base = f"{WAREHOUSE_ROOT}/{SOURCE_VERSION}"
    manifest = hdfs_read(f"{base}/_ETL_MANIFEST.json")
    approval = read_object(hdfs_read(f"{APPROVAL_ROOT}/{SOURCE_VERSION}/_APPROVAL.json"),
                           "Step 52 approval")
    report_path = approval.get("quality_report_path", "")
    require(isinstance(report_path, str) and
            report_path.startswith(f"/data/olist/quality/{SOURCE_VERSION}/runs/") and
            report_path.endswith("/_QUALITY_REPORT.json"), "Invalid approval report path")
    approval_raw = hdfs_read(f"{APPROVAL_ROOT}/{SOURCE_VERSION}/_APPROVAL.json")
    evidence = validate_provenance(manifest, approval_raw, hdfs_read(report_path))
    for source in TABLES:
        require(hdfs_file_exists(f"{base}/{source}/_SUCCESS"),
                f"Incomplete immutable source: {source}")
    return evidence


def catalog_table(suffix: str) -> str:
    require(suffix in {name for _, name in MAPPING}, f"Unknown target table: {suffix}")
    return f"{CATALOG}.{NAMESPACE}.{suffix}"


def property_dict(evidence: Provenance) -> dict[str, str]:
    return {**PROPERTIES, "bigtop.olist.source-manifest-sha256": evidence.manifest_sha256,
            "bigtop.olist.quality-report-sha256": evidence.quality_report_sha256}


def verify_table_properties(spark, table: str, expected: dict[str, str]) -> None:
    rows = spark.sql(f"SHOW TBLPROPERTIES {table}").collect()
    found = {str(row[0]): str(row[1]) for row in rows}
    for key, value in expected.items():
        require(found.get(key) == value,
                f"Existing target {table} has wrong provenance property {key}; refusing overwrite")


def exact_multiset_parity(source, target, name: str) -> None:
    """Bidirectional exceptAll preserves duplicate multiplicities; no sampling/hash shortcut."""
    if source.exceptAll(target).limit(1).count() or target.exceptAll(source).limit(1).count():
        raise MaterializationError(f"Exact row-level multiset parity failed: {name}")


def expected_proof(evidence: Provenance) -> dict:
    return {
        "schema": "bigtop.olist.iceberg-materialization.v1",
        "status": "PASS",
        "source_version": evidence.version,
        "etl_manifest_sha256": evidence.manifest_sha256,
        "quality_report_sha256": evidence.quality_report_sha256,
        "catalog": CATALOG,
        "namespace": NAMESPACE,
        "warehouse_source": f"hdfs://master:9000{WAREHOUSE_ROOT}/{SOURCE_VERSION}",
        "tables": {target: evidence.expected_counts[source] for source, target in MAPPING},
        "parity": "bidirectional_exceptAll_exact_multiset",
        "approval_scope": "Step52 approves source Parquet only; Iceberg publication requires Step59 gate",
    }


def write_or_verify_proof(spark, evidence: Provenance) -> None:
    proof_dir = f"hdfs://master:9000{PROOF_ROOT}/{SOURCE_VERSION}"
    expected = expected_proof(evidence)
    proof_file = f"{PROOF_ROOT}/{SOURCE_VERSION}/_SUCCESS"
    if hdfs_file_exists(proof_file):
        previous = [json.loads(r["value"]) for r in spark.read.text(proof_dir).collect()]
        require(previous == [expected], "Existing Step58 proof differs; refusing overwrite")
        print("STEP58_PROOF_EXISTING_VERIFIED", flush=True)
        return
    # Existing incomplete directory should block writes rather than silently overwrite it.
    require(not hdfs_file_exists(f"{PROOF_ROOT}/{SOURCE_VERSION}"),
            "Step58 proof directory incomplete; inspect manually before retrying")
    spark.createDataFrame([(json.dumps(expected, sort_keys=True),)], ["value"]).coalesce(1).write \
        .mode("errorifexists").text(proof_dir)
    require(hdfs_file_exists(proof_file), "Proof write did not create _SUCCESS")
    print("STEP58_PROOF_CREATED", flush=True)


def run(verify_only: bool = False) -> None:
    # Build SparkSession before doing any driver-side work, especially in YARN cluster mode.
    from pyspark.sql import SparkSession
    spark = (SparkSession.builder.appName("olist-iceberg-step58-v3")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    try:
        evidence = read_provenance()
        if not verify_only:
            spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{NAMESPACE}")
        else:
            # Fail closed if the namespace itself has not yet been created.
            spark.sql(f"SHOW TABLES IN {CATALOG}.{NAMESPACE}").collect()
        properties = property_dict(evidence)
        for source_name, target_name in MAPPING:
            target_table = catalog_table(target_name)
            df = spark.read.parquet(f"hdfs://master:9000{WAREHOUSE_ROOT}/{SOURCE_VERSION}/{source_name}")
            source_count = df.count()
            require(source_count == evidence.expected_counts[source_name],
                    f"Source {source_name} count changed: {source_count}")
            present = spark.catalog.tableExists(target_table)
            if not present:
                require(not verify_only, f"Missing table on verify-only run: {target_table}")
                writer = df.writeTo(target_table).using("iceberg")
                for prop, value in properties.items():
                    writer = writer.tableProperty(prop, value)
                writer.create()  # Never replace or overwrite existing snapshot/table.
                print(f"STEP58_CREATED: {target_table}", flush=True)
            verify_table_properties(spark, target_table, properties)
            actual = spark.table(target_table)
            require([(f.name, f.dataType.simpleString()) for f in df.schema] ==
                    [(f.name, f.dataType.simpleString()) for f in actual.schema],
                    f"Schema mismatch for {target_table}")
            target_count = actual.count()
            require(target_count == source_count, f"Row count mismatch for {target_table}")
            exact_multiset_parity(df, actual, target_table)
            print(f"STEP58_TABLE_PASS: {target_table} rows={target_count} parity=EXACT", flush=True)
        write_or_verify_proof(spark, evidence)
        print("STEP58_ICEBERG_SILVER_GOLD_PASS: 7 tables, exact parity", flush=True)
    finally:
        spark.stop()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--verify-only", action="store_true", help="Never create tables")
    args = parser.parse_args()
    try:
        run(verify_only=args.verify_only)
    except (MaterializationError, ValueError, OSError, subprocess.SubprocessError) as exc:
        print(f"STEP58_ICEBERG_FAIL: {exc}", file=sys.stderr, flush=True)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
