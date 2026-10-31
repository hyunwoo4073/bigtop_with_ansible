#!/usr/bin/env python3
"""Step 47: independently audit immutable Olist Spark ETL Parquet outputs.

This job never modifies step-45 Raw or step-46 warehouse datasets.
It writes a new, immutable JSON quality report per run to HDFS.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

QUALITY_VERSION = "47.1"
TABLES = (
    "curated/orders",
    "curated/order_items",
    "curated/customers",
    "curated/products",
    "curated/order_item_fact",
    "quarantine/rows",
    "mart/daily_category_sales",
)
ORPHAN_REASONS = ("missing_order", "missing_customer", "missing_product")
SOURCE_NAMES = ("orders", "order_items", "customers", "products")
MART_COLS = (
    "purchase_date", "product_category", "customer_state", "orders", "sold_units",
    "gross_item_revenue", "total_freight", "gross_item_total",
)


class QualityError(RuntimeError):
    pass


def check_version(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,31}", value):
        raise QualityError(f"Invalid warehouse version: {value!r}")
    return value


def check_run_id(value: str) -> str:
    if not re.fullmatch(r"[a-f0-9]{32}", value):
        raise QualityError("run ID must be a 32-character lowercase UUID hex string")
    return value


def check_roots(warehouse_root: str, quality_root: str) -> None:
    if warehouse_root != "/data/olist/warehouse" or quality_root != "/data/olist/quality":
        raise QualityError("Only /data/olist/warehouse and /data/olist/quality are allowed")


def check_thresholds(quarantine_rate: float, max_orphans: int) -> None:
    if not 0 <= quarantine_rate <= 1:
        raise QualityError("--max-quarantine-rate must be between 0 and 1")
    if max_orphans < 0:
        raise QualityError("--max-orphans cannot be negative")


def validate_etl_manifest(m: dict) -> None:
    if not isinstance(m, dict) or m.get("pipeline") != "olist_spark_batch_etl":
        raise QualityError("Step 46 ETL manifest has an unexpected pipeline")
    if not isinstance(m.get("pipeline_version"), str) or not m["pipeline_version"]:
        raise QualityError("Step 46 ETL pipeline version missing")
    if (not isinstance(m.get("raw_manifest_sha256"), str) or
            not re.fullmatch(r"[0-9a-f]{64}", m["raw_manifest_sha256"])):
        raise QualityError("Step 46 source manifest fingerprint invalid")
    if not isinstance(m.get("tables"), list) or m["tables"] != list(TABLES):
        raise QualityError("Unexpected Step 46 Parquet table inventory")
    metrics = m.get("metrics")
    if not isinstance(metrics, dict):
        raise QualityError("Step 46 ETL metrics missing")
    if not isinstance(metrics.get("source_rows"), dict) or not isinstance(metrics.get("output_rows"), dict):
        raise QualityError("Step 46 row metric maps missing")
    for k in SOURCE_NAMES:
        if (not isinstance(metrics["source_rows"].get(k), int) or
                isinstance(metrics["source_rows"][k], bool) or metrics["source_rows"][k] < 0):
            raise QualityError(f"Invalid Step 46 source row count: {k}")
    for table in TABLES:
        value = metrics.get("output_rows", {}).get(table)
        if not isinstance(value, int) or isinstance(value, bool) or value < 0:
            raise QualityError(f"Invalid Step 46 output row count: {table}")
    orphan = metrics.get("orphan_order_items")
    if not isinstance(orphan, int) or isinstance(orphan, bool) or orphan < 0:
        raise QualityError("Invalid Step 46 orphan item count")


def rate(numerator: int, denominator: int) -> float:
    return float(numerator) / denominator if denominator else 0.0


class CheckBook:
    def __init__(self) -> None:
        self.checks: list[dict] = []

    def add(self, name: str, passed: bool, observed: int | float, expected: int | float,
            operator: str = "eq") -> None:
        self.checks.append({
            "id": name,
            "status": "PASS" if passed else "FAIL",
            "observed": observed,
            "expected": expected,
            "operator": operator,
        })

    def equal(self, name: str, observed: int, expected: int) -> None:
        self.add(name, observed == expected, observed, expected)

    def maximum(self, name: str, observed: int | float, expected: int | float) -> None:
        self.add(name, observed <= expected, observed, expected, "lte")

    @property
    def failed(self) -> int:
        return sum(c["status"] == "FAIL" for c in self.checks)

    @property
    def status(self) -> str:
        return "FAIL" if self.failed else "PASS"


def build_report(run_id: str, version: str, epoch: int, manifest_hash: str,
                 checks: CheckBook, observations: dict, *, error: str | None = None) -> dict:
    status = "ERROR" if error else checks.status
    report = {
        "schema_version": 1,
        "pipeline": "olist_quality_monitor",
        "quality_version": QUALITY_VERSION,
        "warehouse_version": version,
        "etl_manifest_sha256": manifest_hash,
        "run_id": run_id,
        "run_timestamp_seconds": epoch,
        "checked_at_utc": datetime.fromtimestamp(epoch, timezone.utc).isoformat(),
        "status": status,
        "summary": {
            "checks_total": len(checks.checks),
            "checks_failed": checks.failed,
            **observations,
        },
        "checks": checks.checks,
    }
    if error:
        report["error"] = error[:400]
    return report


class Hdfs:
    def command(self, *args: str) -> subprocess.CompletedProcess:
        proc = subprocess.run(["hdfs", "dfs", *args], check=False, capture_output=True)
        if proc.returncode:
            raise QualityError("HDFS command failed: " + args[0] + ": " +
                               proc.stderr.decode("utf-8", errors="replace")[-500:])
        return proc

    def exists(self, path: str) -> bool:
        proc = subprocess.run(["hdfs", "dfs", "-test", "-e", path],
                              check=False, capture_output=True)
        if proc.returncode == 0:
            return True
        if proc.returncode == 1 and b"Permission denied" not in proc.stderr:
            return False
        raise QualityError("Unable to check HDFS path: " + path)

    def cat(self, path: str) -> bytes:
        return self.command("-cat", path).stdout

    def publish(self, destination: str, stage: str, report: dict) -> None:
        if self.exists(destination) or self.exists(stage):
            raise QualityError("Quality report already exists for this run ID")
        parent = destination.rsplit("/", 1)[0]
        self.command("-mkdir", "-p", parent)
        self.command("-mkdir", "-p", stage)
        with tempfile.TemporaryDirectory(prefix="olist-quality-") as tmp:
            path = Path(tmp) / "_QUALITY_REPORT.json"
            path.write_text(json.dumps(report, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
                            encoding="utf-8")
            self.command("-put", str(path), stage + "/_QUALITY_REPORT.json")
        self.command("-mv", stage, destination)


def inspect(spark, warehouse: str, manifest: dict, maximum_quarantine_rate: float,
            maximum_orphans: int, checks: CheckBook) -> dict:
    """Collect bounded aggregate statistics; no customer/order IDs in the report."""
    from pyspark.sql import functions as F

    frames = {name: spark.read.parquet("hdfs://" + warehouse + "/" + name)
              for name in TABLES}
    counts = {name: frames[name].count() for name in TABLES}
    for name in TABLES:
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


def execute(args: argparse.Namespace) -> int:
    check_roots(args.warehouse_root, args.quality_root)
    version = check_version(args.warehouse_version)
    run_id = check_run_id(args.run_id)
    check_thresholds(args.max_quarantine_rate, args.max_orphans)

    hdfs = Hdfs()
    source = args.warehouse_root + "/" + version
    destination = args.quality_root + "/" + version + "/runs/" + run_id
    staging = args.quality_root + "/_staging/" + run_id
    for path in (source, source + "/_ETL_MANIFEST.json"):
        if not hdfs.exists(path):
            raise QualityError("Step 46 warehouse snapshot/manifest missing: " + path)
    manifest_bytes = hdfs.cat(source + "/_ETL_MANIFEST.json")
    manifest = json.loads(manifest_bytes)
    validate_etl_manifest(manifest)
    if manifest.get("metrics", {}).get("etl_version") != version:
        raise QualityError("Step 46 output version does not match the requested warehouse version")
    for table in TABLES:
        if not hdfs.exists(source + "/" + table + "/_SUCCESS"):
            raise QualityError("Step 46 Parquet output is not complete: " + table)
    manifest_hash = hashlib.sha256(manifest_bytes).hexdigest()

    checks = CheckBook()
    observations: dict = {}
    epoch = int(time.time())
    spark = None
    error = None
    try:
        from pyspark.sql import SparkSession
        spark = (SparkSession.builder.appName("olist-quality-" + version)
                 .config("spark.sql.session.timeZone", "UTC")
                 .getOrCreate())
        observations = inspect(spark, source, manifest,
                               args.max_quarantine_rate, args.max_orphans, checks)
    except Exception as exc:  # persist failure rather than presenting stale PASS
        error = f"{type(exc).__name__}: {str(exc)[:320]}"
    finally:
        if spark is not None:
            try:
                spark.stop()
            except Exception as exc:
                error = error or f"SparkStopError: {str(exc)[:300]}"

    report = build_report(run_id, version, epoch, manifest_hash, checks, observations, error=error)
    hdfs.publish(destination, staging, report)
    print("OLIST QUALITY REPORT " + destination + "/_QUALITY_REPORT.json", flush=True)
    print("OLIST QUALITY STATUS " + report["status"], flush=True)
    return 0 if report["status"] == "PASS" else 2


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--warehouse-root", default="/data/olist/warehouse")
    parser.add_argument("--warehouse-version", default="v1")
    parser.add_argument("--quality-root", default="/data/olist/quality")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--max-quarantine-rate", type=float, default=0.05)
    parser.add_argument("--max-orphans", type=int, default=0)
    args = parser.parse_args()
    try:
        return execute(args)
    except (QualityError, ValueError, subprocess.SubprocessError) as exc:
        print("OLIST QUALITY ERROR: " + str(exc), file=sys.stderr, flush=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
