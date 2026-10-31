#!/usr/bin/env python3
"""Olist (four core raw CSVs) -> immutable Parquet via Spark on YARN.

Read all raw CSVs as strings with explicit header validation, normalize in Spark,
quarantine invalid/orphan rows, and commit a single versioned warehouse snapshot.
Only Python standard-library modules are imported before the Spark preflight.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
import sys
import tempfile
import uuid
from pathlib import Path

PIPELINE_VERSION = "46.1"
RAW_FILES = {
    "orders": "olist_orders_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "customers": "olist_customers_dataset.csv",
    "products": "olist_products_dataset.csv",
    "payments": "olist_order_payments_dataset.csv",
    "reviews": "olist_order_reviews_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
    "geolocation": "olist_geolocation_dataset.csv",
    "category_translation": "product_category_name_translation.csv",
}
# Explicit column order is intentional: Spark's enforceSchema=false verifies CSV headers.
CORE_COLUMNS = {
    "orders": [
        "order_id", "customer_id", "order_status", "order_purchase_timestamp",
        "order_approved_at", "order_delivered_carrier_date",
        "order_delivered_customer_date", "order_estimated_delivery_date",
    ],
    "order_items": [
        "order_id", "order_item_id", "product_id", "seller_id",
        "shipping_limit_date", "price", "freight_value",
    ],
    "customers": [
        "customer_id", "customer_unique_id", "customer_zip_code_prefix",
        "customer_city", "customer_state",
    ],
    "products": [
        "product_id", "product_category_name", "product_name_lenght",
        "product_description_lenght", "product_photos_qty",
        "product_weight_g", "product_length_cm", "product_height_cm",
        "product_width_cm",
    ],
}
OUTPUT_TABLES = (
    "curated/orders", "curated/order_items", "curated/customers",
    "curated/products", "curated/order_item_fact", "quarantine/rows",
    "mart/daily_category_sales",
)


class EtlError(RuntimeError):
    pass


def checked_version(version: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,31}", version):
        raise EtlError(f"Invalid snapshot version: {version!r}")
    return version


def checked_roots(raw_root: str, warehouse_root: str) -> None:
    if raw_root != "/data/olist/raw" or warehouse_root != "/data/olist/warehouse":
        raise EtlError("Only /data/olist/raw and /data/olist/warehouse are permitted")


def validate_raw_manifest(manifest: dict) -> None:
    if manifest.get("dataset") != "olist" or manifest.get("schema_version") != 1:
        raise EtlError("Unexpected Step 45 Olist raw manifest schema")
    files = manifest.get("files")
    if not isinstance(files, dict) or set(files) != set(RAW_FILES):
        raise EtlError("Step 45 raw manifest must contain the nine named Olist CSV files")
    for key, name in RAW_FILES.items():
        entry = files[key]
        if (not isinstance(entry, dict) or entry.get("filename") != name or
                not isinstance(entry.get("rows"), int) or entry["rows"] <= 0 or
                not isinstance(entry.get("bytes"), int) or entry["bytes"] <= 0 or
                not isinstance(entry.get("sha256"), str) or
                not re.fullmatch(r"[0-9a-f]{64}", entry["sha256"])):
            raise EtlError(f"Invalid Step 45 manifest entry: {key}")


def make_run_identity(raw_version: str, manifest_bytes: bytes) -> dict:
    return {
        "pipeline": "olist_spark_batch_etl",
        "pipeline_version": PIPELINE_VERSION,
        "raw_version": checked_version(raw_version),
        "raw_manifest_sha256": hashlib.sha256(manifest_bytes).hexdigest(),
    }


def is_same_run(existing: dict, identity: dict) -> bool:
    return all(existing.get(k) == v for k, v in identity.items())


class Hdfs:
    def command(self, *args: str) -> subprocess.CompletedProcess:
        result = subprocess.run(["hdfs", "dfs", *args], capture_output=True, check=False)
        if result.returncode:
            msg = result.stderr.decode("utf-8", errors="replace")[-1000:]
            raise EtlError(f"hdfs dfs {args[0]} failed (rc={result.returncode}): {msg}")
        return result

    def exists(self, path: str) -> bool:
        proc = subprocess.run(["hdfs", "dfs", "-test", "-e", path],
                              capture_output=True, check=False)
        if proc.returncode == 0:
            return True
        if proc.returncode == 1 and b"Permission denied" not in proc.stderr:
            return False
        raise EtlError(f"HDFS existence check failed: {path}: {proc.stderr[-500:]!r}")

    def cat(self, path: str) -> bytes:
        return self.command("-cat", path).stdout

    def put_json(self, path: str, obj: dict) -> None:
        with tempfile.TemporaryDirectory(prefix="olist-etl-manifest-") as tmp:
            source = Path(tmp) / "manifest.json"
            source.write_text(json.dumps(obj, sort_keys=True, ensure_ascii=False,
                                         indent=2) + "\n", encoding="utf-8")
            self.command("-put", str(source), path)


def check_published(hdfs: Hdfs, final_path: str, identity: dict) -> bool:
    if not hdfs.exists(final_path):
        return False
    marker = f"{final_path}/_ETL_MANIFEST.json"
    if not hdfs.exists(marker):
        raise EtlError(f"Output exists without manifest; refusing overwrite: {final_path}")
    try:
        existing = json.loads(hdfs.cat(marker))
    except (ValueError, UnicodeError) as exc:
        raise EtlError("Published ETL manifest cannot be parsed") from exc
    if not isinstance(existing, dict) or not is_same_run(existing, identity):
        raise EtlError("Output version already contains a different ETL; use new output version")
    for table in OUTPUT_TABLES:
        if not hdfs.exists(f"{final_path}/{table}/_SUCCESS"):
            raise EtlError(f"Incomplete published Parquet table: {table}")
    return True


def read_csv(spark, name: str, raw_version_path: str):
    from pyspark.sql.types import StringType, StructField, StructType

    schema = StructType([StructField(c, StringType(), True) for c in CORE_COLUMNS[name]])
    path = f"hdfs://{raw_version_path}/{name}/{RAW_FILES[name]}"
    return (spark.read.schema(schema)
            .option("header", "true")
            .option("enforceSchema", "false")
            .option("mode", "FAILFAST")
            .option("multiLine", "true")
            .option("encoding", "UTF-8")
            .csv(path))


def quality_table(source, dataset: str, checks: list[tuple[object, str]]):
    """Return (valid, invalid in common schema); no dropped rows."""
    from pyspark.sql import functions as F

    reason = F.lit(None).cast("string")
    # first failing condition wins; all rows have exactly one result class
    for condition, message in reversed(checks):
        reason = F.when(condition, F.lit(message)).otherwise(reason)
    marked = source.withColumn("_dq_reason", reason)
    key_columns = [c for c in ("order_id", "order_item_id", "customer_id", "product_id")
                   if c in source.columns]
    invalid = (marked.where(F.col("_dq_reason").isNotNull())
               .select(F.lit(dataset).alias("dataset"),
                       F.col("_dq_reason").alias("reason"),
                       F.concat_ws("#", *[F.col(c) for c in key_columns]).alias("business_key"),
                       F.to_json(F.struct(*[F.col(c) for c in source.columns])).alias("record_json")))
    valid = marked.where(F.col("_dq_reason").isNull()).drop("_dq_reason")
    return valid, invalid


def reject_duplicate_keys(df, keys: list[str], dataset: str) -> None:
    from pyspark.sql import functions as F

    duplicates = df.groupBy(*keys).count().where(F.col("count") > 1).limit(1).count()
    if duplicates:
        raise EtlError(f"Duplicate business keys in {dataset}: {keys}; no silent join fanout")


def build_tables(raw: dict):
    from pyspark.sql import functions as F

    def missing(column: str):
        return F.col(column).isNull() | (F.trim(F.col(column)) == "")

    orders = raw["orders"].withColumn(
        "purchase_ts", F.to_timestamp("order_purchase_timestamp", "yyyy-MM-dd HH:mm:ss"))
    valid_orders, bad_orders = quality_table(orders, "orders", [
        (missing("order_id"), "missing_order_id"),
        (missing("customer_id"), "missing_customer_id"),
        (F.col("purchase_ts").isNull(), "invalid_purchase_timestamp"),
        (missing("order_status"), "missing_order_status"),
    ])

    items = (raw["order_items"]
             .withColumn("item_no", F.col("order_item_id").cast("integer"))
             .withColumn("price_amount", F.col("price").cast("decimal(18,2)"))
             .withColumn("freight_amount", F.col("freight_value").cast("decimal(18,2)")))
    valid_items, bad_items = quality_table(items, "order_items", [
        (missing("order_id"), "missing_order_id"),
        (F.col("item_no").isNull() | (F.col("item_no") <= 0), "invalid_item_id"),
        (missing("product_id"), "missing_product_id"),
        (F.col("price_amount").isNull() | (F.col("price_amount") < 0), "invalid_price"),
        (F.col("freight_amount").isNull() | (F.col("freight_amount") < 0), "invalid_freight"),
    ])

    valid_customers, bad_customers = quality_table(raw["customers"], "customers", [
        (missing("customer_id"), "missing_customer_id"),
        (missing("customer_unique_id"), "missing_customer_unique_id"),
    ])
    valid_products, bad_products = quality_table(raw["products"], "products", [
        (missing("product_id"), "missing_product_id"),
    ])

    reject_duplicate_keys(valid_orders, ["order_id"], "orders")
    reject_duplicate_keys(valid_items, ["order_id", "item_no"], "order_items")
    reject_duplicate_keys(valid_customers, ["customer_id"], "customers")
    reject_duplicate_keys(valid_products, ["product_id"], "products")

    joined = (valid_items.alias("i")
              .join(valid_orders.alias("o"), F.col("i.order_id") == F.col("o.order_id"), "left")
              .join(valid_customers.alias("c"), F.col("o.customer_id") == F.col("c.customer_id"), "left")
              .join(valid_products.alias("p"), F.col("i.product_id") == F.col("p.product_id"), "left"))
    missing_ref = (F.col("o.order_id").isNull() |
                   F.col("c.customer_id").isNull() |
                   F.col("p.product_id").isNull())
    orphan_reason = (F.when(F.col("o.order_id").isNull(), F.lit("missing_order"))
                     .when(F.col("c.customer_id").isNull(), F.lit("missing_customer"))
                     .otherwise(F.lit("missing_product")))
    orphan_rows = (joined.where(missing_ref)
                   .select(F.lit("order_items").alias("dataset"),
                           orphan_reason.alias("reason"),
                           F.concat_ws("#", F.col("i.order_id"), F.col("i.order_item_id"))
                           .alias("business_key"),
                           F.to_json(F.struct(F.col("i.order_id").alias("order_id"),
                                              F.col("i.order_item_id").alias("order_item_id"),
                                              F.col("i.product_id").alias("product_id")))
                           .alias("record_json")))
    quarantine = (bad_orders.unionByName(bad_items)
                  .unionByName(bad_customers).unionByName(bad_products)
                  .unionByName(orphan_rows))

    fact = (joined.where(~missing_ref)
            .select(
                F.col("i.order_id").alias("order_id"),
                F.col("i.item_no").alias("order_item_id"),
                F.to_date(F.col("o.purchase_ts")).alias("purchase_date"),
                F.col("o.order_status").alias("order_status"),
                F.col("c.customer_unique_id").alias("customer_unique_id"),
                F.col("c.customer_state").alias("customer_state"),
                F.col("i.product_id").alias("product_id"),
                F.coalesce(F.col("p.product_category_name"), F.lit("unknown"))
                .alias("product_category"),
                F.col("i.seller_id").alias("seller_id"),
                F.col("i.price_amount").alias("price"),
                F.col("i.freight_amount").alias("freight_value"),
                (F.col("i.price_amount") + F.col("i.freight_amount")).alias("item_total"),
            ))
    # Aggregate item facts directly. Do NOT join order-level payments before this sum.
    # Only delivered orders enter this mart; all statuses remain in the fact table.
    # This is item-list-value, not verified settled/payment revenue.
    mart = (fact.where(F.col("order_status") == "delivered")
            .groupBy("purchase_date", "product_category", "customer_state")
            .agg(F.countDistinct("order_id").alias("orders"),
                 F.count(F.lit(1)).alias("sold_units"),
                 F.sum("price").alias("gross_item_revenue"),
                 F.sum("freight_value").alias("total_freight"),
                 F.sum("item_total").alias("gross_item_total")))
    return {
        "curated/orders": valid_orders,
        "curated/order_items": valid_items,
        "curated/customers": valid_customers,
        "curated/products": valid_products,
        "curated/order_item_fact": fact,
        "quarantine/rows": quarantine,
        "mart/daily_category_sales": mart,
    }


def execute(args: argparse.Namespace) -> None:
    checked_roots(args.raw_root, args.warehouse_root)
    checked_version(args.raw_version)
    checked_version(args.output_version)
    if not 1 <= args.output_partitions <= 16:
        raise EtlError("--output-partitions must be between 1 and 16")
    hdfs = Hdfs()
    raw_path = f"{args.raw_root}/{args.raw_version}"
    raw_manifest_bytes = hdfs.cat(raw_path + "/_MANIFEST.json")
    try:
        raw_manifest = json.loads(raw_manifest_bytes)
    except (ValueError, UnicodeError) as exc:
        raise EtlError("Step 45 raw manifest is not valid UTF-8 JSON") from exc
    validate_raw_manifest(raw_manifest)
    identity = make_run_identity(args.raw_version, raw_manifest_bytes)
    final_path = f"{args.warehouse_root}/{args.output_version}"
    published = check_published(hdfs, final_path, identity)
    if args.verify_existing:
        if not published:
            raise EtlError(f"No published immutable ETL snapshot to verify: {final_path}")
        print(f"OLIST ETL EXISTING VERIFIED: {final_path}", flush=True)
        return

    # A cluster-mode driver must initialize SparkContext before it exits,
    # even if another submit published an identical snapshot just now.
    # The Ansible role uses --verify-existing to avoid YARN submissions in
    # the ordinary idempotent re-run case.
    from pyspark.sql import SparkSession

    spark = (SparkSession.builder.appName(f"olist-etl-{args.output_version}")
             .config("spark.sql.session.timeZone", "UTC")
             .getOrCreate())
    if published:
        try:
            print(f"OLIST ETL SKIP: verified existing immutable {final_path}", flush=True)
        finally:
            spark.stop()
        return
    # Step 51 opt-in: execution-derived evidence, not hard-coded table lineage.
    # Captures each DataFrame's actual file inputs and completed Parquet write.
    from datetime import datetime, timezone
    started_at = datetime.now(timezone.utc).isoformat()
    runtime_writes = []
    staging_path = (f"{args.warehouse_root}/_staging/"
                    f"{args.output_version}-{uuid.uuid4().hex}")
    try:
        raw = {}
        source_count = {}
        for name in CORE_COLUMNS:
            df = read_csv(spark, name, raw_path)
            count = df.count()
            expected_count = raw_manifest["files"][name]["rows"]
            if count != expected_count:
                raise EtlError(f"{name}: CSV count {count}, expected {expected_count}; abort")
            raw[name] = df
            source_count[name] = count

        tables = build_tables(raw)
        metrics = {
            "source_rows": source_count,
            "output_rows": {},
        }
        hdfs.command("-mkdir", "-p", staging_path)
        for name, df in tables.items():
            metrics["output_rows"][name] = df.count()
            if args.runtime_lineage:
                # DataFrame.inputFiles reports filesystem scan leaves from the
                # executed DataFrame plan. It is not table-level transform lineage.
                observed_inputs = sorted(set(df.inputFiles()))
            write_path = f"hdfs://{staging_path}/{name}"
            (df.coalesce(args.output_partitions).write.mode("errorifexists")
             .parquet(write_path))
            if args.runtime_lineage:
                runtime_writes.append({
                    "output": name,
                    "input_files": observed_inputs,
                    "write_path": write_path,
                    "published_path": f"hdfs://{final_path}/{name}",
                    "output_rows": metrics["output_rows"][name],
                })
        # Every valid item is either an accepted fact or a missing-reference orphan.
        # Bad item rows are included in quarantine, so no item silently disappears.
        valid_item_count = metrics["output_rows"]["curated/order_items"]
        fact_count = metrics["output_rows"]["curated/order_item_fact"]
        from pyspark.sql import functions as F
        orphan_count = tables["quarantine/rows"].where(
            F.col("reason")
            .isin("missing_order", "missing_customer", "missing_product")
        ).count()
        if fact_count + orphan_count != valid_item_count:
            raise EtlError("Order item conservation failed after joins")
        metrics["orphan_order_items"] = orphan_count
        metrics["etl_version"] = args.output_version
        run_manifest = dict(identity, metrics=metrics, tables=list(OUTPUT_TABLES))
        hdfs.put_json(staging_path + "/_ETL_MANIFEST.json", run_manifest)
        if args.runtime_lineage:
            # Event is committed along with immutable ETL snapshot. Failed or
            # skipped submissions cannot claim to have produced execution data.
            evidence = {
                "schema": "bigtop.olist.spark-runtime-lineage.v1",
                "status": "COMPLETE",
                "pipeline": "olist_spark_batch_etl",
                "run_id": str(uuid.uuid4()),
                "started_at": started_at,
                "finished_at": datetime.now(timezone.utc).isoformat(),
                "raw_version": args.raw_version,
                "warehouse_version": args.output_version,
                "writes": runtime_writes,
            }
            hdfs.put_json(staging_path + "/_RUNTIME_LINEAGE.json", evidence)
        if hdfs.exists(final_path):
            raise EtlError("Output appeared during Spark execution; refusing concurrent overwrite")
        hdfs.command("-mv", staging_path, final_path)
        print("ETL METRICS " + json.dumps(metrics, sort_keys=True), flush=True)
        print(f"OLIST ETL OK: published {final_path}", flush=True)
    finally:
        spark.stop()
    # Failed staging stays isolated under _staging for inspection. No overwrite or rm.


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw-root", default="/data/olist/raw")
    parser.add_argument("--raw-version", default="v1")
    parser.add_argument("--warehouse-root", default="/data/olist/warehouse")
    parser.add_argument("--output-version", default="v1")
    parser.add_argument("--output-partitions", type=int, default=2)
    parser.add_argument("--runtime-lineage", action="store_true",
                        help="Capture executed Spark dataframe inputFiles/writes with immutable snapshot")
    parser.add_argument("--no-runtime-lineage", action="store_false", dest="runtime_lineage",
                        help="Default; do not capture extra runtime lineage")
    parser.add_argument("--verify-existing", action="store_true",
                        help="Verify immutable snapshot without importing or starting Spark")
    args = parser.parse_args()
    try:
        execute(args)
    except (EtlError, subprocess.SubprocessError) as exc:
        print(f"OLIST ETL ERROR: {exc}", file=sys.stderr, flush=True)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
