#!/usr/bin/env python3
"""Step53 Spark-on-YARN Iceberg HadoopCatalog smoke, isolated from Step45-52.

The script copies a read-only approved Olist v3 Mart into an independent
LAB Iceberg table. It does not treat the copy as a newly quality-approved
published dataset. Nothing in Step 45-52 is modified.
"""
import argparse
import json
import re
from datetime import datetime, timezone

CATALOG_RE = re.compile(r"^[a-z][a-z0-9_]{0,20}$")
HDFS_PREFIX = "hdfs://master:9000/data/olist/"


def guard_args(args):
    if not CATALOG_RE.fullmatch(args.catalog):
        raise ValueError("Invalid Iceberg catalog name")
    if not args.source.startswith(HDFS_PREFIX + "warehouse/v3/mart/"):
        raise ValueError("Source must be an existing v3 Olist Mart")
    if args.source != HDFS_PREFIX + "warehouse/v3/mart/daily_category_sales":
        raise ValueError("Source is not the approved Mart path")
    if args.approval != HDFS_PREFIX + "published/v3/_APPROVAL.json":
        raise ValueError("Step 52 approval path required")
    if args.proof != HDFS_PREFIX + "iceberg-validation/step53":
        raise ValueError("Proof must be written into isolated validation root")


def smoke_row_ids(rows):
    return sorted(int(r.id) for r in rows)


def should_insert(members, row):
    """Only append a missing smoke id; reject unexpected contents."""
    if members not in ([], [1], [1, 2]):
        raise ValueError("Smoke Iceberg table contains unexpected rows: " + str(members))
    return row not in members


def file_exists(spark, uri):
    jvm = spark.sparkContext._jvm
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    p = jvm.org.apache.hadoop.fs.Path(uri)
    fs = p.getFileSystem(conf)
    return bool(fs.exists(p))


def run(args):
    guard_args(args)
    from pyspark.sql import SparkSession  # imported only in YARN driver

    spark = SparkSession.builder.appName("olist-iceberg-lab-step53").getOrCreate()
    try:
        if not file_exists(spark, args.approval):
            raise RuntimeError("Step52 approval missing; do not copy the v3 Mart")
        if not file_exists(spark, args.source + "/_SUCCESS"):
            raise RuntimeError("Source v3 Mart Parquet is not complete")

        c = args.catalog
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {c}.olist")
        smoke = f"{c}.olist.iceberg_smoke_step53"
        mart = f"{c}.olist.daily_category_sales_v3_lab"

        spark.sql(f"CREATE TABLE IF NOT EXISTS {smoke} (id BIGINT, note STRING) USING iceberg")
        members = smoke_row_ids(spark.sql(f"SELECT id FROM {smoke} ORDER BY id").collect())
        if should_insert(members, 1):
            spark.sql(f"INSERT INTO {smoke} VALUES (1, 'first_snapshot')")
        members = smoke_row_ids(spark.sql(f"SELECT id FROM {smoke} ORDER BY id").collect())
        if should_insert(members, 2):
            spark.sql(f"INSERT INTO {smoke} VALUES (2, 'second_snapshot')")
        members = smoke_row_ids(spark.sql(f"SELECT id FROM {smoke} ORDER BY id").collect())
        if members != [1, 2]:
            raise RuntimeError("Smoke table has inconsistent current data")

        snap_rows = spark.sql(f"SELECT snapshot_id, parent_id FROM {smoke}.snapshots").collect()
        root_snapshots = [int(r.snapshot_id) for r in snap_rows if r.parent_id is None]
        if len(root_snapshots) != 1:
            raise RuntimeError("Expected exactly one initial snapshot")
        first_id = root_snapshots[0]
        first_count = spark.read.option("snapshot-id", str(first_id)).table(smoke).count()
        if first_count != 1 or len(snap_rows) < 2:
            raise RuntimeError("Iceberg time travel or snapshot history not validated")

        source = spark.read.parquet(args.source)
        source_count = source.count()
        if source_count <= 0:
            raise RuntimeError("Approved source Mart has no rows")

        # Iceberg copy is a LAB asset; Step 52's approval does not transfer.
        if not spark.catalog.tableExists(mart):
            source.coalesce(2).writeTo(mart).using("iceberg").create()
        target_count = spark.table(mart).count()
        if target_count != source_count:
            raise RuntimeError("Iceberg Mart copy row count mismatch: %s != %s" % (target_count, source_count))
        mart_snapshots = spark.sql(f"SELECT snapshot_id FROM {mart}.snapshots").collect()
        if not mart_snapshots:
            raise RuntimeError("No Iceberg snapshots for copied Mart")

        proof = {
            "status": "PASS",
            "description": "Isolated Iceberg test; copied Mart is NOT a newly approved release",
            "recorded_at_utc": datetime.now(timezone.utc).isoformat(),
            "catalog": c,
            "smoke_table": smoke,
            "smoke_snapshot_count": len(snap_rows),
            "first_snapshot_id": str(first_id),
            "smoke_first_snapshot_rows": int(first_count),
            "smoke_current_rows": len(members),
            "mart_table": mart,
            "source_parquet": args.source,
            "source_rows": int(source_count),
            "iceberg_rows": int(target_count),
            "mart_snapshot_count": len(mart_snapshots),
        }
        (spark.createDataFrame([(json.dumps(proof, sort_keys=True),)], ["value"])
             .coalesce(1).write.mode("overwrite").text(args.proof))
        print("OLIST ICEBERG STEP53 PASS " + json.dumps(proof, sort_keys=True), flush=True)
    finally:
        spark.stop()


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--catalog", default="lab")
    p.add_argument("--source", required=True)
    p.add_argument("--approval", required=True)
    p.add_argument("--proof", required=True)
    run(p.parse_args())


if __name__ == "__main__":
    main()
