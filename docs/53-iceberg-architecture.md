# 53. Apache Iceberg HDFS HadoopCatalog Pilot (Spark 3.5.6 / Scala 2.12)

## Architecture decisions

- Apache Iceberg **1.10.1**; `iceberg-spark-runtime-3.5_2.12-1.10.1.jar` — correct ABI / Scala binary target for user-confirmed Spark 3.5.6 / Scala 2.12.18. JDK 11 installed.
- Cluster: NameNode master; DataNodes worker1/worker2; YARN RM master; worker3 Ansible controller. No extra service required for the `type=hadoop` pilot.
- HDFS path: `hdfs://master:9000/data/olist/iceberg-lab`; Catalog name: `lab`; Namespace: `olist`.
- Source: existing approved Step 52 v3 Mart Parquet (`/data/olist/warehouse/v3/mart/daily_category_sales`). Read-only. Catalog writes only to Iceberg lab paths and a separately scoped validation directory.
- Spark submit uses `--master yarn --deploy-mode cluster --jars /opt/bigtop-iceberg/...`; jar local to `master`, shipped by Spark to YARN containers. Do **not** add jar globally in `/usr/lib/spark/jars` or change `/etc/spark/conf`.
- Runtime JAR is downloaded manually to worker3 vendor directory and copied via Ansible. No downloads on locked-down Hadoop nodes.

## What it demonstrates

1. Spark can resolve a HadoopCatalog backed by HDFS rather than a path-based Parquet dataset.
2. An Iceberg table is created and two separate append commits produce two snapshots, **without a user-written ETL manifest**.
3. Older snapshot can be read using Spark DataFrame `snapshot-id` option; first snapshot has 1 row and current has 2.
4. A separate **lab** Iceberg table is created from the approved source Mart, using the Spark DataFrameWriterV2 API. Source and target row counts must match. This lab table is **not** automatically an approved 52 release, and the 52 approval cannot be inherited by copying data.
5. Rerun creates no extra smoke rows or additional Mart table if already present (Spark does not append the Mart on every run).

## What it does NOT demonstrate

- Does not replace existing Raw, Curated, Mart or `_APPROVAL.json` with Iceberg.
- Does not automatically migrate the 47 Quality checks, 48 Scheduler, 49 Grafana, 50 Catalog or 51 Lineage.
- Does not prove actual ACID behavior in a concurrent multi-writer benchmark.
- Does not configure Trino or a shared REST/Hive Metastore Catalog.
- Does not implement Iceberg WAP branches; that comes after basic storage/catalog tests.
- HadoopCatalog is a **single-cluster learning pilot**. Multi-engine use, concurrent writers, credentials, catalog availability, snapshot retention and governance require deliberate design before production.

## Correct dependency reference

- https://iceberg.apache.org/releases/ (1.10.1 released 2026-09-29)
- https://central.sonatype.com/artifact/org.apache.iceberg/iceberg-spark-runtime-3.5_2.12/1.10.1
- https://iceberg.apache.org/docs/latest/spark-writes/
- https://iceberg.apache.org/docs/latest/spark-queries/
- https://iceberg.apache.org/docs/latest/spark-configuration/

## Java compatibility

Spark 3.5.6 runs on Java 11 in this Bigtop cluster. Iceberg 1.11+ requires Java 17; this isolated lab uses Iceberg 1.10.1 with bytecode preflight. Spark `--jars` distributes the artifact to YARN Driver/Executor containers without modifying every worker JVM or global Spark jar directory.
