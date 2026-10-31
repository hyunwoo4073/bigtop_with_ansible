# Step 54 — Iceberg Row-Level SQL / Snapshot / Branch WAP Lab

## Goal and exact bounds

Built for the **already verified** Step53 Apache Iceberg **1.10.1**, Spark **3.5.6**, Scala **2.12**, JDK **11**, HDFS Hadoop Catalog named `lab` on master:9000. Existing Step53 JAR is shipped through `spark-submit --jars` to YARN containers (there is no need to copy it to each Worker).

**Only two new dedicated Step54 Iceberg tables** are modified:

- `lab.olist.iceberg_mutations_step54`: INSERT, UPDATE, DELETE, MERGE INTO, ADD COLUMNS, snapshot time travel.
- `lab.olist.iceberg_wap_step54`: baseline `main` state, named `audit_step54` branch, candidate staging, audit checks and explicit fast-forward publish.

HDFS paths: Iceberg-managed tables under `/data/olist/iceberg-lab/olist/`; proof under `/data/olist/iceberg-validation/step54/{stage,publish}`.

This lab **never changes** 53 `iceberg_smoke_step53` and `daily_category_sales_v3_lab`, Raw, Warehouse, Quality, published approvals, Step47 or 52, or OpenMetadata. The quality check here is an intentionally tiny **synthetic** row-ID/positive-amount check, not the real 29-check Step47 quality suite. This lab proves the Iceberg WAP pattern, not a production release policy.

## Stages

1. `deploy_only` is the safe default; only stages the Python app on master.
2. `stage`: demonstrates CRUD, schema evolution, time travel on the first lab table. Creates a WAP second table with one `main` row, creates `audit_step54` from `main`, appends another row **on the audit branch**. Confirms that `main` remains at one row and `audit` has two. Writes proof with both snapshot IDs.
3. `publish`: checks stage proof, validates that branch and main snapshot IDs are unchanged, confirms candidate data quality, and only then calls `lab.system.fast_forward('olist.iceberg_wap_step54', 'main', 'audit_step54')`. Verifies main now contains both rows. Both stages are re-runnable without duplicate rows.

There is **no automatic publish**. A later concurrent write to `main` or `audit_step54` between stage and publish invalidates the saved proof; publication is rejected. Publishing only changes a single table's main snapshot, **not multi-table atomicity or HDFS permissions**. Real production approvals require stronger policies, ownership/roles, release audit trails and access controls.

## Mechanisms used

- Iceberg SQL extensions for row-level DELETE/UPDATE/MERGE.
- V2 format with copy-on-write row-level operations.
- Metadata snapshots, snapshot ID option for time travel.
- Iceberg Schema Evolution (ADD COLUMNS).
- Branch-specific write using `table.branch_audit_step54` and branch ref metadata.
- `fast_forward` procedure only after audit.

Documented SQL features: [Iceberg 1.10.1 branch docs](https://iceberg.apache.org/docs/1.10.1/branching/), [DDL](https://iceberg.apache.org/docs/1.10.1/spark-ddl/), [Spark writes](https://iceberg.apache.org/docs/1.10.1/spark-writes/), [Spark procedures](https://iceberg.apache.org/docs/1.10.1/spark-procedures/).

## Production caution

The lab uses `HadoopCatalog` for Spark-only HDFS experiments. Cross-engine access (Trino) may require choosing a compatible shared Catalog and configuring both engines identically. Avoid assuming filesystem/HadoopCatalog paths alone provide a production multi-engine catalog. Do not promote any existing Step52 published snapshot through this lab.
