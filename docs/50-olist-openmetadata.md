# Step 50 — Olist HDFS Catalog & Lineage (OpenMetadata 2.0.6)

## Scope

The existing Step 45 HDFS `v1` has **9 CSV files**. Step 46 Spark-on-YARN
`v1` produced **7 Parquet folders**, which Step 47 checks for correctness.
Step 50 registers these **16 actual HDFS assets** in OpenMetadata using its
`CustomStorage` service and Container API. It adds **13 evidence-based lineage
edges**, corresponding to Step 46 `build_tables()` transformations.

This is **manual catalog registration**, not an HDFS connector and not runtime
Spark/OpenLineage. Container-level lineage cannot be treated as automatic
column-level Spark lineage. No data is copied to OpenMetadata, and no HDFS
content is modified.

## Architectural boundaries

- Ansible Controller and API client: `worker3` (`ops` group)
- Read-only HDFS preflight: `master` with `hdfs dfs` as `hadoop`
- OpenMetadata API: `metadata:8585/api` (actual address via inventory)
- JWT: **reuse** Step 42 root-only `/etc/bigtop-governance/pg-catalog/bot.token`
- New service name: `bigtop_hdfs_olist` (`CustomStorage`)
- No Airflow, connector ingestion, Java Spark listener or automatic ingestion scheduler is added.
- Does not change 45–49 Playbooks, PostgreSQL, Spark, scheduler or Grafana.

## Exact registered assets

Raw (9): `orders`, `order_items`, `customers`, `products`, `payments`,
`reviews`, `sellers`, `geolocation`, `category_translation`.

Spark Parquet outputs (7): `curated/orders`, `curated/order_items`,
`curated/customers`, `curated/products`, `curated/order_item_fact`,
`quarantine/rows`, `mart/daily_category_sales`.

Entity names are e.g. `bigtop_hdfs_olist.raw_v1_orders` and
`bigtop_hdfs_olist.mart_v1_daily_category_sales`.

Lineage (13): four Raw→Curated, four Curated→Fact,
Fact→Mart, and four Raw→Quarantine. This is a **manual mapping based on code**.
The five unused Raw CSVs are cataloged but **not** falsely linked to the ETL.

## Install and validate

```bash
cd ~/bigtop-cluster-ansible
unzip -o 50_olist_openmetadata_ansible.zip -d .
python3 -m unittest discover -s tests -p 'test_olist_catalog.py' -v
make lint

# First run = plan only (no OpenMetadata writes).
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/50-olist-openmetadata.yml

# Review proposed service, containers and lineage, THEN apply.
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/50-olist-openmetadata.yml \
  -e data_pipeline_olist_catalog_apply=true
```

**Do not pass** `-e ansible_connection=local` globally: HDFS manifest and
`_SUCCESS` verification delegates over SSH to `master`.

Check OpenMetadata UI Storage Service `bigtop_hdfs_olist` and Container lineage;
re-running the apply command should show `OLIST CATALOG UNCHANGED`.

## Prerequisites and troubleshooting

- Step 45 Raw manifest and Step 46 Warehouse manifest must exist in HDFS.
- All 9 Raw CSVs and 7 Parquet `_SUCCESS` markers must exist.
- Step 42 Bot JWT must be a **root-only file, 0600**, and token privileges must
  permit StorageService/Container creation and Lineage edits.
- If API responds `401` / `403`, inspect JWT validity and Bot permissions; do not
  expose the token in logs or Git.
- If API responds `400`, check the OpenMetadata **2.0.6** server's schema and
  validation message locally before altering any existing entity.
- If API responds `404` for an existing service or Container, the plan will
  propose creation; the program never deletes an existing entity.
- `CustomStorage` is a cataloging abstraction and does NOT provide automatic HDFS
  filesystem discovery or Parquet schema ingestion. To get actual field lineage,
  a later step needs a Spark/OpenLineage event producer or schema-aware ingestion.

## Safety

Writes (only with `-e ...apply=true`): create **missing** StorageService,
Containers, and lineage edges. Existing entity identities and paths are checked;
conflicting assets cause failure before any write. Partial API success is safe
to re-run: existing matching entities/edges are skipped. Step 50 never invokes
HDFS `-rm`, Spark submit, Docker prune or database writes.

Git: `playbooks/50-olist-openmetadata.yml`, `roles/data_pipeline_olist_catalog/`,
`tests/test_olist_catalog.py`, and this document. **Never commit** `/etc/` token,
`olist/` source CSVs or execution artifacts.
