# 47 — Olist Data Quality & Monitoring (Spark on YARN Cluster)

## Objective

Independently validate the **already published** 46 Parquet dataset and publish bounded quality metrics. Do not rerun the 46 ETL or modify any raw/warehouse data. Prerequisite: successful step 45 (nine original CSVs in HDFS Raw) and 46 (four-table Curated, Fact, Quarantine, Mart in HDFS warehouse).

```text
/data/olist/raw/v1                          # unchanged
/data/olist/warehouse/v1                    # unchanged
  _ETL_MANIFEST.json
  curated/{orders,order_items,customers,products,order_item_fact}/
  quarantine/rows/
  mart/daily_category_sales/
                       |
            Spark on YARN Cluster (47)
                       |
/data/olist/quality/v1/runs/<uuid>/_QUALITY_REPORT.json
                       |
               worker3 Node Exporter textfile collector
            /var/lib/prometheus/node-exporter/olist_quality.prom
                       |
               Prometheus (only if scraping worker3 Node Exporter)
```

## Apply on worker3

Unzip the new code into `~/bigtop-cluster-ansible`, from the project root. It adds a new Role, Playbook, tests and documentation; nothing from steps 45/46 is overwritten.

```bash
cd ~/bigtop-cluster-ansible
unzip -o 47_olist_data_quality_ansible.zip -d .
python3 -m unittest discover -s tests -p 'test_olist_quality.py' -v
make lint
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/47-olist-data-quality.yml
```

Execution: Ansible on worker3 SSHs to `master`, which starts `spark-submit --master yarn --deploy-mode cluster` as `hadoop`. Spark Driver runs in a YARN container on one of the two worker nodes. Do not set `-e ansible_connection=local` globally. Spark job waits for completion, reports are retrieved from HDFS, and metrics are copied to the `ops` inventory group host (worker3) via Ansible delegation.

**On every invocation a unique run ID is generated.** Reports are stored under separate immutable `runs/<uuid>` paths; rerunning **does not overwrite** or delete earlier reports. The `.prom` file contains only the latest result, intentionally. Scheduling/retention is not enabled in 47.

### Quality checks

1. Check presence of step-46 manifest and seven `_SUCCESS` markers.
2. Compare actual row counts of all **seven** Parquet tables with the step-46 manifest counts.
3. Verify unique business keys on `orders`, `order_items`, `customers`, `products` and `order_item_fact`.
4. Check mandatory Fact fields, nonnegative price and freight, `item_total == price + freight`.
5. Verify Fact rows have matching order items, orders and products.
6. Reconcile raw record counts with Curated and Quarantine for each of four source CSVs, accounting for orphan item rows separately.
7. Reconcile orphan records with step-46 manifest, and check `fact_rows + orphan_rows == valid_item_rows`.
8. Recompute delivered-order daily/category/state Mart and compare **both directions** using `exceptAll` (no arbitrary rounding). Also verify sold units equal delivered Fact count.
9. Apply configured quality gates for orphan count and invalid-source Quarantine rate.

Default gates: `max_orphans=0` and `max_quarantine_rate=0.05` (5%). `quarantine_rate` excludes orphan rows because they were already counted among valid order items; it divides invalid source rows by the **four** core CSV source row counts, not all nine raw tables. A FAIL is an explicit quality outcome, not a reason to delete source records. If actual dataset requires different expectations, confirm report breakdown and set Ansible group_vars intentionally before rerunning; do not weaken rules to hide defects.

Example for controlled threshold adjustments:

```bash
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/47-olist-data-quality.yml \
  -e data_pipeline_olist_quality_max_quarantine_rate=0.10 \
  -e data_pipeline_olist_quality_max_orphans=0
```

### HDFS output & error semantics

`/data/olist/quality/v1/runs/<uuid>/_QUALITY_REPORT.json` includes run date, source ETL manifest SHA-256, counts, bounded reason breakdown, checks and PASS/FAIL/ERROR status. No order/customer IDs, original JSON records or tokens are included. HDFS quality runs are separate from the versioned warehouse results.

- `PASS`: all checks passed. `spark-submit` exits 0; Ansible publishes worker3 monitoring metric with run_success=1.
- `FAIL`: check results differ from the published manifest or quality thresholds. Driver publishes report and exits 2. Ansible still reads it, publishes run_success=0, and **fails the play**.
- `ERROR`: unexpected Spark analysis/runtime issue after manifest preflight; Driver attempts to publish ERROR report and exits 2. Ansible publishes failed metrics if the report was published.
- No report: preflight/authentication/HDFS/Spark launch failure. The play stops with failure. Previous `.prom` data may remain and must not be interpreted as new success; use its timestamp and a freshness alert (future step).

No HDFS deletion, table rewrite, or overwrite is performed. Concurrent runs use different UUIDs but 47 does not attempt to coordinate multiple Spark applications or cap resource use. Keep one at a time on the small VM cluster.

### Verify (worker3)

```bash
# The playbook debug task prints a run_id and exact report path.
.venv/bin/ansible master -b --become-user hdfs -m shell \
  -a 'hdfs dfs -ls /data/olist/quality/v1/runs/'

# On worker3, confirm last recorded metrics file:
sudo cat /var/lib/prometheus/node-exporter/olist_quality.prom

# Check whether node_exporter actually exposes the metrics:
curl -fsS http://127.0.0.1:9100/metrics | grep '^olist_quality_'

# Verify Prometheus receives them, if worker3 exporter is a scrape target:
curl -fsSG 'http://127.0.0.1:9090/api/v1/query' \
  --data-urlencode 'query=olist_quality_run_success' | python3 -m json.tool
```

If the metrics are written to `.prom` but **not** shown by Node Exporter, check the exporter systemd command line for `--collector.textfile.directory` (must match the `.prom` path). Do **not** mistake file creation for successful Prometheus collection. If the exporter is not configured, either update it via existing Node Exporter Ansible configuration or run with `-e data_pipeline_olist_quality_publish_prometheus=false` to do HDFS-only validation while investigating.

PromQL samples (manual Grafana dashboard, no rule/dashboard is installed by step 47):

```promql
olist_quality_run_success{warehouse_version="v1"}
olist_quality_checks_failed{warehouse_version="v1"}
olist_quality_quarantine_rate{warehouse_version="v1"}
time() - olist_quality_last_run_timestamp_seconds{warehouse_version="v1"}
```

Use stale/absent-series checks when automating in a subsequent step. **No Airflow timer, Grafana dashboard, or alert rules are installed in 47.**

### Rollback / safety

If the run fails, inspect the report and use `yarn logs -applicationId application_...` for Driver logs. Do not touch `/data/olist/raw/v1` or `/data/olist/warehouse/v1`. To disable metrics publication without removing files, set `data_pipeline_olist_quality_publish_prometheus=false` on the next run; existing `.prom` remains until manually reconciled. Clean up report history only after adopting an explicit retention policy.

## Future work

48: scheduled runs (Airflow or systemd), freshness/error alerts and Grafana panel; later connect the HDFS Curated/Mart assets to OpenMetadata with actual Spark lineage. Existing 38/43/44 verification items remain separate.
