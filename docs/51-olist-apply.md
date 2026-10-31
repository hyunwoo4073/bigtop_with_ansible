# 51 — worker3 installation and execution instructions

All commands are executed on `worker3` in `~/bigtop-cluster-ansible`. The ZIP is
expected in that directory (transfer using the same workflow as Steps 45–50).

## 0. Backup / apply / test

```bash
cd ~/bigtop-cluster-ansible
cp -a roles/data_pipeline_olist_etl /tmp/data_pipeline_olist_etl_before_step51
unzip -o 51_olist_spark_runtime_lineage_ansible.zip -d .
python3 -m unittest discover -s tests -p test_olist_runtime_lineage.py -v
python3 -m unittest discover -s tests -p test_olist_etl.py -v
make lint
# Review Step 46 changes rather than committing unrelated files.
git diff -- roles/data_pipeline_olist_etl/
```

## 1. Generate **real** Spark execution evidence in a NEW output version

**Do not delete/rewrite `v1`.** `v1` is already published and will skip;
therefore an event cannot be fabricated for `v1`.

```bash
.venv/bin/ansible-playbook -i inventory/hosts.ini \
  playbooks/46-olist-spark-etl.yml \
  -e data_pipeline_olist_etl_raw_version=v1 \
  -e data_pipeline_olist_etl_output_version=v2 \
  -e data_pipeline_olist_etl_capture_runtime_lineage=true
```

Verify on worker3 by remote inspection via master:

```bash
ssh master 'sudo -u hadoop hdfs dfs -ls /data/olist/warehouse/v2/_RUNTIME_LINEAGE.json'
ssh master 'sudo -u hadoop hdfs dfs -cat /data/olist/warehouse/v2/_RUNTIME_LINEAGE.json' | python3 -m json.tool | head -100
```

In environments where `ssh master` uses a different user or sudo policy,
log in to master and execute `sudo -u hadoop hdfs dfs -cat ...` directly.

If `v2` already exists, do not overwrite it. Choose a truly new output
version and use that version consistently in Steps 46, 50, and 51.

## 2. Register the NEW outputs in existing Step 50 catalog

```bash
.venv/bin/ansible-playbook -i inventory/hosts.ini \
  playbooks/50-olist-openmetadata.yml \
  -e data_pipeline_olist_catalog_version=v1 \
  -e data_pipeline_olist_catalog_warehouse_version=v2 \
  -e data_pipeline_olist_catalog_apply=true
```

This creates `curated_v2_*`, `quarantine_v2_rows`, `mart_v2_*` containers
without touching existing `v1` assets. It also adds the original Step 50
*manual* mapping for v2, which is different from Step 51 observed provenance.

## 3. Plan and apply OBSERVED runtime lineage

```bash
.venv/bin/ansible-playbook -i inventory/hosts.ini \
  playbooks/51-olist-spark-runtime-lineage.yml
```

When the plan shows the actual observed sources and outputs, apply it:

```bash
.venv/bin/ansible-playbook -i inventory/hosts.ini \
  playbooks/51-olist-spark-runtime-lineage.yml \
  -e data_pipeline_olist_runtime_lineage_apply=true
```

Rerun the apply command; expected `OLIST RUNTIME LINEAGE UNCHANGED: 0 new edges`.
Actual first-run observed edge counts can vary according to Spark's
`DataFrame.inputFiles()` and edges already present from Step 50; **do not**
assume a fixed edge count.

## 4. OPTIONAL continuous reconciliation (not Spark execution)

After manual validation, enable a 15 minute systemd timer on worker3:

```bash
.venv/bin/ansible-playbook -i inventory/hosts.ini \
  playbooks/51-olist-spark-runtime-lineage.yml \
  -e data_pipeline_olist_runtime_lineage_apply=true \
  -e data_pipeline_olist_runtime_lineage_timer_enabled=true
systemctl status bigtop-olist-runtime-lineage.timer --no-pager
```

The timer runs Step 51 on the **existing v2 evidence**, only creates missing
edges, and NEVER resubmits Spark. Disable when the PoC is complete:

```bash
sudo systemctl disable --now bigtop-olist-runtime-lineage.timer
```

## 5. UI and Git

OpenMetadata Explore -> Container -> `curated_v2_order_item_fact` -> Lineage.
The new observed raw-v1 -> Parquet-v2 edges supplement the old manual
transformation lineage. Verify first with a specific output that actually has
reported Spark `input_files`.

```bash
git add playbooks/51-olist-spark-runtime-lineage.yml \
  roles/data_pipeline_olist_runtime_lineage \
  roles/data_pipeline_olist_etl/files/olist_etl.py \
  roles/data_pipeline_olist_etl/tasks/main.yml \
  tests/test_olist_runtime_lineage.py \
  docs/51-olist-apply.md docs/51-olist-runtime-lineage.md
git commit -m 'feat: capture Olist Spark runtime file lineage'
```

## Troubleshooting

- `No ... _RUNTIME_LINEAGE.json`: the job was SKIPPED or capture flag absent;
  use a NEW immutable output version instead of deleting data.
- `Register Olist catalog containers with Step 50 first`: run 50 for the same
  output version, not just v1.
- `WARNING no physical file inputs`: Spark did not expose scan paths through
  `inputFiles()` for that output; inspect the HDFS event file, do not synthesize
  an edge.
- If an output contains unregistered sources, bridge deliberately fails
  closed instead of producing inaccurate OpenMetadata lineage.
- Timer and Spark versions are independent. Step 48 timer remains unchanged.
