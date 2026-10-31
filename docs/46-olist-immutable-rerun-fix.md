# Step 46: Immutable Snapshot Re-run Fix

## Root cause

Cluster-mode PySpark previously called `check_published()` and returned before
`SparkSession` was initialized when the warehouse `v1` already existed.
YARN ApplicationMaster treated that driver as failed with `exitCode: 13` and
`User did not initialize spark context!`. Step 48 calls step 46 on every run,
so it exposed this bug even though the first step 46 deployment had succeeded.

## Fix

1. Step 46's Ansible role checks the existing `_ETL_MANIFEST.json` before
   submission. When present, it runs the CLI-only `--verify-existing` check
   as the Spark submit user. It verifies the original raw manifest hash,
   pipeline identity, and `_SUCCESS` markers for all seven Parquet outputs.
2. `spark-submit` runs **only when the output manifest is absent**.
3. The PySpark driver initializes SparkSession before any residual in-driver
   SKIP, to handle race conditions without YARN's exitCode 13.
4. A mismatched or incomplete version fails rather than overwriting data.

## Validation

```bash
python3 -m unittest discover -s tests -p 'test_olist_etl.py' -v
make lint
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/46-olist-spark-etl.yml
# Re-run of the same v1 should say EXISTING VERIFIED without submitting a new YARN app.
sudo systemctl start bigtop-olist-pipeline.service
sudo systemctl show bigtop-olist-pipeline.service -p Result -p ExecMainStatus
sudo cat /var/lib/prometheus/node-exporter/olist_pipeline.prom
```

Do not delete the HDFS warehouse or raw snapshot to work around this issue.
