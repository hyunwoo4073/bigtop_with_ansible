# 46 — Olist Spark on YARN Batch ETL (initial four-table slice)

## Goal / inputs

- Prerequisite: step 45 has published **all nine original CSVs** into immutable `/data/olist/raw/v1` with `_MANIFEST.json`.
- Step 46 reads **four** of them: `orders`, `order_items`, `customers`, `products` (and verifies the nine-file step-45 manifest).
- Spark on YARN, **cluster deploy mode (Driver on YARN NodeManager)**, one executor initially (2 NodeManagers, 2 GiB each).
- Other five original CSVs remain untouched in Raw; later steps may ingest payments, reviews, sellers, geolocation, translation separately.

## Deployment: on worker3 (Ansible controller)

```bash
cd ~/bigtop-cluster-ansible
unzip -o 46_olist_spark_etl_cluster_ansible.zip -d .
python3 -m unittest discover -s tests -p 'test_olist_etl.py' -v
make lint
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/46-olist-spark-etl.yml
```

Deploy to `master` using SSH. **Do not** set `-e ansible_connection=local`: master is a different node from worker3.

The installed job is `/usr/local/lib/bigtop-olist/etl/olist_etl.py` on `master`, run via `spark-submit` as the `hadoop` user. Existing HDFS Raw, Web Console, PostgreSQL and OpenMetadata are not changed. The playbook creates `/data/olist/warehouse`, assigns that directory to the Spark submit user and then launches one YARN application. It does not schedule any periodic task.

## Spark transformation and quality semantics

1. Read CSV with ordered explicit **string schema**, `header=true`, `enforceSchema=false`, `mode=FAILFAST`; compare row counts to step-45 manifest. Parse purchase timestamps, item number, and price/freight as numeric/decimal types.
2. Identify invalid required keys, timestamps, amounts: put their original row information and reason into `quarantine/rows` Parquet.
3. **Reject duplicate business keys** rather than quietly multiplying join results. Build valid orders/order items/customers/products.
4. Left join item → order → customer → product; missing parent references are written to quarantine and **not silently dropped**.
5. Write `curated/order_item_fact` with one row per valid order item. Do not join payments to this fact as a raw one-to-many table, since this would double count item revenue.
6. Build `mart/daily_category_sales` using **delivered orders only**, by purchase date, product category and customer state. Sums reflect **ordered item prices**, not verified payment settlement, taxes or refunds.
7. Generate `_ETL_MANIFEST.json` including raw manifest fingerprint, source/output row counts, orphan count, transformation version, output list.

## Result layout (HDFS)

```text
/data/olist/warehouse/v1/
├── _ETL_MANIFEST.json
├── curated/
│   ├── orders/                  (Parquet + _SUCCESS)
│   ├── order_items/             (Parquet + _SUCCESS)
│   ├── customers/               (Parquet + _SUCCESS)
│   ├── products/                (Parquet + _SUCCESS)
│   └── order_item_fact/         (Parquet + _SUCCESS)
├── quarantine/
│   └── rows/                    (Parquet + _SUCCESS; may be empty)
└── mart/
    └── daily_category_sales/   (Parquet + _SUCCESS)
```

Spark writes to an isolated per-run staging directory under `/data/olist/warehouse/_staging/`, then performs an HDFS directory rename to publish the complete snapshot. No `overwrite` mode is used. An existing complete snapshot with the same source manifest and pipeline version is **SKIP**, not reprocessed; an incomplete or different existing snapshot is an **ERROR**. Concurrent execution for the *same output version* is not supported; serialize submissions.

If a job fails, its private `_staging/<version>-<uuid>` directory is intentionally left for diagnosis. Do not delete `/data/olist/raw` or a published warehouse version. Review failed staging only after identifying the exact path/ownership.

## Verify after first run

From worker3:

```bash
# Cluster-mode Driver logs live in YARN, not in spark-submit stdout.
# Ansible prints PUBLISHED/EXISTING_VERIFIED and metrics from HDFS manifest.

.venv/bin/ansible master -b --become-user hdfs -m shell -a \
  'hdfs dfs -ls -R /data/olist/warehouse/v1 | tail -70'

.venv/bin/ansible master -b --become-user hdfs -m shell -a \
  'hdfs dfs -cat /data/olist/warehouse/v1/_ETL_MANIFEST.json'

# Check YARN application and History Server; master:
.venv/bin/ansible master -b --become-user hadoop -m shell -a \
  'yarn application -list -appStates FINISHED'
```

Run the same playbook a **second time**. It should report `EXISTING_VERIFIED` from the published HDFS manifest without overwriting data. The Driver's `OLIST ETL SKIP` message is in YARN container logs, not necessarily in Ansible stdout.

Note: `yarn application -list -appStates FINISHED` may not show older completed apps if the RM state retention limit has been reached. For a job-specific history check, use the application ID in spark-submit logs or Spark History Server.

## HDFS output version and rerun

For intentional transformations/reprocessing with a changed definition, keep raw v1 immutable and write a new output version:

```bash
.venv/bin/ansible-playbook -i inventory/hosts.ini \
  playbooks/46-olist-spark-etl.yml \
  -e data_pipeline_olist_etl_output_version=v2
```

The raw snapshot is still `v1` by default. If input source data is published as raw v2, select it with `-e data_pipeline_olist_etl_raw_version=v2` **and choose a new output version**.

## Troubleshooting

- `YARN`/queue/no resources: confirm `master` ResourceManager, two NodeManagers, `batch` queue, available memory, and Spark submit config. Default small test: one executor x 512 MiB, driver 512 MiB, 1 core, shuffle 4.
- Missing `hadoop` HDFS permissions: check `/data/olist/warehouse` owner and `hdfs dfs -ls -d` permissions; do not change Raw permissions or chown the whole `/data/olist` tree.
- CSV schema mismatch: Spark checks header *order* against `CORE_COLUMNS`; do not use schema inference or swap datasets.
- Duplicate keys or nonmatching row counts: ETL aborts to avoid silent loss or join explosion. Inspect raw snapshot and manifest.
- 45 initial upload has a different Raw version: override `data_pipeline_olist_etl_raw_version` to match the successful 45 snapshot.
- Failed job (`spark-submit` nonzero) is not proof of successful publication. Always check the published ETL manifest and seven `_SUCCESS` markers.
- Cluster-mode Driver logs: find the application ID from `spark-submit` output or `yarn application -list -appStates ALL`, then run `yarn logs -applicationId application_...`. Driver requires the Hadoop `hdfs` CLI plus `/usr/bin/python3` on the NodeManager hosts; confirm those binaries and Hadoop config are available to YARN containers.
- Application stuck in `ACCEPTED`: check Capacity Scheduler AM limit and NodeManager memory; the cluster-mode Driver+AM needs YARN container memory overhead in addition to `--driver-memory`.
- Python/Spark runtime failure: verify `spark-submit --version`, `/usr/bin/python3`, `/etc/hadoop/conf`, `/etc/spark/conf` on master and worker nodes.

## Test scope

`tests/test_olist_etl.py` has pure-Python **safety/contract tests**, runnable without PySpark. This artifact is **not** claimed to have been executed on the user's YARN cluster. Actual full ETL execution, row counts and Parquet content must be verified with the above commands.

### Lint (2026-10-30)

The 10 tasks setting `become_user` also declare `become: true` on the same task to satisfy `partial-become`. The playbook keeps `become: true` at play level as well.
