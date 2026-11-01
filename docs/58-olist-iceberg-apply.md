# Step 58: apply approved Olist v3 → Iceberg Silver/Gold (isolated)

**Prerequisites**: Step 52 approved `v3` Parquet snapshot, Step 53 runtime JAR `/opt/bigtop-iceberg/iceberg-spark-runtime-3.5_2.12-1.10.1.jar` on master, Step 56 Gravitino REST worker3:9001 and Step 57 Trino metadata:8080 functional. No dependency on public internet; ZIP contains code only.

Run from Ansible controller **worker3**:

```bash
cd ~/bigtop-cluster-ansible
unzip -o 58_olist_iceberg_silver_gold_ansible.zip -d .
python3 -m unittest discover -s tests -p 'test_olist_iceberg_materialization.py' -v
make lint
```

**Dry plan** (no HDFS/catalog writes, no Spark submit):

```bash
.venv/bin/ansible-playbook -i inventory/hosts.ini \
  playbooks/58-olist-iceberg-materialization.yml
```

**Catalog configuration caution (Step 58 fix):** This Gravitino standalone REST server uses its default catalog when the Spark client omits `spark.sql.catalog.shared.warehouse`. The physical HDFS Warehouse is configured **on the server** (Step 56), not as a Spark REST catalog selector. Setting the Spark client's warehouse to the HDFS URI caused `Couldn't find Iceberg configuration for catalog hdfs://...`. The Step 58 role now omits that option, matching the successful Step 56 smoke test. Existing `olist58_v3` data is never deleted or overwritten.

**Materialize seven tables** in a **new** REST Catalog namespace, read-only source and no overwrite of previous tables:

```bash
.venv/bin/ansible-playbook -i inventory/hosts.ini \
  playbooks/58-olist-iceberg-materialization.yml \
  -e olist_iceberg_materialization_apply=true
```

Expected Ansible final result: 7 tables parity PASS and `/data/olist/iceberg-validation/step58/v3/_SUCCESS` plus `artifacts/58-iceberg-v3-proof.json`. Spark `print()` messages appear in the **YARN cluster-mode Driver logs**, not reliably in spark-submit stdout.

```bash
# Full parity revalidation of existing tables without new table creation:
.venv/bin/ansible-playbook -i inventory/hosts.ini \
  playbooks/58-olist-iceberg-materialization.yml \
  -e olist_iceberg_materialization_apply=true \
  -e olist_iceberg_materialization_verify_only=true

# Worker3 independent Trino reads (7 counts and one real Gold aggregation):
python3 scripts/58-verify-trino.py \
  --url http://192.158.2.104:8080 \
  --proof artifacts/58-iceberg-v3-proof.json
```

Expected final marker: `STEP58_SPARK_TRINO_SILVER_GOLD_CROSS_QUERY_PASS`.

### HDFS and Trino manual inspection

```bash
ssh master 'sudo -u hadoop hdfs dfs -text /data/olist/iceberg-validation/step58/v3/part-*'

# Trino HTTP endpoint status
curl -fsS http://192.158.2.104:8080/v1/info | python3 -m json.tool

# YARN Driver logs: use application ID from spark-submit if query fails
ssh master 'yarn application -list -appStates ALL | grep olist-iceberg-materialize-step58-v3'
```

The generated proof JSON under `artifacts/` is a local runtime artifact, **not source code**; avoid staging it in Git if it contains environment information. Do not run `git add -A` without checking. Do not commit `vendor/` JARs.

### Operational constraints

- Trino service and Gravitino must remain available and HDFS permissions must allow reading/writing from current `hadoop` and service accounts; Step 57 verified the baseline for the smoke table.
- Full exact parity across ~1M input records entails costly Spark stages; this is intentional for one-time migration verification, not recommended for every micro-batch.
- If a run fails after creating a subset of tables, **do not delete or overwrite**; rerun safely and inspect YARN Driver logs. An existing table with wrong contents/provenance is an explicit error requiring investigation.
- A fully written but `_SUCCESS`-less proof path causes a safe failure and requires manual inspection before retry.
- No change to existing Step 45–57 resources. New tables are under `olist58_v3`; Step 59 will be responsible for a proper quality-gated Iceberg release.
