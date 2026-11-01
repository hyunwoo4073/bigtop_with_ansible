# Step 58 — fix Gravitino REST catalog selection error

## Observed failure

YARN application `application_1793511506713_0002` failed in the Driver at `spark.sql(CREATE NAMESPACE ...)`, with:

```text
org.apache.iceberg.exceptions.RESTException: Unable to process:
Couldn't find Iceberg configuration for catalog hdfs://master:9000/data/olist/iceberg-rest
```

Root cause: Step 58 set `spark.sql.catalog.shared.warehouse=hdfs://master:9000/data/olist/iceberg-rest` in the **Spark REST client**. Gravitino uses the REST `warehouse` selection to identify a configured catalog backend, whereas this deployment uses the **default** backend. The physical Iceberg warehouse is already configured in the Step 56 Gravitino server (`gravitino.iceberg-rest.warehouse`). Step 56 Spark smoke succeeded **without** the client-side `warehouse` setting, and Trino's cross-query succeeded on the same backend.

## Patch scope

- `roles/olist_iceberg_materialization/tasks/apply.yml`: remove only the bad Spark client `--conf` pair. No changes to 56/57, Spark/YARN, HDFS, or PostgreSQL.
- `tests/test_olist_iceberg_materialization.py`: regression assertion that the client does not set an HDFS `warehouse`; fix legacy-role isolation test to work in a real existing repository.
- `docs/58-olist-iceberg-apply.md`: document distinction between server-side data warehouse and client-side catalog selector.

## Apply on worker3

```bash
cd ~/bigtop-cluster-ansible
unzip -o 58_iceberg_rest_warehouse_fix.zip -d .
python3 -m unittest discover -s tests -p 'test_olist_iceberg_materialization.py' -v
make lint
.venv/bin/ansible-playbook -i inventory/hosts.ini \
  playbooks/58-olist-iceberg-materialization.yml \
  -e olist_iceberg_materialization_apply=true
```

**Before rerunning:** Do not delete existing Iceberg tables or HDFS files. The first run may have created a partial namespace/table; the Step 58 job checks existing tables and validates them without replacement. It only writes its final proof after all 7 tables pass. If the same error persists, inspect the full new YARN Driver log for the newest Application ID.

## Independent verification

```bash
python3 scripts/58-verify-trino.py \
  --url http://192.158.2.104:8080 \
  --proof artifacts/58-iceberg-v3-proof.json
```

This patch was unit-tested and YAML-parsed in an isolated copy of the package. It cannot establish successful runtime operation on the user's VMs; that requires rerunning the Ansible playbook and Trino verification.
