# Step 55 — Trino + shared Iceberg Catalog preflight

## Scope

Read-only status inventory of the existing 5 VMs. This step **does not install Trino, a REST Catalog, a JDK, or any additional Hadoop package** and does not modify existing HDFS data, Spark configurations, or Iceberg snapshots. Ansible's `script` module uses an ephemeral remote staging file and removes it on completion. One local report is written under `artifacts/` with mode `0600`.

## Known working baseline

- Spark 3.5.6 / Scala 2.12.18 / Java 11.0.32.1.
- Iceberg 1.10.1 via `spark-submit --jars` (YARN distributes the jar), Step 53 PASS.
- Iceberg 53 HadoopCatalog at `/data/olist/iceberg-lab` is **not a Trino-shared catalog**.
- Step 54 transactions and WAP stage/publish verified. Do not alter these lab tables.
- OpenMetadata runs on `metadata`; web console, Grafana, Prometheus, and PostgreSQL on `worker3`. Both need memory headroom.

## Apply on worker3

```bash
cd ~/bigtop-cluster-ansible
unzip -o 55_iceberg_trino_preflight_ansible.zip -d .
python3 -m unittest discover -s tests -p 'test_iceberg_trino_preflight.py' -v
make lint
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/55-iceberg-trino-preflight.yml
python3 scripts/55-review-preflight.py artifacts/55-iceberg-trino-preflight.json
```

Do **not** add `-e ansible_connection=local`: the playbook probes master, workers, worker3, and metadata over SSH. If a node is unreachable, inspect its Ansible inventory/SSH settings; the diagnostic must not silently substitute data.

`artifacts/55-iceberg-trino-preflight.json` is generated on worker3, *not* committed to Git. It includes hostnames, selected listening ports, available RAM, installed binaries, and a whitelist of Hadoop/Hive properties. It deliberately avoids reading secrets from config files. Although no secrets are collected by design, keep infrastructure details private.

## Catalog decision for Step 56

Prefer a REST Catalog if its implementation supports the chosen HDFS warehouse and the selected client versions. Verify this in an isolated smoke test rather than assuming that every REST implementation supports HDFS (some object-store-centered implementations assume S3). If an existing HMS is working, using it as the shared catalog is a pragmatic fallback. The Trino Iceberg connector supports both REST and Hive metastores; it also supports JDBC catalogs, but Trino documents forward-compatibility caveats for JDBC catalog schemas.

Trino 484 requires **Java 25.0.1 or later in the Java 25 series**. Keep Spark/Hadoop Java 11 untouched and install a separate JDK for the Trino service only. Trino's HDFS filesystem support must be explicitly enabled with `fs.hadoop.enabled=true` in the Iceberg connector. If the cluster requires extra `core-site.xml`/`hdfs-site.xml` configuration, use `hive.config.resources` with paths present on every Trino node.

Do not deploy Trino on an already overloaded 4-GB VM without checking available memory and service allocation. Once the report is reviewed, choose service placement and JVM limits. A new dedicated VM or carefully constrained single-node Trino may be safer than co-locating it with OpenMetadata or the worker3 operational stack.

## What to send for the next step

The terminal output of `55-review-preflight.py` plus whether there is an existing Hive Metastore and whether a new VM is possible. Do not paste passwords or tokens.

## References

- https://trino.io/docs/current/installation/deployment.html
- https://trino.io/docs/current/connector/iceberg.html
- https://trino.io/docs/current/object-storage/file-system-hdfs.html
- https://trino.io/docs/current/object-storage/metastores.html
- https://iceberg.apache.org/docs/latest/spark-configuration/

## Permission-safe probe fix

On `metadata`, the ordinary Ansible SSH user may not traverse `/opt/bigtop-governance/openmetadata`. The path inventory must **not** be run with global privilege escalation or change directory permissions. `scripts/55-probe-node.py` now records `"permission_denied"` in `paths_exist` when `Path.exists()` raises `PermissionError`; accessible paths continue returning `true`/`false`. If an unrelated OS error occurs, it records `"unavailable"` instead. This does not claim the protected directory is absent.

After applying the patch, run the full five-node preflight again to regenerate the report. A path permission error is a diagnostic finding, not grounds to abort all five-node collection.
