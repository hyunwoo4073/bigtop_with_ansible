# 57 — Trino + Gravitino Iceberg REST + HDFS

## Scope (additive, no modification of Hadoop/Spark/56)

```
Spark 3.5.6 (Java 11 / YARN cluster)
       | writes: shared.olist56.catalog_smoke (id=56, source=spark)
       v
Gravitino 1.3.1 (worker3:9001, Java 17, PostgreSQL 5432)
       ^
       | Iceberg REST HTTP API
       |
Trino 484 (metadata:8080, Java 25 private, single coordinator+worker)
       | HDFS filesystem data read
       v
HDFS: master:9000 -> worker1/worker2 DataNode transfer ports
```

`shared` is Spark's catalog alias, `iceberg_rest` is Trino's catalog alias. The Iceberg *table identity* is `olist56.catalog_smoke` in Gravitino REST. `iceberg_rest.olist56.catalog_smoke` is the same table, not a copy.

The server is a single-node LAB installation, not a performance/production sizing baseline. `metadata` has approximately 4.46 GiB MemAvailable as of Step 55; we reserve 1.5 GiB JVM heap, cap the service at 3 GiB cgroup memory, and require at least 3000 MiB available on a **first installation**. The OpenMetadata Docker stack shares the host and must be checked when capacity changes. The host Java is untouched.

### Version pin and supply-chain validation

- Trino 484 release archive: `https://github.com/trinodb/trino/releases/download/484/trino-server-484.tar.gz`, SHA-256 `4b2d05f2e52fcc156b2f137cabc886353e5dde782d12bf71e290ffc13463b81a` (Homebrew core formula v484).
- Java 25.0.1+ from Adoptium API's official metadata, SHA-256 provided by the selected JDK asset, recorded in `vendor/trino57/manifest.json`.
- `scripts/57-verify-artifacts.py` checks *both* payloads before Ansible installs anything. Keep these binaries in `vendor/` and outside Git. Download requires external connectivity and disk space on worker3 and metadata.

### REST compatibility nuance

Step 56 Spark code used `spark.sql.catalog.shared.uri=http://192.158.2.103:9001/iceberg/` without an explicit REST prefix. To keep the same static/default Gravitino catalog, the Trino catalog does **not** set `iceberg.rest-catalog.prefix` or `warehouse`. Introducing those properties without verifying `/iceberg/v1/config` might point Trino to a different catalog or fail. Gravitino standalone REST does not provide access control; HTTP is unencrypted on the private lab network.

### HDFS

Trino's Iceberg connector uses `fs.hadoop.enabled=true` and `hive.config.resources` pointing to a small dedicated Hadoop `core-site.xml`; this is for HDFS and does not imply running Hive. Trino JVM supplies `-DHADOOP_USER_NAME=hadoop` for the current non-Kerberos lab where HDFS permission checks are disabled. This is **not secure end-user authorization**; don't expose Trino or REST service beyond the isolated test network. External SQL can modify tables unless separate controls are applied. There is no production RBAC, TLS, impersonation, or Kerberos in Step 57.

### Existing data integrity

Step 57 creates no HDFS tables and executes only `SELECT` queries. It neither re-runs Spark nor manipulates Iceberg snapshots. It leaves HDFS `/data/olist/iceberg-lab` (53/54) and `/data/olist/iceberg-rest` (56) unchanged.

### Preconditions

1. Step 56 published the exact row `(56, 'spark')` to REST table `olist56.catalog_smoke`.
2. On `metadata` the addresses/names `master`, `worker1`, and `worker2` resolve. TCP from `metadata` to worker3:9001, master:9000, and each DataNode:9866 works. If DataNode transfer ports differ, identify actual `dfs.datanode.address` and update the Role default `trino_iceberg_rest_datanode_port` and probe; do not change HDFS to fit the example.
3. metadata:8080 is not in use. Sufficient VM RAM/disk remains to run a *new* Trino JVM alongside Docker/OpenMetadata. No changes to system `java`.

### Completion gate

- `systemctl is-active bigtop-trino57` on metadata -> `active`;
- `GET http://192.158.2.104:8080/v1/info` -> `nodeVersion.version=484`;
- `SELECT id,source FROM iceberg_rest.olist56.catalog_smoke ORDER BY id` -> exactly `(56, 'spark')`;
- `SELECT count(*) ...` -> exactly `1`;
- `STEP57_TRINO_SPARK_CROSS_QUERY_PASS` from the real Trino HTTP statement protocol.

A successful HTTP health check by itself is **not** a passed cross-engine read.

### Official references

- https://trino.io/docs/current/installation/deployment.html
- https://trino.io/docs/current/connector/iceberg.html
- https://trino.io/docs/current/object-storage/file-system-hdfs.html
- https://trino.io/docs/current/object-storage/metastores.html
- https://gravitino.apache.org/docs/1.3.1/iceberg-rest-engine/trino/
