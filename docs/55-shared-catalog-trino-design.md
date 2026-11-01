# Step 55 — Shared Iceberg catalog & Trino architecture decision

**Status:** diagnostic package, not deployed. Review `artifacts/55-iceberg-trino-preflight.json` before modifying any services.

## Why not point Trino to the Step 53 Hadoop Catalog?

Step 53/54 `lab.olist.*` tables use a directory-based Iceberg Hadoop Catalog at `hdfs://master:9000/data/olist/iceberg-lab`. The Trino Iceberg connector supports explicit catalog types (`hive_metastore`, `rest`, `jdbc`, etc.), not `hadoop` for a shared metadata catalog. It will not automatically discover the existing lab namespace. Keep it intact and create a **new** shared catalog and an isolated validation namespace/warehouse. Do not copy `metadata/` folders into a new catalog or manually modify pointer files.

## Candidate A — Iceberg REST Catalog (preferred after compatibility proof)

```
Spark 3.5.6 / Iceberg 1.10.1 ----+-- Iceberg REST API --- catalog backend
                                |
Trino 484 / Java 25 ------------+         |
                                           +--- HDFS Iceberg warehouse
```

REST Catalog is an API protocol, not a deployable database by itself. Select a concrete REST server implementation that explicitly supports **HDFS**-based Iceberg table locations and the matching auth model. Many OSS REST implementations are object-store-centric and need validation before selection. Do not assume an S3-only setup works with HDFS.

## Candidate B — existing Hive Metastore (pragmatic fallback)

```
Spark SparkCatalog(type=hive) ----+-- HMS Thrift (9083) -- HMS DB
                                 |
Trino Iceberg(hive_metastore) ---+       |
                                         +--- HDFS Iceberg warehouse
```

HiveServer2 and Hive's SQL engine are **not required**. Only the HMS process and metadata database are needed. If HMS is already healthy this reduces the service count. Requires a properly configured metastore endpoint and table permissions.

## Candidate C — JDBC catalog backed by PostgreSQL

Both Spark Iceberg and Trino have JDBC catalog support, but Trino explicitly warns of potential catalog schema incompatibilities across versions and requires metadata tables to exist. Do not repurpose the Web Console's operational database without a dedicated isolated database/role and migration plan.

## Hard deployment gates

1. Verify the selected Trino release and dedicated JDK. Trino 484 requires **Java 25.0.1+**, and should run with its own `JAVA_HOME`. Spark/HDFS/YARN must stay on Java 11.
2. Validate VM memory, swap, CPU, and existing service footprint, especially `worker3` (PostgreSQL, metrics stack, Web Console) and `metadata` (OpenMetadata stack).
3. `fs.hadoop.enabled=true` for the Trino Iceberg connector. If NameNode HA/custom Hadoop client config exists, copy the correct `core-site.xml`/`hdfs-site.xml` as `hive.config.resources` on each Trino node.
4. Confirm DNS/hostnames `master`, `worker1`, `worker2` resolvable from Trino and firewall allows access to both NameNode **and DataNodes**, not only port 9000.
5. Decide HDFS identity/ACL for the Trino process. Trino must be able to read Iceberg metadata, manifest, and Parquet files (and write only in explicitly assigned test namespace if needed).
6. Confirm the selected REST catalog implementation can handle table creation and commits to HDFS (or choose HMS). Avoid an accidental filesystem-level Catalog split.
7. Initially run a **single-node Trino deployment** with limited memory; add Workers only when monitoring/benchmarking justifies the overhead.

## Subsequent milestones

- **56:** deploy an isolated shared catalog in an HDFS-compatible configuration; Spark creates a single new Iceberg test table and validates snapshot commits.
- **57:** install separate JDK 25 & Trino (initially single-node), configure HDFS, query that same table by SQL; verify schema, row count, metadata tables and simple analytics.
- **58:** migrate Olist transformation output to Iceberg Silver/Gold without changing existing Raw v1, Parquet warehouse v1/v3 or approval JSON.
- **59:** WAP integration with real quality checks and explicit publication step; address multi-table release consistency.

## References

- Trino deployment: https://trino.io/docs/current/installation/deployment.html
- Trino Iceberg connector: https://trino.io/docs/current/connector/iceberg.html
- Trino metastores: https://trino.io/docs/current/object-storage/metastores.html
- Trino HDFS: https://trino.io/docs/current/object-storage/file-system-hdfs.html
- Iceberg Spark config: https://iceberg.apache.org/docs/latest/spark-configuration/
