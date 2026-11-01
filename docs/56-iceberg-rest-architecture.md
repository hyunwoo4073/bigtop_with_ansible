# 56 — Shared Iceberg REST Catalog (Gravitino standalone)

## Why

Spark 3.5.6 on YARN (Java 11 / Scala 2.12) and future Trino must share a persistent Iceberg catalog. Step 53/54 HadoopCatalog is intentionally kept separate as a lab. A REST catalog lets clients use REST operations instead of sharing JDBC catalog implementation/database credentials.

## Topology

- `worker3:9001`: standalone **Apache Gravitino Iceberg REST Server 1.3.1**, **JDK 17 in isolated path**, 256 MiB initial / 768 MiB max Java heap + 384 MiB max metaspace.
- `worker3:5432` (loopback): existing PostgreSQL installation with newly created `iceberg_rest56` database and user. Only the REST server uses this JDBC connection.
- `master:9000`: new HDFS warehouse `hdfs://master:9000/data/olist/iceberg-rest`. Existing `/data/olist/iceberg-lab` and `/data/olist/warehouse/v1..v3` remain untouched.
- `master` submitting Spark job to YARN `worker1/worker2`: runtime `iceberg-spark-runtime-3.5_2.12-1.10.1.jar` sent via `--jars`; Java 11 remains unchanged on Hadoop nodes.
- Future Trino (57): REST connector endpoint `http://192.158.2.103:9001/iceberg/`, separate JDK 25, not installed in this step.

```text
Spark (YARN) ─┐
              ├──► Gravitino standalone Iceberg REST (worker3:9001)
Trino (later) ┘            │
                           ├──► PostgreSQL local-only DB (Iceberg catalog registry)
                           └──► HDFS warehouse metadata/files (master:9000)
```

## REST vs JDBC distinction

Spark/Trino only connect to REST API, not PostgreSQL directly. Gravitino uses the Iceberg **JDBC backend internally** as a durable metadata registry. `backend=memory` is intentionally not used.

## Java and Iceberg compatibility

- Spark 3.5.6 / Scala 2.12 / Java 11 → Iceberg Spark 3.5 Runtime **1.10.1** pinned from Step 53.
- Gravitino **1.3.1** REST server → Java **17** (standalone portable JDK, not system default).
- The server supports Iceberg REST spec 1.11; actual interoperability of Spark's Iceberg 1.10.1 REST client is validated via a small Spark YARN write/read in 56.
- Trino installation and independent Java runtime are part of Step 57; do not deploy Trino in 56.

## Security / scope limitations

**Isolated lab, not a production network configuration**. Gravitino standalone mode does not provide built-in authorization, and this package does not configure OAuth2/TLS. Bind only to an isolated VM-only network address and restrict ingress to Spark/Trino nodes. Never expose port 9001 via public NAT forwarding. `192.158.2.x` is NOT RFC1918 private space, so verify the VirtualBox network isolation.

`HADOOP_USER_NAME=hadoop` only works with **simple/non-Kerberos HDFS authentication** and is not a production auth/impersonation design. The REST server uses this identity for its HDFS metadata writes; Spark also uses `hadoop` for smoke test. PostgreSQL account password is generated on worker3 at `/etc/bigtop-iceberg-rest/postgres.pass` (0600), and REST config remains root-owned and service-group-readable (0640). Secrets are not shipped in the ZIP or exposed in Ansible logs.

Note Gravitino 1.3.1 documentation warns its bundled Hadoop 2.10 dependencies might be incompatible with a Hadoop 3.x cluster. The REST API being healthy does *not* prove it can write to HDFS. The Spark write smoke or REST POST create-table test is the required end-to-end validation, and may expose compatibility problems.

## Memory and operational monitoring

Set `GRAVITINO_MEM` only in the systemd unit, not system global JVM env. Initial heap 256 MiB, max 768 MiB, metaspace max 384 MiB; process RSS will be higher due to thread stacks and native memory. Monitor `systemctl status bigtop-iceberg-rest`, `ps -o rss`, and VM available memory during Spark/Gravitino operations. Increase heap only when required and after checking memory headroom.

## Reproducibility

Vendor binaries under `/vendor/iceberg-rest/` are **NOT** committed to Git. Official Gravitino archive is SHA256-pinned, PostgreSQL jar and Temurin JDK17 files checked against HTTPS-supplied checksums. Store the source and manifest in a trusted internal artifact registry for fully offline rebuilds. Run `scripts/56-verify-artifacts.py` before Ansible.

## Known future risks

- standalone REST lacks per-user access control; plan OAuth/TLS and authZ design before real multi-user access.
- JDBC-backed catalog is a metadata registry, not a multi-table transaction coordinator.
- Gravitino REST + Spark 1.10.1 interoperability and Hadoop 3.x HDFS compatibility require real smoke verification.
- Existing HadoopCatalog tables are not automatically imported into REST; `Step 56` creates only a separate smoke table.

## Step 56 role variable naming

All variables defined or registered inside `roles/iceberg_rest_catalog` use the required `iceberg_rest_catalog_` prefix. Existing command-line flags `iceberg_rest_apply` and `iceberg_rest_spark_smoke` are read-only backwards-compatible aliases for Step 56.
