# Step76 — Metadata Backup & DR: live topology discovery

Status: **Phase A (read-only preflight). No backup or restore has been executed.**

## Backing stores discovered from project configuration

- `worker3` standalone Gravitino Iceberg REST Catalog 1.3.1: **PostgreSQL `iceberg_rest56`** at localhost:5432. This is **not** the separate Web Console `bigtop_console` database.
- `metadata` OpenMetadata 2.0.6 Compose stack (`bigtop-openmetadata`): **MySQL `openmetadata_db`** and persistent **Elasticsearch** search index. Do not assume OpenMetadata uses PostgreSQL.
- Iceberg tables use HDFS warehouse `hdfs://master:9000/data/olist/iceberg-rest`, plus possibly other table locations. Restoring PostgreSQL alone **does not** restore Iceberg table metadata JSON/manifest/data files.
- Existing Web Console PostgreSQL backup timers are separate and out of scope for Step76.

## Run

```bash
cd ~/bigtop-cluster-ansible
unzip -n step76_metadata_dr_preflight.zip -d .
make validate
bash scripts/run_step76_preflight.sh
```

Expected marker: `STEP76_PREFLIGHT_PASS`. The report contains only nonsecret topology, table counts, approximate disk usage, Docker volume names, and warehouse usage.

## Follow-up phases (not implemented in this read-only package)

1. **B — Backup:** protected `pg_dump -Fc` for `iceberg_rest56`; MySQL transactional logical dump for `openmetadata_db`; checksums and immutable manifests; backup artifacts outside Git; capture Iceberg warehouse / metadata references; document Elasticsearch recovery strategy.
2. **C — Restore proof:** verify archives, restore into isolated temporary DBs with explicit approval, query table counts/sample catalog metadata, drop only temporary DBs. Never restore into production databases.
3. **D — DR readiness:** create an off-host/off-volume copy, retention/rotation, and test recovery of HDFS metadata files and Elasticsearch search indexes. Only then claim comprehensive DR readiness.

Safety: This preflight uses no mutations to databases, HDFS, Docker volumes, Kafka, or streaming applications. Ansible `script` stages transient temp files; it does not install services. Existing Step65 and Step66 status is unchanged.
