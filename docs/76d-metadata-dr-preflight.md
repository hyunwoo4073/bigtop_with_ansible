# Step76 Phase D1: remote copy, HDFS and Elasticsearch discovery

Step76 A/B/C have already validated metadata-store discovery, logical backups of
`iceberg_rest56` (on worker3) and `openmetadata_db` (on metadata), and scratch DB restores.
The scope of this D1 playbook is **read-only readiness**, not completed DR.

## Why discovery first

- The PostgreSQL and MySQL dumps reside on their source VMs, stored as `root:root`,
  directory `0700`, files `0600`, at `/var/backups/bigtop-step76/<BACKUP_ID>`.
  Both dump manifests and SHA-256 checksums are reverified here.
- A backup on `worker1` or `worker2` would protect against one VM's filesystem loss,
  **not physical host loss**, because the lab uses shared VirtualBox infrastructure.
  Avoid storing backup archives in Git or in the HDFS DataNode data directory.
- An online Elasticsearch Docker `es-data` volume **must not** be copied directly
  as a consistent database backup. Use the Elasticsearch Snapshot API and an
  independently mounted/registered repository. Whether a repository already exists
  must be established before choosing a deployment/restart plan.
- HDFS Iceberg `metadata/` files, manifests, and the catalog DB must be protected as
  a consistent recovery set. Merely copying HDFS table metadata without HDFS data
  can leave a recovered table unusable.

## Run (no changes to services)

```bash
cd ~/bigtop-cluster-ansible
unzip -n step76_metadata_dr_phase_d_preflight.zip -d .
make validate
bash scripts/run_step76_dr_preflight.sh
```

By default the off-host candidate is `worker1`. Use
`STEP76_DR_TARGET_HOST=worker2 bash scripts/run_step76_dr_preflight.sh` to inspect
worker2 instead. The script reads the backup identity from
`artifacts/step76/latest_backup_id` produced by Phase B.

Expected `STEP76_DR_PREFLIGHT_PASS` means only that all inventory probes executed.
Check `disk_threshold_pass`, `snapshot_ready`, `scope_confirmed`,
`metadata_discovery_ok` separately; a passing preflight does not indicate
DR protection is already in place.

## Next phases

1. Phase D2: select and verify an off-host target, copy root-protected logical DB
   dumps off host without exposing them in Git, and verify checksums on target.
2. Phase D3: inventory HDFS tables via REST Catalog and archive the required
   metadata/manifest/data with a consistent cut; test recovery into an isolated
   namespace rather than changing production.
3. Phase D4: configure a supported Elasticsearch snapshot repository if needed,
   create a snapshot, verify status, and test recovery to isolated indices.
4. Phase D5: rehearse a loss-of-node recovery by recreating the catalog and
   OpenMetadata services on separated infrastructure; record RPO/RTO and runbook.

No Phase D1 command modifies PostgreSQL, MySQL, HDFS, Elasticsearch, Docker
volumes, Kafka, Spark/YARN, service units, or the Step66 Watchdog state.
