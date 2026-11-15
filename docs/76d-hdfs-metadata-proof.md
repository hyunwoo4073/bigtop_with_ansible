# Step76 Phase D3 — HDFS Iceberg smoke-table off-host recovery proof

## Exact scope and limits

- Reads only `/data/olist/iceberg-rest/olist56/catalog_smoke` from HDFS on `master` as `hadoop`.
- Captures **whole smoke table** (metadata JSON, manifests, and data files under that table directory) with a pre/post HDFS file-inventory comparison.
- Retains a protected tarball and per-file SHA-256 manifest on `master` at `/var/backups/bigtop-step76-hdfs/<backup-id>/`.
- Transfers archive and manifest through worker3 SSH pipes (memory only) to `worker1` at the same protected path (root ownership, 0700 directories, 0600 files).
- Extracts on worker1 into a new temporary isolated directory, compares every file SHA-256 and file set, removes temporary extracted data, and retains `restore-proof.json`.
- **Never writes to HDFS, changes Iceberg tables, Spark, Kafka, PostgreSQL, MySQL, Elasticsearch, or restarts services.**
- This is a proof for **one immutable smoke table**, not a point-in-time Warehouse backup, not a restored Catalog registration, and not a full-platform DR demonstration. The pre/post listing is not an atomic HDFS snapshot. The off-host VM shares the same physical host risk.

## Runbook (worker3)

```bash
cd ~/bigtop-cluster-ansible
unzip -n step76_d3_hdfs_iceberg_proof.zip -d .
make validate
bash scripts/run_step76_hdfs.sh preflight
# Proceed only if source.ready_for_capture=true and target.ready_for_copy=true
STEP76_ALLOW_HDFS_CAPTURE=YES bash scripts/run_step76_hdfs.sh capture
STEP76_ALLOW_HDFS_COPY=YES bash scripts/run_step76_hdfs.sh copy
STEP76_ALLOW_HDFS_RESTORE=YES bash scripts/run_step76_hdfs.sh restore
bash scripts/run_step76_hdfs.sh verify
```

Expected final marker: `STEP76_HDFS_VERIFY_PASS`, plus `STEP76_HDFS_ISOLATED_RESTORE_PASS` and exact per-file SHA-256 matches.

**Do not rerun `capture`, `copy`, or `restore` after partial failure.** Protected paths are deliberately not overwritten. Review the partial path or exception before reattempting. If the source table changes during capture, the operation fails; a changed HDFS source is not a successful DR proof.

## Next steps

D4: Elasticsearch snapshot repository and snapshot/restore planning (no filesystem copy of live ES volumes). D5: explicit recovery scope, RPO/RTO and operability summary, together with consistent HDFS data + Catalog backups before declaring full DR.
