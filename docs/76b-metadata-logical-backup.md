# Step76 Phase B — Protected Metadata Logical Backups

**Status:** backup and integrity verification only. Does not perform restore, cross-VM backup copy, or claim full disaster recovery.

## Scope and locations

- `worker3` PostgreSQL database `iceberg_rest56`: `pg_dump -Fc` using local PostgreSQL OS identity, checksummed, verified using `pg_restore --list`.
- `metadata` VM, `bigtop-openmetadata` Compose MySQL database `openmetadata_db`: `mysqldump` with `--single-transaction` and consistent SQL format checks. Credentials stay inside existing Docker container environment and never appear in Ansible output or shell command arguments.
- Artifacts are intentionally kept **outside Git** in `/var/backups/bigtop-step76/<BACKUP_ID>/` on **each respective VM**. Backup directories are root-owned `0700`; backup and manifest files root-owned `0600`.
- Script places the most recent successful backup identifier in local `artifacts/step76/latest_backup_id` (should be Git-ignored). This identifier contains no secrets.

## Run from worker3

```bash
cd ~/bigtop-cluster-ansible
unzip -n step76_metadata_logical_backup.zip -d .
make validate && bash scripts/run_step76_backup.sh backup
bash scripts/run_step76_backup.sh verify
```

Expected marker from Ansible: `STEP76_LOGICAL_BACKUP_PASS`, followed by `STEP76_LOGICAL_VERIFY_PASS` and local `STEP76_LOGICAL_VERIFY_PASS: <BACKUP_ID>`.

To verify a specific backup again, without modifying it:

```bash
bash scripts/run_step76_backup.sh verify 20261115T084500Z-1234abcd
```

Never blindly delete or overwrite backups. If only one node succeeded, do not treat that backup ID as a complete set; inspect the failure and create a **new ID** for a new set. Each attempt refuses overwrite. The dumps are sensitive because they may contain service credentials or other metadata; store only on protected hosts, do not add to Git or attach them to chats.

## What is deliberately not protected by Phase B

- HDFS Iceberg metadata JSON, manifests, warehouse contents, and data files
- Elasticsearch/OpenMetadata search index and its configuration
- Off-host backups, disaster recovery against total loss of a VM/disk
- Cross-database/warehouse point-in-time consistency
- Restore proof in an isolated database or test VM

Continue to **Phase C** only after both backups and their SHA-256/formats verify. Phase C must restore into isolated destination databases with explicit authorization and confirm schema and record integrity, never restore into `iceberg_rest56` or `openmetadata_db` production databases. Phase D covers HDFS, search index, off-host copy, retention and actual recovery procedure.

## Notes

`mysqldump --single-transaction` provides a transactional snapshot for InnoDB tables; concurrent DDL or non-transactional table engines can require a different consistency strategy. Dump operations can create CPU/IO load; run during a low-load window. Backups cannot be considered complete disaster recovery until they have been restored successfully and copied outside their original failure domain.
