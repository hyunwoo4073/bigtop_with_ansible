# Step76 Phase C — Isolated logical restore proof

**Status:** runnable package, actual VM execution unverified. Phase B must have completed on both VMs.

## Purpose

Restore the existing *immutable* Phase B logical backup `backup_id` into **new temporary databases** with distinct names, verify the restored table inventory and sample row counts, and clean only those temporary databases. Original databases remain in place and are never selected as restore targets.

### Isolation model

- **PostgreSQL**: create `step76_r_<date>_<hex>` and a matching `NOLOGIN`, `NOSUPERUSER` role on worker3. `pg_restore` connects only to the scratch DB and uses `--role` to execute DDL under the restricted role. After restore/count checks, drop only the newly created scratch DB and role.
- **OpenMetadata MySQL**: create scratch DB and one-time `'step76_r_*'@'localhost'` user inside the *existing* Docker MySQL server on metadata VM. Grant access only to scratch database. Stream the Phase B dump using this restricted account. After validation, drop only the scratch user and DB.
- **No off-host isolation**: scratch DBs share the production database engines and consume server disk/IO; this is a logical restore proof, **not a whole-VM DR exercise**. No new MySQL/Postgres server, port, Docker volume, or service restart.

Important: the Phase B dumps may contain CREATE routines/views/definers incompatible with restricted restoration. In that case the test **fails**, cleans scratch objects, and requires investigation; do not bypass privilege boundaries to make a test pass.

### Safety boundaries

- Backup ID must have the existing format, e.g., `20261115T084500Z-1234abcd`.
- Dumps are read directly from root-only `/var/backups/bigtop-step76/<BACKUP_ID>/` on each VM. `sha256`, size, filename, owner/mode, and DB identity are rechecked.
- The **only** writable restore destinations are a derived scratch name (`step76_r_*`), not caller-supplied names.
- Existing scratch DB/user from a previous attempt **blocks preflight**; script never removes an unrelated or preexisting resource.
- Source DBs can change after Phase B's snapshot. A comparison with their *current* table counts is informational, not a point-in-time equality guarantee.
- The proof confirms table definitions restored, performs exact row counts for up to five sample tables, and shows whether those sample counts match the *current* live source. It does **not** compare every row byte-for-byte or prove application-level recoverability.
- Artifacts remain on source nodes, root-owned and mode `0600`; **do not upload dumps or manifests to Git or chat**.

## Commands (worker3)

```bash
cd ~/bigtop-cluster-ansible
unzip -n step76_metadata_isolated_restore.zip -d .
make validate
bash scripts/run_step76_backup.sh verify
bash scripts/run_step76_restore.sh preflight
STEP76_ALLOW_SCRATCH_RESTORE=YES bash scripts/run_step76_restore.sh restore
bash scripts/run_step76_restore.sh verify
```

By default, the script uses `artifacts/step76/latest_backup_id` produced by Phase B. You can also provide a specific ID:

```bash
bash scripts/run_step76_restore.sh preflight pg 20261115T084500Z-1234abcd
STEP76_ALLOW_SCRATCH_RESTORE=YES bash scripts/run_step76_restore.sh restore pg 20261115T084500Z-1234abcd
bash scripts/run_step76_restore.sh verify pg 20261115T084500Z-1234abcd
```

For a partial failure (PG PASS, MySQL FAILED), **do not rerun `both restore`**. Inspect error and rerun only `mysql` after confirming temporary resources were cleaned; use `preflight mysql` first. `restore` refuses overwrite if proof exists.

Expected markers: `STEP76_ISOLATED_PREFLIGHT_PASS`, `STEP76_ISOLATED_RESTORE_PASS`, `STEP76_ISOLATED_VERIFY_PASS`. Also check `tables_restored > 0` and `scratch_cleanup: verified` in output.

### Why Phase D is still required

The same server can fail together with the original database and scratch restore. Disaster recovery needs protected off-node copies, Elasticsearch snapshot/re-index strategy, HDFS Iceberg metadata/warehouse recovery, and a tested recovery order. Proceed to Phase D only after these Phase C tests pass.
