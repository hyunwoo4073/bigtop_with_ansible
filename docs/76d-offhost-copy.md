# Step76 D2 — Protected offhost copy of existing logical backups

This step copies the **existing verified Phase B backup ID**, not new DB dumps. It is
non-disruptive to the Kafka brokers, OpenMetadata services, databases and Step65 Streaming.

## Scope

- worker3: PostgreSQL `iceberg_rest56`, source `/var/backups/bigtop-step76/<ID>/`
- metadata: OpenMetadata MySQL `openmetadata_db`, same protected source path
- worker1 (default; worker2 allowed): protected target `/var/backups/bigtop-step76-offhost/<ID>/`
- Each archive and JSON manifest copied over **encrypted SSH**; worker3 streams
  data in memory only, never writes the dumps to its project or temp directory.
- On the destination, directories are root-owned `0700`, files are `0600`.
- Both copies are checked against source SHA-256, byte count and manifest identity.
  A root-only `offhost-proof.json` is created after complete verification.
- **No database restore** and no Elasticsearch/HDFS backup in this phase.

## Prerequisites

- `make validate` succeeds.
- D1 `STEP76_DR_PREFLIGHT_PASS` and Phase B/C verification already passed.
- `worker3` can SSH non-interactively to `worker3`, `metadata`, and `worker1`.
- Noninteractive `sudo -n python3` is allowed on those hosts. Host keys must
  already be trusted; the script uses strict host key checking.
- `artifacts/step76/latest_backup_id` exists. A fresh backup is NOT created.

## Commands

```bash
make validate && bash scripts/run_step76_offhost.sh preflight
STEP76_ALLOW_OFFHOST_COPY=YES bash scripts/run_step76_offhost.sh copy
bash scripts/run_step76_offhost.sh verify
```

Expected markers: `STEP76_OFFHOST_PREFLIGHT_PASS`, `STEP76_OFFHOST_COPY_PASS`,
`STEP76_OFFHOST_VERIFY_PASS`.

## Recovery / failed transfers

**Never blindly rerun `copy`** after an error. It intentionally refuses an
existing backup ID at the target. On partial failure, inspect the protected
folder on `worker1` with root access; clean up only after identifying the
incomplete ID and preserving the source and logs. Running `verify` does not
mutate the files and can help identify whether the copy was sealed.

VM-to-VM copies inside one VirtualBox physical host protect against one VM/disk
failure, not host-wide destruction. The separate Phase D tasks for HDFS and
Elasticsearch Snapshot remain unverified. This does not assert full DR.
