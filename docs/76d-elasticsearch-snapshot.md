# Step76 D4 — OpenMetadata Elasticsearch Snapshot and Isolated Restore

**Scope**: existing `metadata` VM; OpenMetadata Docker Compose project `bigtop-openmetadata`, service `elasticsearch` only.

This phase does **not** create new containers, VMs, brokers, MySQL servers or Elasticsearch clusters. It does **not** change Step65 Streaming.

## Existing Step76 observations

- Phase D1 discovered an OpenMetadata Elasticsearch cluster with `yellow` health, 83 indices, **no snapshot repository** and **no configured `path.repo`**.
- MySQL `openmetadata_db` is the OpenMetadata **primary metadata database**, and Elasticsearch is a **search index**. The existing MySQL backup is kept. A fresh Elasticsearch index may also be rebuilt from authoritative OpenMetadata metadata, but that is not demonstrated here.
- A supported Elasticsearch filesystem snapshot requires `path.repo` and a writable mounted directory. Enabling this requires **recreating the Elasticsearch container**, which may temporarily affect OpenMetadata search. The rest of the Compose stack is not restarted.

## Commands

```bash
cd ~/bigtop-cluster-ansible
bash scripts/run_step76_es.sh preflight

# Only when a short OpenMetadata search interruption is acceptable:
STEP76_ALLOW_ES_RESTART=YES bash scripts/run_step76_es.sh configure

bash scripts/run_step76_es.sh snapshot

# Only with enough free disk and a small non-empty sample index:
STEP76_ALLOW_ES_RESTORE=YES bash scripts/run_step76_es.sh restore

bash scripts/run_step76_es.sh verify
```

**Do not run `make validate` here.** The full project lint/syntax check runs before Git commit.

## Operations and safety

- Repo is fixed as `step76_om_fs` at container `/usr/share/elasticsearch/snapshots`; host bind directory `/var/backups/bigtop-step76-es` is `root:root`, mode `0770` (compatible with Elasticsearch Docker `uid:gid 1000:0`).
- Config is added to `/opt/bigtop-governance/openmetadata/compose.yaml` with an original backup in the same protected directory. Only `docker compose ... up -d --no-deps elasticsearch` is used. A best-effort rollback restores the original Compose file and ES container if startup fails.
- `scripts/step76_d4_patch_template.py` updates the Ansible source-of-truth `roles/governance_openmetadata/templates/compose.yaml.j2` after successful live config, avoiding reversion on future Step41 deployments. This backup `.bak` is local-only and should **not** be staged to Git.
- Snapshots use `include_global_state=false`, `partial=false`, `feature_states=[]`, and exclude hidden & Step76 test indices. Existing snapshot IDs are never overwritten.
- A sample restore uses rename-on-restore into an exact `step76_verify_*` index, `include_aliases=false`, and `include_global_state=false`. A sample populated index must be <=8MiB. The test index is deleted after verification. All regular source indices remain untouched.
- Index contents may change while snapshotting. This test proves snapshot and restore usability, **not** cross-service point-in-time consistency.
- The repository is still **on the metadata VM**. **D5** must copy a completed, quiescent snapshot repository to `worker1`, validate transfer and document restore procedures; do not run raw filesystem copies of active Elasticsearch `data` volume.
- OpenMetadata's actual MySQL + Elasticsearch roles, and the separate Platform PostgreSQL & Iceberg REST Catalog PostgreSQL roles, must be distinguished in README.md and docs/PROJECT_GUIDE.md at session end.

## Pass markers

- `STEP76_ES_PREFLIGHT_PASS`
- `STEP76_ES_CONFIGURE_PASS`
- `STEP76_ES_SNAPSHOT_PASS`
- `STEP76_ES_RESTORE_SAMPLE_PASS` and `STEP76_ES_TEST_INDEX_CLEANUP_PASS`
- `STEP76_ES_VERIFY_PASS`

On any failure, **do not blindly repeat `configure`, `snapshot` or `restore`**; inspect the Elasticsearch container state, existing snapshot and Step76 test index first.

## References

- https://www.elastic.co/docs/deploy-manage/tools/snapshot-and-restore/shared-file-system-repository
- https://www.elastic.co/docs/deploy-manage/tools/snapshot-and-restore/restore-snapshot
- https://www.elastic.co/docs/deploy-manage/deploy/self-managed/install-elasticsearch-docker-configure
