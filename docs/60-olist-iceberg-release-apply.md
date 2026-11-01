# 60 — Apply and verify automated Iceberg release workflow

Run all commands on **worker3** from `~/bigtop-cluster-ansible`. Step58 and Step59 must already be deployed and validated. Step60 is a one-shot **orchestrator**, not a periodic ingestion job.

## 1. Unpack, test, lint

```bash
cd ~/bigtop-cluster-ansible
unzip -o 60_olist_iceberg_release_ansible.zip -d .
python3 -m unittest discover -s tests -p 'test_olist_iceberg_release60.py' -v
make lint
python3 scripts/60-run-iceberg-release.py --mode plan
```

`plan` changes nothing.

## 2. Read-only verification of already published v3

The user completed Step59 before this package. Check the already published release first:

```bash
python3 scripts/60-run-iceberg-release.py --mode verify
cat artifacts/60-iceberg-release-last-attempt.json
```

Expected terminal markers: `STEP60_READ_ONLY_VERIFY_PASS`, `STEP60_PIPELINE_PASS: mode=verify`. This uses local 58/59 proof artifacts and independent Trino queries; it does not submit a Spark job or move any branch.

## 3. Full stage + publish pipeline (explicit opt-in)

If demonstrating or checking the full execution chain against the **same immutable v3**:

```bash
python3 scripts/60-run-iceberg-release.py --mode stage
# Only to explicitly authorize publishing main (will verify existing publication if already done):
python3 scripts/60-run-iceberg-release.py --mode publish --allow-publish
```

Or run full chain at once, with required explicit permission to publish:

```bash
python3 scripts/60-run-iceberg-release.py --mode run --allow-publish
```

This calls Step58 Spark, 58 Trino, 59 Spark Stage, independent stage/main gate (or existing-publication check), Step59 Spark Publish and 59 Trino. **Already published** v3 is revalidated and not rewritten. An immutable FAIL stage proof blocks publication.

If failed, inspect `artifacts/60-iceberg-release-last-attempt.json` and YARN logs; do **not** remove HDFS proof, branch or tables to "retry from scratch".

## 4. Optional systemd manual one-shot

The Ansible playbook **by default does nothing except display the plan**:

```bash
.venv/bin/ansible-playbook -i inventory/hosts.ini \
  playbooks/60-olist-iceberg-release.yml
```

Install only a systemd one-shot service on worker3 (no timer, not automatically started):

```bash
.venv/bin/ansible-playbook -i inventory/hosts.ini \
  playbooks/60-olist-iceberg-release.yml \
  -e olist_iceberg_release60_apply=true

sudo systemctl start bigtop-olist-iceberg-release60.service
sudo systemctl status bigtop-olist-iceberg-release60.service --no-pager -l
sudo journalctl -u bigtop-olist-iceberg-release60.service -n 120 --no-pager
```

Service executes as `bae` by default and uses the existing project `.venv` and inventory. Override `olist_iceberg_release60_runner_user` if the worker3 controller account differs. The controller user needs the same passwordless sudo/become and SSH access used by Steps 58/59; the service intentionally does not set `NoNewPrivileges=true` because that would block Ansible privilege escalation. It does **not** alter the previous `bigtop-olist-pipeline.service`/timer installed in Step48.

## 5. Git hygiene

`artifacts/` holds local runtime evidence, `vendor/` holds downloaded binaries. Neither should be staged by default. The new role, playbook, runner, tests and docs can be committed. Review `git status --short`, `git diff --check` and `make lint` before committing.

## Status meaning

- `STEP60_PLAN_ONLY`: no changes.
- `STEP60_STAGE_GATED_PASS`: exact 58 parity, Trino and 29 quality checks, main still technical one-row seed.
- `STEP60_ALREADY_PUBLISHED_VERIFIED`: main already published; proof and Trino state reverified.
- `STEP60_GOLD_RELEASE_PASS`: Step59 Gold-only fast_forward approved or idempotently revalidated, Trino agrees.
- `STEP60_PIPELINE_FAIL`: fail closed; inspect failed phase and YARN Driver logs.

The source is still approved v3 Parquet materialized as Iceberg at Step58. This is **not** a recurring Raw CSV -> Bronze/Silver/Gold ingest pipeline.
