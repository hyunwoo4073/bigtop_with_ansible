# Step 60 — Olist v3 Iceberg Gold release orchestration

## Scope and data lineage

Step 60 **orchestrates previously installed components**; it does not install another database/SQL engine, copy new Olist CSVs, or change a table schema. The entire release remains pinned to approved, immutable **v3**. Existing 46/47/52 evidence is checked by the Step 58/59 Spark jobs.

```text
worker3 (Ansible controller)
  60-run-iceberg-release.py --mode run --allow-publish
          |
          |-- (1) 58 Spark verification/materialization on YARN (master submits)
          |          |-- HDFS approved /data/olist/warehouse/v3
          |          |-- exact bidirectional exceptAll multiset match
          |          '-- shared.olist58_v3: 5 silver, 1 quarantine, 1 gold
          |
          |-- (2) Trino 484 verifies all 7 Step58 tables, Gold aggregate
          |
          |-- (3) 59 Stage on YARN: verify approved source refs, 29 DQ rules,
          |          audit_v3 on isolated Gold shared.olist59_v3
          |          '-- HDFS _STAGE_REPORT.json immutable proof
          |
          |-- (4) Trino: check Gold `main` before publication
          |          |-- 1 row: requires Trino stage protected-main verification
          |          '-- expected count: only if existing publication proof validates
          |
          |-- (5) Explicitly authorized Step59 Publish on YARN
          |          |-- recheck 29 DQ and source + audit + main snapshot refs
          |          '-- fast_forward only Gold main -> approved audit_v3
          |
          '-- (6) Trino independently validates released Gold count + aggregates
                    |
                    '-- worker3 artifacts/60-iceberg-release-last-attempt.json
```

### Names and boundaries

- Source: `shared.olist58_v3` via Spark / `iceberg_rest.olist58_v3` via Trino, seven tables (read-only after Step 58 materialization).
- Release target: only `shared.olist59_v3.gold_daily_category_sales`, branch `audit_v3`. **No transaction spanning all seven tables**.
- Shared Gravitino 1.3.1 REST service: `http://192.158.2.103:9001/iceberg/`.
- Trino 484: `http://192.158.2.104:8080`.
- Spark on YARN using existing Java 11 / Iceberg 1.10.1. Trino's Java 25 remains isolated.
- HDFS immutable proof: `/data/olist/iceberg-validation/step59/v3/_STAGE_REPORT.json` and `_PUBLISH_REPORT.json`.
- Local controller mirrors: `artifacts/58-iceberg-v3-proof.json`, `59-olist-wap-stage.json`, `59-olist-wap-publish.json`.

### Failure behavior

- Any subprocess error, missing PASS marker, 29-DQ FAIL, invalid proof digest, unexpected row count or Trino query failure **stops before starting the next step**.
- The Step59 Spark publish job **rechecks immutable HDFS proofs and Iceberg source/main/audit refs**, even if local artifacts are present. The local 60 JSON receipt is operational state, **not** permission to publish.
- `flock` rejects concurrent Step60 runner invocations on the same controller. A direct Step59 playbook outside this runner does not honor this lock; do not launch concurrent release operations manually.
- In-progress failures can leave an Iceberg audit branch or a published Gold snapshot. Never delete either blindly. Step59's existing recovery checks handle reruns of identical approved v3 data.
- For a previously published v3 release, `stage` reports an existing publication and verifies the Trino published state; it **does not** assert the one-row seed still exists. `run` repeats validations and the idempotent Step59 publish check.
- `verify` performs read-only local-proof checks and Trino queries. It does **not** independently re-read HDFS proof or Iceberg refs; `run`/`publish` invokes Step59 for stronger HDFS snapshot checks.

### Why no timer yet?

Step 58 and Step 59 use immutable approved v3 source data and a fixed release namespace/branch. Re-running hourly/daily **would not process new data**. Therefore Step 60 provides a manual one-shot systemd service, with **no timer** or auto-start. A production-style recurring workflow needs version parameterization, a new immutable dataset + quality report per run, distinct release references, retention and rollback policy, and appropriate concurrency control before scheduling.

## Not yet implemented

- Raw CSV -> Iceberg direct ETL for a *new* warehouse version.
- Atomic multi-table transaction or snapshot-consistent multi-table Trino reads.
- Scheduler/timer that ingests new data, central release registry, alerting, authorization/TLS for users, or production HA.

These are separate follow-up tasks, rather than claims about this release.
