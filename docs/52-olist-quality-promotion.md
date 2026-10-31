# Step 52 — Olist HDFS Quality Gate / Immutable Promotion

## Why

Steps 46/47/50/51 produce immutable Warehouse Parquet, separate quality reports,
OpenMetadata catalog/lineage. **None of them enforces an "approved for consumption"
state**. Step 52 creates an explicit immutable HDFS approval record, but **does not
change any HDFS ACL on Warehouse or enforce authorization on downstream consumers**.
Downstream workflows must explicitly require a valid approval before reading Mart.

## Gate policy

A version such as `v3` can be approved only if all checks are met:

- ETL manifest at `/data/olist/warehouse/v3/_ETL_MANIFEST.json` is present,
  identifies `olist_spark_batch_etl`, Raw `v1`, Warehouse `v3`, and the expected
  inventory of 7 Parquet datasets.
- Every dataset has an HDFS `_SUCCESS` marker.
- Quality reports are enumerated from `/data/olist/quality/v3/runs/*/_QUALITY_REPORT.json`.
  The latest `run_timestamp_seconds` must be a **PASS**, contain all-PASS
  individual checks, and have a consistent summary count.
- The latest PASS report's `etl_manifest_sha256` must match the exact
  **bytes** of the current HDFS ETL manifest (SHA-256).
- The report must be at most `168` hours old by default. This freshness
  relates to **quality verification time**, not fresh source data; Olist Raw
  and Warehouse are fixed immutable snapshots.
- No report, stale report, newer FAIL, checksum mismatch, incomplete
  Parquet, or ambiguous latest timestamps **block** approval.
- Each successful version creates at most one approval record. Existing
  approvals are revalidated against their **original** quality report and
  unchanged ETL digest. Newer PASS reports do not rewrite them.

## Paths & ownership

- Reads ETL: `/data/olist/warehouse/<version>/...`
- Reads quality: `/data/olist/quality/<version>/runs/<run_id>/_QUALITY_REPORT.json`
- Writes **only on APPLY**: `/data/olist/published/<version>/_APPROVAL.json`
- Installs script on `master`:
  `/usr/local/lib/bigtop-olist/promotion/approve_olist.py`
- Runs HDFS operations as existing `hadoop` account, no new service or secret.
- Does not delete/overwrite Raw, ETL, Quality, OpenMetadata, 51 lineage, or
  48/49 monitoring artifacts.

Ansible `--plan` is read-only on HDFS (installs the small local script on master).
`--apply` first performs a read-only preflight, then creates a private
`/data/olist/published` HDFS directory if needed. Actual approval uses HDFS
`-put` without `-f`; existing approval is never overwritten.

`_APPROVAL.json` records approval time, warehouse/Mart HDFS paths, immutable
ETL manifest SHA-256, quality report SHA-256, quality run ID and check count.
This is a provenance/operational release signal rather than a tamper-proof
cryptographic attestation: a privileged HDFS admin could alter the sources or
approval. It does not contain row-level customer data.

## Consumers

Do not infer "approved" merely from `_SUCCESS`, `_ETL_MANIFEST.json` or the
OpenMetadata Lineage graph. Downstream workflows should check that the
matching `_APPROVAL.json` exists, is parseable, points to the target Warehouse
version, and its source evidence has not changed. They then may read the
explicit approved mart path in the record.

An approval is a historical fact as of its `approved_at_utc`; a later quality
FAIL is **not automatically a revocation** of an older approval. If automated
consumer-time deny/revoke is required, implement and enforce a dedicated
release resolver in a subsequent step.

## Tests and known limitations

Run `python3 -m unittest discover -s tests -p 'test_olist_promotion.py' -v`.
The package unit tests run without Hadoop. Test coverage: read-only plan,
first publish, idempotency, latest FAIL, timestamps ties/staleness, manifest
SHA mismatch, HDFS `_SUCCESS` missing, report missing, approval evidence
mutation, version validation, check-count mismatch, report identity mismatch.

No live HDFS/YARN was accessed during package authoring. Run the Ansible
playbook in the actual worker3/master lab to validate Hadoop permissions and
report age. This does **not** run Step 47 for you.
