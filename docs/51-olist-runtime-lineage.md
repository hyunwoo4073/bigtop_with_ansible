# Step 51 — Olist Spark Execution-Derived Lineage

## What this does (and does not do)

This is an **Olist-specific Spark application instrumentation** PoC. In an
opted-in **new** immutable ETL snapshot (e.g., `v2`), the Spark driver obtains
`DataFrame.inputFiles()` from each output DataFrame before writing it, records
the *actual completed* Parquet write path and output record count, and commits
`_RUNTIME_LINEAGE.json` in the same HDFS snapshot. A worker3 importer verifies
the execution event against `_ETL_MANIFEST.json`, resolves the already-registered
Step 50 Containers, and **only creates observed missing edges** in OpenMetadata.

- No event is created for an existing `v1` SKIP; no data is deleted or overwritten.
- Only 4 core Olist source CSVs and 7 actual Step 46 output directories are in scope.
- Observed raw CSV -> output Parquet provenance. **Not** column lineage,
  intermediate Spark DataFrame lineage, an OpenLineage-compliant RunEvent,
  or a generic cluster-wide Spark Listener.
- `DataFrame.inputFiles()` is **best-effort**; inputs may be absent for some
  transformations. Missing inputs are reported and never fabricated.
- The Step 50 manually established curated -> fact -> mart edges remain distinct
  from the runtime-observed raw-file -> produced-Parquet edges.
- The runtime trace is only captured after the ETL job actually processes data;
  it cannot be retrofitted to historical snapshots by rerunning an immutable
  skip. Generate a **new** output version rather than rewriting old output.
- No external Spark listener JAR, dependency download, new daemon, or token
  in YARN driver configs. This is a staged path to later OpenLineage listeners.

## Existing infrastructure reused

- worker3 executes Ansible and accesses the root-only Step 42 bot token.
- master runs Spark on YARN Cluster mode and supplies HDFS access.
- OpenMetadata 2.0.6 metadata VM already owns `bigtop_hdfs_olist` containers.
- Step 50 registers the new output version before runtime edges are applied.

## New artifacts

- `roles/data_pipeline_olist_etl/files/olist_etl.py`: opt-in file provenance
  emitted alongside successful immutable ETL manifest.
- `roles/data_pipeline_olist_etl/tasks/main.yml`: forwards opt-in flag to
  `spark-submit` without changing default v1 behavior.
- `playbooks/51-olist-spark-runtime-lineage.yml`: reads HDFS runtime evidence;
  worker3 OpenMetadata idempotent plan/apply.
- `roles/data_pipeline_olist_runtime_lineage/`: importer and opt-in 15min
  reconcile timer (no Spark submissions).
- `tests/test_olist_runtime_lineage.py`: pure-Python validation and idempotency.

## Safety & correctness

1. Verify source and target snapshot versions, known HDFS URI and recorded row
   counts against published manifest before any OpenMetadata write.
2. Verify all 7 immutable outputs are present in the event.
3. Preflight every referenced OpenMetadata Container and upstream graph before
   creating any edge.
4. Avoid exact duplicates on repeated import; existing/manual edges are left
   untouched. No automatic deletion or overwrites.
5. Step 51 never runs Spark; optional systemd timer only replays existing valid
   evidence to maintain OpenMetadata state. Disabled by default.
6. Protected bot token is read on worker3 only and never sent to YARN.

## Caveats

- If a given Spark plan reports no inputs via `DataFrame.inputFiles()`, its
  output will still be recorded with `input_files: []`; the importer logs a
  warning and does **not** invent lineage for that output.
- The generic listener path (OpenLineage SparkListener / SQL query planning,
  Kafka transport etc.) is a separate follow-up, not falsely claimed as done.
- Running Step 50 for `v2` also adds *manual code-based* edges into `v2`;
  that is expected, and the Step 51 runtime edges are additive where missing.
- No guarantee that event data was delivered by a remote listener; event is an
  instrumented per-job report written to the HDFS snapshot, not an external
  full Spark SQL trace.
