# Step65 Architecture — persistent Kafka -> Spark Structured Streaming -> isolated Iceberg

## Purpose and boundaries

- Previous Step64 was `AvailableNow` (finite). Step65 is a **manually controlled continuous YARN cluster-mode** streaming application. It has a 10-second processing trigger, not a schedule that submits one job every minute.
- Protected, read-only source: `shared.olist64_kafka.fact_cdc_stream_v1` (verified 520 rows; main snapshot and row digest checked against immutable Step64 proof).
- Step65-only write table: `shared.olist65_stream.fact_cdc_running_v1`. Created once from Step64 post-image. Step59 Gold, Step63 and Step64 tables never mutated.
- Step65-only topic: `bigtop_olist_step65_cdc_v1` (1 partition, RF3, minISR2, ISR3; 7-day retention, explicit create).
- Step65-only HDFS streaming checkpoint: `hdfs://master:9000/data/olist/iceberg-checkpoints/step65/v1`. Never delete or rename to solve a retry.
- Spark 3.5.6 Python YARN cluster app `olist-iceberg-kafka65-continuous` (768m driver and 768m executor, one executor). Uses previously verified Spark Kafka offline JARs from `vendor/kafka64` and Iceberg 1.10.1 REST catalog (`worker3:9001`).
- Strict YARN lifecycle controller on `master` executes `start`, `status`, `stop`; it checks **exact application name and user**, refuses multiple active matches, and uses a local exclusive lock. `stop` kills only its exact YARN application ID; it doesn't remove Kafka offsets, data or HDFS checkpoints.
- 64-A KRaft brokers remain worker1/2/3 on `:9092`. Brokers are never stopped or reconfigured by Step65. 64-B, 64-C, Trino, Grafana and metadata containers remain unchanged.

## Data path

```text
Step64 verified CDC Iceberg (520 rows) -- read only --> Step65 Iceberg seed (520 rows)
                                                          ^
                                                          | MERGE by (order_id, order_item_id)
                                                          | if event_seq > last_event_seq
Kafka Step65 topic 1 partition -> Spark readStream -> foreachBatch -> Iceberg
       12 synthetic messages      10s trigger       HDFS checkpoint (commits+offsets)
           (4 keys, duplicates and stale sequences)

Expected Step65 postimage: 521 total, 512 live, 9 tombstones.
```

- Step65 synthetic test events: two updates, one delete (soft-delete tombstone), one insert; 4 logical keys and 3 events per key including duplicates and stale `event_seq=350` vs current `400`.
- No external source CDC connector and no business Gold publish in this step.
- Per-batch safety cap is 500 input messages, implemented with Kafka `maxOffsetsPerTrigger=500`, driver-side 501-row guard, 1 partition. It is deliberately **small-lab only**; larger workloads need distributed MERGE design and progress monitoring.
- `foreachBatch` may be replayed if checkpoint commit fails after Iceberg commit. The state-change MERGE is guarded by monotonic `last_event_seq`. This supplies **idempotent final target state**, NOT a distributed Kafka+Iceberg exactly-once transaction.
- First successful `verify` writes immutable proof with the Iceberg `main_snapshot`. On subsequent verifies, the original snapshot must remain unchanged after replays.
- `verify` reads the latest Kafka end offset through the Spark Kafka batch source, finds the **highest committed** Spark batch under `checkpoint/commits`, reads the corresponding `checkpoint/offsets/N`, and requires the committed Kafka offset to be at least the observed Kafka end offset. This prevents passing a replay test before the stream has actually consumed the new duplicate events.
- A normal Kafka consumer group lag display may not reflect Spark Structured Streaming's progress because Spark keeps source offsets in its checkpoint. Consult the Spark/YARN progress logs and the checkpoint verifier, not only Kafbat group lag.

## Failure-recovery behavior

1. `stop`: controller kills the **single exact** YARN application ID and waits for it to leave active states. Checkpoint remains intact; it is not a graceful application-level `query.stop()`.
2. `start` again: same app name, topic, table and checkpoint; Spark resumes from committed offsets.
3. `replay`: deterministic payload re-published to the **same dedicated Step65 topic**, no Step64/Gold mutation.
4. `verify`: must show Kafka checkpoint caught up AND unchanged Iceberg postimage/snapshot.

`start` does **not** install a self-healing systemd service or auto-restart watchdog; if YARN AM retries are exhausted and app is terminal, the operator manually reruns `start`. Prevents accidental restart storms during lab validation. An optional supervised restart/monitoring can be Step66.

### Safety exceptions

- Running `apply` when the target table exists **without** the immutable seed proof fails closed; do not drop the table. Reconcile the existing snapshot/rows/proof first.
- If Spark published Kafka events and failed before the immutable producer proof was written, **do not blindly rerun produce**. Inspect Kafka offsets and evidence.
- 7-day topic retention is not infinite; a stream offline longer than retention can have irrecoverable gaps (`failOnDataLoss=true`).
- Concurrent Spark streams writing the same Iceberg table are unsupported. The controller prevents duplicate Step65 app submissions through itself, but cannot guard arbitrary manually submitted jobs.
- If a YARN ResourceManager changes application state during recovery, manually inspect the YARN UI and logs. Do not delete checkpoint or reformat Kafka.
- Internal-only HTTP and Kafka listeners match the pre-existing lab. Do not expose unauthenticated endpoints to an untrusted network.
