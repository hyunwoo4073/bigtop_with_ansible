# Step64 Architecture — Kafka → Spark Structured Streaming → Iceberg CDC MERGE

## Scope

- **Existing platform**: 5-VM Bigtop; Spark 3.5.6 (Java 11) on YARN, Iceberg 1.10.1, Gravitino REST on worker3:9001, Trino 484 on metadata:8080, HDFS on master:9000.
- **Prerequisite**: Step64-A installed/verified Kafka 4.2.2 KRaft nodes worker1:9092, worker2:9092, worker3:9092 (controller at 19093); Step64-B monitoring integrated. Step64-C Kafbat UI is optional. Existing broker endpoint defaults are explicitly pinned to those three IPs.
- **Kafka topic**: `bigtop_olist_step64_cdc_v1` (dedicated to this lab; user explicitly creates it; one partition initially).
- **Spark Kafka packages** (pinned): `spark-sql-kafka-0-10_2.12:3.5.6`, `spark-token-provider-kafka-0-10_2.12:3.5.6`, Kafka clients `3.4.1`, Commons Pool2 `2.11.1`, `jsr305` `3.0.0`. Fetch on internet-connected machine then copy `vendor/kafka64` to worker3 for closed-network operation.
- **Protected**: Step58 Silver, Step59 Published Gold, Step63 CDC target, Step53/54 historical labs. No writes to any of them.
- **Only mutable Iceberg table**: `shared.olist64_kafka.fact_cdc_stream_v1` (separate namespace).
- **Checkpoint**: `hdfs://master:9000/data/olist/iceberg-checkpoints/step64/v1`, deliberately stable; never delete to 'fix' retries.
- **Evidence**: `/data/olist/iceberg-validation/step64/v1/_{SEED,PRODUCE,INGEST}_REPORT.json` (immutable).

## Data flow

```text
Step63 verified CDC Iceberg target (516 rows) --read only--> Step64 isolated Iceberg seed
                                                              (516 rows)
                                                |
                   make Olist-derived 48 synthetic events (16 unique changed keys)
                                                |
                                 Spark Kafka producer (ack=all)
                                                v
                               64-A Kafka KRaft 3 brokers: 1 dedicated topic
                                                |
                Spark 3.5.6 readStream('kafka'), AvailableNow, fixed checkpoint
                                                |
                         foreachBatch / validate / per-key latest sequence
                                                |
                        guarded Iceberg MERGE into Step64 ONLY
                                                |
                               Trino verification (520 rows)
```

## CDC semantics

- 8 updates + 4 delete tombstones + 4 inserts; 48 input messages with duplicates and late sequences; 16 unique keys.
- Ingest target: **520 total rows, 512 live, 8 tombstones**.
- Duplicate messages and old sequences do not alter already applied target state; a same-key/same-sequence conflicting payload is rejected.
- Spark Structured Streaming `foreachBatch` callbacks have at-least-once execution semantics. **The Kafka checkpoint and Iceberg MERGE are not a single atomic transaction**. Idempotent state is implemented by `last_event_seq` and tombstones, not claimed to be broker-to-Iceberg exactly-once delivery.
- `AvailableNow` processes messages currently available then terminates. This is *not* a 24/7 streaming service, Debezium connector, automatic Gold re-aggregation, or automatic WAP Publish.
- The Spark Driver collects a maximum of 1,000 events per micro-batch to check exact conflicts in this **small lab**. Production requires distributed validation and scalable change application.

## Safety

- Internal/on-premises deployment: no Kafka topic auto-creation, no TLS assumption; apply enterprise ACL/Kerberos/TLS when the target network's actual policy requires them.
- Kafka topic must exist before `produce`; use `scripts/64-kafka-lab-topic.sh create` on worker3. It uses the Kafka dedicated Java 17, validates RF=3/minISR=2/ISR=3, and sets bounded 7-day retention.
- Broker-side `advertised.listeners` must be resolvable/reachable by Spark YARN Driver and Executor nodes, not just by Ansible controller.
- Kafka topic retention (7d in this lab) must cover unreconciled offsets; `failOnDataLoss=true` stops rather than silently dropping missing offsets.
- If producer wrote messages but crashed **before proof publication**, do not blindly republish; inspect offsets and proof state. Existing writes may be committed even if the application reports failure.
- No `DROP`, `OVERWRITE`, `expire_snapshots`, or global Spark/Java/YARN setting changes.
- A successful MERGE with failed checkpoint update can be replayed; sequence guard prevents duplicate state mutation. Snapshot history may nonetheless include commits from failed or partial attempts, and manual reconciliation is required for conflicting concurrent writers.

## Source reference

- Spark 3.5.6 Kafka integration: https://spark.apache.org/docs/3.5.6/structured-streaming-kafka-integration.html
- Spark foreachBatch delivery guarantees: https://spark.apache.org/docs/3.5.6/structured-streaming-programming-guide.html
- Iceberg writes: https://iceberg.apache.org/docs/latest/spark-writes/

## UI and lag caveat

Kafka UI 64-C can display the topic contents, but Spark Structured Streaming tracks offsets in its checkpoint; ordinary Kafka committed consumer-group lag may not reflect Spark checkpoint progress. Observe YARN application logs, HDFS checkpoint and Step64 proofs; Broker monitoring remains Step64-B.
