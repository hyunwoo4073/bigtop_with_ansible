# Step 64B — Kafka KRaft platform integration

This step **does not** run a Kafka source→Spark/Iceberg pipeline. It integrates an already
installed Step 64A KRaft cluster into existing worker3 observability and OpenMetadata.

- worker1 (`192.158.2.101`): Kafka broker+controller ID 1; HDFS DN and YARN NM remain unchanged
- worker2 (`192.158.2.102`): Kafka broker+controller ID 2; HDFS DN and YARN NM remain unchanged
- worker3 (`192.158.2.103`): Kafka broker+controller ID 3; Prometheus, Grafana, PostgreSQL
- metadata (`192.158.2.104`): OpenMetadata 2.0.6 and Trino (not changed by Step 64B)
- Grafana: existing file provider, new dashboard UID `bigtop-kafka-kraft64`
- Prometheus: existing Node Exporter textfile directory on worker3, independent `kafka_platform64b.prom`
- OpenMetadata: one Messaging Service `bigtop_kafka_kraft64` and currently existing non-internal Topics via authenticated API

## What is actually monitored

Worker3 polls Kafka CLI `kafka-metadata-quorum.sh describe --status` and
`kafka-topics.sh --describe`; it checks each broker's TCP port separately.
Collected metrics: CLI collection success, polling freshness, metadata leader presence,
voter count, topic count, partition count, under-replicated partitions and offline partitions,
plus each broker's TCP reachability. TCP checks alone are **not** broker health.

This is a non-invasive baseline and **not** a JMX exporter. JVM heap, per-broker request
rate, bytes in/out, ISR change rate, consumer lag, Kafka Connect, Kafka ACL and automatic
source-to-sink runtime lineage are **not yet collected**. Add JMX exporter and consumer
monitoring later when the CDC processing stream is available; those may require broker restarts.

The independent Step64B observer is a manual systemd oneshot. An optional timer is disabled
by default. A failed CLI poll writes `bigtop_kafka64_collector_success 0`, never a false
positive with stale topic values. Collector freshness should be evaluated with
`time() - bigtop_kafka64_last_attempt_unixtime` and `absent()` checks in alerting.

## OpenMetadata scope

Explicit `--mode metadata` registers a Kafka Messaging Service and live non-internal topics
using the OpenMetadata API. It does **not** run OpenMetadata's Kafka ingestion connector,
parse Kafka messages, infer schemas, or claim Kafka→Spark lineage. Registration leaves
existing service/topic descriptions unchanged. Read the private Bot token only from the
existing root-controlled mode 0600 file. No token in Git or Ansible logs.

## Safety

Step64B makes no `kafka-topics --create/alter/delete`, no consumer-group offset operations,
no broker restart, no `server.properties` changes, no user/password updates, no Spark/Iceberg
writes, no changes to the existing Step49/61 dashboards. Execute `64A verify` first if only
the Kafka preflight passed.
