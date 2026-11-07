# Kafka KRaft — Step 64A architecture

```text
               Spark master (Step64 Kafka connector)
                         |
             Broker endpoints 9092
                         |
              +----------+-----------+
              |          |           |
         worker1      worker2      worker3
         ID=1         ID=2         ID=3
         9092/9093    9092/9093    9092/9093
         DataNode     DataNode     REST/PG/Grafana
         NodeManager  NodeManager  Prometheus
              |          |           |
              +-- controller quorum (static voters, 2/3 majority) --+
```

- Kafka 4.2.2 / JVM17, no ZooKeeper. Combined broker/controller is for this **resource-constrained lab only**; for production, separate 3 controllers and brokers and add capacity.
- Brokers store topic data in local `/var/lib/bigtop-kafka-kraft64/data`, metadata in `/var/lib/bigtop-kafka-kraft64/metadata`. **Kafka does not store its logs in HDFS.**
- Kafka 9092 is client/broker traffic, controller 9093 is cluster internal. `advertised.listeners` must be reachable from Spark master, YARN Driver+Executors and broker peers.
- Topic replication 3, `min.insync.replicas=2`, producer `acks=all` gives durability for healthy ISR but not application-level end-to-end exactly-once; Step64 uses checkpoint plus idempotent Iceberg MERGE.
- `data/` and `metadata/` markers plus `cluster.id` must be kept intact during maintenance. Do not regenerate cluster ID after initial format, and do not format on every restart.
- 512 MiB JVM heap and 1.5 GiB systemd cap are **small lab values**. Measure off-heap and OS page cache, and do not place production traffic alongside YARN with these budgets.

Official guidance: https://kafka.apache.org/42/operations/kraft/ and https://kafka.apache.org/42/getting-started/quickstart/
