# Step 64-C architecture — Internal Kafka Web UI

```
Kafka KRaft (worker1/worker2/worker3 :9092)
                    ↑ (Kafka Admin/Consumer API)
                 Kafbat UI
            metadata VM, Docker
          192.158.2.104:18081

         64-B Kafka collector
           worker3 → Prometheus → Grafana
```

UI responsibility: brokers/topics/partitions/messages/consumer groups/lag inspection.
Grafana responsibility: ongoing metric series, alerts and retention.
OpenMetadata responsibility: messaging service and topic catalog.

**Security:** `KAFKA_CLUSTERS_0_READONLY=true` and `DYNAMIC_CONFIG_ENABLED=false`. No auth supplied. Bound to VM internal interface. Kafka broker ACLs must still be configured before treating UI as secured production infrastructure.

**Resource:** v1.5.0 image, 1024 MiB Docker limit and 1 vCPU. New JVM only on `metadata`; no Java alternatives or Kafka service changes. Use `docker stats bigtop-kafbat-ui64c` to assess actual RSS and OOM kills; `docker inspect` to check restart status.
