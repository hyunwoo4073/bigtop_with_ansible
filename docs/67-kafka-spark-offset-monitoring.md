# Step67 — Kafka end offset vs Spark committed checkpoint offset

## Purpose & scope

Step65 runs one persistent Spark Structured Streaming job on YARN. Step66 checks app liveness. Step67 adds **read-only progress/backlog observability** with *no additional Spark job*.

| Execution host | Read-only command | Metric file |
| --- | --- | --- |
| worker3 | Kafka 4.2.2 `kafka-get-offsets.sh --time -1` with dedicated Java17 | `/var/lib/prometheus/node-exporter/olist_step67_kafka.prom` |
| master | `hdfs dfs -ls .../commits`, `-cat .../offsets/<latest-committed-batch>`, `-stat %Y .../commits/<batch>` as `hadoop` | `/var/lib/prometheus/node-exporter/olist_step67_spark.prom` |

The Kafka timestamp is not used as event-time latency. The latest completed Spark checkpoint is found **from `commits/`**, not merely the newest `offsets/` file (which might represent an uncommitted in-flight batch). The offset log format is checked for the known Step65 one-topic, one-partition configuration. Failed collectors overwrite their metric file with a failure flag and remove stale offset values.

**Spark's Kafka integration tracks its source offsets in the streaming checkpoint.** These are not standard Kafka consumer group committed offsets; do not expect a Kafbat UI group lag to match this metric. Kafka end offset - Spark last *completed* batch offset is a useful backlog **snapshot**, not a transactional guarantee or wall-clock delivery SLA. Since worker3 and master sample separately, brief transient discrepancies can arise.

**No actual micro-batch execution duration in this step.** Checkpoint commit age (`time() - ...spark_last_commit_timestamp_seconds`) and committed batch ID indicate progress only. Idle streams may retain a very old last commit time without being faulty. For true processing durations, add Spark `StreamingQueryProgress.durationMs` instrumentation later; this requires careful driver-side modification / restart and is excluded here. No paging alerts are automatically enabled.

## Apply (worker3 Ansible controller)

```bash
unzip -o 67_kafka_spark_progress_ansible.zip -d .
python3 -m unittest discover -s tests -p 'test_olist_kafka67.py' -v
make lint
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/67-kafka-spark-progress.yml
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/67-kafka-spark-progress.yml -e olist_kafka67_mode=preflight
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/67-kafka-spark-progress.yml -e olist_kafka67_mode=apply
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/67-kafka-spark-progress.yml -e olist_kafka67_mode=verify
```

`apply` installs two systemd oneshot units, two **disabled by default** timers, and one Grafana dashboard on worker3. It does not touch Step65/66 services, data, or checkpoint. `verify` runs each oneshot once and confirms valid metrics locally.

Inspect:

```bash
ssh worker3 'sudo journalctl -u bigtop-olist-step67-kafka.service -n 50 --no-pager; cat /var/lib/prometheus/node-exporter/olist_step67_kafka.prom'
ssh master 'sudo journalctl -u bigtop-olist-step67-spark.service -n 50 --no-pager; cat /var/lib/prometheus/node-exporter/olist_step67_spark.prom'
```

Dashboard on your Windows host forwarding: `http://127.0.0.1:23000/d/bigtop-olist-step67/`. Its data requires Prometheus to **scrape Node Exporter on both master and worker3** with `--collector.textfile.directory=/var/lib/prometheus/node-exporter` (exact service flags should be confirmed; Step66 and 64B previously use the same directories). No Grafana/Prometheus global configuration is overwritten.

Example PromQL to compare partition-aligned offsets:

```promql
sum(clamp_min(
  sum by (topic, partition) (bigtop_olist_step67_kafka_end_offset)
  - on (topic, partition)
    sum by (topic, partition) (bigtop_olist_step67_spark_committed_offset),
  0
))
```

If a collector fails, the corresponding offset metric is omitted and the above expression produces no comparable sample. **No data is not equivalent to zero lag.** Check `bigtop_olist_step67_collector_success` and freshness `time() - bigtop_olist_step67_last_run_timestamp_seconds`; these must show both sources and reasonably recent runs. When checking lag also inspect Step66 `bigtop_olist_step66_running` to distinguish idle, blocked, and stopped apps.

After successful manual verification, start polling every 60 seconds (explicit opt-in):

```bash
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/67-kafka-spark-progress.yml -e olist_kafka67_mode=enable
```

Disable *only* observation timers (does not stop Spark):

```bash
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/67-kafka-spark-progress.yml -e olist_kafka67_mode=disable
```

## Safety and limits

- The known Step65 Kafka topic is **one partition** and is immutable for this lab. Topology changes fail the collector rather than silently summing different partitions.
- No offset reset, topic creation/deletion, Kafka consumer messages, YARN submit, Iceberg writes, or checkpoint cleanup.
- HDFS client uses existing `hadoop` identity; root only writes local Node Exporter textfile, avoiding new sudo policies.
- Kafka CLI uses dedicated Java17 with 192 MiB maximum heap instead of Java8.
- Because this uses two different collection hosts, successful metrics do not establish a fully atomic snapshot between Kafka and Spark.
- Future work: true `StreamingQueryProgress` duration and throughput, threshold-based warnings tailored to workload, then test stalled consumption scenario.
