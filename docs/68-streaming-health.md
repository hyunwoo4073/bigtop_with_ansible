# Step68 - Kafka/Spark committed progress: health, lag and alerts

## Purpose and dependencies

This change is additive and on-prem/private-network oriented. It leaves the Step65 streaming application, HDFS checkpoint, Kafka offsets, Step67 timer/collector configuration, existing Prometheus scrape configuration and existing alerts unchanged. It deploys a distinct rules file and Grafana dashboard to worker3.

* Kafka end offset (worker3 Node Exporter): `bigtop_olist_step67_kafka_end_offset{topic,partition}`
* Spark committed offset (master Node Exporter): `bigtop_olist_step67_spark_committed_offset{topic,partition}`
* Collector freshness/success: `bigtop_olist_step67_last_run_timestamp_seconds{source}`, `bigtop_olist_step67_collector_success{source}`
* Spark last committed batch age: `bigtop_olist_step67_spark_last_commit_timestamp_seconds`

## Safe execution

From the repository root on worker3:

```bash
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/68-streaming-health.yml
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/68-streaming-health.yml -e olist_kafka68_mode=preflight
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/68-streaming-health.yml -e olist_kafka68_mode=apply
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/68-streaming-health.yml -e olist_kafka68_mode=verify
```

Default is `plan`. `preflight` and `verify` do not change Prometheus, Grafana or Spark state. `apply` renders and validates rules, installs only dedicated Step68 artifacts, and reloads Prometheus only when the rules file changes. Grafana provisioning should pick up the JSON; verify the actual UI separately. The `preflight` refuses changes if the existing `rule_files` patterns do not match the target rule filename; it never silently patches `prometheus.yml`.

## Queries

```promql
bigtop_olist_step68_offset_lag
bigtop_olist_step68_collector_age_seconds
bigtop_olist_step68_spark_commit_age_seconds
bigtop_olist_step67_collector_success
```

Cross-instance matching is on `(topic,partition)`, not `instance`. If either partition-side is missing, subtracting offsets yields *no result* for that partition, not zero; a separate alert catches Kafka partitions lacking a matching committed Spark offset.

The recording rule and collector freshness rely on the Step67 timer sampling at roughly 60s and Prometheus evaluations of 1m. The first recorded sample may not exist immediately after deployment. When testing manually, query the raw expression or rerun `verify` after an evaluation interval.

## Alerting and operational meaning

| Alert | Default trigger | Interpretation |
| --- | --- | --- |
| `BigtopOlistStep68CollectorFailed` | success=0 for 2m | Collector script reported an error |
| `BigtopOlistStep68CollectorMissing` | spark/kafka series absent for 3m | Scrape/series missing; check targets |
| `BigtopOlistStep68CollectorStale` | last run age >240s for 2m | Timer or collector not updating |
| `BigtopOlistStep68StreamingLag` | lag >0 for 5m | Sustained uncommitted input; tune threshold to workload |
| `BigtopOlistStep68SparkPartitionMissing` | Kafka partition unmatched for 5m | Missing checkpoint progress series |
| `BigtopOlistStep68CommittedAhead` | lag <0 for 5m | Topic truncation, scrape freshness or interpretation issue |
| `BigtopOlistStep68StreamingStalled` | Kafka offset grows in 10m, Spark offset does not; for 5m | Possible stalled stream; not automatically proof of failure |

`delta()` is appropriate for offset *gauges* in this observational scenario; it assumes topic identity/partition and Kafka offsets are stable across the window. The sustained lag and stall alerts can have false positives due to lagged samples, bursty input, or checkpoint parsing semantics. Alert rules are advisory only; no automatic Spark/Kafka recovery is performed. Check the Step65 end-offset/commit semantics before treating lag as an exactly-once guarantee.

**Delivery caveat:** a Prometheus alert rule being loaded/evaluated is not proof that Alertmanager notification routing is configured. Inspect existing Alertmanager routes/receivers separately before asserting that any operator will be paged.

## Validation and rollback

```bash
sudo promtool check rules /etc/prometheus/rules/bigtop-olist-step68.yml
sudo promtool check config /etc/prometheus/prometheus.yml
curl -fsSG http://127.0.0.1:9090/api/v1/query --data-urlencode 'query=bigtop_olist_step68_offset_lag'
curl -fsS http://127.0.0.1:9090/api/v1/rules | python3 -m json.tool
```

If Step68 needs to be withdrawn, on worker3 back up the Step68 files, then remove only `/etc/prometheus/rules/bigtop-olist-step68.yml` and `/var/lib/grafana/dashboards/bigtop-olist-step68.json`; validate `promtool check config` and `sudo systemctl reload prometheus`. Do not remove existing Step67 files or edit its timer. In a shared deployment, perform rollback within the established change process.
