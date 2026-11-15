# Step75 — Kafka metadata/ISR/Quorum UI (read-only)

## Scope

Extend the **existing Step70 Kafka Operations page** on worker3; preserve Services registration, Step65 CDC, existing Nginx Basic Auth, Kafbat/Grafana links, and Step70 topic/group lag operations. Kafka brokers/Controller Quorum/Spark/YARN are not restarted, reconfigured or mutated. `apply` builds the existing React UI and restarts only `bigtop-web-console`.

## Data sources

- `kafka-broker-api-versions.sh --bootstrap-server ...` enumerates brokers answering Kafka Admin commands; this is not a liveness probe of every process.
- `kafka-metadata-quorum.sh --bootstrap-server ... describe --status` gives the KRaft metadata leader, voter IDs, high watermark and max follower lag. Voter IDs can be distinct from broker IDs.
- `kafka-topics.sh --bootstrap-server ... --describe` yields partition leaders/replicas/ISR; counts under-replicated partitions and leader-missing partitions. Abnormal details are capped at 200, counts include all parsed entries. Metrics are **on-demand snapshots**; success is not a time-series SLA.
- Step70's existing `groups/{group_name}/lag` provides selected consumer group partition lag; it does not claim all-group aggregated lag.

No topic writes, deletes, offset resets, Kafka configuration changes, credential exposure or shell command interpolation. Metadata CLI command failures render `unknown` and clear errors, **not** healthy zero counts.

## Installation

Copy ZIP to worker3, then run:

```bash
cd ~/bigtop-cluster-ansible
mkdir -p /tmp/step75 && unzip -oq step75_kafka_operations_ui.zip -d /tmp/step75
bash /tmp/step75/install.sh "$PWD"
make validate
bash scripts/run_step75.sh preflight
bash scripts/run_step75.sh apply
bash scripts/run_step75.sh verify
```

The installer checks Step70 base anchors and changes **only** two lines of integration in existing tracked files (`kafka_ops70.py`, `KafkaOperationsView.jsx`), with source backups under `artifacts/step75/source-backups-<timestamp>/`; it refuses divergent new paths or unsupported source variants. It is idempotent. Step75's implementation lives in distinct files and role. API endpoint: `GET /api/ops/kafka/cluster-health`.

After verify PASS, in authenticated Web Console Services → Kafka Operations select **Inspect Kafka cluster**. Validate KRaft leader, voters, ISR warnings, broker metadata, and existing group lag; use Kafbat/Grafana for detailed inspection.

## Failure and rollback

If `verify` fails, inspect `curl -fsS http://127.0.0.1:18090/api/ops/kafka/cluster-health | python3 -m json.tool`; check `errors` and verify CLI compatibility. Do not treat an empty/unknown result as healthy. Don't run `apply` repeatedly while modifying original Step70 files. Keep backups out of Git; if frontend rebuild fails, restore `dist.pre-step75`, restore the original tracked files from the source backup, redeploy existing Step70 router/view, restart only `bigtop-web-console`. No Kafka restart required.

Step75 CI source changes: playbook, role, standalone backend and frontend, two small integration points, tests, and docs. Commit these as one set with other session changes at day's end, not a ZIP or backup artifacts.
