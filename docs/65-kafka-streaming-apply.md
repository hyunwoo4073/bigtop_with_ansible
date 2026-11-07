# Step65 runbook — persistent Spark CDC and checkpoint restart

**Prerequisites:** Step64 Spark/Kafka/Iceberg/Trino verification `PASS` (Step64 immutable proof and target unchanged); Step64-A Kafka brokers installed; Step64 Kafka connector vendor files already downloaded on worker3. Step65 starts an extra long-running Spark application on two YARN workers, so check RAM/YARN `batch` queue. No Kafka broker restart, Step64 replay, or Gold modifications.

## 1. worker3 — extract and run local tests

```bash
cd ~/bigtop-cluster-ansible
unzip -o 65_kafka_streaming_operations_ansible.zip -d .
python3 -m unittest discover -s tests -p 'test_olist_kafka65.py' -v
make lint
```

## 2. Preflight and apply isolated Iceberg seed

```bash
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/65-kafka-streaming-operations.yml
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/65-kafka-streaming-operations.yml -e olist_kafka65_mode=preflight
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/65-kafka-streaming-operations.yml -e olist_kafka65_mode=apply
```

`apply` copies pinned existing JARs and scripts onto `master`, sets up the single-app lifecycle lock, seeds `shared.olist65_stream.fact_cdc_running_v1` from verified Step64 table to 520 rows. **It does not start streaming.** If it fails after creating table but before HDFS seed evidence, stop for reconciliation; do not drop.

## 3. Create the **Step65-only** Kafka topic

```bash
bash scripts/65-kafka-topic.sh create
bash scripts/65-kafka-topic.sh describe
```

Expected `STEP65_TOPIC_READY ... RF=3 minISR=2 ISR=3` with topic `bigtop_olist_step65_cdc_v1`.

## 4. Launch continuous YARN cluster app, verify status

```bash
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/65-kafka-streaming-operations.yml -e olist_kafka65_mode=start
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/65-kafka-streaming-operations.yml -e olist_kafka65_mode=status
```

Expected `STEP65_YARN_START_PASS: application_...` and `STEP65_YARN_RUNNING: application_... RUNNING` (can briefly be ACCEPTED). The app stays running in YARN cluster mode after the `spark-submit` client exits. The app is **not** automatically restarted after its YARN attempts are exhausted.

## 5. Kafka production and streaming verification

```bash
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/65-kafka-streaming-operations.yml -e olist_kafka65_mode=produce
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/65-kafka-streaming-operations.yml -e olist_kafka65_mode=verify
```

The producer sends **12 events (4 PKs)** once, or skips republishing if immutable evidence exists. The first `verify` might report a not-yet-caught-up offset immediately after the produce job. Check YARN progress and run `verify` again after the stream micro-batch commits. Do not republish to fix lag.

Expected YARN driver log output:

```text
STEP65_STREAM_READY: checkpoint=hdfs://master:9000/data/olist/iceberg-checkpoints/step65/v1
STEP65_MICROBATCH_MERGED: batch=... pending=...
```

Expected Step65 verification in YARN driver output:

```text
STEP65_KAFKA_CHECKPOINT_PASS: committed=... kafka_end=...
STEP65_ICEBERG_VERIFY_PASS: rows=521 live=512 tombstones=9 snapshot=...
```

Expected Ansible output: `STEP65_VERIFY_ANSIBLE_PASS`. The `verify` Playbook also exports immutable Step65 HDFS proof copies into controller-local `artifacts/65-kafka-cdc-{seed,produce,apply}.json`.

Perform Trino 484 independent verification from worker3:

```bash
python3 scripts/65-verify-trino.py --url http://192.158.2.104:8080
```

Expected `STEP65_TRINO_KAFKA_CDC_ROWS_PASS: total=521 live=512 tombstones=9` and `STEP65_SPARK_KAFKA_ICEBERG_TRINO_PASS`. Repeat the Trino verifier after the replay verification as well.

## 6. Controlled stop/restart from the same checkpoint

```bash
# Stop ONLY the exact Step65 YARN application; keep HDFS checkpoint and Iceberg table.
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/65-kafka-streaming-operations.yml -e olist_kafka65_mode=stop
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/65-kafka-streaming-operations.yml -e olist_kafka65_mode=status

# Resume same checkpoint; no topic/table reset.
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/65-kafka-streaming-operations.yml -e olist_kafka65_mode=start
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/65-kafka-streaming-operations.yml -e olist_kafka65_mode=status
```

## 7. Idempotency + offset catch-up after replay

```bash
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/65-kafka-streaming-operations.yml -e olist_kafka65_mode=replay
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/65-kafka-streaming-operations.yml -e olist_kafka65_mode=verify
```

`replay` deliberately creates new Kafka **offsets** with old logical event sequences. After the stream commits them, `verify` requires Kafka end offset <= committed checkpoint offset AND no Iceberg snapshot change after initial verified application. If `verify` runs too soon, it fails safely; inspect the stream and retry `verify` only.

## 8. HDFS evidence, YARN, and Trino optional cross-check

```bash
ssh master 'sudo -u hadoop hdfs dfs -cat /data/olist/iceberg-validation/step65/v1/_APPLY_REPORT.json'
ssh master 'yarn application -list -appStates RUNNING,ACCEPTED'
# YARN app logs: yarn logs -applicationId application_<...> | grep STEP65_
# Trino 484 SQL example in CLI or UI:
# SELECT count(*), count_if(NOT is_deleted), count_if(is_deleted)
# FROM iceberg_rest.olist65_stream.fact_cdc_running_v1;
```

Expected Trino `(521, 512, 9)`. Trino query uses the existing catalog mapping; if catalog name differs, query the existing Step64 Trino catalog first.

To leave the lab without a long-running YARN workload, run `mode=stop` at the end. To keep true 24/7 operation, leave the app running; note automatic crash recovery and automated alerts are **not** configured in Step65.

## Troubleshooting

- `RUNNING` but no data: examine YARN driver logs, Kafka advertised listeners and Spark checkpoint source offsets.
- `ACCEPTED` indefinitely: inspect YARN queue capacity, driver/executor memory and ResourceManager scheduler.
- Failed `verify` after produce: stream may have not yet committed latest batch; run `status` and inspect YARN logs, retry only `verify`.
- Missing Kafka connector: recheck `vendor/kafka64` verification and staged `/opt/bigtop-iceberg/kafka64/` JARs.
- YARN duplicate apps: lifecycle controller fails closed. Resolve manually with exact YARN app IDs after identifying ownership.
- A failed/aborted stream may still have committed Iceberg changes; do not reset HDFS checkpoint or overwrite target.
- Kafka retention is 7 days. Replaying after retention gaps is not guaranteed.
- End of workday: when README/PROJECT_GUIDE update requested, stage ALL of that day's Ansible/scripts/tests/docs with a single Git commit, excluding `vendor/`, secrets, downloaded archives, and runtime artifacts.
