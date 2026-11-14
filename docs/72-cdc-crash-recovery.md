# Step72 — Crash after Iceberg merge, before Spark checkpoint commit

## Purpose

Step71 verified that replaying nine identical CDC events does not change the final
Iceberg state or snapshot. Step72 tests a **different failure boundary**: the first
Structured Streaming microbatch writes the Iceberg MERGE successfully, then the
`foreachBatch` callback deliberately throws **before Spark writes its microbatch
`commits/0` record**. A new one-shot Spark application resumes **the same Step72
checkpoint** and must recognize the existing `event_seq` state as a no-op.

This tests one explicitly chosen failure boundary, **not** arbitrary crashes,
transactional Kafka-to-Iceberg exactly-once, or all possible retries.

## Isolated assets

| Resource | Exactly scoped ID |
| --- | --- |
| Kafka topic | `bigtop_olist_step72_crash_v1` (1 partition, RF=3) |
| Iceberg table | `shared.olist72_recovery.fact_cdc_crash_v1` |
| Spark checkpoint | `hdfs://master:9000/data/olist/iceberg-checkpoints/step72/v1` |
| After-MERGE fault proof | `/data/olist/iceberg-validation/step72/v1/_AFTER_MERGE_FAULT.json` |
| Recovery proof | `/data/olist/iceberg-validation/step72/v1/_RECOVERY_VERIFIED.json` |

**Forbidden:** editing Step65/Step71 topics, tables or checkpoints; resetting offsets;
deleting any checkpoint; changing Gold Iceberg tables; stopping applications
implicitly. The role only writes Step72 resources.

## VM constraint

This five-VM lab currently supports **one active Spark application at a time**.

Before Step72 `preflight`, stop the Step65 continuous streaming application using
its original playbook; verify that YARN has **no** active Spark application. Step72
rejects active YARN Spark workloads and never kills them automatically.

```bash
cd ~/bigtop-cluster-ansible
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/65-kafka-streaming-operations.yml -e olist_kafka65_mode=stop
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/65-kafka-streaming-operations.yml -e olist_kafka65_mode=status
```

## Execute (each phase is intentionally one-shot)

```bash
cd ~/bigtop-cluster-ansible
unzip -n step72_cdc_crash_recovery_ansible.zip -d .

.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/72-cdc-crash-recovery.yml --syntax-check

for MODE in preflight apply publish fault inspect_fault recover verify; do
  echo "========== Step72 $MODE =========="
  .venv/bin/ansible-playbook -i inventory/hosts.ini \
    playbooks/72-cdc-crash-recovery.yml -e "olist_kafka72_mode=$MODE" || break
done
```

**Do not rerun the complete loop after a failure.** `publish` is guarded to
`offset=0`, and `fault` creates one immutable fault marker. If a phase fails,
read the error and resume *only* after confirming the actual state.

### Expected outcomes

| Phase | Expected evidence |
| --- | --- |
| `preflight` | `STEP72_PREFLIGHT_PASS` |
| `apply` | Dedicated topic/table; `STEP72_APPLY_PASS` |
| `publish` | `STEP72_PUBLISH_PASS: offsets=0->9 messages=9` |
| `fault` | Submitted YARN app **fails deliberately** but Role reports `STEP72_FAULT_PROOF_PASS` |
| `inspect_fault` | `offsets/0` exists; `commits/0` absent; Iceberg snapshot already recorded |
| `recover` | Spark app succeeds via same checkpoint, `STEP72_RECOVER_PASS` |
| `verify` | Checkpoint >= Kafka end=9; exact 3 rows/2 live/1 tombstone; **identical** pre-fault and post-recovery Iceberg Snapshot and data SHA256; `STEP72_VERIFY_PASS` |

The fault marker must exist AND the Spark application must have returned nonzero.
An unrelated job failure (resource starvation, HDFS error, Python syntax error)
without the marker is **not** accepted. Spark/YARN may show `FAILED` for this
intentional negative test; this is expected only for the `fault` phase.

## Resume Step65

When all Step72 applications have finished (including the expected failed app),
restore Step65 with the existing checkpoint:

```bash
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/65-kafka-streaming-operations.yml -e olist_kafka65_mode=start
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/65-kafka-streaming-operations.yml -e olist_kafka65_mode=status
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/65-kafka-streaming-operations.yml -e olist_kafka65_mode=verify
```

The `status` output must eventually show `RUNNING`, not merely `ACCEPTED`.

## Diagnosis

```bash
ssh master 'yarn application -list -appStates SUBMITTED,ACCEPTED,RUNNING'
ssh master 'sudo -u hadoop hdfs dfs -ls /data/olist/iceberg-checkpoints/step72/v1/commits'
ssh master 'sudo -u hadoop hdfs dfs -cat /data/olist/iceberg-validation/step72/v1/_AFTER_MERGE_FAULT.json'
ssh master 'sudo -u hadoop hdfs dfs -cat /data/olist/iceberg-validation/step72/v1/_RECOVERY_VERIFIED.json'
```

The failure injection is **test-only**. Never port it to a production
Structured Streaming application. For more complete fault coverage, add later
scenarios for crashes before the sink commit, container loss, data retention
expiry and competing writers.
