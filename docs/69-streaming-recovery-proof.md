# Step69: Streaming recovery evidence — read-only foundation

## Goal and scope

Compare **before/after** Kafka end offsets, Spark *committed* checkpoint offsets, Spark batch progress and HDFS checkpoint (`offsets/`, `commits/`) file listings. Reuse Step65/66/67/68 instead of creating a new streamer, another checkpoint, or another Kafka consumer. This stage **does not itself stop or restart Spark, inject faults, produce events, change the topic, alter HDFS or Iceberg, or validate Iceberg row equality**. A `COMPARE_PASS` means only the recorded offset/checkpoint observations meet the checks; it must not be reported as exactly-once/replay proof.

Known context:

* Step65 stream: `/opt/bigtop-iceberg/step65/olist_kafka65.py` (master).
* Topic: `bigtop_olist_step65_cdc_v1`.
* Checkpoint: `hdfs://master:9000/data/olist/iceberg-checkpoints/step65/v1`.
* Step67: Kafka end offset on worker3; Spark committed offset and batch id on master; Node Exporter → Prometheus on worker3:9090.
* Step68: lag and health rule verification previously completed.
* Step65 operational playbook: `playbooks/65-kafka-streaming-operations.yml` has `start/status/produce/verify/stop/replay`; exact risk and command behavior must be inspected in the working repo before any state-changing exercise.

## Safe commands (worker3 repo root)

```bash
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/69-streaming-recovery-proof.yml --syntax-check
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/69-streaming-recovery-proof.yml
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/69-streaming-recovery-proof.yml -e olist_kafka69_mode=preflight
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/69-streaming-recovery-proof.yml -e olist_kafka69_mode=baseline
```

Baseline writes only local JSON evidence on worker3 (`artifacts/step69/`, not Git). It lists HDFS through `delegate_to: master` as `hadoop`, without modifying HDFS.

After an **independently authorized** controlled test (or simply a second observation with no new input):

```bash
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/69-streaming-recovery-proof.yml -e olist_kafka69_mode=after
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/69-streaming-recovery-proof.yml -e olist_kafka69_mode=compare
```

If proving the newly produced messages were observed and fully committed, run comparison with:

```bash
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/69-streaming-recovery-proof.yml \
  -e olist_kafka69_mode=compare \
  -e olist_kafka69_require_input_advance=true \
  -e olist_kafka69_require_drained=true
```

This strict comparison only establishes offset progression and catch-up at captured observation instants. It does NOT prove application restart occurred or Iceberg sink idempotency. Production replay scenarios need a separate controlled fault/stop/restart plan with ownership, stable application IDs, checkpoint retention checks, and **Trino/Iceberg before-after row or key-version parity**.

## Evidence output

* `artifacts/step69/baseline-progress.json`
* `artifacts/step69/baseline-checkpoint.json`
* `artifacts/step69/after-progress.json`
* `artifacts/step69/after-checkpoint.json`

Artifacts are deliberately **not overwritten** during `capture`. For a new run, use a new evidence directory:

```bash
-e olist_kafka69_evidence_dir=$HOME/bigtop-cluster-ansible/artifacts/step69/run02
```

Do not commit artifacts or sensitive operational checkpoint listings. Put `artifacts/` under `.gitignore` if not already present.

## Semantics and cautions

1. Checkpoint paths are treated as read-only. Never delete or copy back `/offsets`, `/commits`, or state as a way of forcing replay.
2. Offset observations from different Node Exporter targets are not atomic and can be up to one collection/scrape interval apart. If Kafka grows during capture, temporary lag is expected; collect again after convergence.
3. No input advance means the comparison checks only stability, not replay.
4. `foreachBatch` has at-least-once output behavior by default; actual sink exactly-once claims require replay-safe/transactional writes or demonstrable idempotency. See Apache Spark Structured Streaming documentation.
5. This collector does not validate Kafka retention start offsets or Iceberg sink state; those remain explicit acceptance criteria for a controlled Step69 drill and Step70 end-to-end CDC consistency.
6. Ansible `changed` on baseline/after reflects **local evidence file creation**, not platform mutation.
7. On partial capture failure, choose a new evidence directory rather than reusing partially recorded files.

## Next controlled drill (do not auto-run)

* Capture baseline; record **exact** Step65 YARN application ID, Trino target table and current Iceberg snapshot/row-key parity.
* Check Step65 code and role's `stop/replay` procedures, and confirm a safe isolated test with a separate application/checkpoint/table if possible; never run duplicate consumers pointing to the same checkpoint.
* Inject a small, uniquely identifiable test event with known expected outcome using the existing Step65 producer only in a designated test namespace.
* Under an explicitly approved controlled failure, observe YARN/Spark stop/restart and catch-up. Do not destroy Kafka brokers or checkpoint files.
* Capture `after`; strict compare, plus check Trino/Iceberg data and Iceberg snapshots. Only then label full recovery test PASS.

## Files added

* `playbooks/69-streaming-recovery-proof.yml`
* `roles/olist_kafka69/` (Ansible read-only wrapper, HDFS file listings, Prometheus observation script)
* `docs/69-streaming-recovery-proof.md`

Source-only installation: no new systemd units, no Prometheus reload, no Grafana change. Default mode is `plan`.
