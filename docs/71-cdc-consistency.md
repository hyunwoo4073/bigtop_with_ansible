# Step71 — Isolated CDC Consistency & Replay Proof

## Scope

A one-partition test topic and isolated Iceberg table validate I/U/D, deduplication,
late arrival handling and replay convergence without touching Step64/65 or Gold.

| Resource | Step71-only identifier |
| --- | --- |
| Topic | `bigtop_olist_step71_cdc_v1` (1 partition, RF=3) |
| Target | `shared.olist71_cdc.fact_cdc_consistency_v1` |
| Checkpoint | `hdfs://master:9000/data/olist/iceberg-checkpoints/step71/v1` |
| Baseline proof | `/data/olist/iceberg-validation/step71/v1/_BASELINE_SNAPSHOT.json` |
| Job | `olist-step71-cdc-consistency`, YARN cluster mode, availableNow trigger |

**Does not** stop or replace the Step65 streaming application; **does not**
prove crash-after-Iceberg-commit/before-checkpoint exactly-once yet.

## Expected events

Exactly 9 JSON events are sent in the first publication, and the same 9 bytes are
sent again in replay. Each includes `run_id`, `order_id`, `order_item_id`,
`event_seq`, `op`, `price`, `freight_value`, `item_total`.

- Order A: insert seq 1, update seq 3, late update seq 2, duplicate seq 3 → live seq 3.
- Order B: insert seq 1, delete seq 2, duplicate seq 1 → tombstone seq 2.
- Order C: insert seq 2, late update seq 1 → live seq 2.
- Final target: **3 rows, 2 live, 1 tombstone**; after replay **same Iceberg snapshot**.

## Commands, in order

```bash
cd ~/bigtop-cluster-ansible
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/71-cdc-consistency.yml --syntax-check
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/71-cdc-consistency.yml -e olist_kafka71_mode=preflight
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/71-cdc-consistency.yml -e olist_kafka71_mode=apply
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/71-cdc-consistency.yml -e olist_kafka71_mode=publish
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/71-cdc-consistency.yml -e olist_kafka71_mode=run
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/71-cdc-consistency.yml -e olist_kafka71_mode=verify
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/71-cdc-consistency.yml -e olist_kafka71_mode=baseline
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/71-cdc-consistency.yml -e olist_kafka71_mode=replay
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/71-cdc-consistency.yml -e olist_kafka71_mode=run
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/71-cdc-consistency.yml -e olist_kafka71_mode=verify_replay
```

**Run one command at a time and inspect failures before continuing.** `apply`,
`publish`, `run`, `baseline`, and `replay` write exclusively Step71 resources.

`publish` refuses unless Kafka end offset is 0; `replay` refuses unless it is 9
and a baseline proof exists. `verify_replay` requires end=18, checkpoint commit
covering that offset, exact final rows, and the unchanged Iceberg snapshot.

## Safety / known limits

- Never delete Kafka topics, HDFS checkpoint or Iceberg table in this playbook.
- No changes to Step65/Step67/Step68/Step69 or existing dashboards.
- Requires installed Spark 3.5.6 Kafka dependency JARs and Iceberg runtime from
  previous Steps 64–65; does not download or upgrade packages.
- Verifies **idempotent final state and no new Iceberg snapshot on a replay batch**,
  not true sink transaction-level exactly-once under arbitrary crashes.
- In YARN cluster mode driver logs appear in YARN aggregated logs; Spark-submit
  returns after each one-shot job. If it fails, inspect the job's YARN diagnostics.
- Do not run test publishes in an unrelated topic; only Step71 topic is allowed.
- The baseline proof is immutable: repeat baseline creation is refused.
- `--syntax-check`, role unit tests, or successful package creation do **not**
  prove that runtime Spark/YARN/Kafka/REST connectivity is healthy. Run preflight.

## Useful status

```bash
# worker3: test-only topic
/opt/bigtop-kafka-kraft64/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server 192.158.2.103:9092 --topic bigtop_olist_step71_cdc_v1

# master: test-only checkpoint
sudo -u hadoop hdfs dfs -ls /data/olist/iceberg-checkpoints/step71/v1/commits
```
