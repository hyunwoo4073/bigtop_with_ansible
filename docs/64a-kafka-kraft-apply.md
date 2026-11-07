# Step 64A — Kafka 4.2.2 KRaft 3-node deployment (before Step 64 CDC)

## Scope and design

- On-prem lab; 3 combined broker/controller processes on **worker1 (192.158.2.101), worker2 (192.158.2.102), worker3 (192.158.2.103)**. No ZooKeeper.
- **Kafka 4.2.2**, port **9092** broker / **9093** controller, separate **Temurin Java 17**. Leave system Java 11, Spark, Hadoop, Trino, OpenMetadata unchanged.
- Static 3-voter KRaft quorum, RF=3, min ISR=2, auto.create.topics.enable=false, retention 24h, 512 MiB broker heap, systemd MemoryMax=1.5GiB each. This is a **lab-only combined-mode topology**, not production controller isolation.
- No destructive reset: data dirs are formatted **exactly once**, shared cluster ID is persisted, existing `meta.properties` are checked and never reformatted. Setup fails if ports in use, RAM/disk too low or data directory partially formatted.
- Before installing: check that **all three nodes can communicate bidirectionally over TCP 9092, 9093**, and clients (master for Spark, metadata for Trino) can reach 9092. Network restrictions need separate firewall approval; this package does not change firewall rules.

## 1. Apply and validate repository files — Ansible controller worker3

```bash
cd ~/bigtop-cluster-ansible
unzip -o 64a_kafka_kraft_cluster_ansible.zip -d .
python3 -m unittest discover -s tests -p 'test_kafka_kraft64.py' -v
make lint
```

## 2. Download into offline vendor (on worker3 or Internet PC)

```bash
python3 scripts/64a-fetch-kafka-broker.py
python3 scripts/64a-verify-kafka-assets.py \
  vendor/kafka-broker64 \
  vendor/iceberg-rest/temurin17-jdk-linux-x64.tar.gz \
  vendor/iceberg-rest/temurin17-jdk-linux-x64.tar.gz.sha256
```

**Kafka 4.2.2 binary ~130 MB** downloaded from Apache `downloads.apache.org`, SHA-512 checked against Apache sidecar. Java17 already verified and downloaded in Step56; no new Java download required. For closed networks copy these verified artifact directories from an Internet-connected PC without adding to Git.

## 3. Preflight & deployment

```bash
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/64a-kafka-kraft-cluster.yml
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/64a-kafka-kraft-cluster.yml -e kafka_kraft64_mode=preflight
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/64a-kafka-kraft-cluster.yml -e kafka_kraft64_mode=install
```

`plan` and `preflight` are read-only. `install` modifies only new Kafka paths and services, and persists the shared KRaft cluster ID at `/etc/bigtop-kafka-kraft64/cluster.id` **on worker3**. **Back it up together with broker storage**. Missing cluster ID after existing storage is formatted is not an invitation to start a fresh cluster; restore original ID before using installation again. Do not delete Kafka data dirs to repair a start failure.

## 4. Read-only cluster verification

```bash
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/64a-kafka-kraft-cluster.yml -e kafka_kraft64_mode=verify
ssh worker1 'sudo systemctl status bigtop-kafka-kraft64 --no-pager'
ssh worker2 'sudo systemctl status bigtop-kafka-kraft64 --no-pager'
sudo systemctl status bigtop-kafka-kraft64 --no-pager
```

## 5. Dedicated topic produce/consume smoke (worker3)

```bash
bash scripts/64a-smoke-kafka.sh
```

Expected: `STEP64A_KAFKA_PRODUCE_CONSUME_PASS`. The smoke test writes only `bigtop_kafka64_smoke_v1`, no existing topic/group offsets are deleted. Expect 3 replicas and ISR 3 in steady state.

## 6. Continue existing Step 64 CDC

```bash
BOOTSTRAP=192.158.2.101:9092,192.158.2.102:9092,192.158.2.103:9092
KAFKA_TOPICS_BIN=/opt/bigtop-kafka-kraft64/kafka/bin/kafka-topics.sh \
  bash scripts/64-kafka-lab-topic.sh create "$BOOTSTRAP" 3
```

Then continue Step 64 `preflight -> seed -> produce -> ingest -> verify`. Add `-e "olist_iceberg_kafka64_bootstrap=$BOOTSTRAP"` for preflight, produce, ingest, replay. **Step64 Spark Kafka connector JARs are a separate dependency**; fetch/verify using Step64 scripts before produce/ingest.

## Troubleshooting

- `ss -lntp | grep -E ':(9092|9093)'` — port conflicts.
- `sudo journalctl -u bigtop-kafka-kraft64 -n 150 --no-pager` — startup logs.
- `systemctl show bigtop-kafka-kraft64 -p MemoryCurrent -p MemoryMax` — JVM and total process memory. Systemd cap is not heap size.
- `kafka-metadata-quorum.sh --bootstrap-server '...' describe --status` — leader/voters. Three controllers can tolerate one failure **only if quorum and remaining ISRs are healthy**.
- `kafka-topics.sh --bootstrap-server '...' --describe --topic bigtop_kafka64_smoke_v1` — replicas/ISR.
- `master` and `metadata` clients must resolve/connect to all advertised broker IPs. Initial bootstrap success alone does not guarantee metadata broker connectivity.
- Spark/YARN can exhaust worker1/worker2 RAM if Kafka and full YARN allocations overlap; measure and reduce YARN budgets or increase VM memory before increasing load.
- The sandbox uses PLAINTEXT and combined roles only within trusted host-only virtual network; enterprise production requires separate controller placement, authenticated access, transport protection when appropriate, and capacity planning.
