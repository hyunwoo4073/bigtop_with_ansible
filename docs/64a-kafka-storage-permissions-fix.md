# Step 64A — Kafka KRaft 0750 storage probe permission fix

## Diagnosis

The first install stopped after extracting Kafka 4.2.2, staging Java 17 and rendering server.properties on all three nodes. The first unprivileged `ansible.builtin.stat` attempted to traverse `/var/lib/bigtop-kafka-kraft64/metadata` (0750, owned by `bigtopkafka64`), causing `Permission denied` for the SSH user. No Kafka formatter or broker service start task ran. The stable cluster ID on worker3 was already persisted and **must be reused**.

## Changed files

- `roles/kafka_kraft64/tasks/install.yml`: run only the protected metadata/data marker `stat` probes and the safe unformatted-directory `find` probes with `become: true`.
- `roles/kafka_kraft64/tasks/preflight.yml`: run worker3's protected metadata marker probe and cluster ID file check with `become: true` so repeat preflight is also safe.
- `tests/test_kafka_kraft64.py`: assert permission-aware probes and create-once format guards.
- No changes to cluster ID generation, Kafka listeners (`9092`, `19093`), KRaft storage, Kafka/JDK binaries or services.

## Apply on worker3

```bash
cd ~/bigtop-cluster-ansible
unzip -o 64a_kafka_storage_permission_fix.zip -d .
python3 -m unittest discover -s tests -p 'test_kafka_kraft64.py' -v
make lint
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/64a-kafka-kraft-cluster.yml -e kafka_kraft64_mode=preflight
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/64a-kafka-kraft-cluster.yml -e kafka_kraft64_mode=install
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/64a-kafka-kraft-cluster.yml -e kafka_kraft64_mode=verify
bash scripts/64a-smoke-kafka.sh
```

## Safety notes

Do **not** manually delete `meta.properties`, run `kafka-storage.sh format`, clear `/var/lib/bigtop-kafka-kraft64`, regenerate `/etc/bigtop-kafka-kraft64/cluster.id`, or change directory ownership to `bae`. The role already checks consistent marker state, compares persisted cluster/node IDs, and only formats new empty directories once. After a successful 64A run, retry 64B Kafka monitoring integration.
