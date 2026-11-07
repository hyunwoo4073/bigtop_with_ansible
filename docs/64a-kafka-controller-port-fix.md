# Step 64A — Kafka KRaft controller port conflict fix (2026-11-07)

## Root cause

On `worker3`, TCP 9093 was already listening before Kafka was installed. This is the default Alertmanager port and may belong to the existing monitoring stack; verify with `sudo ss -lntp | grep ':9093'`. Do not stop an existing service to install the Kafka lab. The preflight failure skipped the cluster ID initialization play; worker1/2 then failed because there was no stable cluster ID. There is no evidence of a Kafka data format or successful broker installation in that run.

## Changes

- Keep Kafka broker/client port **9092** unchanged across all three nodes. Change only the Kafka KRaft Controller port to **19093** in `roles/kafka_kraft64/defaults/main.yml`.
- Template **all 3** `controller.quorum.voters` addresses and each node's `CONTROLLER` listener from one configurable value. Never mix old port 9093 with new 19093.
- Preflight checks the **configured** broker and controller ports even when run in `preflight` mode (not just install).
- Abort the entire playbook after any node preflight failure with `any_errors_fatal: true`, avoiding cascaded missing cluster ID errors.
- No other Kafka configuration, HDFS, YARN, REST Catalog, Java alternatives, or existing services are changed. Do not reformat old KRaft storage.

## Apply (worker3)

```bash
cd ~/bigtop-cluster-ansible
sudo ss -lntp | grep -E ':(9092|9093|19093)\b' || true
unzip -o 64a_kafka_controller_port_fix.zip -d .
python3 -m unittest discover -s tests -p 'test_kafka_kraft64.py' -v
make lint
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/64a-kafka-kraft-cluster.yml -e kafka_kraft64_mode=preflight
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/64a-kafka-kraft-cluster.yml -e kafka_kraft64_mode=install
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/64a-kafka-kraft-cluster.yml -e kafka_kraft64_mode=verify
bash scripts/64a-smoke-kafka.sh
```

Confirm 19093 is free **on all three machines** before installing. For a newly provisioned cluster, broker-to-controller and controller-to-controller traffic must allow 19093; broker/client traffic continues on 9092. If any KRaft storage has previously been formatted with different voters, stop and inspect persisted config/state before making any controller endpoint change.

After Step64A passes, continue 64B integration as before; its Kafka CLI and bootstrap port remain 9092.
