# Step 64B — undefined mode in outer playbook `when`

## Symptoms

All Kafka broker and metadata quorum checks pass, but the next playbook task fails with:

```text
A `when` expression failed: `kafka_platform64b_mode` is undefined
```

## Root cause

`kafka_platform64b_mode: plan` was declared in `roles/kafka_platform64b/defaults/main.yml`. The task-level outer `when` of a subsequent `include_role` is evaluated before that role's defaults are available to the parent play.

## Fix

Use `kafka_platform64b_mode | default('plan')` in BOTH outer conditional tasks. The default stays read-only; explicit `-e kafka_platform64b_mode=apply|verify` still works.

## How to apply on worker3

```bash
cd ~/bigtop-cluster-ansible
unzip -o 64b_kafka_mode_default_fix.zip -d .
python3 -m unittest discover -s tests -p 'test_kafka_platform64b.py' -v
make lint
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/64b-kafka-platform-integration.yml
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/64b-kafka-platform-integration.yml -e kafka_platform64b_mode=apply
sudo systemctl start bigtop-kafka-platform64b.service
sudo journalctl -u bigtop-kafka-platform64b.service -n 80 --no-pager
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/64b-kafka-platform-integration.yml -e kafka_platform64b_mode=verify
```

Only the playbook mode guards and a regression test change. No broker restarts, Kafka data mutations, or KRaft reformatting.
