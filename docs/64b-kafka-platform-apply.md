# Step 64B apply — worker3

**Precondition:** Step64A install + Kafka Quorum verify + Kafka produce/consume smoke already passed.
Passing only the `preflight` mode does **not** imply brokers are installed or running.

```bash
cd ~/bigtop-cluster-ansible
unzip -o 64b_kafka_platform_integration_ansible.zip -d .
python3 -m unittest discover -s tests -p 'test_kafka_platform64b.py' -v
make lint

# Strict read-only preflight: Kafka 3 services active and quorum leader present
.venv/bin/ansible-playbook -i inventory/hosts.ini   playbooks/64b-kafka-platform-integration.yml

# Install independent read-only collector, Grafana dashboard and disabled timer
.venv/bin/ansible-playbook -i inventory/hosts.ini   playbooks/64b-kafka-platform-integration.yml -e kafka_platform64b_mode=apply

# Collect actual Kafka topic/quorum metrics without changing Kafka
sudo systemctl start bigtop-kafka-platform64b.service
sudo journalctl -u bigtop-kafka-platform64b.service -n 80 --no-pager
cat /var/lib/prometheus/node-exporter/kafka_platform64b.prom

# Verify the existing Prometheus sees the new series
curl -fsSG 'http://127.0.0.1:9090/api/v1/query'   --data-urlencode 'query=bigtop_kafka64_collector_success' | python3 -m json.tool

# Explicitly register Kafka service and topics in existing OpenMetadata
sudo python3 scripts/64b-kafka-platform.py --mode metadata

# Full read-only recheck
.venv/bin/ansible-playbook -i inventory/hosts.ini   playbooks/64b-kafka-platform-integration.yml -e kafka_platform64b_mode=verify
```

Grafana URL inside worker3: `http://192.158.2.103:3000/d/bigtop-kafka-kraft64/`
On the Windows host, the existing forwarded Grafana port is `127.0.0.1:23000`.
OpenMetadata URL: `http://192.158.2.104:8585`.

After verifying a real successful collection, optionally enable polling every 5 minutes:

```bash
sudo systemctl enable --now bigtop-kafka-platform64b.timer
systemctl list-timers bigtop-kafka-platform64b.timer
```

This does **not** add a CDC topic or stream. Subsequent Step64 will create its own dedicated
topic, and this monitor will automatically count it on its next poll. To register that topic
in OpenMetadata, rerun the explicit metadata command.

Troubleshooting: `systemctl is-active bigtop-kafka-kraft64.service` separately on all 3 nodes;
`journalctl -u bigtop-kafka-kraft64 -n 100`; on worker3 check
`/opt/bigtop-kafka-kraft64/kafka/bin/kafka-metadata-quorum.sh --bootstrap-server 192.158.2.103:9092 describe --status` using the Kafka JDK 17.
If Prometheus has no matching metric, verify the existing Node Exporter textfile collector
watches `/var/lib/prometheus/node-exporter` and that Prometheus scrapes worker3.
