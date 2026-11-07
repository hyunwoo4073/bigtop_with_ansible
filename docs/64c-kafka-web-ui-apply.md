# Step 64-C — Kafbat UI (read-only) deployment

## Scope

- Keep Kafka 4.2.2 3-node KRaft cluster, Grafana, OpenMetadata and Trino unchanged.
- Install **Kafbat UI v1.5.0** in one Docker container on `metadata` (`192.158.2.104`).
- HTTP UI: `http://192.158.2.104:18081` (must be reachable from the user's browser over their VM/internal network; VirtualBox NAT may require forwarding).
- Broker bootstrap: `192.158.2.101:9092,192.158.2.102:9092,192.158.2.103:9092`.
- Read-only UI; disable dynamic configuration; constrain container to 1 GiB RAM and 1 vCPU.
- Default mode `plan`. No Kafka restart, topic create/delete, Docker daemon installation, or existing container replacement.

## 1. Local tests on worker3

```bash
cd ~/bigtop-cluster-ansible
unzip -o 64c_kafka_web_ui_ansible.zip -d .
python3 -m unittest discover -s tests -p 'test_kafka_ui64c.py' -v
make lint
```

## 2. Preflight (non-mutating)

```bash
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/64c-kafka-web-ui.yml
```

## 3. Prepare Docker image on metadata

The Ansible Role **never pulls an image automatically**. On a host allowed to access the official container registry:

```bash
ssh metadata 'sudo docker pull ghcr.io/kafbat/kafka-ui:v1.5.0'
```

If the metadata VM is air-gapped, on an internet-connected Linux machine with Docker:

```bash
docker pull ghcr.io/kafbat/kafka-ui:v1.5.0
docker save -o kafbat-ui-v1.5.0.tar ghcr.io/kafbat/kafka-ui:v1.5.0
# Transfer the tar into metadata VM through approved enterprise procedures.
# On the metadata VM:
sudo docker load -i /path/to/kafbat-ui-v1.5.0.tar
```

Keep Docker image immutable by version tag. For higher supply-chain assurance, review and pin the linux/amd64 image digest from GitHub Container Registry as part of the image import approval.

## 4. Explicit installation

```bash
.venv/bin/ansible-playbook -i inventory/hosts.ini \
  playbooks/64c-kafka-web-ui.yml -e kafka_ui64c_mode=apply
```

## 5. Verify

```bash
.venv/bin/ansible-playbook -i inventory/hosts.ini \
  playbooks/64c-kafka-web-ui.yml -e kafka_ui64c_mode=verify
curl -fsS http://192.158.2.104:18081/actuator/health
ssh metadata 'sudo docker logs --tail 100 bigtop-kafbat-ui64c'
```

Expected: `STEP64C_KAFBAT_UI_READY`; browser UI should list one Kafka cluster and show 3 brokers and `bigtop_kafka64_smoke_v1` topic. If UI loads but brokers are unavailable, check `advertised.listeners` address resolution and metadata→broker port 9092 connectivity. Check consumer group lag only when a consumer group has committed offsets.

## 6. Important limitations

- Kafbat read-only is an **application-level mode**, not Kafka ACLs. The current Kafka cluster has no enforced Kafka-side permissions. Do not treat it as an authorization boundary.
- UI is not configured with user authentication. Restrict network access to trusted internal users; add an auth proxy or application authentication for broader internal deployment. Reading messages may disclose data.
- Kafbat adds topic/message/consumer UI, **not** a replacement for 64-B Prometheus/Grafana time-series monitoring. Detailed broker JVM, ISR trends and throughput require Kafka JMX exporter or equivalent instrumentation.
- Docker image version and memory cap are pinned, but resource suitability must be verified on the actual metadata VM. The role refuses installation if available memory is under 1.5 GiB.
- To remove the UI manually, only remove the dedicated `bigtop-kafbat-ui64c` container after confirming no dependencies; never run `docker system prune` or touch OpenMetadata containers.
