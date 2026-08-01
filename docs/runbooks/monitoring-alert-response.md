# Monitoring Alert Response Runbook

Prometheus, Grafana, Alertmanager alert 발생 시 기본 확인 및 복구 절차입니다.

## 1. 대상 상황

```text
Prometheus alert firing
Grafana dashboard No data
Alertmanager UI에 alert 표시
Slack alert 수신
TargetDown 발생
JMX target down 발생
```

## 2. Monitoring stack 상태 확인

```bash
ansible ops -b -m command -a "systemctl is-active prometheus"
ansible ops -b -m command -a "systemctl is-active grafana-server"
ansible ops -b -m command -a "systemctl is-active prometheus-alertmanager"
```

포트 확인:

```bash
ansible ops -m shell -a "curl -fsS http://127.0.0.1:9090/-/healthy"
ansible ops -m shell -a "curl -fsS http://127.0.0.1:9093/-/healthy"
ansible ops -m shell -a "curl -fsS http://127.0.0.1:3000/api/health"
```

## 3. Prometheus 설정 검증

```bash
ansible ops -b -m shell -a "promtool check config /etc/prometheus/prometheus.yml"
ansible ops -b -m shell -a "promtool check rules /etc/prometheus/rules/platform-alerts.yml"
```

## 4. Alertmanager 설정 검증

```bash
ansible ops -b -m shell -a "amtool check-config /etc/prometheus/alertmanager.yml"
ansible ops -b -m shell -a "cat /etc/default/prometheus-alertmanager"
```

단일 노드 Alertmanager에서는 다음 옵션이 필요합니다.

```text
--cluster.listen-address=
```

## 5. TargetDown 확인

```bash
ansible ops -m shell -a "curl -s 'http://127.0.0.1:9090/api/v1/query?query=up' | python3 -m json.tool | head -120"
```

JMX target만 확인:

```bash
ansible ops -m shell -a "curl -s 'http://127.0.0.1:9090/api/v1/query?query=up%7Bjob%3D~%22.*jmx%22%7D' | python3 -m json.tool | head -120"
```

## 6. 원 서비스 확인

NameNode:

```bash
ansible master -b -m command -a "systemctl is-active hadoop-hdfs-namenode"
ansible master -m shell -a "curl -fsS http://127.0.0.1:19101/metrics | head"
```

ResourceManager:

```bash
ansible master -b -m command -a "systemctl is-active hadoop-yarn-resourcemanager"
ansible master -m shell -a "curl -fsS http://127.0.0.1:19103/metrics | head"
```

Spark History Server:

```bash
ansible master -b -m command -a "systemctl is-active spark-history-server"
ansible master -m shell -a "curl -fsS http://127.0.0.1:19105/metrics | head"
```

NodeManager:

```bash
ansible workers -b -m command -a "systemctl is-active hadoop-yarn-nodemanager"
ansible workers -m shell -a "curl -fsS http://127.0.0.1:19104/metrics | head"
```

DataNode:

```bash
ansible workers -b -m command -a "systemctl is-active hadoop-hdfs-datanode"
ansible workers -m shell -a "curl -fsS http://127.0.0.1:19102/metrics | head"
```

## 7. Grafana dashboard 파일 확인

```bash
ansible ops -b -m shell -a "ls -lh /var/lib/grafana/dashboards"
ansible ops -b -m shell -a "journalctl -u grafana-server -n 100 --no-pager | grep -iE 'provision|dashboard|datasource|error' || true"
```

## 8. Slack receiver 확인

Slack receiver가 활성화된 경우:

```bash
ansible ops -b -m shell -a "ls -lh /etc/prometheus/alertmanager-secrets/slack_webhook_url"
ansible ops -b -m shell -a "wc -c /etc/prometheus/alertmanager-secrets/slack_webhook_url"
```

webhook URL은 출력하지 않습니다.

## 9. 복구

Monitoring stack 재적용:

```bash
ansible-playbook playbooks/14-prometheus.yml
ansible-playbook playbooks/15-grafana.yml
ansible-playbook playbooks/17-alertmanager.yml
```

전체 health check:

```bash
ansible-playbook playbooks/20-cluster-health-check.yml
```

## 10. 완료 기준

```text
promtool check config 성공
promtool check rules 성공
amtool check-config 성공
Prometheus healthy
Grafana healthy
Alertmanager healthy
필수 target up
health check failed=0
```
