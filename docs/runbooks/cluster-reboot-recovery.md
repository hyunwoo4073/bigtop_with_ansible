# Cluster Reboot Recovery Runbook

VM 또는 전체 클러스터 재기동 이후 HDFS, YARN, Spark, monitoring stack 상태를 복구하고 검증하는 절차입니다.

## 1. 대상 상황

```text
- VM 전체 재기동
- 일부 서비스가 자동 기동되지 않음
- HDFS/YARN/Spark History Server UI 접속 불가
- Prometheus/Grafana/Alertmanager 일부 target down
```

## 2. 기본 접속 확인

```bash
cd ~/bigtop-cluster-ansible

ansible all -m ping
```

정상 기준:

```text
master  SUCCESS
worker1 SUCCESS
worker2 SUCCESS
worker3 SUCCESS
```

## 3. 서비스 복구

```bash
ansible-playbook playbooks/21-cluster-recover.yml
```

이 playbook은 이미 active 상태인 서비스는 그대로 두고, stopped 또는 failed 상태인 서비스만 정상 기동 상태로 맞춥니다.

복구 대상:

```text
Node Exporter
HDFS NameNode
HDFS DataNode
YARN ResourceManager
YARN NodeManager
Spark History Server
NameNode metadata backup timer
Prometheus
Grafana
Alertmanager
```

## 4. 전체 상태 점검

```bash
ansible-playbook playbooks/20-cluster-health-check.yml
```

점검 항목:

```text
서비스 active 상태
주요 포트 open 상태
JMX Exporter endpoint
HDFS live DataNode count
YARN active NodeManager count
NameNode backup metric
YARN log aggregation 설정
Spark History cleaner 설정
Spark submit profile wrapper
NodeManager local/log directory
NodeManager storage metric
Grafana dashboard files
Prometheus alert rule syntax
Alertmanager config syntax
```

## 5. 기능 검증

```bash
ansible-playbook playbooks/22-cluster-smoke-test.yml
```

검증 항목:

```text
HDFS mkdir
HDFS put
HDFS cat
YARN node 상태
SparkPi on YARN 실행
Spark History Server API
Spark History Server JMX metric
```

## 6. 주요 확인 명령

HDFS:

```bash
ansible master -b -m shell -a "sudo -u hdfs hdfs dfsadmin -report | grep -E 'Live datanodes|Dead datanodes'"
```

YARN:

```bash
ansible master -b -m shell -a "sudo -u yarn yarn node -list"
```

Spark History Server:

```bash
ansible master -m shell -a "curl -fsS http://127.0.0.1:18080/api/v1/applications | head"
```

Prometheus targets:

```bash
ansible ops -m shell -a "curl -s 'http://127.0.0.1:9090/api/v1/query?query=up' | python3 -m json.tool | head -80"
```

## 7. 복구 완료 기준

```text
20-cluster-health-check.yml failed=0
22-cluster-smoke-test.yml failed=0
HDFS Live datanodes = 2
YARN Total Nodes = 2
SparkPi application FINISHED/SUCCEEDED
Prometheus JMX targets up
Grafana dashboard files present
```
