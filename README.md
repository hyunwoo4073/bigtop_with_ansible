# Bigtop Cluster Ansible

Apache Bigtop과 Ansible을 사용해 Hadoop 기반 미니 데이터 플랫폼을 자동화 방식으로 구성하는 프로젝트입니다.

개인 VM 환경에서 Ambari 같은 통합 관리 도구 없이, Ansible playbook과 role 구조를 통해 서버 공통 설정, Java 설치, Bigtop repository 등록, Hadoop/HDFS/YARN/Spark 설치, 설정 파일 배포, 서비스 기동, 모니터링, 대시보드, 알림 구성을 자동화하는 것을 목표로 합니다.

## 구성

현재 클러스터는 4대 VM 기준입니다.

```text
master   - HDFS NameNode / YARN ResourceManager / Spark History Server
worker1  - HDFS DataNode / YARN NodeManager
worker2  - HDFS DataNode / YARN NodeManager
worker3  - Ansible Control Node / Ops Node / Prometheus / Grafana / Alertmanager
```

현재는 HDFS, YARN, Spark on YARN, Prometheus/Grafana 기반 모니터링, JMX Exporter 기반 JVM 메트릭 수집, Alertmanager 기반 알림, Grafana datasource/dashboard provisioning 구성을 완료한 상태입니다.

HA 구성은 적용하지 않았으며, 개인 VM 환경에서 Non-HA 구조로 먼저 구성했습니다.

## 주요 구성 요소

```text
Ansible        - 서버 설정 및 서비스 자동화
Apache Bigtop  - Hadoop/Spark 패키지 repository
Hadoop HDFS    - 분산 파일 시스템
YARN           - 리소스 관리 및 작업 실행
Spark on YARN  - Spark 작업 실행 환경
systemd        - Hadoop/YARN/Spark 서비스 관리
Prometheus     - 메트릭 수집 및 alert rule 평가
Grafana        - 메트릭 시각화 및 dashboard provisioning
Alertmanager   - Prometheus alert 수신 및 알림 관리
Node Exporter  - OS 메트릭 수집
JMX Exporter   - Hadoop/YARN/Spark JVM 메트릭 수집
```

## 디렉터리 구조

```text
bigtop-cluster-ansible/
├── inventory/
│   ├── hosts.ini.example
│   └── group_vars/
│       └── all.yml.example
├── playbooks/
│   ├── 01-common.yml
│   ├── 02-java.yml
│   ├── 03-bigtop-repo.yml
│   ├── 04-hadoop-install.yml
│   ├── 05-hdfs-config.yml
│   ├── 06-hdfs-start.yml
│   ├── 06-hdfs-stop.yml
│   ├── 07-yarn-install.yml
│   ├── 08-yarn-config.yml
│   ├── 09-yarn-start.yml
│   ├── 09-yarn-stop.yml
│   ├── 10-spark-install.yml
│   ├── 11-spark-config.yml
│   ├── 12-spark-history-start.yml
│   ├── 12-spark-history-stop.yml
│   ├── 13-node-exporter.yml
│   ├── 14-prometheus.yml
│   ├── 15-grafana.yml
│   ├── 16-jmx-exporter.yml
│   └── 17-alertmanager.yml
└── roles/
    ├── common/
    ├── java/
    ├── bigtop_repo/
    ├── hadoop/
    ├── hdfs_config/
    ├── hdfs_service/
    ├── yarn/
    ├── yarn_config/
    ├── spark/
    ├── spark_config/
    ├── node_exporter/
    ├── prometheus/
    ├── grafana/
    ├── jmx_exporter/
    └── alertmanager/
```

## 사전 준비

Ansible Control Node에서 각 서버로 SSH 접속이 가능해야 합니다.

```bash
ansible all -m command -a "whoami"
```

대상 서버에서 passwordless sudo가 가능해야 합니다.

```bash
ansible all -b -m command -a "whoami"
```

정상이라면 `root`가 출력됩니다.

## 설정 파일 준비

실제 inventory와 변수 파일은 Git에 포함하지 않습니다.
예제 파일을 복사해서 각자 환경에 맞게 수정합니다.

```bash
cp inventory/hosts.ini.example inventory/hosts.ini
cp inventory/group_vars/all.yml.example inventory/group_vars/all.yml
```

예제 inventory:

```ini
[master]
master ansible_host=192.168.56.101

[workers]
worker1 ansible_host=192.168.56.102
worker2 ansible_host=192.168.56.103

[ops]
worker3 ansible_host=192.168.56.104

[hadoop_cluster:children]
master
workers

[all:vars]
ansible_user=your_user
ansible_ssh_private_key_file=~/.ssh/id_rsa
ansible_python_interpreter=/usr/bin/python3
```

Hadoop/YARN/Spark/Prometheus 설정에서는 `vm1`, `vm2` 같은 임시 inventory alias 대신 실제 클러스터 hostname인 `master`, `worker1`, `worker2`, `worker3`를 사용합니다.

inventory alias와 실제 hostname을 맞춰두면 다음과 같은 ad-hoc 명령을 직관적으로 사용할 수 있습니다.

```bash
ansible master -m shell -a "hostname"
ansible worker1 -m shell -a "hostname"
ansible worker2 -m shell -a "hostname"
ansible worker3 -m shell -a "hostname"
```

inventory 구조는 다음 명령으로 확인합니다.

```bash
ansible-inventory --graph
```

예상 결과:

```text
@all:
  |--@ungrouped:
  |--@ops:
  |  |--worker3
  |--@hadoop_cluster:
  |  |--@master:
  |  |  |--master
  |  |--@workers:
  |  |  |--worker1
  |  |  |--worker2
```

## 실행 순서

전체 구성은 다음 순서로 실행합니다.

```bash
ansible-playbook playbooks/01-common.yml
ansible-playbook playbooks/02-java.yml
ansible-playbook playbooks/03-bigtop-repo.yml
ansible-playbook playbooks/04-hadoop-install.yml
ansible-playbook playbooks/05-hdfs-config.yml
ansible-playbook playbooks/06-hdfs-start.yml
ansible-playbook playbooks/07-yarn-install.yml
ansible-playbook playbooks/08-yarn-config.yml
ansible-playbook playbooks/09-yarn-start.yml
ansible-playbook playbooks/10-spark-install.yml
ansible-playbook playbooks/11-spark-config.yml
ansible-playbook playbooks/12-spark-history-start.yml
ansible-playbook playbooks/13-node-exporter.yml
ansible-playbook playbooks/14-prometheus.yml
ansible-playbook playbooks/15-grafana.yml
ansible-playbook playbooks/16-jmx-exporter.yml
ansible-playbook playbooks/17-alertmanager.yml
```

서비스 중지는 다음 playbook으로 수행합니다.

```bash
ansible-playbook playbooks/12-spark-history-stop.yml
ansible-playbook playbooks/09-yarn-stop.yml
ansible-playbook playbooks/06-hdfs-stop.yml
```

## HDFS 설정 기준

```text
NameNode: master
DataNode: worker1, worker2
fs.defaultFS: hdfs://master:9000
dfs.replication: 2
NameNode dir: /data/hadoop/hdfs/namenode
DataNode dir: /data/hadoop/hdfs/datanode
```

HDFS 상태 확인:

```bash
ansible master -b -m command -a "systemctl is-active hadoop-hdfs-namenode"
ansible workers -b -m command -a "systemctl is-active hadoop-hdfs-datanode"
ansible master -b -m shell -a "sudo -u hdfs hdfs dfsadmin -report"
```

정상 상태에서는 `Live datanodes (2)`가 출력됩니다.

NameNode RPC/Web UI 포트 확인:

```bash
ansible master -m shell -a "ss -lntp | grep -E '9000|9870' || true"
```

## YARN 설정 기준

```text
ResourceManager: master
NodeManager: worker1, worker2
ResourceManager Web UI: http://master:8088
YARN UI2: http://master:8088/ui2
```

YARN 상태 확인:

```bash
ansible master -b -m command -a "systemctl is-active hadoop-yarn-resourcemanager"
ansible workers -b -m command -a "systemctl is-active hadoop-yarn-nodemanager"
ansible master -b -m shell -a "sudo -u yarn yarn node -list"
```

정상 상태에서는 `Total Nodes:2`가 출력됩니다.

## Spark 설정 기준

```text
Spark 실행 모드: Spark on YARN
Spark History Server: master
Spark History Server UI: http://master:18080
Event log dir: hdfs:///spark-history
```

Spark 상태 확인:

```bash
ansible hadoop_cluster -m shell -a "spark-submit --version | head -20"
ansible master -b -m command -a "systemctl is-active spark-history-server"
```

SparkPi 테스트:

```bash
ansible master -b -m shell -a "sudo -u hadoop spark-submit --master yarn --deploy-mode client --class org.apache.spark.examples.SparkPi \$(find /usr -name 'spark-examples*.jar' 2>/dev/null | head -1) 10"
```

정상 실행 시 YARN UI와 Spark History Server에서 Spark application을 확인할 수 있습니다.

## Monitoring 설정 기준

```text
Prometheus: worker3:9090
Grafana: worker3:3000
Alertmanager: worker3:9093
Node Exporter: all nodes:9100
```

Prometheus, Grafana, Alertmanager는 `worker3`에서 실행합니다.
Node Exporter는 전체 노드에 설치하여 CPU, Memory, Disk, Network 등 OS 메트릭을 수집합니다.

상태 확인:

```bash
ansible all -b -m command -a "systemctl is-active prometheus-node-exporter"
ansible ops -b -m command -a "systemctl is-active prometheus"
ansible ops -b -m command -a "systemctl is-active grafana-server"
ansible ops -b -m command -a "systemctl is-active prometheus-alertmanager"
```

Prometheus health check:

```bash
ansible ops -m shell -a "curl -s http://127.0.0.1:9090/-/healthy"
```

Alertmanager health check:

```bash
ansible ops -m shell -a "curl -s http://127.0.0.1:9093/-/healthy"
```

접속 URL:

```text
Prometheus Targets: http://worker3:9090/targets
Prometheus Alerts : http://worker3:9090/alerts
Grafana           : http://worker3:3000
Alertmanager      : http://worker3:9093
```

## Grafana Provisioning 설정 기준

Grafana datasource와 dashboard는 UI에서 수동 등록하지 않고 Ansible로 자동 배포합니다.

```text
Datasource: Prometheus
Datasource URL: http://localhost:9090
Dashboard folder: Bigtop Cluster
Dashboard file dir: /var/lib/grafana/dashboards
Datasource provisioning dir: /etc/grafana/provisioning/datasources
Dashboard provisioning dir: /etc/grafana/provisioning/dashboards
```

Grafana provisioning 확인:

```bash
ansible ops -b -m shell -a "ls -al /etc/grafana/provisioning/datasources"
ansible ops -b -m shell -a "ls -al /etc/grafana/provisioning/dashboards"
ansible ops -b -m shell -a "ls -al /var/lib/grafana/dashboards"
```

Grafana 로그 확인:

```bash
ansible ops -b -m shell -a "journalctl -u grafana-server -n 100 --no-pager | grep -iE 'provision|dashboard|datasource|error' || true"
```

Grafana UI에서 다음 경로로 dashboard를 확인합니다.

```text
Dashboards
→ Bigtop Cluster
→ Bigtop Platform Overview
```

기본 dashboard에는 다음 항목을 포함합니다.

```text
Node Exporter UP
Targets Down
JMX Targets UP
Active Alerts
CPU Usage by Node
Memory Usage by Node
Disk Usage by Node
Target Availability
```

## JMX Exporter 설정 기준

JMX Exporter를 사용해 Hadoop/YARN/Spark JVM 메트릭을 Prometheus에서 수집합니다.

```text
NameNode JMX Exporter: master:19101
DataNode JMX Exporter: worker1/worker2:19102
ResourceManager JMX Exporter: master:19103
NodeManager JMX Exporter: worker1/worker2:19104
Spark History Server JMX Exporter: master:19105
```

JMX Exporter 확인:

```bash
ansible master -m shell -a "curl -s http://localhost:19101/metrics | head"
ansible master -m shell -a "curl -s http://localhost:19103/metrics | head"
ansible master -m shell -a "curl -s http://localhost:19105/metrics | head"
ansible workers -m shell -a "curl -s http://localhost:19102/metrics | head"
ansible workers -m shell -a "curl -s http://localhost:19104/metrics | head"
```

정상 상태에서는 Prometheus metric 형식의 `# HELP`, `# TYPE` 출력이 확인됩니다.

Prometheus에서 JMX target 상태 확인:

```bash
ansible ops -m shell -a "curl -s 'http://127.0.0.1:9090/api/v1/query?query=up%7Bjob%3D~%22.*jmx%22%7D'"
```

## Alertmanager 설정 기준

Alertmanager는 Prometheus에서 발생한 alert를 수신하고 관리하는 역할을 합니다.
현재는 외부 Slack, Email 연동 없이 Alertmanager UI에서 alert를 확인하는 최소 구성으로 설정했습니다.

```text
Alertmanager host: worker3
Alertmanager port: 9093
Alertmanager config: /etc/prometheus/alertmanager.yml
Prometheus rule dir: /etc/prometheus/rules
```

Prometheus rule 검증:

```bash
ansible ops -b -m shell -a "promtool check config /etc/prometheus/prometheus.yml"
ansible ops -b -m shell -a "promtool check rules /etc/prometheus/rules/platform-alerts.yml"
```

Alertmanager 설정 확인:

```bash
ansible ops -b -m shell -a "cat /etc/prometheus/alertmanager.yml"
ansible ops -b -m shell -a "cat /etc/default/prometheus-alertmanager"
```

Alertmanager 기본 실행 옵션은 단일 노드 환경에 맞게 cluster gossip 기능을 비활성화합니다.

```text
--cluster.listen-address=
```

예시:

```text
ARGS="--config.file=/etc/prometheus/alertmanager.yml --storage.path=/var/lib/prometheus/alertmanager --web.listen-address=0.0.0.0:9093 --cluster.listen-address="
```

## Alert Rule 기준

현재 기본 alert rule은 다음 항목을 포함합니다.

```text
TargetDown
HighMemoryUsage
HighDiskUsage
HDFSNameNodeJMXDown
YARNResourceManagerJMXDown
SparkHistoryServerJMXDown
```

Alert 테스트는 Node Exporter를 잠시 중지해서 확인할 수 있습니다.

```bash
ansible worker1 -b -m service -a "name=prometheus-node-exporter state=stopped"
```

Prometheus `/alerts`와 Alertmanager UI에서 `TargetDown` alert가 firing 되는지 확인합니다.

```text
http://worker3:9090/alerts
http://worker3:9093
```

복구:

```bash
ansible worker1 -b -m service -a "name=prometheus-node-exporter state=started"
```

Grafana dashboard에서도 `Targets Down`, `Active Alerts`, `Target Availability` 패널 변화를 확인할 수 있습니다.

## 주요 트러블슈팅

### Bigtop repository 경로

`downloads.apache.org` 경로를 APT repository로 직접 사용하면 `Release file` 오류가 발생할 수 있습니다.

실제 APT repository는 다음 경로를 사용합니다.

```text
http://repos.bigtop.apache.org/releases/3.5.0/ubuntu/24.04/$(ARCH) bigtop contrib
```

### Ansible shell source 문제

Ansible shell 모듈의 기본 shell은 `/bin/sh`입니다.
`source` 명령 대신 `.`을 사용합니다.

```bash
ansible all -m shell -a ". /etc/profile.d/java.sh && echo \$JAVA_HOME"
```

### inventory alias 불일치 문제

Ansible ad-hoc 명령의 host pattern은 OS hostname이 아니라 inventory alias 기준입니다.

예를 들어 inventory에 `vm2`로 등록되어 있으면 다음 명령은 실패합니다.

```bash
ansible worker1 -b -m service -a "name=prometheus-node-exporter state=stopped"
```

이 경우 다음과 같은 warning이 발생합니다.

```text
Could not match supplied host pattern, ignoring: worker1
No hosts matched, nothing to do
```

해결 방법은 inventory alias를 실제 hostname과 동일하게 맞추는 것입니다.

```ini
[workers]
worker1 ansible_host=192.168.56.102
worker2 ansible_host=192.168.56.103
```

확인:

```bash
ansible-inventory --graph
ansible all -m shell -a "hostname"
```

### group 이름과 host 이름 중복 warning

inventory를 다음처럼 구성하면 Ansible warning이 발생할 수 있습니다.

```ini
[master]
master ansible_host=192.168.56.101
```

warning:

```text
Found both group and host with same name: master
```

이는 group 이름과 host alias가 모두 `master`라서 발생하는 warning입니다.
실행 실패는 아니므로 그대로 진행해도 됩니다.

장기적으로 warning을 제거하려면 group 이름을 `hadoop_master`, `hadoop_workers`처럼 바꾸고 playbook의 `hosts` 값도 함께 수정합니다.

예시:

```ini
[hadoop_master]
master ansible_host=192.168.56.101

[hadoop_workers]
worker1 ansible_host=192.168.56.102
worker2 ansible_host=192.168.56.103

[hadoop_cluster:children]
hadoop_master
hadoop_workers
```

### hostname 불일치 문제

Hadoop 설정에는 임시 inventory alias가 아니라 실제 클러스터 hostname을 사용합니다.

```text
master
worker1
worker2
worker3
```

각 노드 내부에서 자기 자신의 포트를 확인할 때는 `inventory_hostname` 대신 `127.0.0.1`을 사용하는 것이 안전합니다.

예시:

```yaml
- name: Wait for service port
  ansible.builtin.wait_for:
    host: "127.0.0.1"
    port: "{{ service_port }}"
    timeout: 60
```

### Prometheus YAML 오류

Prometheus 설정 파일은 YAML 들여쓰기에 민감합니다.
문제가 발생하면 다음 명령으로 확인합니다.

```bash
ansible ops -b -m shell -a "promtool check config /etc/prometheus/prometheus.yml"
ansible ops -b -m shell -a "cat -n /etc/prometheus/prometheus.yml"
ansible ops -b -m shell -a "journalctl -u prometheus -n 100 --no-pager"
```

### Prometheus handler 위치 문제

Ansible role에서 handler는 `tasks/main.yml` 안에 직접 작성하지 않고 별도 경로에 둡니다.

```text
roles/prometheus/
├── tasks/
│   └── main.yml
├── handlers/
│   └── main.yml
└── templates/
    └── prometheus.yml.j2
```

### Grafana provisioning 확인

Grafana datasource나 dashboard가 UI에 보이지 않으면 provisioning 파일과 로그를 확인합니다.

```bash
ansible ops -b -m shell -a "ls -al /etc/grafana/provisioning/datasources"
ansible ops -b -m shell -a "ls -al /etc/grafana/provisioning/dashboards"
ansible ops -b -m shell -a "ls -al /var/lib/grafana/dashboards"
ansible ops -b -m shell -a "journalctl -u grafana-server -n 100 --no-pager | grep -iE 'provision|dashboard|datasource|error' || true"
```

dashboard JSON 안에서 Grafana legend template과 Ansible Jinja template이 모두 `{{ }}` 문법을 사용하므로, dashboard JSON template에서는 Grafana legend 값을 Jinja escape 처리해야 합니다.

예시:

```text
{{ '{{' }}instance{{ '}}' }}
```

### JMX Exporter duplicate metric 문제

JMX Exporter 설정에서 JVM 기본 메트릭과 같은 이름을 직접 정의하면 duplicate metric 오류가 발생할 수 있습니다.

예시:

```text
duplicate metric name: jvm_threads_current
```

이 경우 `java.lang` 기반 JVM metric rule을 제거하고 Hadoop/Spark 관련 rule만 별도로 관리합니다.

예시:

```yaml
---
lowercaseOutputName: true
lowercaseOutputLabelNames: true

rules:
  - pattern: "Hadoop<service=(.*), name=(.*)><>(.*)"
    name: "hadoop_$1_$2_$3"
    type: GAUGE
```

### JMX target down alert

Prometheus에서 다음과 같은 alert가 발생할 수 있습니다.

```text
TargetDown
instance="master:19101"
job="hdfs-namenode-jmx"

TargetDown
instance="master:19105"
job="spark-history-jmx"
```

이는 alert rule 자체가 정상 동작하고 있으며, 해당 JMX Exporter endpoint가 실제로 응답하지 않는다는 의미입니다.

먼저 원 서비스가 살아 있는지 확인합니다.

```bash
ansible master -b -m command -a "systemctl is-active hadoop-hdfs-namenode"
ansible master -b -m command -a "systemctl is-active spark-history-server"
```

서비스가 failed 상태라면 JMX Exporter 문제가 아니라 원 서비스 기동 실패를 먼저 해결해야 합니다.

포트 확인:

```bash
ansible master -m shell -a "ss -lntp | grep -E '19101|19105|9870|18080' || true"
```

### Alertmanager gossip mesh 오류

Alertmanager 단일 노드 구성에서 다음 오류가 발생할 수 있습니다.

```text
couldn't deduce an advertise address: no private IP found
unable to initialize gossip mesh
```

이 경우 `/etc/default/prometheus-alertmanager`의 `ARGS`에 다음 옵션을 추가하여 cluster gossip 기능을 비활성화합니다.

```text
--cluster.listen-address=
```

예시:

```text
ARGS="--config.file=/etc/prometheus/alertmanager.yml --storage.path=/var/lib/prometheus/alertmanager --web.listen-address=0.0.0.0:9093 --cluster.listen-address="
```

서비스가 반복 실패 상태라면 reset 후 재시작합니다.

```bash
ansible ops -b -m command -a "systemctl reset-failed prometheus-alertmanager"
ansible ops -b -m service -a "name=prometheus-alertmanager state=restarted enabled=true"
```

### NameNode FSImage 손상 복구

NameNode가 시작되지 않고 RPC port `9000` 대기에서 timeout이 발생할 수 있습니다.

증상:

```text
Timeout when waiting for master:9000
hadoop-hdfs-namenode: failed
```

NameNode 로그에서 다음과 같은 메시지가 확인될 수 있습니다.

```text
Failed to load image from FSImageFile
java.io.IOException: Premature EOF from inputStream
Failed to load FSImage file
```

이는 최신 `fsimage` 파일이 손상되었거나 불완전하게 기록된 상태를 의미합니다.

복구 전 HDFS 관련 서비스를 중지합니다.

```bash
ansible workers -b -m service -a "name=hadoop-hdfs-datanode state=stopped"
ansible master -b -m service -a "name=hadoop-hdfs-namenode state=stopped"
```

NameNode metadata를 먼저 백업합니다.

```bash
ansible master -b -m shell -a 'ts=$(date +%Y%m%d_%H%M%S); tar czf /root/namenode-current-$ts.tgz -C /data/hadoop/hdfs/namenode current && ls -lh /root/namenode-current-$ts.tgz'
```

fsimage 파일 상태를 확인합니다.

```bash
ansible master -b -m shell -a "ls -lh /data/hadoop/hdfs/namenode/current/fsimage_* 2>/dev/null || true"
ansible master -b -m shell -a "ls -lh /data/hadoop/hdfs/namenode/current/edits_* 2>/dev/null || true"
```

손상된 최신 fsimage와 md5 파일을 별도 recovery 디렉터리로 이동합니다.

```bash
ansible master -b -m shell -a 'RECOVERY_DIR=/data/hadoop/hdfs/namenode/recovery_$(date +%Y%m%d_%H%M%S); mkdir -p $RECOVERY_DIR; mv /data/hadoop/hdfs/namenode/current/fsimage_0000000000000000043* $RECOVERY_DIR/; ls -lh $RECOVERY_DIR'
```

NameNode를 재기동합니다.

```bash
ansible master -b -m command -a "systemctl reset-failed hadoop-hdfs-namenode"
ansible master -b -m service -a "name=hadoop-hdfs-namenode state=started enabled=true"
```

확인:

```bash
ansible master -b -m command -a "systemctl is-active hadoop-hdfs-namenode"
ansible master -m shell -a "ss -lntp | grep -E '9000|9870' || true"
```

NameNode가 정상 기동되면 DataNode를 다시 시작합니다.

```bash
ansible workers -b -m service -a "name=hadoop-hdfs-datanode state=started enabled=true"
ansible master -b -m shell -a "sudo -u hdfs hdfs dfsadmin -report"
```

정상 상태에서는 `Live datanodes (2)`가 출력됩니다.

복구 후에는 새 fsimage 생성을 유도할 수 있습니다.

```bash
ansible master -b -m shell -a "sudo -u hdfs hdfs dfsadmin -safemode enter"
ansible master -b -m shell -a "sudo -u hdfs hdfs dfsadmin -saveNamespace"
ansible master -b -m shell -a "sudo -u hdfs hdfs dfsadmin -safemode leave"
```

주의:

```text
hdfs namenode -format
```

위 명령은 HDFS metadata를 초기화하는 작업이므로 복구 과정에서 먼저 실행하지 않습니다.
실습 환경이라도 metadata 백업과 이전 fsimage 복구를 먼저 시도한 뒤 마지막 수단으로만 고려합니다.

### NameNode metadata directory redundancy

복구 과정에서 다음 warning이 확인될 수 있습니다.

```text
Only one image storage directory configured.
Only one namespace edits storage directory configured.
```

이는 `dfs.namenode.name.dir`, `dfs.namenode.edits.dir`이 단일 경로만 사용 중이라는 의미입니다.
실습 환경에서는 단일 디렉터리로도 구성할 수 있지만, 운영 환경에서는 NameNode metadata 손상에 대비해 복수 디렉터리를 사용하는 것이 안전합니다.

예시:

```xml
<property>
  <name>dfs.namenode.name.dir</name>
  <value>file:///data/hadoop/hdfs/namenode,file:///data2/hadoop/hdfs/namenode</value>
</property>

<property>
  <name>dfs.namenode.edits.dir</name>
  <value>file:///data/hadoop/hdfs/namenode,file:///data2/hadoop/hdfs/namenode</value>
</property>
```

## Git에 포함하지 않는 파일

실제 IP, 사용자명, SSH key, 로그 파일은 Git에 올리지 않습니다.

```text
inventory/hosts.ini
inventory/group_vars/all.yml
inventory/host_vars/
*.retry
*.log
*.pem
*.key
id_rsa
id_rsa.pub
.env
vault_password*
.vault_pass
```

Git에는 example 파일만 포함합니다.

```text
inventory/hosts.ini.example
inventory/group_vars/all.yml.example
```

## 현재 완료 상태

```text
1. Ansible 기본 구조 구성
2. 서버 공통 설정 자동화
3. Java 설치 자동화
4. Bigtop repository 등록
5. Hadoop/HDFS 설치 및 설정
6. HDFS NameNode/DataNode 기동 확인
7. YARN ResourceManager/NodeManager 기동 확인
8. YARN UI2 활성화
9. Spark on YARN 실행 확인
10. Spark History Server 기동 확인
11. Node Exporter 설치
12. Prometheus 설치 및 target 확인
13. Grafana 설치
14. JMX Exporter 기반 JVM 메트릭 수집
15. Alertmanager 설치 및 기동 확인
16. Prometheus alert rule 구성
17. Prometheus와 Alertmanager 연동
18. TargetDown alert 동작 확인
19. Grafana Prometheus datasource 자동 provisioning
20. Grafana dashboard provider 자동 provisioning
21. Bigtop Platform Overview dashboard 자동 배포
22. inventory alias를 실제 hostname 기준으로 정리
23. NameNode FSImage 손상 상황 복구 확인
24. Prometheus/Grafana/Alertmanager 기반 장애 감지 흐름 확인
```

## 향후 계획

```text
1. Grafana dashboard 패널 고도화
2. Hadoop/YARN/Spark 주요 metric 패널 추가
3. Alertmanager Slack 또는 Email receiver 연동
4. HDFS/YARN 장애 상황별 alert rule 고도화
5. 서비스별 runbook 문서화
6. NameNode metadata 이중 디렉터리 구성 검토
7. HA 구성 설계 문서화
8. Ansible role 리팩토링
9. README 및 운영 문서 보강
```
