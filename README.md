# Bigtop Cluster Ansible

Apache Bigtop과 Ansible을 사용해 Hadoop 기반 미니 데이터 플랫폼을 자동화 방식으로 구성하는 프로젝트입니다.

개인 VM 환경에서 Ambari 같은 통합 관리 도구 없이, Ansible playbook과 role 구조를 통해 서버 공통 설정, Java 설치, Bigtop repository 등록, Hadoop/HDFS/YARN/Spark 설치, 설정 파일 배포, 서비스 기동, 모니터링 구성을 자동화하는 것을 목표로 합니다.

## 구성

현재 클러스터는 4대 VM 기준입니다.

```text
master   - HDFS NameNode / YARN ResourceManager / Spark History Server
worker1  - HDFS DataNode / YARN NodeManager
worker2  - HDFS DataNode / YARN NodeManager
worker3  - Ansible Control Node / Ops Node / Prometheus / Grafana
```

현재는 HDFS, YARN, Spark on YARN, Prometheus/Grafana 기반 모니터링 구성을 완료한 상태입니다.

HA 구성은 적용하지 않았으며, 개인 VM 환경에서 Non-HA 구조로 먼저 구성했습니다.

## 주요 구성 요소

```text
Ansible        - 서버 설정 및 서비스 자동화
Apache Bigtop  - Hadoop/Spark 패키지 repository
Hadoop HDFS    - 분산 파일 시스템
YARN           - 리소스 관리 및 작업 실행
Spark on YARN  - Spark 작업 실행 환경
systemd        - Hadoop/YARN/Spark 서비스 관리
Prometheus     - 메트릭 수집
Grafana        - 메트릭 시각화
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
│   └── 16-jmx-exporter.yml
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
    └── jmx_exporter/
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

Hadoop/YARN/Spark 설정에서는 `vm1`, `vm2` 같은 임시 이름 대신 실제 클러스터 hostname인 `master`, `worker1`, `worker2`, `worker3`를 사용합니다.

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
Node Exporter: all nodes:9100
```

Prometheus와 Grafana는 `worker3`에서 실행합니다.
Node Exporter는 전체 노드에 설치하여 CPU, Memory, Disk, Network 등 OS 메트릭을 수집합니다.

상태 확인:

```bash
ansible all -b -m command -a "systemctl is-active prometheus-node-exporter"
ansible ops -b -m command -a "systemctl is-active prometheus"
ansible ops -b -m command -a "systemctl is-active grafana-server"
```

Prometheus health check:

```bash
ansible ops -m shell -a "curl -s http://127.0.0.1:9090/-/healthy"
```

Prometheus Targets:

```text
http://worker3:9090/targets
```

Grafana:

```text
http://worker3:3000
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

### hostname 불일치 문제

Hadoop 설정에는 inventory alias가 아니라 실제 클러스터 hostname을 사용합니다.

```text
master
worker1
worker2
worker3
```

### Prometheus YAML 오류

Prometheus 설정 파일은 YAML 들여쓰기에 민감합니다.
문제가 발생하면 다음 명령으로 확인합니다.

```bash
ansible ops -b -m shell -a "promtool check config /etc/prometheus/prometheus.yml"
ansible ops -b -m shell -a "cat -n /etc/prometheus/prometheus.yml"
```

### JMX Exporter duplicate metric 문제

JMX Exporter 설정에서 JVM 기본 메트릭과 같은 이름을 직접 정의하면 duplicate metric 오류가 발생할 수 있습니다.

예시:

```text
duplicate metric name: jvm_threads_current
```

이 경우 `java.lang` 기반 JVM metric rule을 제거하고 Hadoop/Spark 관련 rule만 별도로 관리합니다.

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
```

## 향후 계획

```text
1. Grafana dashboard 정리
2. Hadoop/YARN/Spark 주요 metric 패널 구성
3. Alertmanager 구성
4. HDFS/YARN 장애 상황별 alert rule 작성
5. HA 구성 설계 문서화
6. Ansible role 리팩토링
7. README 및 운영 문서 보강
```
