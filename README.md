# Bigtop Cluster Ansible

Apache Bigtop과 Ansible을 사용해 Hadoop/HDFS 기반 미니 데이터 플랫폼을 자동화 방식으로 구성하는 프로젝트입니다.

개인 VM 환경에서 Ambari 같은 통합 관리 도구 없이, Ansible playbook과 role 구조를 통해 서버 공통 설정, Java 설치, Bigtop repository 등록, Hadoop/HDFS 설치, HDFS 설정 배포, 서비스 기동까지 자동화하는 것을 목표로 합니다.

## 구성

현재 클러스터는 4대 VM 기준입니다.

```text
master   - HDFS NameNode
worker1  - HDFS DataNode
worker2  - HDFS DataNode
worker3  - Ansible Control Node / Ops Node
```

현재는 HDFS Non-HA 구성을 먼저 진행합니다.
YARN, Spark, Prometheus/Grafana 구성은 이후 단계에서 확장할 예정입니다.

## 주요 구성 요소

```text
Ansible      - 서버 설정 및 서비스 자동화
Apache Bigtop - Hadoop 패키지 repository
Hadoop HDFS  - 분산 파일 시스템
systemd      - Hadoop 서비스 관리
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
│   └── 06-hdfs-stop.yml
└── roles/
    ├── common/
    ├── java/
    ├── bigtop_repo/
    ├── hadoop/
    ├── hdfs_config/
    └── hdfs_service/
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

## 실행 순서

```bash
ansible-playbook playbooks/01-common.yml
ansible-playbook playbooks/02-java.yml
ansible-playbook playbooks/03-bigtop-repo.yml
ansible-playbook playbooks/04-hadoop-install.yml
ansible-playbook playbooks/05-hdfs-config.yml
ansible-playbook playbooks/06-hdfs-start.yml
```

HDFS 중지:

```bash
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

Hadoop 설정에서는 `vm1`, `vm2` 같은 임시 이름 대신 실제 클러스터 hostname인 `master`, `worker1`, `worker2`를 사용합니다.

## 상태 확인

NameNode 상태 확인:

```bash
ansible master -b -m command -a "systemctl is-active hadoop-hdfs-namenode"
```

DataNode 상태 확인:

```bash
ansible workers -b -m command -a "systemctl is-active hadoop-hdfs-datanode"
```

HDFS report 확인:

```bash
ansible master -b -m shell -a "sudo -u hdfs hdfs dfsadmin -report"
```

정상 상태에서는 `Live datanodes (2)`가 출력됩니다.

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
```

## 향후 계획

```text
1. HDFS 구성 안정화
2. YARN ResourceManager / NodeManager 구성
3. Spark on YARN 구성
4. Spark History Server 구성
5. Prometheus / Grafana 모니터링 구성
6. HA 구성 설계 문서화
```

