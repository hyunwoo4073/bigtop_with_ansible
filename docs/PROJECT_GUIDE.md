# Bigtop Data Platform — 상세 아키텍처·운영·Playbook 가이드

> **문서 범위:** 2026-10-31까지의 01~54번 단계. 45~52번 Olist 운영 파이프라인 및 53~54번 Iceberg 별도 실습을 추가했다.  
> **빠른 개요:** [프로젝트 README](../README.md)  
> **이 문서의 목적:** 처음 보는 사람이 왜 이 플랫폼을 구축했는지, 각 서버와 컴포넌트가 왜 필요한지, 어떤 Playbook이 무엇을 바꾸는지, 일상 운영·복구·다음 개발을 어떻게 이어갈지 이해하도록 한다.

## 0. 확인 근거와 현재 상태

이 문서는 *기존 `README.md`, 기존 31~44번 안내, 45~54번 ZIP의 Playbook·Role·운영 문서, 대화에서 공유된 실제 성공/실패 로그와 사용자 검증 확인*을 기반으로 갱신했다. 기존 README의 수백 개 작업 일지를 그대로 늘어놓는 방식이 아니다.

**제약:** worker3의 현재 Git 작업 트리 전체를 이 문서 작성 환경에서 직접 열람하지는 않았다. 따라서 아래 목록의 파일명·구현 구조는 *확인 가능한 버전의 소스 기준*이고, 실제 노드의 세부 설정/버전/현재 가동 상태와 다를 수 있다. 다음과 같이 분리해서 해석한다.

- **사용자 확인:** Docker 40, OpenMetadata 설치·UI 로그인 41, PostgreSQL 카탈로그 수집 42, UI 바로가기 수정 39, Web Console 백업 대시보드 37 등. 42번은 최종 수정본(v3) 적용 후 실제 수집 성공을 확인했다.
- **진행되었다고 대화에서 이어간 기능:** 43번 분류·용어집 구성. 단위 테스트와 YAML 형식 문제 수정은 확인되지만, 실제 OpenMetadata에 적용한 systemd 로그/화면 확인은 공유되지 않았다.
- **별도 검증 필요:** 38번 PostgreSQL 원격 복제는 최초 Ansible 실패 후 수정 패키지를 제공했지만 최종 worker2 수신·SHA-256 검증 성공 로그가 없다. **44번은 배포 패키지를 제공한 상태이며 실제 적용 결과가 없다.**
- **45~54번의 확인 근거:** Olist Raw, Spark ETL v1/v3, Quality 및 v3 승인, OpenMetadata Container 계보 UI, Iceberg 53번 `status=PASS`와 55,877건 일치, 54번 stage/publish 사용자 확인. 48번 Timer 활성화·Alert 활성화, 51번 Timer 활성화 등은 별도 확인 대상이다.
- **HA 범위:** 단일 NameNode/ResourceManager 기반의 실습 구성이며 HA를 구축했다고 주장하지 않는다.

## 1. 프로젝트 목표 및 설계 원칙

### 1.1 해결하려는 문제

Ambari 같은 플랫폼 도구를 사용하지 않고, Hadoop 에코시스템의 설치·환경 구성·장애 감지·복구·데이터 거버넌스까지 *직접 관리 가능한 작은 시스템*을 만드는 것이 목적이다.

- **재현성:** VM이 새로 준비되면 Ansible Inventory와 Role로 동일한 환경을 재구성한다.
- **관측 가능성:** 서버가 떠 있는 것뿐 아니라 HDFS/YARN 기능, Spark 처리, 파일 시스템·메트릭·백업 상태를 별도 검증한다.
- **운영 작업 표준화:** `make`, Playbook, 중앙 Console, Runbook으로 반복 명령을 통일한다.
- **데이터 보존:** HDFS NameNode 메타데이터, Web Console PostgreSQL, OpenMetadata MySQL/검색 인덱스는 서로 다른 백업·복구 전략이 필요함을 인식한다.
- **데이터 거버넌스:** 서버 운영 정보와 실제 데이터셋·스키마·소유자·계보 정보를 구분해 관리한다.
- **역할 분리:** Hadoop 처리 노드, 운영 Controller, 거버넌스 노드를 분리한다.

### 1.2 왜 Ansible + Bigtop인가?

**Ansible**은 각 노드에 전용 에이전트를 설치하지 않고 SSH와 Python으로 OS·패키지·설정·systemd를 선언적으로 관리한다. Playbook은 적용 순서를, Role은 서비스 단위 구현을 담당한다.

**Apache Bigtop**은 Hadoop/Spark 등 빅데이터 구성요소의 배포 패키지를 제공한다. 이 프로젝트는 Bigtop 패키지를 Ansible이 설치·설정·기동하므로, 기본 Hadoop 서비스 구축에 Docker를 사용하지 않는다. 반면 OpenMetadata는 별도의 VM에서 Docker Compose로 실행한다.

**구별할 점:** Ansible 자체가 클러스터 상태를 지속 감시하는 Daemon은 아니다. 지속 모니터링은 Prometheus/systemd timer, 요청 시 서비스 조치는 Web Console이 수행한다.

## 2. 물리·논리 아키텍처

### 2.1 VM 및 인벤토리

| 호스트 | 확인된 실습 주소 | 할당 자원 | 핵심 역할 | 그룹 |
|---|---|---|---|---|
| `master` | `192.158.2.100` | 2 vCPU / 4GB | NameNode, ResourceManager, Spark History | `master`, `hadoop_cluster` |
| `worker1` | `192.158.2.101` | 2 vCPU / 4GB | DataNode, NodeManager | `workers`, `hadoop_cluster` |
| `worker2` | `192.158.2.102` | 2 vCPU / 4GB | DataNode, NodeManager, 38번 DB 백업 수신 대상 | `workers`, `hadoop_cluster` |
| `worker3` | `192.158.2.103` | 2 vCPU / 4GB | Ansible Controller, Web Console, PostgreSQL, Prometheus, Grafana, Alertmanager | `ops` |
| `metadata` | `192.158.2.104` | 4 vCPU / 8GB | Docker, OpenMetadata, MySQL, Elasticsearch | `governance_nodes` |

물리 PC: 6코어/12논리 프로세서, RAM 32GB. VM 총 할당 RAM은 24GB라 호스트 OS에 계산상 8GB가 남지만 실제 사용 가능 메모리는 VM/호스트 오버헤드와 동시 실행 작업에 따라 달라진다. 가상 CPU 12개의 합계는 물리 코어 12개를 뜻하지 않는다.

> **네트워크 주의:** `192.158.2.0/24`는 RFC1918 사설 대역이 아니다. 이 문서의 IP는 사용자가 확인한 실습 주소이며, 외부 라우팅 노출을 의미하지 않는다. 실제 네트워크 경계·방화벽·VirtualBox NAT/Host-only 설정을 점검할 것.

Inventory 구조의 핵심 예시는 다음과 같다. 실제 자격 증명·개인키는 저장소에 커밋하지 않는다.

```ini
[master]
master ansible_host=192.158.2.100

[workers]
worker1 ansible_host=192.158.2.101
worker2 ansible_host=192.158.2.102

[ops]
worker3 ansible_host=192.158.2.103

[hadoop_cluster:children]
master
workers

[governance_nodes]
metadata ansible_host=192.158.2.104

[all:vars]
ansible_user=bae
ansible_ssh_private_key_file=~/.ssh/id_rsa
ansible_python_interpreter=/usr/bin/python3
```

`master`가 그룹명과 호스트명으로 중복되어 Ansible 경고가 발생할 수 있다. 해당 경고와 실제 Inventory 그룹 누락 문제는 별개다. `metadata`를 `[workers]` 또는 `[hadoop_cluster]`에 추가하면 Hadoop 설치/설정이 잘못 적용될 수 있으므로 분리한다.

### 2.2 데이터 처리 흐름

```text
[HDFS client / Spark application]
               │
               ├── HDFS namespace 질의 ──────► master: NameNode
               │                                  └─ DataNode 위치 안내
               └── 실제 블록 읽기·쓰기 ───────► worker1/worker2: DataNode

[Spark submit]
      │
      └── YARN RM(master)에 작업 제출
              └── NodeManager(worker1/worker2)에 컨테이너 할당
                     └── Spark application 수행
                            └── event log → HDFS → Spark History(master)
```

- **HDFS**: NameNode가 파일·블록의 메타데이터를 관리하고 DataNode가 실제 블록을 저장한다.
- **YARN**: ResourceManager가 클러스터 자원을 조정하고 NodeManager가 작업 컨테이너 실행을 담당한다.
- **Spark**: YARN의 자원을 빌려 작업을 실행한다. Spark History는 완료된 작업의 event log를 시각화한다.
- **Capacity Scheduler**: `default`/`batch`/`adhoc` 같은 큐로 자원 사용을 분리해 특정 유형 작업의 독점을 줄인다.
- **Log Aggregation**: 애플리케이션 로그를 HDFS 등에 집약하여 컨테이너 종료 후에도 `yarn logs`로 조회한다.

이 구성은 **실습용 비HA** 환경이다. NameNode의 메타데이터 디렉터리를 복수로 만드는 것은 같은 노드 내 저장경로 보호이며 다른 VM에서의 자동 페일오버가 아니다.

### 2.3 Olist 배치·품질·계보·승인 흐름 (45~54)

```text
Kaggle Olist CSV(9개)
  └─45 HDFS Raw v1 + _MANIFEST.json
       └─46 Spark YARN Cluster (4개 CSV)
            └─Warehouse v1/v2/v3 (Curated 5 · Quarantine 1 · Mart 1)
                 ├─47 Quality 검증 → quality/<version>/runs/<id>/_QUALITY_REPORT.json
                 ├─50 OpenMetadata Container 및 수동 변환 관계
                 ├─51 v3 _RUNTIME_LINEAGE.json → 실제 파일 입출력 계보
                 └─52 v3 최신 PASS + Manifest digest 확인 → published/v3/_APPROVAL.json

48 systemd (46→47) ── worker3 olist_pipeline.prom ─┐
47 품질 결과 ─────── worker3 olist_quality.prom ────┼─ 49 Prometheus / Grafana
                                                   └─ 대시보드 기본 버전 v1

53 Iceberg Hadoop Catalog `lab` (기존 v3 Mart 별도 복사)
 └─54 별도 Iceberg 실습 테이블: UPDATE/DELETE/MERGE, 스키마 진화, audit Branch WAP
```

**버전은 입력 데이터 도착일이 아니라 출력 스냅샷의 식별자:** Raw는 모두 v1, Warehouse v1은 최초 정상 처리, v2는 실행 계보 없이 게시, v3는 실행 계보 생성·품질 PASS·승인에 사용했다. 48~49번의 기본 버전은 v1이므로 v3의 최신 상태를 자동으로 표시한다고 가정하지 않는다.

### 2.4 관측·운영·백업 흐름

```text
Linux/디스크 ──► Node Exporter ────┐
Hadoop JVM ───► JMX Exporter ─────┤
백업·디스크 ──► textfile collector ├──► Prometheus ──► Grafana
                                  │          └────────► Alertmanager
                                  │
                            Web Console Backend ──► React UI
                            (상태·메트릭·로그·조치)

NameNode(master) ── fsimage/edits 보호 및 online backup ──► worker3 백업
Web Console DB(worker3) ── pg_dump + 복원 검증 ──► worker3 로컬 백업
                                              └─(38)─► worker2 원격 복제 후보
```

- **Node Exporter**: CPU, RAM, 디스크 등의 호스트 메트릭.
- **JMX Exporter**: Hadoop/YARN/Spark의 JVM 및 서비스 내부 메트릭. OS 지표만으로는 NameNode 블록 문제나 RM 상태를 설명하기 어렵다.
- **Prometheus**: 메트릭 Pull 수집, PromQL 평가 및 Alert Rule 처리.
- **Grafana**: 저장소가 아니라 Prometheus 데이터의 Dashboard/시각화 계층.
- **Alertmanager**: Prometheus가 firing으로 평가한 알림의 라우팅/그룹화/전송. Slack receiver는 선택 구성.
- **systemd timers**: 백업·메트릭 수집을 지정 시간에 반복 실행. Ansible은 timer 설정을 배포하지만 직접 매 시각 실행하는 것은 systemd다.
- **Web Console**: 데이터 처리 엔진이나 메트릭 저장소를 대체하지 않는다. 운영자가 상태·로그·작업 내역을 한곳에서 조회하는 UI다.

### 2.5 거버넌스 흐름 — 운영 Inventory와 데이터 카탈로그의 차이

```text
Ansible Inventory ──► (32) PostgreSQL platform_hosts / platform_service_instances
                               │
                          (33) Web Console Inventory 조회
                               │
                               └── (42) PostgreSQL 메타데이터만 수집
                                         │
                                         ▼
                                OpenMetadata (metadata VM)
                                ├─ MySQL: 메타데이터·설정
                                ├─ Elasticsearch: 자산 검색
                                ├─ (43) Glossary/Tags/Descriptions
                                └─ (44 예정) PostgreSQL View Lineage
```

**`platform_hosts`는 실제 호스트 인벤토리 기록**이고 **OpenMetadata 카탈로그의 테이블 자산**은 그 기록을 포함한 데이터베이스 객체 자체를 설명한다. 두 메타데이터 시스템은 저장소도 책임도 다르다.

42번의 수집기는 worker3에서 PostgreSQL `127.0.0.1:5432`를 읽고 `metadata:8585/api`로 보낸다. PostgreSQL 원격 포트를 새로 열 필요가 없다. **현재 이 워크플로에는 Airflow DAG나 범용 OpenLineage Spark Listener가 없다.** 51번은 Olist ETL에 내장한 실행 근거 수집이며 둘은 다르다.

## 3. 컴포넌트 선정 이유 / 설치 위치 / 책임

| 계층 | 컴포넌트 | 위치 | 왜 필요한가 | 대체하거나 혼동하면 안 되는 것 |
|---|---|---|---|---|
| 구축 | Ansible | worker3 → 각 VM | 자동 설치/수정, 환경 재현 | 지속 모니터링 Daemon이 아님 |
| 패키지 | Apache Bigtop | Hadoop 노드 | Hadoop/Spark 패키지 저장소 | 별도 실행 서비스가 아님 |
| 기반 | Java/JVM | Hadoop 노드 | Hadoop/YARN/Spark 실행 | Python-only 서버와 다름 |
| 저장 | HDFS NameNode | master | 파일 네임스페이스, 블록 위치 관리 | DataNode의 실제 파일 블록 아님 |
| 저장 | HDFS DataNode | worker1/2 | 분산 블록 저장·복제 | NameNode 메타데이터를 대체하지 않음 |
| 자원 | YARN RM | master | 클러스터 자원 배분, 큐 스케줄링 | 작업을 실제 실행하는 NM과 구분 |
| 자원 | YARN NM | worker1/2 | 컨테이너 시작, 로컬 디스크·로그 | RM의 스케줄러 역할과 구분 |
| 처리 | Spark on YARN | 클러스터 | 병렬 데이터 처리 | 자체 독립 클러스터 모드와 다름 |
| 테이블 포맷 실습 | Apache Iceberg 1.10.1 | JAR master, 실행은 YARN | HDFS Parquet 데이터 파일의 스냅샷·ACID 커밋·Time Travel·Branch WAP 검증 | Spark·YARN 대체 아님, 기존 Olist Warehouse 마이그레이션 아님 |
| 이력 | Spark History | master | Spark event log 재생·분석 | 실행 중 Spark Driver와 다름 |
| 모니터링 | Node Exporter | 각 대상 노드 | OS/디스크 상태 수집 | Hadoop JVM 내부 메트릭은 JMX 필요 |
| 모니터링 | JMX Exporter | Hadoop/JVM 프로세스 | Hadoop/YARN/Spark 내부 지표 | 애플리케이션 로그 자체는 아님 |
| 모니터링 | Prometheus | worker3 | 메트릭 보관, Alert Rule 평가 | Grafana는 저장소 아님 |
| 시각화 | Grafana | worker3 | 성능·용량·장애 추세 분석 | 운영 action 도구는 아님 |
| 알림 | Alertmanager | worker3 | 장애 알림 라우팅 및 통지 | Prometheus가 평가한 alert를 수신 |
| 운영 UI | Nginx | worker3 | Basic Auth와 프록시 제공 | 앱 비즈니스 로직은 FastAPI |
| 운영 UI | FastAPI/Uvicorn | worker3 | 상태/로그/action/이력/API 제공 | Hadoop/YARN 자체 UI와 다름 |
| 운영 UI | React/Vite | worker3 | Home, Services, Grid, Incidents, Admin UI | 백엔드 없이 직접 root 명령 수행 X |
| 운영 DB | PostgreSQL | worker3 | Action History, 클러스터 메타데이터 | OpenMetadata의 내부 DB와 별개 |
| 백업 | pg_dump/pg_restore | worker3 | 논리 백업 및 임시 DB 복원 검증 | PostgreSQL 전체 인스턴스 물리 백업 아님 |
| 백업 | rsync/SSH | master→worker3, (38) worker3→worker2 | 노드 장애 대비 백업 파일 이동 | 복구 성공 자체를 보장하지 않음 |
| 거버넌스 | Docker/Compose | metadata | 별도 서버의 다중 컴포넌트 구동·재현 | 기존 Hadoop 설치에는 사용하지 않음 |
| 거버넌스 | OpenMetadata Server | metadata | 카탈로그·용어집·소유권·계보 API/UI | 인증/DB 권한 집행 도구를 대체하지 않음 |
| 거버넌스 | MySQL | metadata Docker | OpenMetadata 앱의 영속 데이터 | worker3 PostgreSQL과 별개 |
| 거버넌스 | Elasticsearch | metadata Docker | 자산 검색·인덱싱 | 데이터 레이크 자체가 아님 |
| 수집 | OpenMetadata Python Ingestion | worker3 | PostgreSQL 카탈로그 수집·API 전송 | Airflow 자동 DAG가 아닌 외부 CLI 수집 |

### 3.1 왜 OpenMetadata에 MySQL과 Elasticsearch까지 있나?

OpenMetadata Server 하나만으로는 카탈로그를 영속 저장하고 검색하기 어렵다. **MySQL은 엔티티·설정의 영속 저장소**, **Elasticsearch는 자산 검색 인덱스**, **OpenMetadata 서버는 API와 웹 애플리케이션**을 담당한다. 서버 기동 전 데이터베이스 스키마 마이그레이션도 필요하다. 41번은 세 컴포넌트와 마이그레이션을 Compose로 구성하고, `/opt/bigtop-governance/openmetadata/.env`의 비밀번호를 최초 한 번 생성해 보존한다.

Docker가 서비스를 줄이거나 메모리 문제를 해결하는 것은 아니다. `metadata` VM 8GB RAM이므로 Airflow 별도 컨테이너를 추가하지 않고 worker3의 수집 CLI를 사용했다. 대량 프로파일링/수집 시 메모리 경합을 모니터링한다.

### 3.2 왜 기존 운영용 PostgreSQL을 그대로 OpenMetadata 내부 DB로 쓰지 않나?

운영 콘솔의 이벤트 기록·호스트/서비스 인벤토리와 OpenMetadata의 데이터 자산 카탈로그는 **서로 다른 생명주기와 스키마**를 가진다. 41번은 OpenMetadata 전용 MySQL을 별도 VM에 두어 DB 업그레이드·백업·보안 범위를 분리했다. 42번은 worker3 PostgreSQL 자체를 *수집 대상*으로 읽는다. 이 흐름을 단일 DB로 오해하면 안 된다.

## 4. 소스 코드 구조와 변경 책임

```text
bigtop-cluster-ansible/
├── README.md                         # 프로젝트 개요 (이 문서의 진입점)
├── docs/PROJECT_GUIDE.md             # 전체 설계·Playbook 안내 (이 파일)
├── docs/runbooks/                   # 장애·복구 실무 절차
├── docs/web_console_*.md            # 31~39번 운영 문서
├── docs/4*-governance-*.md           # 40~44번 단계별 문서 (파일명은 실제 저장소 기준)
├── inventory/
│   ├── hosts.ini.example             # 공개 가능한 Inventory 예시
│   ├── hosts.ini                     # 실제 장비/IP, Git 제외 권장
│   └── group_vars/all.yml(.example)  # 기본값 덮어쓰기, 실값 관리
├── playbooks/                       # 배포/관리 진입점
├── roles/                           # 컴포넌트별 Ansible Role
│   ├── hdfs_config/, yarn_config/, spark_config/
│   ├── node_exporter/, jmx_exporter/, prometheus/, grafana/, alertmanager/
│   ├── web_console/                  # FastAPI / React / Nginx
│   ├── web_console_postgres/, web_console_metadata/
│   ├── web_console_pg_backup/, web_console_pg_observability/
│   ├── web_console_pg_backup_alerts/, web_console_pg_remote_replica/
│   ├── web_console_service_ui_links/
│   ├── governance_docker_bootstrap/, governance_openmetadata/
│   ├── governance_postgres_catalog/, governance_catalog_enrichment/
│   ├── governance_lineage/          # 44번 제공 패키지 (배포 미확인)
│   ├── data_pipeline_olist_raw/, data_pipeline_olist_etl/
│   ├── data_pipeline_olist_quality/, data_pipeline_olist_scheduler/
│   ├── data_pipeline_olist_observability/, data_pipeline_olist_catalog/
│   ├── data_pipeline_olist_runtime_lineage/, data_pipeline_olist_promotion/
│   └── iceberg_spark_lab/, iceberg_transactions_lab/
├── vendor/iceberg/                   # 53 JAR의 로컬 다운로드 위치 (Git 제외)
├── collections/requirements.yml     # community.postgresql 등 Collection
├── requirements-dev.txt             # 개발 lint·Ansible 패키지
├── scripts/, tests/                 # 정적 검사·운영 검사·단위 테스트
├── Makefile                         # 자주 쓰는 운영 명령
└── ansible.cfg, .ansible-lint, .yamllint
```

**수정 시 기준**: 앱 구현은 `roles/web_console/...` Git 소스를 수정하고, Ansible 변수를 통해 배포한다. `/opt/...`, `/etc/...` 하위의 Ansible이 관리하는 런타임 파일을 직접 수정하면 재배포 시 덮어써질 수 있다. ZIP 기반 `apply.sh`는 과거 기능 병합에 사용된 *한 번 실행하는 소스 통합 보조도구*이며, 정상적으로 통합된 이후에는 Git에서 관리하는 **원본 소스와 Playbook**이 기준이다.

## 5. 모든 Playbook의 역할 — 왜 있고 무엇을 하나?

각 표의 `대상`은 역할상 주 대상이다. 실제 Ansible `hosts`, `when`, `delegate_to`, import 관계는 반드시 **현재 저장소 Playbook 소스**를 최종 기준으로 확인한다. **모든 파일을 순서대로 매번 실행해야 한다는 뜻이 아니다.** 구축/설정 Playbook, 상태 조회 Playbook, 복구 Playbook, 서비스 중지 Playbook은 목적이 다르다.

### 5.1 기반 환경 및 HDFS/YARN/Spark: 01~12

| Playbook | 주요 대상 | 주요 동작 | 존재 이유 / 주의 |
|---|---|---|---|
| `01-common.yml` | Hadoop 노드 | 호스트 공통 패키지·OS 기본 설정 | 노드마다 기본 환경이 달라 생기는 문제를 줄임 |
| `02-java.yml` | Hadoop 노드 | Java 런타임 준비 | Hadoop, YARN, Spark JVM 구동 의존성 |
| `03-bigtop-repo.yml` | Hadoop 노드 | Apache Bigtop APT repository 설정 | 일관된 Hadoop/Spark 패키지 공급원 확보 |
| `04-hadoop-install.yml` | Hadoop 노드 | HDFS/Hadoop 패키지 설치 | 설정과 기동에 앞서 실행 바이너리 확보 |
| `05-hdfs-config.yml` | NameNode·DataNode | `core-site.xml`, `hdfs-site.xml` 등 HDFS 구성 관리 | 네임스페이스, 복제, 데이터·메타데이터 경로 통일 |
| `06-hdfs-start.yml` | NameNode·DataNode | HDFS 서비스 시작 | NameNode와 DataNode 연결, 블록 서비스 활성화 |
| `06-hdfs-stop.yml` | NameNode·DataNode | HDFS 서비스 정지 | 계획된 중지에 사용; **데이터 I/O에 영향** |
| `07-yarn-install.yml` | RM·NM | YARN 패키지 설치 | 분산 작업 리소스 관리 기반 |
| `08-yarn-config.yml` | RM·NM | `yarn-site.xml`, 스케줄러, NM 설정 관리 | Queue 자원 분배·실행 환경 표준화 |
| `09-yarn-start.yml` | RM·NM | ResourceManager/NodeManager 시작 | Spark on YARN 작업 수행 전제 |
| `09-yarn-stop.yml` | RM·NM | YARN 정지 | 실행 중인 YARN 애플리케이션에 영향 |
| `10-spark-install.yml` | Spark 적용 노드 | Spark 패키지 설치 | YARN 위에서 분산 처리 실행 |
| `11-spark-config.yml` | Spark 적용 노드 | Spark 설정, 이벤트 로그, submit 프로파일 등 | 재현 가능한 리소스·로그 설정 확보 |
| `12-spark-history-start.yml` | master | Spark History Server 구동 | 완료된 Spark 애플리케이션 분석 |
| `12-spark-history-stop.yml` | master | Spark History Server 정지 | History UI 중지; Spark 전체 클러스터 중지와 구별 |

**구축 의존성:** `공통 OS → Java → Bigtop → HDFS → YARN → Spark`가 기본 순서다. 이미 실행 중인 환경에서 이 목록을 처음부터 재실행할 필요는 없다. 특히 NameNode 초기화(`format`)는 일상 재배포 작업에 포함되어선 안 된다.

### 5.2 관측, 경고, HDFS 메타데이터 백업: 13~19

| Playbook | 주요 대상 | 수행 기능 | 도입 이유 |
|---|---|---|---|
| `13-node-exporter.yml` | 서버 노드 | Node Exporter, textfile collector 준비 | CPU/RAM/파일시스템 및 별도 스크립트 지표를 Prometheus에 노출 |
| `14-prometheus.yml` | worker3 | scrape 설정, recording/alert rule 등 관리 | 수집한 메트릭의 중앙 조회·알림 평가 |
| `15-grafana.yml` | worker3 | Grafana 및 datasource/dashboard provisioning | HDFS/YARN/Spark/백업/용량 상황을 한눈에 시각화 |
| `16-jmx-exporter.yml` | JVM 실행 노드 | JMX Exporter 설정·실행 연동 | NameNode/RM/NM/Spark의 내부 메트릭 확보 |
| `17-alertmanager.yml` | worker3 | Alertmanager 설치 및 라우팅 구성 | Prometheus firing 알림을 수신 채널로 전달 |
| `18-namenode-backup.yml` | master | NameNode metadata online backup, 검증, timer | 단일 NN 장애 또는 FSImage 손상 시 복구 지점 제공 |
| `19-namenode-remote-backup.yml` | master + worker3 | NN 메타데이터 백업을 worker3로 복제·체크섬 확인 | master 단독 장애에 대비한 다른 노드 보관 |

**구조 차이:** NameNode 백업(18·19)은 **HDFS 네임스페이스** 보호가 목적이다. PostgreSQL 백업(34·38)은 **Web Console 운영 DB** 보호가 목적이다. OpenMetadata의 MySQL/Elasticsearch 백업은 **아직 별도 체계가 확인되지 않음**.

### 5.3 검증·복구·운영 기능: 20~27

| Playbook | 주요 대상 | 수행 기능 | 필요 이유 / 주의 |
|---|---|---|---|
| `20-cluster-health-check.yml` | 클러스터 + 모니터링 | HDFS/YARN/Spark/Exporter/Monitoring 서비스 상태 및 설정·파일 존재 등 조회 | 서비스가 시작됐는지 일괄 확인. 반드시 기능 성공까지 보장하진 않음 |
| `21-cluster-recover.yml` | 클러스터 | 중지·실패 상태 서비스의 재기동 보정 | VM 재부팅 후 의도치 않은 서비스 중단 복구. **상태를 변경**함 |
| `22-cluster-smoke-test.yml` | HDFS/YARN/Spark | HDFS 쓰기·읽기 및 SparkPi on YARN 테스트 | `systemctl active`와 실제 기능 정상 동작을 구별 |
| `23-yarn-app-diagnostics.yml` | YARN/Spark | 애플리케이션 로그·상태·큐 지표를 진단 자료로 모음 | 장애 분석 시 정보 수집 시간 단축; APP_ID 필요 |
| `24-yarn-log-aggregation.yml` | YARN | 컨테이너 로그 HDFS 집약 정책 | 종료 후 `yarn logs -applicationId`로 이력 조회 |
| `25-spark-history-maintenance.yml` | Spark/HDFS | Spark event log 보관·정리 정책 및 용량 점검 | 무제한 로그 누적으로 HDFS가 차는 문제 방지 |
| `26-yarn-nodemanager-storage.yml` | worker1/worker2 | NM local/log 디렉터리, 디스크 헬스 체크 설정 | YARN 컨테이너 실행에 필요한 디스크 경로 오류·고갈 방지 |
| `27-yarn-nodemanager-storage-metrics.yml` | NodeManager + 관측 | NM storage 메트릭·알림·Grafana 대시보드 연계 | NM disk health를 운영자가 지속적으로 확인 |

이 시기의 `Makefile`은 `health`, `recover`, `smoke`, `post-reboot`, `diagnostics`, `monitoring`, `nm-storage` 등 반복 작업을 표준화한다. 특히 `make post-reboot`는 *ping → recovery → health → smoke* 순서를 의도하며 **단순 조회보다 더 강한 조치**라는 점을 이해해야 한다.

### 5.4 중앙 Web Console 및 영속 저장: 30~33

| Playbook | 대상 | 무엇을 배포하나 | 도입 이유·의존 관계 |
|---|---|---|---|
| `30-web-console.yml` | worker3 | Nginx Basic Auth, FastAPI/Uvicorn, React/Vite, systemd | Ambari 없는 소형 클러스터에서 통합 상태·로그·서비스 제어 UI 제공 |
| `31-web-console-postgres.yml` | worker3 | PostgreSQL 서버/DB/계정, Action History 영속화, JSONL backfill·동기화 | 콘솔 재시작 후에도 실행 이력 보존. 기존 `web_console` Role도 재배포 |
| `32-web-console-metadata.yml` | worker3 | Ansible Inventory + Console 서비스 레지스트리를 PostgreSQL에 동기화 | 호스트·서비스 구성의 검색 가능한 스냅샷 보유; **31번 선행 import** |
| `33-web-console-inventory.yml` | worker3 | `/api/metadata` 및 React Cluster Inventory 조회 UI 배포 | 운영자가 선언된 호스트/서비스 구성을 조회; **32번을 활용해 최신화** |

**중요 개념:** `32`는 *Ansible 선언 정보*를 운영 DB에 스냅샷으로 쓰는 것이고, 현재 서비스의 `active/inactive` 진실값을 DB에 영구 저장하는 기능이 아니다. 실시간 상태 확인은 Web Console의 별도 API·systemd 조회 영역이다. 사라진 호스트/서비스는 즉시 삭제하지 않고 `is_present=false`로 표시하도록 설계했다.

**추가 Console 기능:** `Home`, `Services`, `Grid`, `Incidents`, `Admin`, `Action History`, `Runbooks`, `SelfCheck`, `Service Details`가 단계적으로 추가됐다. Start/Stop/Restart는 최종 `systemctl ActiveState`를 확인해 이력에 기록하도록 개선했으며, Nginx/Console 자체 Stop 등 위험한 조치는 제한한다. Incidents의 `?incident_demo=1`는 테스트 UI로 운영 서비스를 멈추는 기능이 아니다.

### 5.5 PostgreSQL 백업·알림·서비스 바로가기: 34~39

| Playbook | 대상 | 기능 | 왜 필요한가 / 주의 |
|---|---|---|---|
| `34-web-console-postgres-backup.yml` | worker3 | `pg_dump -Fc`, 매일 백업, 로컬 14일 보관, 임시 DB 복원 검증 도구 | 운영 DB 유실 대비; **DB를 재생성하지 않음** |
| `35-web-console-pg-backup-observability.yml` | worker3 | 백업 파일 상태·수량·크기·시간·timer를 10분마다 textfile metric으로 게시 | 백업 서비스는 살아 있어도 최근 백업이 없거나 손상될 수 있음. 34번 역할 재사용 |
| `36-web-console-pg-backup-alerts.yml` | worker3 | PostgreSQL 백업용 Prometheus 7개 Alert Rule, 기존 Alertmanager 연동 | 장애·지연 자동 감지. 35번 메트릭이 전제 |
| `37-web-console-pg-backup-dashboard.yml` | worker3 | Admin PostgreSQL Backup 읽기 전용 패널, Prometheus 조회 API | 백업 현황·알림·연결 상태를 중앙 콘솔에서 확인 |
| `38-web-console-pg-remote-replica.yml` | **worker3 + worker2** | 비특권 계정, 전용 SSH/rsync, SHA-256, 원격 30일 보관 | worker3 장애 시 다른 노드의 백업 보존; **실전송 성공 로그 미확인** |
| `39-web-console-service-ui-links.yml` | worker3 | Home/Services에서 관리 Web UI 링크를 API/설정으로 제공 | 서비스별 주소를 외우지 않고 상세 관리 화면으로 이동 |

`38`은 **유일하게 이 구간에서 worker2를 직접 설정**하는 다중 호스트 절차다. Playbook이 worker3 내부 작업과 worker2 원격 작업을 구분하므로, **절대로 전체 실행에 `-e ansible_connection=local`을 붙이지 않는다.** 초기 38번 패키지에서 `roles: tasks_from`/변수 접두사 오류가 있었고 수정본으로 교체됐다. 원격 파일 존재·체크섬·복원 가능 여부는 별도로 검증해야 한다.

`39`의 기본 UI 바로가기는 **NameNode, ResourceManager, Spark History, Prometheus, Grafana, Alertmanager 총 6개**다. DataNode/NodeManager 4개 링크는 사용자 요청으로 제거됐다. 포트도 YARN `18088`, Grafana `23000`, Alertmanager `19093`으로 수정했다. 이 Playbook은 링크 URL을 변경할 뿐 서비스 데몬의 listen 포트를 바꾸지 않는다.

### 5.6 OpenMetadata / Data Governance: 40~44

| Playbook | 대상 | 핵심 기능 | 이유 / 확인 상태 |
|---|---|---|---|
| `40-governance-docker-bootstrap.yml` | **metadata VM** | Docker Engine/Compose Plugin, `vm.max_map_count` 설정 | OpenMetadata 실행환경. 설치 완료 확인 |
| `41-governance-openmetadata.yml` | **metadata VM** | OpenMetadata 2.0.6, MySQL 8.0, Elasticsearch 9.3.0, schema migration, systemd | Data Catalog 서버/저장소/검색. UI 로그인 확인 |
| `42-governance-postgres-catalog.yml` | worker3 | 읽기 전용 PostgreSQL 수집 계정, Ingestion Python 환경, Bot Token 등록 도구, oneshot 수집 서비스 | `bigtop_console.public` 테이블·컬럼을 OpenMetadata에 수집. **최종 수정 v3 후 수집 성공 확인** |
| `43-governance-catalog-enrichment.yml` | worker3 | 분류, 태그, Glossary, 테이블/컬럼 설명 정책, plan/apply | 기계적 스키마 수집을 비즈니스 의미로 확장. **실제 적용 로그는 확인되지 않음** |
| `44-governance-lineage.yml` | worker3 | SQL JOIN View, reader SELECT, Catalog 재수집, OpenMetadata 테이블·컬럼 Lineage PoC | 실제 SQL 변환을 기반으로 계보를 시각화. **패키지만 제공, 실행 여부 미확인** |

**43/44는 자동 생성된 분류·계보를 무조건 쓰지 않는다.** 43은 `--plan`으로 확인 후 기존 사람의 설명·태그를 존중하며 신규 정보만 추가한다. 44는 실제 JOIN View를 만든 뒤 OpenMetadata에 View가 수집된 것을 확인하고 필요한 계보만 등록한다. 외래키는 데이터 이동의 증거가 아니므로 계보로 착각하지 않는다.

**42/43/44의 OpenMetadata API 인증**은 worker3 보호 파일에 저장된 Bot JWT를 사용한다. 토큰을 커맨드 인자로 직접 붙여넣거나 Git에 넣지 않는다. 42번 reader에는 PostgreSQL `CONNECT/USAGE/SELECT` 권한이 있으며 *수집 Workflow 정책상 메타데이터만 수집*하지만, 계정이 기술적으로 SELECT를 통해 행을 읽을 수 있다는 점은 구분한다.

### 5.7 번호·범위에 관한 주의

- 현재 확인 가능한 자료에는 **28번·29번 Playbook이 없다**. 임의로 이름이나 역할을 만들지 않는다.
- `01~27`은 초창기 README에 근거한 설명이고, `30~54`는 각 단계의 패키지 Playbook·Role 문서와 사용자 실습 결과를 함께 검토해 적었다.
- 모든 Playbook이 자동으로 한 번에 실행되는 방식은 아니다. 일부는 서비스 중지·복구·진단이고, 43·44는 수동 적용 의사결정을 요구한다.

### 5.8 실제 Olist 파이프라인과 Iceberg: 45~54

| Playbook | 대상 / 실행 조건 | 하는 일 | 사용자 검증과 경계 |
|---|---|---|---|
| `45-olist-hdfs-raw.yml` | worker3 Controller → master HDFS | 원본 CSV 9개를 Raw v1 불변 스냅샷으로 업로드, 파일 SHA-256 Manifest | 실제 Raw 적재 성공 |
| `46-olist-spark-etl.yml` | master, `spark-submit --deploy-mode cluster` | 4개 CSV → Curated 5/Quarantine 1/Mart 1 Parquet; 신규 출력 버전에서 51번 실행 계보 선택 생성 | v1 처리와 v3 실행 성공, 기존 정상 버전은 제출 전 SKIP |
| `47-olist-data-quality.yml` | master Spark, 메트릭은 worker3 | 29개 검사 PASS/FAIL, HDFS 실행별 리포트, `olist_quality.prom` | v1 성공, v3 승인 근거 검증 |
| `48-olist-pipeline-schedule.yml` | worker3 ops systemd | 46 → 47 순차 실행, `olist_pipeline.prom`, 선택적 Timer·Alert | 수동 실행 성공, Timer/Alert 기본 disabled |
| `49-olist-observability.yml` | worker3 Grafana | 16패널 대시보드 Provisioning, Prometheus 데이터 소스 | API/UI 등록 성공, 날짜 단위 초→밀리초 수정 |
| `50-olist-openmetadata.yml` | worker3 API + master HDFS 점검 | Storage Service `bigtop_hdfs_olist`, CSV/Parquet Container, ETL 코드에 근거한 계보 | v1 1 service·16 container·13 edge 생성, UI 확인; v3 자산 등록 |
| `51-olist-spark-runtime-lineage.yml` | worker3 importer, master HDFS 증빙 | Spark `inputFiles()`+출력 경로의 런타임 증빙을 Container에 연결 | v3 `_RUNTIME_LINEAGE.json` 생성 및 계보 등록; 범용 Listener 아님 |
| `52-olist-quality-promotion.yml` | master HDFS | 최신 PASS, ETL Manifest SHA, 7 `_SUCCESS` 검사 후 승인 증빙 생성 | v3 `_APPROVAL.json` 확인; HDFS ACL 강제 아님 |
| `53-iceberg-hdfs-lab.yml` | master Spark YARN, Iceberg 1.10.1 JAR | Hadoop Catalog·Snapshot·Time Travel, 승인 v3 Mart 읽어 독립 Iceberg 테이블 생성 | `PASS`: snapshot 2개, Mart 55,877건 일치; 원본 미변경 |
| `54-iceberg-transactions-wap.yml` | master Spark YARN, `deploy_only`/`stage`/`publish` | 별도 테이블 UPDATE/DELETE/MERGE/ADD COLUMN, audit branch 검증 후 fast-forward | stage·publish 사용자 검증 완료. 운영 Mart 릴리스 아님 |

각 Playbook은 해당 단계의 `docs/NN-*-apply.md`와 Role `defaults/main.yml`을 기준으로 변수·의존성을 확인한다. 단순 배포 성공과 실제 Spark/품질/브랜치 검증 성공은 구분한다. 특히 50/51/52번 기본 plan과 실제 apply, 53번 `run_smoke=false`와 54번 `deploy_only`를 혼동하지 않는다.

## 6. 배포·재배포 의존성 및 실행 원칙

### 6.1 초도 구축과 현재 운영의 차이

초기 환경 구축 순서를 한 번에 이해하면 다음과 같다.

```text
[기존 Hadoop 클러스터]         [worker3 운영 환경]         [metadata 거버넌스 VM]
01 공통                       13 Exporter                 40 Docker
  ↓                           14 Prometheus                 ↓
02 Java                       15 Grafana                  41 OpenMetadata Core
  ↓                           17 Alertmanager               ↑
03 Bigtop Repo                 ↓                            │ API
  ↓                           30 Web Console               │
04~06 HDFS                     ↓                            │
  ↓                           31 PostgreSQL History         │
07~09 YARN                     ↓                            │
  ↓                           32/33 Inventory              │
10~12 Spark                    ↓                            │
                              34~37 Backup/Monitoring      │
                              38 원격 복제 (worker2)         │
                              39 UI links                  │
                              42 Ingestion CLI ────────────┘
                              43 Metadata Enrichment
                              44 SQL View Lineage (미확인)
                              45 Raw CSV 적재 → 46 Spark ETL → 47 Quality
                                 ├─ 48 systemd / 49 Grafana
                                 ├─ 50 Catalog / 51 Runtime Lineage
                                 └─ 52 v3 Approval
                              53 Iceberg HDFS Lab → 54 Iceberg SQL / WAP
```

위는 **이해를 위한 의존도**다. 실제 31번은 Web Console/DB 통합을 포함하고, 32번은 31번을 import한다. 35번은 34번 Role을 사용하며, 33·37·39번은 Web Console 앱을 재배포할 수 있다. 그러므로 각각을 무작정 병렬 실행하거나 전체에 일괄 `local` 연결을 강제해서는 안 된다.

### 6.2 기본 명령 및 대상 확인

다음 명령은 **worker3의** `~/bigtop-cluster-ansible`에서 실행한다.

```bash
cd ~/bigtop-cluster-ansible

.venv/bin/ansible-inventory -i inventory/hosts.ini --graph
.venv/bin/ansible -i inventory/hosts.ini hadoop_cluster -m ansible.builtin.ping
.venv/bin/ansible -i inventory/hosts.ini governance_nodes -m ansible.builtin.ping

make lint
make validate
```

- `make lint`: yamllint/ansible-lint. 0 failure는 **문법·규칙 검사 성공**을 뜻하며 실제 서비스 기동 성공을 보장하지 않는다.
- `make validate`: 기존 정적 검증 스크립트, Inventory/Collection/Playbook 구문 검증 등을 포함한다. CI 환경에 필요한 `community.postgresql`은 `collections/requirements.yml`에 포함되어 있어야 한다.
- `make health`: 서비스/구성 상태 점검.
- `make smoke`: HDFS IO, SparkPi 등 실제 기능 검사.
- `make recover`/`make post-reboot`: 중지된 서비스의 재기동을 수행하므로 운영 영향이 있다.

### 6.3 `ansible_connection=local` 사용 기준

| 작업 | 실행 방식 | 이유 |
|---|---|---|
| 30~37, 39 (worker3만 대상) | worker3에서 `-e ansible_connection=local`을 사용할 수 있음 | Controller와 대상 호스트가 같음 |
| 42~44 (worker3만 대상) | 기존 Playbook의 `[ops]` 대상과 설정을 따름 | 원격 노드의 OS 변경 없음 |
| **38** (worker3와 worker2) | **전역 local 금지** | worker2에 실제 SSH 접속이 필요 |
| **40, 41** (metadata VM) | **전역 local 금지** | worker3가 metadata VM으로 SSH 접속해야 함 |
| 01~27 (Hadoop/다중 노드) | Inventory 지정, SSH 연결 | master/worker에 실제 설정을 배포해야 함 |

`-K`는 Ansible sudo(`become`) 비밀번호를 프롬프트로 입력받는 옵션이다. metadata의 `bae` 계정이 비밀번호 없는 sudo로 구성되어 있으면 생략 가능하지만, `NOPASSWD: ALL`은 권한 범위가 매우 넓으므로 개인 실습용 구성과 운영 환경을 구별한다.

### 6.4 재배포의 멱등성과 경계

Ansible의 멱등성은 **변경이 불필요하면 다시 변경하지 않는 방향**이라는 뜻이지, 데이터가 무조건 안전하다는 보증은 아니다.

- DB 비밀번호는 처음 한 번 생성·보관하고 재실행 시 재사용하도록 설계됐다.
- 기존 Action History와 JSONL 로그는 삭제하지 않고 누락 이벤트를 반영한다.
- 메타데이터 동기화(32)는 upsert와 `is_present=false`를 사용하며 즉시 삭제하지 않는다.
- 34번 `pg_dump`는 성공한 백업을 검증한 뒤 기존 보관 정책을 적용한다.
- 37/39의 프론트엔드 패치 스크립트는 *소스 통합 단계*에만 사용하고 그 뒤에는 Ansible 원본 파일로 재배포한다.
- 38번은 안전성상 수신 호스트명을 검사한다. 잘못된 노드에 복제를 구성하지 않는다.
- 43번은 사람의 기존 설명·태그·소유자 변경을 보존하는 정책을 우선한다.
- 44번은 실제 View 객체를 새로 만들 수 있으므로 적용 전 변경 영향 및 View 이름 충돌을 확인한다.

### 6.5 45~54번 실행 의존성 및 재현 명령

**worker3** `~/bigtop-cluster-ansible`에서 실행한다. 기본 원칙은 **출력 버전 일치**, `-e ansible_connection=local`을 master/metadata까지 일괄 적용하지 않기, 데이터 삭제·덮어쓰기 금지다.

```bash
# v3 Warehouse에 최신 PASS Quality가 있어야 52번 승인이 가능
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/47-olist-data-quality.yml \
  -e data_pipeline_olist_quality_warehouse_version=v3

# 50번은 v3 Container를 먼저 등록한 뒤, 51번에서 v3 실행 근거를 Import
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/50-olist-openmetadata.yml \
  -e data_pipeline_olist_catalog_version=v1 \
  -e data_pipeline_olist_catalog_warehouse_version=v3 \
  -e data_pipeline_olist_catalog_apply=true
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/51-olist-spark-runtime-lineage.yml \
  -e data_pipeline_olist_runtime_lineage_warehouse_version=v3 \
  -e data_pipeline_olist_runtime_lineage_apply=true

# 52 승인 증빙은 독립 단계; 이미 승인됐으면 다시 생성하지 않음
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/52-olist-quality-promotion.yml \
  -e data_pipeline_olist_promotion_warehouse_version=v3 \
  -e data_pipeline_olist_promotion_apply=true

# 53 독립 Iceberg 실습: 52 승인된 v3 Mart를 복사해 검증
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/53-iceberg-hdfs-lab.yml \
  -e iceberg_spark_lab_run_smoke=true
# 54 독립 WAP 실습: stage 확인 후 publish 별도 opt-in
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/54-iceberg-transactions-wap.yml \
  -e iceberg_transactions_lab_mode=stage
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/54-iceberg-transactions-wap.yml \
  -e iceberg_transactions_lab_mode=publish
```

**53 JAR 관리:** Spark 3.5.6·Scala 2.12.18·Java 11.0.32.1에 Iceberg 1.10.1을 사용한다. worker3 `vendor/iceberg/iceberg-spark-runtime-3.5_2.12-1.10.1.jar`를 master `/opt/bigtop-iceberg/`에 Ansible로 배치하고 `spark-submit --jars`로 YARN 컨테이너에 전달한다. **모든 Worker `/usr/lib/spark/jars`에 복사할 필요 없다.** JAR 바이트코드 호환성 검사는 `scripts/53-check-iceberg-java11.py`로 확인한다. 1.12.0은 Java 17 필요로 해당 환경에서 실패했으므로 재사용하지 않는다.

**상태 조회(재실행 무관):**

```bash
ssh master 'sudo -u hadoop hdfs dfs -ls /data/olist/warehouse/v3/_RUNTIME_LINEAGE.json'
ssh master 'sudo -u hadoop hdfs dfs -cat /data/olist/published/v3/_APPROVAL.json' | python3 -m json.tool
ssh master "sudo -u hadoop hdfs dfs -text '/data/olist/iceberg-validation/step53/part-*'"
ssh master "sudo -u hadoop hdfs dfs -text '/data/olist/iceberg-validation/step54/stage/part-*'"
ssh master "sudo -u hadoop hdfs dfs -text '/data/olist/iceberg-validation/step54/publish/part-*'"
```

## 7. 주요 API, Web UI와 서비스 점검

### 7.1 Web Console API/화면

| 경로 / 화면 | 목적 | 비고 |
|---|---|---|
| `/api/health` | Backend HTTP 정상 여부 | 내부 포트 18090 |
| `/api/status` | 서비스 상태 목록 | systemd 기반 |
| `/api/actions/history` | 운영 action 이력 | PostgreSQL 영속 저장 + JSONL 보조 기록 |
| `/api/storage/health` | PostgreSQL 연동 및 이벤트 수 | DB 연결 문제 구분 |
| `/api/metadata` | 운영 Inventory/서비스 스냅샷 | 32번 동기화 결과, 읽기 전용 |
| `/api/service-ui-links` | 관리 Web UI 바로가기 목록 | 39번 Ansible 변수 기반 |
| `/api/ops/postgresql-backup` | DB 백업·Prometheus alert 상태 | 37번 읽기 전용 대시보드 데이터 |
| `/api/self-check` | Web Console 자체 점검 | Backend/Frontend 통합 상태 확인 |
| `/api/runbooks` | 장애 대응 문서 목록 | Incidents와 Runbook 연결 |
| `Home` | 전체 서비스·이벤트 요약, UI 바로가기 | 콘솔의 시작 화면 |
| `Services` | 서비스 상태·로그·Start/Stop/Restart | 종료 조치는 주의 |
| `Grid` | 호스트별 서비스 카드를 한눈에 조회 | 서버/서비스 매핑 |
| `Incidents` | DOWN/UNKNOWN 서비스와 대응 문서 | 테스트 `?incident_demo=1`는 UI 시연 |
| `Admin` | SelfCheck, 기능 헬스, 백업 대시보드 | 상태가 불명확하면 UNKNOWN |

예시 조회:

```bash
curl -fsS http://127.0.0.1:18090/api/health | python3 -m json.tool
curl -fsS http://127.0.0.1:18090/api/storage/health | python3 -m json.tool
curl -fsS http://127.0.0.1:18090/api/metadata | python3 -m json.tool
curl -fsS http://127.0.0.1:18090/api/service-ui-links | python3 -m json.tool
curl -fsS http://127.0.0.1:18090/api/ops/postgresql-backup | python3 -m json.tool
```

Nginx는 `:1337`에서 Basic Auth를 요구하고, Backend는 내부 `127.0.0.1:18090`에 바인딩된다. **내부 API를 외부에 직접 공개하지 않는 배포 구성을 유지해야 한다.**

### 7.2 UI 포트 및 설정의 의미

| 서비스 | 콘솔 기본 바로가기 | 기준 |
|---|---|---|
| HDFS NameNode | `http://master:9870` | NameNode HTTP UI |
| YARN ResourceManager | `http://master:18088` | 사용자 지정 콘솔 링크 |
| Spark History | `http://master:18080` | Spark 이벤트 이력 UI |
| Prometheus | `http://worker3:9090` | Prometheus UI/API |
| Grafana | worker3 내부 `http://127.0.0.1:3000`; 호스트 PC `http://127.0.0.1:23000` | VirtualBox 포트포워딩으로 구분 |
| Alertmanager | `http://worker3:19093` | 사용자 지정 콘솔 링크 |
| OpenMetadata | `http://192.158.2.104:8585` | 거버넌스 전용 VM |
| OpenMetadata health | `http://127.0.0.1:8586/healthcheck` | metadata VM **내부** |

**주의:** `23000`은 호스트 PC에서 사용하는 VirtualBox 포트포워딩 주소이고 Grafana가 worker3 내부에서 listen하는 포트는 `3000`이다. `18088`, `19093` 등 나머지 링크는 *39번 Console 바로가기 기본값*을 확인한 것이다. 실제 데몬 listen 포트, NAT 포트포워딩, 리버스 프록시 방식은 `ss -lntp`, OS 설정, VirtualBox 설정으로 따로 확인해야 한다. `all.yml` 등에서 링크 목록을 override했다면 그 값이 기본값보다 우선한다.

## 8. 저장소·보호 정책 및 복구 가능성

### 8.1 데이터가 실제로 어디에 저장되는가?

| 데이터 | 원본 위치 | 백업/보호 방식 | 유의점 |
|---|---|---|---|
| HDFS 데이터 블록 | worker1/worker2 DataNode 저장 경로 | HDFS 복제 정책 | 블록 복제와 NameNode 메타데이터 보호는 별개 |
| HDFS NameNode metadata | master 설정된 name/edits 디렉터리 | 복수 디렉터리, 18번 online backup, 19번 worker3 보관 | 자동 HA가 아님 |
| Spark 이벤트 로그 | HDFS Spark History 경로 | 25번 보존기간·정리 | 장기간 보존이 필요하면 별도 정책 |
| Web Console Action History | worker3 `bigtop_console` PostgreSQL | 34번 pg_dump, 38번 원격 복제 설계, JSONL 이력 | JSONL은 DB 전체 백업을 대체하지 않음 |
| 운영 Inventory 스냅샷 | `bigtop_console` PostgreSQL | 34번 pg_dump, 32번 재동기화 가능 | 소스 오브 트루스는 Inventory/콘솔 구성 |
| PostgreSQL 로컬 백업 | worker3 `/var/backups/bigtop-web-console/postgresql/` | Custom-format, 14일 로컬 보관 | worker3와 동시 손실 가능 |
| PostgreSQL 원격 백업 | worker2 `/var/backups/bigtop-web-console-replica/postgresql/` | 38번 설정: 매시간 rsync, SHA-256, 30일 보관 | **실제로 파일이 있는지 미확인** |
| OpenMetadata DB/검색 | metadata Docker named volumes | Docker 볼륨 영속화 | **외부 백업·복원 절차 아직 없음** |
| OpenMetadata MySQL 비밀번호 | metadata `/opt/bigtop-governance/openmetadata/.env` | root 보호 파일, Git 제외 | `.env` 임의 수정 시 DB 인증 불일치 위험 |
| Catalog reader 비밀번호/JWT | worker3 `/etc/bigtop-governance/...` | 권한 보호, Git 제외 | 토큰 교체 시 명시적 `--replace` 필요 |

### 8.2 PostgreSQL 백업의 실제 보장 범위

`pg_dump -Fc`는 대상 DB의 논리적 백업이며 전체 PostgreSQL 클러스터 설정·OS 계정·외부 환경까지 완전히 보관하는 것은 아니다. 34번 복원 검증은 **별도 임시 DB**에 `pg_restore`를 실행해 테이블을 확인하고 제거한다. 운영 DB로 바로 복원하는 기능이 아니다.

```bash
# worker3
sudo systemctl start bigtop-web-console-pg-backup.service
sudo journalctl -u bigtop-web-console-pg-backup.service -n 40 --no-pager
sudo -u postgres /usr/local/lib/bigtop-web-console/pg_restore_check.sh
sudo systemctl list-timers --all | grep bigtop-web-console-pg-backup
```

34번 기본: 서버 로컬 시간 매일 03:30(+최대 5분 랜덤 지연), 14일 로컬 보관. 35번은 10분마다 `pg_restore --list`로 **아카이브 구조**를 검사하지만, 그것만으로 실제 복원 성공을 증명하지는 않는다.

### 8.3 백업 모니터링과 알림 체인

```text
pg_dump 결과
    → /var/backups/bigtop-web-console/postgresql/*.dump
    → observer timer (10분 주기)
    → /var/lib/prometheus/node-exporter/web_console_pg_backup.prom
    → Node Exporter :9100
    → Prometheus :9090
    → 7개 Alert Rule
    → Alertmanager / Admin 패널
```

```bash
# 백업 observer
systemctl is-active bigtop-web-console-pg-backup-observer.timer
sudo cat /var/lib/prometheus/node-exporter/web_console_pg_backup.prom
curl -fsS http://127.0.0.1:9100/metrics | grep bigtop_web_console_pg_backup_

# 실제로 발견된 Alertmanager 연결은 worker3:9093 API 경로였음
curl -fsS http://127.0.0.1:9090/api/v1/alertmanagers | python3 -m json.tool
```

주요 7개 조건: **백업 없음, 백업 26시간 초과, 최신 파일 무효, Timer 중지, 수집 실패, 메트릭 갱신 지연, 메트릭 누락**. 36번 배포는 최초 Prometheus 리로드 직후 Alertmanager가 `activeAlertmanagers`에 표시되지 않아 실패했으나 이후 정상 연결 확인·재실행 완료가 보고됐다. 새로 장애가 발생하면 `Prometheus rule loaded`와 `Alertmanager active`를 구분해야 한다.

### 8.4 OpenMetadata의 데이터 보존

OpenMetadata의 MySQL/Elasticsearch 데이터는 Docker named volume에 저장된다. **`docker compose down -v` / `docker volume rm` / `docker system prune --volumes` 금지.** `compose down`과 `down -v`는 다르다. 41번은 영속 볼륨을 구성했지만 **원격 백업 및 복원검증은 아직 구축하지 않았다**. Data Catalog 설명과 용어집은 Git 정책 파일로 일부 재생성 가능해도 사용자가 UI에서 추가한 자산 메타데이터까지 전부 재생성되는 것은 아니다.

## 9. 거버넌스 수집·분류·계보 세부 구조

### 9.1 42번 PostgreSQL Catalog 수집 — 확인된 성공 경로

1. 기존 `bigtop_console` PostgreSQL에 읽기 전용 `bigtop_catalog_reader` 역할 생성.
2. worker3에서 격리된 Python 환경에 `openmetadata-ingestion[postgres]==2.0.6.0` 설치.
3. OpenMetadata UI에서 `ingestion-bot` JWT 발급 → `install_token.py`가 입력을 숨기고 worker3 보호 파일에 저장.
4. systemd oneshot 서비스에서 workflow를 실행하여 `bigtop_console.public`의 테이블·컬럼·뷰 등 메타데이터 수집.
5. OpenMetadata API(`http://metadata:8585/api`)에 결과 등록. `ingestAllDatabases`와 destructive delete 정책은 보수적으로 설정.

```bash
# 최초 또는 토큰 교체 시에만 실행 (암호 원문은 입력/출력하지 않음)
sudo python3 /usr/local/lib/bigtop-governance/pg-catalog/install_token.py

# 매번 필요한 수집 실행
sudo systemctl start bigtop-governance-pg-catalog.service
sudo journalctl -u bigtop-governance-pg-catalog.service -n 80 --no-pager
```

`Type=oneshot` 서비스가 성공 후 `inactive (dead)`로 보이는 것은 정상일 수 있다. **exit code와 `CATALOG INGESTION OK` 로그**를 확인한다. 자동 스케줄은 기본값이 **비활성**이며, 필요 시 `governance_postgres_catalog_enable_timer: true`를 Ansible 변수에 선언하고 42번을 다시 적용한다.

**실제 디버깅 이력:** 42번은 `partial-become` Lint → `postgresql_privs` 필수 `login_db` 누락 → 잘못된 커넥터 타입 `PostgreSQL`(정답 `Postgres`) 문제를 해결하고 수집 성공했다. 실패 재발 시 임의로 DB 포트를 열거나 비밀번호를 재발급하지 말고 로그에서 해당 오류를 구분한다.

### 9.2 43번 Governance 정책 — 무엇을 수정하나?

정책 파일: `roles/governance_catalog_enrichment/files/governance_policy.yaml` (Ansible 관리).

| 메타데이터 | 초기 PoC 값 | 주의 |
|---|---|---|
| Classification | `BigtopDataPurpose` | 분류 네임스페이스, 민감정보 자동 판단 아님 |
| Tag | `BigtopDataPurpose.OperationalMetadata` | 실제 접근제어를 집행하지 않음 |
| Glossary | `BigtopPlatform` | 팀이 사용하는 업무 용어 관리 |
| Glossary Terms | `ClusterHost`, `ServiceInstance` | 테이블/컬럼 의미에 연결 |
| 적용 테이블 | `platform_hosts`, `platform_service_instances` | 42번 수집 완료가 선행 |
| Owner | 기본값 미지정 | 실제 존재하는 팀/사용자만 명시적으로 지정 |

```bash
# 43번 배포 후: 먼저 읽기 전용 계획 확인
sudo python3 /usr/local/lib/bigtop-governance/catalog-enrichment/enrich_catalog.py \
  --plan --url http://192.158.2.104:8585/api \
  --token-file /etc/bigtop-governance/pg-catalog/bot.token \
  --policy /etc/bigtop-governance/catalog-enrichment/policy.yaml

# 계획이 타당할 때만 실제 적용
sudo systemctl start bigtop-governance-catalog-enrichment.service
sudo journalctl -u bigtop-governance-catalog-enrichment.service -n 60 --no-pager
```

단위 테스트는 신규 객체 생성→두 번째 실행 시 SKIP의 멱등성을 검증했다. YAML 파일 문서 시작 `---` 누락 Lint도 수정하도록 안내됐지만, **실제 API 적용 로그가 별도로 확인되지 않았다**.

### 9.3 44번 SQL View Lineage — *설계·패키지 제공, 배포 미확인*

```text
platform_hosts ───────────────────────┐
                                      ├─ SQL JOIN → v_bigtop_service_inventory_lineage
platform_service_instances ───────────┘
```

기존 두 테이블의 `is_present=true` 데이터를 JOIN해 View를 만들고, 42번 Catalog 수집을 재실행해 View를 등록한 뒤, 이미 자동 추출된 계보가 없다면 OpenMetadata API로 필요한 관계를 추가한다. 소스/타깃은 **실제로 실행 가능한 SQL 표현식**에서 결정하며, 외래키 관계만 가지고 임의로 계보라고 부르지 않는다.

**변경 가능 범위:** 44번은 **PostgreSQL에 새로운 View 객체를 생성**한다. 원본 테이블 행은 수정하지 않지만 DB 스키마는 변경된다. 정책 실행(`--plan` → 명시적 `--apply`) 전에 View 이름 충돌/쿼리 내용을 확인해야 한다.

```bash
# 아직 배포 여부를 확인하지 못한 단계
.venv/bin/ansible-playbook -i inventory/hosts.ini \
  playbooks/44-governance-lineage.yml

# View 수집 후 API 계획 검사
sudo systemctl start bigtop-governance-pg-catalog.service
sudo python3 /usr/local/lib/bigtop-governance/lineage/manage_lineage.py \
  --plan --url http://192.158.2.104:8585/api \
  --token-file /etc/bigtop-governance/pg-catalog/bot.token \
  --policy /etc/bigtop-governance/lineage/policy.yaml
```

Spark read/write 이벤트의 자동 추적과는 별개의 **SQL View 기반 PoC**다. Spark Lineage를 자동 수집하려면 OpenLineage 연동, 이벤트 전달, 데이터셋 식별, Spark Job 실행 환경 테스트가 후속 과제다.

### 9.4 50~51번 HDFS 데이터셋의 Catalog와 실제 실행 근거 계보

- **50번:** OpenMetadata 서비스 `bigtop_hdfs_olist`의 Container(`raw_v1_*`, `curated_v3_*`, `quarantine_v3_rows`, `mart_v3_daily_category_sales`)를 HDFS Manifest와 7개 `_SUCCESS` 검증 후 등록한다. 최초 v1은 StorageService 1개·Container 16개·수동 계보 13개가 생성됐고, 사용자가 **개별 Container의 Lineage 탭**에서 연결선을 확인했다. *Service 계보 화면*은 조회 대상이 다르다.
- **51번:** 새 버전 ETL이 `data_pipeline_olist_etl_capture_runtime_lineage=true`로 실행될 때만 `_RUNTIME_LINEAGE.json`을 기록한다. Importer는 동일 버전 OpenMetadata Container 존재를 먼저 검증하고 실제 읽기 파일/쓰기 경로 근거만 추가한다. v2에서는 실행 증빙이 빠져 v3에서 새로 생성했다. **Spark 실행 기반 파일 계보이지, OpenLineage Listener나 컬럼 계보가 아니다.**
- **인증:** Bot JWT는 worker3 보호 디렉터리에 두며 YARN 컨테이너에 전달하지 않는다. v3 Lineage 등록 시 50번 v3 Container 등록이 선행돼야 한다.

### 9.5 52번 Quality Gate와 53~54번 Iceberg WAP의 차이

| 비교 | Step 52 — Parquet Warehouse 승인 | Step 54 — Iceberg WAP 샘플 |
|---|---|---|
| 대상 | 실제 Olist Warehouse v3 | 별도 샘플 Iceberg 테이블 |
| 검증 기준 | 최신 PASS 29개 검사, ETL Manifest SHA, Parquet `_SUCCESS`, 기본 168h | 합성 데이터의 ID·금액·브랜치 스냅샷 조건 |
| 결과 | `/data/olist/published/v3/_APPROVAL.json` 기록 | Iceberg audit 브랜치 → 명시적 `fast_forward`로 main 공개 |
| 소비자 차단 | **미구현** (승인 기록만 발행) | 실습 테이블의 기본 main과 audit 분리; HDFS RBAC 아님 |
| 실제 운영 승인 자동화 | 별도 소비자 측 정책 필요 | 운영 Olist 품질 정책과 연결하는 작업 미구현 |

Iceberg 스냅샷은 **테이블 단위**다. `main`으로 fast-forward한다고 여러 테이블의 공개가 원자적으로 수행되는 것은 아니며, 운영 SQL 소비자 승인 강제·권한 관리도 별도다. 현재 Catalog는 Spark 실습용 Hadoop Catalog `lab`; Trino 같은 엔진과 공유할 Catalog를 선정·검증해야 한다.

## 10. 운영 점검 순서 및 장애 시 접근법

### 10.1 하루 운영 점검 예시

1. Inventory 대상과 SSH 정상 여부 확인 (`ansible ping`).
2. `make health` 및 필요 시 기능 점검(`make smoke`).
3. Prometheus Targets/Alerts와 Grafana 대시보드 확인.
4. Web Console Services/Incidents/Action History 점검.
5. PostgreSQL 백업 최신성·검증·Alert 상태 확인.
6. 별도 노드에 백업이 있는지 확인(38번은 특히 필요).
7. metadata Docker `compose ps`·OpenMetadata UI·수집 로그 확인.
8. 48번 Scheduler 서비스/Timer, 49번 Grafana Olist 패널에 실제 메트릭 존재 여부 확인(기본 v1).
9. 운영에서 필요한 경우 v3 최신 Quality 리포트 및 `_APPROVAL.json`과 Iceberg WAP stage/publish 증빙 확인.

```bash
# worker3: 일상
make health
make smoke
curl -fsS http://127.0.0.1:18090/api/ops/postgresql-backup | python3 -m json.tool
sudo systemctl list-timers --all | grep -E 'pg-backup|pg-catalog'

# metadata VM: Docker / UI
sudo systemctl status bigtop-governance-openmetadata.service --no-pager
sudo docker compose -p bigtop-openmetadata \
  -f /opt/bigtop-governance/openmetadata/compose.yaml \
  --env-file /opt/bigtop-governance/openmetadata/.env ps
curl -fsS http://127.0.0.1:8586/healthcheck
sudo docker stats --no-stream
```

### 10.2 장애 영역별 1차 확인

| 증상 | 우선 확인 | 무턱대고 하지 말 것 |
|---|---|---|
| HDFS 파일 읽기/쓰기 실패 | `make health`, `make smoke`, NN/DN 로그, Safe Mode, 디스크 | NameNode format |
| YARN App Pending/실패 | RM/NM 상태, 큐 용량, NM local/log 디스크, diagnostics | 전체 서비스 일괄 재시작 |
| Spark History 로그 누락 | event log HDFS 경로·cleaner·History 서비스 | 이벤트 파일 즉시 삭제 |
| Prometheus Alert 부재 | target up, metric 존재, rule 파일·promtool·Alertmanager API | Alertmanager가 문제라고 단정 |
| Web Console UI 502/401 | Nginx Basic Auth, Uvicorn systemd, 내부 `:18090` | 내부 포트 외부 무조건 공개 |
| Action History 누락 | `/api/storage/health`, PostgreSQL, JSONL reconciliation | PostgreSQL 테이블 삭제·재생성 |
| 백업 오래됨/손상 | 34번 timer·journal, `pg_restore --list`, 임시 DB 복원 | 운영 DB에 `pg_restore --clean` 직접 실행 |
| 원격 백업 없음 | 38번 timer·journal, SSH 권한, SHA-256, worker2 용량 | 백업 파일 수동 삭제 후 재시도 |
| OpenMetadata 불안정 | metadata RAM/Swap, Docker/Compose 로그, MySQL·Elasticsearch health | `docker compose down -v` |
| Catalog 수집 실패 | 42번 oneshot journal, Bot 권한, 연결 타입 `Postgres` | JWT를 Git에 기록 / 5432 원격 공개 |
| Governance 적용 403 | Bot/API 역할·권한, plan, token 유효성 | Admin 토큰을 로그에 출력 |
| Olist Pipeline Grafana `No data` | 48번 서비스 실행 이력, `olist_pipeline.prom`, Node Exporter 수집, Prometheus 라벨·대시보드 버전 | 누락값을 0/성공으로 간주 |
| Spark 재실행 `exitCode 13` | 이미 완성된 불변 스냅샷이 SparkContext 생성 전에 종료됐는지, 46번 선제 검증/SKIP 적용 | 동일 버전 HDFS 출력 삭제 |
| Runtime Lineage 파일 없음 | 46번 실행 시 `capture_runtime_lineage=true`였는지, 신규 버전인지 | 기존 게시 v2 덮어쓰기 |
| Runtime Lineage 사전 검사 실패 | 50번 동일 Warehouse 버전 Container 등록 여부 | 존재하지 않는 데이터셋의 계보 강제 생성 |
| Iceberg `UnsupportedClassVersionError: 61.0` | Java 11(55)에 Java 17(61) 컴파일 Iceberg 1.12 사용 여부 | Java 클러스터 전역 업그레이드를 바로 시행 |
| Iceberg YARN Application failed | `yarn logs -applicationId ...`의 Driver Python/Java Traceback | `spark-submit` 표준 에러만 보고 원인 단정 |
| WAP Publish 실패 | stage 증빙, main/audit snapshot ID 및 후보 품질 | 테이블 삭제·무조건 fast-forward |

### 10.3 이 프로젝트에서 실제 겪은 문제와 교훈

| 이슈 | 원인 또는 관찰 | 재발 방지 |
|---|---|---|
| `governance_nodes` 미인식 | 수정한 `hosts.ini`와 실제 `inventory/hosts.ini` 불일치 | `ansible-inventory --graph`로 현재 파일 확인 |
| GitHub Actions에서 PostgreSQL Role Lint 실패 | `community.postgresql` Collection CI 누락 | `collections/requirements.yml`에 의존성 명시 |
| Web Console Inventory Lint import 실패 | 패치 번들 내부에 상대 경로가 깨지는 중복 `.yml` 존재 | 실제 `playbooks/`에만 정식 Playbook을 보관 |
| 36번 마지막 Alertmanager assert 실패 | 리로드 직후 연결 발견이 지연된 가능성 | 실제 API 확인 후 재시도; false failure와 실제 설정 누락 구분 |
| 38번 Playbook 진입점 실패 | `roles:`의 잘못된 `tasks_from` 사용, 변수 role-prefix Lint | `ansible.builtin.import_role`과 접두사 일치 |
| 42번 `partial-become` Lint | Task의 `become_user`에 `become: true` 누락 | 역할별 Lint·회귀 테스트 |
| 42번 `missing required arguments: login_db` | `community.postgresql.postgresql_privs` 인자 불일치 | 실제 Collection API에 맞는 인자 검사 |
| 42번 Workflow schema validation | PostgreSQL connector `type` 문자열 오류 | `Postgres`로 수정, Pydantic schema 검증 |
| 43번 `yaml[document-start]` | `governance_policy.yaml`의 첫 `---` 누락 | YAML 작성 후 lint 반드시 수행 |

## 11. 보안·운영 경계

- **Ansible SSH 키:** worker3의 공개키를 제어 대상의 `authorized_keys`에 등록한다. metadata의 개인키를 전체 Hadoop 노드에 복사할 필요는 없다. *개인키 배포 금지*.
- **비밀번호 없는 sudo:** 개인 실습 편의 설정이지만 실운영에서는 sudo 명령 허용 범위를 제한해야 한다.
- **Nginx Basic Auth:** 콘솔 접근 제어의 출발점이다. 사용자별 RBAC, 작업 승인, 정밀 감사 추적을 대신하지 않는다.
- **OpenMetadata HTTP:** 현재 실습 서버는 평문 HTTP를 사용한다. 공개 네트워크에 노출하지 않고 추후 TLS·SSO 등을 구성한다.
- **PostgreSQL reader:** 메타데이터 수집 용도로만 사용하되 DB `SELECT` 권한은 행 데이터까지 읽을 수 있으므로 비밀번호를 강하게 보호한다.
- **Data Classification:** 태그나 용어를 생성했다고 해서 자동으로 민감정보 탐지·마스킹·DB 접근제어가 이뤄지는 것은 아니다.
- **Git 제외 대상:** `inventory/hosts.ini`, `inventory/group_vars/all.yml`, SSH 개인키, `*.env`/실제 `.env`, JWT, 데이터베이스 덤프/백업/복구용 덤프, 실행 로그, 임시 ZIP/패치 번들, 캐시·테스트 결과.
- **모니터링 UI 바로가기:** 중앙 콘솔에 링크를 표시하는 기능은 브라우저 접근 제어를 우회하거나 실제 서비스 포트를 재설정하는 기능이 아니다.
- **문서의 IP/포트:** 실습 환경 정보로 취급하고 운영 환경의 공개용 README에는 필요 시 일반화한다.

## 12. 완료 상태·미완료 사항·다음 작업

### 12.1 기능 완료/확인 구분

| 영역 | 현재 문서상 상태 | 확인 근거 및 다음 점검 |
|---|---|---|
| HDFS/YARN/Spark + 운영/모니터링(01~27) | 구축된 기본 플랫폼 | 기존 README 기록; 현재 실시간 health는 별도 확인 |
| Web Console + DB + Inventory(30~33) | 구축 | API 및 DB 연동, Inventory 기능 적용 이력 |
| PostgreSQL 백업·알림·Admin(34~37) | 구축 | 백업·복원 및 35·36·37 단계 사용자 진행 확인 |
| PostgreSQL worker2 복제(38) | **확인 필요** | 최초 패키지 실패→수정본. receiver 수신/무결성 로그 미제공 |
| 관리 UI 링크(39) | 구축 | 6개 관리 UI, 수정된 포트 확인 후 진행 |
| metadata Docker/OpenMetadata(40~41) | 구축 | Docker 확인, UI 로그인 확인 |
| PostgreSQL Data Catalog(42) | **수집 성공 확인** | v3 커넥터 타입 수정 후 사용자 성공 보고 |
| 분류/용어집 정책(43) | **실제 적용 결과 확인 필요** | 단위 테스트·YAML 수정, 실행 완료 로그 미제공 |
| SQL View Lineage(44) | **배포 미확인** | ZIP 패키지 제공 단계까지 진행 |
| Olist 원본/ETL/품질/실행 관측(45~49) | **구축·검증** | Raw 9개, ETL 7개 Parquet, 29 품질 검사, 수동 48 서비스·49 대시보드 성공 |
| OpenMetadata Olist Container·계보(50) | **등록 및 UI 확인** | v1 16 Container·13 관계, v3 자산 등록 |
| Spark 애플리케이션 실행 파일 계보(51) | **v3 성공** | `_RUNTIME_LINEAGE.json` 및 API Import; `inputFiles()` 기반 Olist 전용 구현 |
| Olist 승인 증빙(52) | **v3 승인 성공** | `_APPROVAL.json` 발행, 소비자 측 강제 접근제어는 없음 |
| Iceberg HDFS Catalog·Snapshot(53) | **PASS** | 1.10.1/Java 11, Time Travel, v3 Mart 55,877행 일치 |
| Iceberg DML/Schema/샘플 WAP(54) | **stage·publish 사용자 확인** | 샘플 테이블만 변경, 운영용 릴리스 정책 아님 |
| 범용 Spark OpenLineage Listener / 컬럼 계보 | **미구축** | 51번 파일 단위 계보와 구분 |
| Trino/공유 Catalog + 승인 강제 조회 | **미구축** | Iceberg Hadoop Catalog는 현재 단독 Spark Lab 범위 |
| 48 Scheduler/51 Reconcile Timer 주기 실행 | **상태 별도 확인** | 기본 비활성; 설치·수동 실행과 주기 자동 실행은 다름 |
| OpenMetadata 전용 백업·복원 | 미구축 | 41번 Docker 볼륨 영속화만 확인 |
| 실운영 인증·TLS·RBAC | 미구축 또는 보강 필요 | 실습용 Basic Auth·Bot 토큰 중심 |

### 12.2 다음 단계 우선순위

1. **Iceberg + 실제 Olist Data Quality 연동:** 54번의 합성 WAP를 47번 29개 검사와 연결하고, 승인 정책/감사 기록/소비자 조회 통제를 설계한다. 53~54번 기존 실습 테이블과 운영 자산을 혼용하지 않는다.
2. **Spark/Trino 공유 Catalog 선정:** Hadoop Catalog 실습을 그대로 Trino 운영 구성으로 간주하지 않는다. 호환 Catalog, 스키마, 권한·자격증명, 메타데이터 백업을 검토한다.
3. **48/49 버전·스케줄 정책:** 기본 v1 고정 메트릭과 v3 승인 상태의 불일치를 해소하고, 필요 시 Timer/Alert를 명시적 승인으로 활성화한다.
4. **실행 계보 고도화:** 51번 Olist 전용 `inputFiles()` 방식의 한계를 문서화하고, 범용 OpenLineage/컬럼 계보는 별도로 검증한다.
5. **미확인 과제 재검증:** 38번 원격 백업 복제, 43번 거버넌스 정책, 44번 SQL View Lineage 적용 결과를 확인한다.
6. **OpenMetadata 백업과 보안:** MySQL/검색 인덱스/Compose 설정 복구, HTTPS 및 RBAC 계획.
7. **CI/문서 자동 검증:** 현재 저장소의 Playbook·Role 경로, 테스트 및 lint를 문서의 안내와 대조한다.

## 13. 문서 유지보수 규칙

새 기능을 추가할 때 기존 README 말미에 작업 로그를 계속 누적하지 않는다.

1. **`README.md`:** 현재 구조·실제 동작·대표 기능·빠른 명령만 유지한다. 단계 번호를 홍보성 이력으로 나열하지 않는다.
2. **`docs/PROJECT_GUIDE.md`:** 아키텍처, 컴포넌트 선정 근거, Playbook별 역할·의존성·운영 흐름을 **현재 상태 기준으로 갱신**한다.
3. **`docs/<기능>.md`:** 해당 기능의 변수, 신규 API, systemd unit, 세부 실행/복구 명령을 보관한다.
4. **`docs/runbooks/`:** 장애 시 상황/탐지/확인/완화/복구/재발 방지 절차로 구성한다.
5. **미확인 기능은 미확인으로 기재:** 패키지를 생성했다는 사실과 VM에서 실제 배포·검증되었다는 사실을 혼동하지 않는다.
6. **프로젝트 구조 변경 시 둘 다 수정:** 신규 VM/서비스가 생기면 README 구성표, 본 문서 아키텍처, Playbook 매핑을 같이 고친다.
7. **실제 소스 기준 검증:** `find playbooks -maxdepth 1 -name '*.yml' | sort`와 `find roles -maxdepth 1 -mindepth 1 -type d | sort`로 현재 작업 트리를 비교한다.

### 관련 기존 문서

- `docs/runbooks/` — 재부팅·모니터링 알림·NodeManager 디스크·Spark History/YARN logs·애플리케이션 실패.
- `docs/web_console_postgres.md`, `docs/web_console_metadata.md` — 31·32번.
- `docs/web_console_pg_backup.md`, `docs/web_console_pg_backup_observability.md`, `docs/web_console_pg_backup_alerts.md` — 34~36번.
- `docs/web_console_pg_remote_replica.md`, `docs/web_console_service_ui_links.md` — 38·39번.
- `docs/40-governance-bootstrap.md`, `docs/41-governance-openmetadata.md`, `docs/42-governance-postgresql-catalog.md`, `docs/43-governance-catalog-enrichment.md`, `docs/44-governance-lineage.md` — 기존 거버넌스 개별 문서.
- `docs/45-olist-*.md` ~ `docs/52-olist-*.md` — 실데이터 적재·ETL·품질·운영·거버넌스·승인 상세 문서.
- `docs/53-iceberg-apply.md`, `docs/53-iceberg-architecture.md`, `docs/54-iceberg-apply.md`, `docs/54-iceberg-architecture.md` — Iceberg Snapshot/Time Travel/WAP 실습 문서.

---

**요약:** 이 프로젝트는 `HDFS/YARN/Spark`의 **데이터 처리**, `Prometheus/Alertmanager/Web Console/PostgreSQL`의 **운영**, `OpenMetadata/MySQL/Elasticsearch/수집 CLI`의 **거버넌스**, 그리고 Iceberg **테이블 포맷 실습**이 맞물린 5대 VM 실습 시스템이다. 각 계층의 책임을 구분하고 Ansible을 단일 배포 원천으로 유지하는 것이 핵심 설계다.
