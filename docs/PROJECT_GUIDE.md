# Bigtop Data Platform — 상세 아키텍처·운영·Playbook 가이드

> **문서 범위:** 2026-11-08까지 01~69번(64-A/B/C 및 65-B 포함). Olist HDFS/Parquet·Iceberg(45~63), Kafka KRaft/CDC(64), 상시 Streaming의 경량 Producer/Verifier(65), Watchdog(66), Kafka/Spark Progress(67), Lag/Alert(68), 읽기 전용 복구 증빙 및 정상 재기동 시험(69)까지 포함한다. **미완료 Batch 장애 재처리와 Replay 재실행을 통한 멱등성 실증은 남아 있다.**
> **빠른 개요:** [프로젝트 README](../README.md)  
> **이 문서의 목적:** 처음 보는 사람이 왜 이 플랫폼을 구축했는지, 각 서버와 컴포넌트가 왜 필요한지, 어떤 Playbook이 무엇을 바꾸는지, 일상 운영·복구·다음 개발을 어떻게 이어갈지 이해하도록 한다.

## 0. 확인 근거와 현재 상태

이 문서는 *기존 `README.md`/`PROJECT_GUIDE.md`, 31~44번 안내, 45~65번 ZIP의 Playbook·Role·운영 문서, 실제 사용자 실행 로그와 단계별 완료 확인*을 기반으로 갱신했다. 단순 작업 일지 대신 **현재 아키텍처·실행 의존성·구현 한계**를 구분한다.

**제약:** worker3의 현재 Git 작업 트리 전체를 이 문서 작성 환경에서 직접 열람하지는 않았다. 따라서 아래 목록의 파일명·구현 구조는 *확인 가능한 버전의 소스 기준*이고, 실제 노드의 세부 설정/버전/현재 가동 상태와 다를 수 있다. 다음과 같이 분리해서 해석한다.

- **사용자 확인:** Docker 40, OpenMetadata 설치·UI 로그인 41, PostgreSQL 카탈로그 수집 42, UI 바로가기 수정 39, Web Console 백업 대시보드 37 등. 42번은 최종 수정본(v3) 적용 후 실제 수집 성공을 확인했다.
- **진행되었다고 대화에서 이어간 기능:** 43번 분류·용어집 구성. 단위 테스트와 YAML 형식 문제 수정은 확인되지만, 실제 OpenMetadata에 적용한 systemd 로그/화면 확인은 공유되지 않았다.
- **별도 검증 필요:** 38번 PostgreSQL 원격 복제는 최초 Ansible 실패 후 수정 패키지를 제공했지만 최종 worker2 수신·SHA-256 검증 성공 로그가 없다. **44번은 배포 패키지를 제공한 상태이며 실제 적용 결과가 없다.**
- **45~54번 확인 근거:** Raw, Spark ETL v1/v3, Quality·v3 승인, OpenMetadata Container/UI, Iceberg 53번 `status=PASS`·55,877행, 54번 Stage/Publish 사용자 확인.
- **55~57번 확인 근거:** 55번 5 VM 진단 출력, 56번 Gravitino REST Server `STEP56_REST_READY`·Spark Smoke 실행 및 사용자 검증, 57번 Trino API `state=ACTIVE`와 Spark 작성 `[[56, 'spark']]`·COUNT=1·`STEP57_TRINO_SPARK_CROSS_QUERY_PASS` 실제 로그.
- **58~60번 확인 근거:** 58번 첫 YARN 실패는 REST Catalog `warehouse` 옵션 제거로 수정 후 완료 확인, 59번 실제 Gold WAP 성공 확인. 60번 systemd 로그에 `STEP60_ALREADY_PUBLISHED_VERIFIED`, `STEP60_GOLD_RELEASE_PASS`, `STEP60_PIPELINE_PASS: mode=run` 및 Trino 55,877행·정상 종료 기록.
- **61~63번:** 사용자 단계별 완료 확인. 61번은 Iceberg 8개 OpenMetadata 자산/8개 엣지와 별도 Grafana 대시보드, 62번은 격리 테이블 compaction/rollback, 63번은 516행 합성 CDC MERGE. 일상 Timer 설정과 장기 운용 결과는 별도다.
- **64-A Kafka 실측:** Kafka 4.2.2 3노드 KRaft 설치 후 `STEP64A_KAFKA_CLI_JAVA17_OK`·`STEP64A_KAFKA_PRODUCE_CONSUME_PASS`, RF3/ISR3/minISR2 로그 확인. 초기 9093 충돌을 피하려 Controller 포트 19093 적용.
- **64-B/64-C:** 64-B Kafka 3서비스 active·Metadata Quorum·`STEP64B_PREFLIGHT_PASS` 실제 로그와 모드 변수 수정 이후 사용자 확인. Kafbat UI는 사용자 화면/기능 질문이 있었으나 64-C의 `verify` 성공 로그는 별도 제공되지 않았다. 64-C의 최초 9개 테스트 중 1개는 프로젝트 전역 `.jar/.tar` 존재를 잘못 실패 처리하는 테스트 결함이므로 이 문서 묶음에 수정본을 포함한다.
- **64 CDC:** `AvailableNow` Kafka→Spark→Iceberg→Trino 통과를 사용자 확인. 520행(유효512/삭제8) 결과.
- **65~69 Streaming 운영:** 과거 `produce/verify` 별도 Spark 앱은 자원 부족으로 YARN `ACCEPTED` 대기했으나, 65-B 경량 Kafka CLI + Trino/HDFS Verifier 도입 후 521/512/9, Kafka End/Checkpoint=24, Snapshot 불변을 실제 검증했다. Step67 메트릭 수집/Step68 Lag 관측/Step69 안정성 비교도 통과했다. 단, 실제 미완료 Batch 재처리 및 새 YARN App `RUNNING` 최종 확인은 미완료다.
- **아직 별도 확인 필요:** 48번/51번 Timer 활성화, 38/43/44번, Kafka 64-B Timer/메타데이터 주기 실행, Kafbat 64-C 실배포 verify, Gravitino/HDFS/DB 백업 복원.
- **HA 범위:** 단일 NameNode/ResourceManager 기반의 실습 구성이며 HA를 구축했다고 주장하지 않는다.

## 1. 프로젝트 목표 및 설계 원칙

### 1.1 해결하려는 문제

**기업 내부망/On-Premise 환경을 염두에 두고**, Ambari 없이 Hadoop 설치·운영·장애 감지·복구, Iceberg Lakehouse, SQL 분석, 데이터 거버넌스를 직접 관리하는 작은 시스템을 만드는 것이 목적이다. 공개 인터넷용 SaaS가 아니라 **내부 데이터 엔지니어·분석가·BI 사용자**를 대상으로 한다.

- **재현성:** VM이 새로 준비되면 Ansible Inventory와 Role로 동일한 환경을 재구성한다.
- **관측 가능성:** 서버가 떠 있는 것뿐 아니라 HDFS/YARN 기능, Spark 처리, 파일 시스템·메트릭·백업 상태를 별도 검증한다.
- **운영 작업 표준화:** `make`, Playbook, 중앙 Console, Runbook으로 반복 명령을 통일한다.
- **데이터 보존:** HDFS NameNode 메타데이터, Web Console PostgreSQL, OpenMetadata MySQL/검색 인덱스는 서로 다른 백업·복구 전략이 필요함을 인식한다.
- **데이터 거버넌스:** 서버 운영 정보와 실제 데이터셋·스키마·소유자·계보 정보를 구분해 관리한다.
- **역할 분리:** Hadoop 처리 노드, 운영 Controller/Catalog, 거버넌스/Trino 노드를 분리한다.
- **공유 데이터 테이블:** Spark와 Trino가 동일한 Iceberg REST Catalog를 사용하고 HDFS Parquet를 직접 읽되 역할(ETL vs 분석)을 구분한다.
- **품질 기반 공개:** 검증된 Gold 후보만 Iceberg Audit Branch에서 `main`으로 공개한다. 원본 스냅샷 및 실패 증빙은 보존한다.
- **스트리밍 신뢰성:** Kafka 수신·Spark `foreachBatch`·Iceberg MERGE 간 분산 원자 트랜잭션을 가정하지 않는다. HDFS Checkpoint, `event_seq` 멱등 MERGE, Trino 최종 상태, 재전송/복구 검증을 별개로 다룬다.
- **한정 자원 운영:** Kafka와 Spark가 공유하는 소형 VM에서는 상시 Streaming 1개만 YARN에 유지하고 경량 Producer/Verifier는 YARN 외부로 분리하는 방향을 채택한다(65번 개선 작업 예정).

### 1.2 왜 Ansible + Bigtop인가?

**Ansible**은 각 노드에 전용 에이전트를 설치하지 않고 SSH와 Python으로 OS·패키지·설정·systemd를 선언적으로 관리한다. Playbook은 적용 순서를, Role은 서비스 단위 구현을 담당한다.

**Apache Bigtop**은 Hadoop/Spark 등 빅데이터 구성요소의 배포 패키지를 제공한다. 이 프로젝트는 Bigtop 패키지를 Ansible이 설치·설정·기동하므로, 기본 Hadoop 서비스 구축에 Docker를 사용하지 않는다. 반면 OpenMetadata는 별도의 VM에서 Docker Compose로 실행한다.

**구별할 점:** Ansible 자체가 클러스터 상태를 지속 감시하는 Daemon은 아니다. 지속 모니터링은 Prometheus/systemd timer, 요청 시 서비스 조치는 Web Console이 수행한다.

## 2. 물리·논리 아키텍처

### 2.1 VM 및 인벤토리

| 호스트 | 확인된 실습 주소 | 할당 자원 | 핵심 역할 | 그룹 |
|---|---|---|---|---|
| `master` | `192.158.2.100` | 2 vCPU / 4GB | NameNode, ResourceManager, Spark History | `master`, `hadoop_cluster` |
| `worker1` | `192.158.2.101` | 2 vCPU / 4GB | DataNode, NodeManager, Kafka 4.2.2 Broker+Controller #1 | `workers`, `hadoop_cluster` |
| `worker2` | `192.158.2.102` | 2 vCPU / 4GB | DataNode, NodeManager, Kafka Broker+Controller #2, 38번 DB 백업 수신 대상 | `workers`, `hadoop_cluster` |
| `worker3` | `192.158.2.103` | 2 vCPU / 4GB | Ansible, Web Console, PostgreSQL, **Gravitino REST(:9001)**, Prometheus/Grafana/Alertmanager, Kafka Broker+Controller #3, Kafka 64-B Collector | `ops` |
| `metadata` | `192.158.2.104` | 4 vCPU / 8GB | Docker/OpenMetadata/MySQL/Elasticsearch, **Trino 484(:8080)**, Kafbat Web UI(:18081, 배포 검증 상태 별도 확인) | `governance_nodes` |

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

### 2.3 Olist 배치·품질·계보·승인 + Iceberg Lakehouse (45~60)

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

55 5-VM Preflight → 56 worker3 Gravitino Iceberg REST(:9001) + PostgreSQL(iceberg_rest56)
                     │ HDFS /data/olist/iceberg-rest
                     ├─ Spark(Java 11, Iceberg 1.10.1, YARN)
                     └─ 57 Trino 484(Java 25, metadata:8080) SQL 조회
                            │
58 승인 v3 Parquet → REST Catalog `olist58_v3` 7 Iceberg 테이블
                            │ Silver5 + Quarantine1 + Gold1; 전체 데이터 parity
59 `olist59_v3` Gold 하나: audit_v3 → 29 DQ PASS → main fast_forward
                            │
60 worker3 systemd manual one-shot: Stage/Publish/Trino 교차 검증
   └─ v3 이미 공개되면 기존 상태와 증빙을 검증(정기 신규 수집 없음)
```

**버전은 입력 데이터 도착일이 아니라 출력 스냅샷의 식별자:** Raw v1, Parquet Warehouse v1/v2/v3, Iceberg 공유 Namespace `olist58_v3`/`olist59_v3`는 서로 다른 논리 범위다. v3는 승인된 **불변 입력**이며, 60번은 이를 **재검증·재공개**할 수 있지만 신규 데이터 수집/증분 처리용 스케줄러는 아니다. 48~49번 기본 버전 v1은 별도로 유지된다.

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

### 2.6 공유 Iceberg REST Catalog, Spark, Trino — 실제 설치 아키텍처 (55~57)

```text
  master (Spark submit, Java 11)                  metadata (Trino 484, Java 25)
             │                                             │
  YARN Driver/Executors (worker1/2)              Coordinator + Worker (동일 VM)
             │                                             │
             └────── Iceberg REST HTTP API ────────────────┘
                                     │
                          worker3:9001 Gravitino 1.3.1
                             dedicated Java 17
                              │            │
                       PostgreSQL       HDFS Warehouse
                    iceberg_rest56      /data/olist/iceberg-rest
                  (Catalog 등록정보)        │
                                    Iceberg metadata.json
                                    Manifest / Parquet
```

| 구분 | 호스트 / 포트 / 런타임 | 책임 |
|---|---|---|
| Spark 3.5.6 / Iceberg 1.10.1 | master에서 YARN 제출 → worker1/2 Java 11 | 데이터 정제·생성·정합성 검사·Iceberg 커밋 |
| Gravitino 1.3.1 Standalone Iceberg REST | worker3 `192.158.2.103:9001`, 별도 Java 17, systemd `bigtop-iceberg-rest` | 테이블 식별·메타데이터 조회·Iceberg REST 커밋 처리 |
| Gravitino JDBC Backend | worker3 로컬 PostgreSQL `iceberg_rest56`, 포트 5432 | 테이블 등록정보·메타데이터 위치 보존. 판매 데이터 미저장 |
| Trino 484 | metadata `192.158.2.104:8080`, 별도 Java 25, systemd `bigtop-trino57` | SQL 파싱/최적화·HDFS 파일 읽기·집계·결과 반환 |
| HDFS | master NameNode, worker1/2 DataNode | Iceberg Metadata/Manifest/실제 Parquet 저장 |
| OpenMetadata 2.0.6 | metadata Docker :8585 | 데이터 거버넌스·계보; 56번 Gravitino와 다른 목적 |

**조회 경로:** Trino Iceberg Connector → REST로 테이블 metadata location 확인 → Iceberg Metadata/Manifest 로드 → HDFS NameNode/DataNode에서 직접 Parquet 읽기 → Trino Task SQL 처리. **Gravitino는 Parquet를 전송하거나 SQL을 실행하지 않는다.** Spark는 YARN을 사용하지만 Trino는 YARN을 사용하지 않는 독립 JVM 서버다. 현재 Trino는 **단일 노드 Coordinator+Worker**이고 여러 VM에 Trino Worker를 추가한 분산 클러스터가 아니다.

**Catalog 이름:** Spark `shared.olist56.catalog_smoke`와 Trino `iceberg_rest.olist56.catalog_smoke`는 다른 물리 테이블이 아니라 **동일한 REST Catalog를 가리키는 각 엔진의 로컬 별칭**이다. 성공 증빙은 57번 실데이터 `id=56, source=spark` 및 `COUNT=1`이다. HMS와 HiveServer2는 이번 공유 Catalog 구성에 사용하지 않는다.

**안전성 경계:** HadoopCatalog `lab`(53~54번)과 REST Catalog(56~60번)는 독립적으로 유지된다. DB 백엔드가 있다고 다중 테이블 트랜잭션을 제공하는 것은 아니다. HDFS 3.x와 Java 런타임 호환성은 실제 Spark·Trino Smoke Test로 확인했다. Gravitino standalone은 내부 멀티사용자 인증/권한 분리가 완료된 엔터프라이즈 게이트웨이가 아니다.

### 2.7 Iceberg Silver/Gold 릴리스 — 실제 데이터 흐름 (58~60)

```text
승인된 Olist Parquet v3 (52 Approval)
     │
     ▼ 58 Spark materialization: schema/rows/exceptAll both directions
`shared.olist58_v3`: Silver 5개 + Quarantine 1개 + Gold Mart 1개
     │                                       └─ Trino 7 tables + SUM 검증
     ▼
59 새로운 Gold release table: `shared.olist59_v3.gold_daily_category_sales`
  ├─ main      : 처음에는 기술적 1행 seed (승인 전 BI 제공 금지)
  └─ audit_v3  : 승인 후보 전체 Gold 55,877행
          │
          ▼
     29개 Olist DQ + 원본 승인 기록 + Source Snapshot + exact parity
          │
          ├─ FAIL ──► 증빙 보존·공개 중단
          └─ PASS ──► 명시적 fast_forward(main ← audit_v3)
                                     │
                                     ▼
                       Trino 공개 Gold 55,877행 검증

60 worker3 One-shot: 58→Trino→59 Stage→Trino→59 Publish→Trino
   이미 공개한 동일 v3는 idempotent revalidation, 신규 ingest/timer 아님
```

**메달리온 계층:** Raw 9개 CSV = Bronze(파일 보존), 정제 주문·고객·상품·주문상품 Fact = Silver(Iceberg), 일자/카테고리 매출 집계 = Gold(Iceberg). 분류는 **프로젝트의 논리적 설계**이며 Iceberg가 Bronze/Silver/Gold를 강제하거나 자동 생성하는 기능이 아니다. 현재 58번은 Parquet v3 복사/동등성 검증이지 Raw CSV → Iceberg 직접 ETL로 변경한 것은 아니다.

**릴리스 경계:** 59번 WAP는 **Gold 하나**만 fast_forward한다. 58번 7개 테이블을 원자적으로 공개하거나, 모든 직접 SQL 조회자를 ACL로 차단하는 구성은 아니다. 60번은 실패 차단·중복 실행 제어를 추가한 **단일 컨트롤러** 오케스트레이션이다. 다른 경로로 직접 59번 Publish를 실행하면 60번 runner의 `flock` 잠금을 공유하지 않는다.


### 2.8 Kafka KRaft/관측/Web UI와 CDC 운영 경로 (61~69)

```text
61 58/59 Iceberg 8 assets → OpenMetadata 8 Lineage → Grafana release dashboard
62 별도 Iceberg maintenance table (compaction / snapshot rollback)
63 별도 Iceberg CDC MERGE (516 rows; live 512 / tombstones 4)

worker1:9092 ─┐
worker2:9092 ─┼── Kafka 4.2.2 KRaft 3 brokers + 3 controllers (:19093)
worker3:9092 ─┘         │                            └─ Kafbat UI metadata :18081
                       │
64 CDC Topic → Spark AvailableNow → Iceberg 520 rows (live 512 / tombstones 8)
                       │
65 Topic bigtop_olist_step65_cdc_v1 (Kafka End Offset 24)
        ↓
   Spark Structured Streaming (master submit, YARN cluster, 10s trigger)
        ↓ foreachBatch seq-guarded MERGE
   shared.olist65_stream.fact_cdc_running_v1 (521 / 512 live / 9 tombstones)
        + HDFS checkpoint /data/olist/iceberg-checkpoints/step65/v1
        + Step65-B: worker3 lightweight Kafka publisher, Trino/HDFS verifier
        + Step66: master opt-in watchdog (자원 부족 시 중복 submit 보호)
                       │
67 master Spark Commit / worker3 Kafka End (Node Exporter textfile, 60s timer)
        ↓
68 Prometheus recording/alert rules + Grafana (topic/partition Offset Lag)
        ↓
69 worker3 baseline/after Prometheus JSON + HDFS offsets/commits 목록 비교
```

- **Kafka 계층:** 64-A는 별도 Java17(`/opt/bigtop-kafka-kraft64/jdk17`)을 사용한다. `:9092`는 broker, `:19093`은 KRaft controller. 기존 Alertmanager와 포트 충돌을 피해 Controller를 `:19093`으로 변경했다. Kafka quorum과 HDFS/YARN HA는 독립적이다.
- **65-B:** 과거 `produce`/`verify` Spark 배치가 YARN `ACCEPTED`에 머물던 자원 병목을 제거하려고 worker3 Kafka CLI + master Trino/HDFS 검증으로 분리했다. 2026-11-08 `STEP65_LIGHT_VERIFY_PASS: 521/512/9 checkpoint=24 kafka_end=24 snapshot=7676611440228973561` 확인. 별도 Spark 검증 앱을 추가로 제출하지 않는다.
- **66:** master의 단일 지정 app 보호형 Watchdog. `apply`는 서비스와 비활성 Timer를 설치하며, `enable`을 명시해야 자동 감시를 켠다. `ACCEPTED`/`SUBMITTED` 상태에선 중복 제출 방지, 수동 stop 의도 반영, 장애 복구 빈도 제한 설계. **실제 enable/자동 복구 동작 증빙은 이 문서 기준 미확인**이다.
- **67:** master는 Step65 HDFS `commits/offsets`에서 *committed* Offset/Batch를 수집하고, worker3는 Kafka End Offset을 수집한다. 양쪽 Textfile Collector 60초 Timer 활성 및 Prometheus `node-exporter` Target 수집/Success=1 확인. Spark systemd Unit 템플릿의 Jinja2 줄바꿈 오류는 수정·재배포했다.
- **68:** Kafka와 Spark 수집 `instance`가 달라 `(topic,partition)` 기준 PromQL 결합. Offset Lag=0, 신규 Recording/Alert Rule과 Grafana는 사용자 apply/verify 확인. Alertmanager 라우팅/알림 실제 전달은 별도 확인한다. 일반 Kafka Consumer Group Lag와 구분한다.
- **69:** Prometheus Offset과 HDFS `offsets/commits/` file listing을 읽기 전용 JSON으로 수집·비교한다. 최근 baseline/after에서 Offset `24→24`, Batch `2→2`, Lag 0, `COMPARE_PASS`. 정상 Step65 stop/start와 Iceberg/Trino 검증을 별도로 수행했으나 **미완료 Micro-batch 재처리 또는 Exactly Once 실증은 아니다**.

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
| 테이블 포맷 | Apache Iceberg 1.10.1 | JAR master, 실행은 YARN | HDFS Parquet의 스냅샷·ACID 커밋·Time Travel·Branch WAP와 Olist Silver/Gold | Spark·YARN 실행 엔진 대체 아님 |
| 공유 Catalog | Gravitino REST 1.3.1 + Java 17 | worker3:9001 | Spark와 Trino의 공통 Iceberg REST API, 커밋/등록정보 관리 | SQL 엔진·Parquet 전송 서버 아님 |
| Catalog DB | PostgreSQL `iceberg_rest56` | worker3:5432 로컬 | Gravitino 내부 메타데이터 영속 저장 | Iceberg 행 데이터 저장 아님 |
| SQL 분석 | Trino 484 + Java 25 | metadata:8080 | Iceberg HDFS 데이터 SQL 분석·교차 검증 | Spark ETL 대체가 아니고 YARN Worker 아님 |
| 릴리스 | Olist 58~60 | master Spark + worker3 One-shot | 7 Iceberg 테이블 동등성, 실제 Gold WAP, 29 DQ, 멱등 공개 | 신규 실시간 수집/멀티테이블 원자 커밋 아님 |
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

### 3.3 Hive Metastore 대신 REST Catalog를 사용한 이유

HMS는 Iceberg Catalog의 유효한 선택지이며 Hive SQL 엔진/HiveServer2와는 분리해서 사용할 수 있다. 그러나 현재 프로젝트는 새 HMS를 추가하기보다 **Iceberg REST 프로토콜을 통일 인터페이스**로 사용했다. Spark 3.5.6의 Iceberg 1.10.1과 Trino 484가 공통 REST API에 접속하며, 메타데이터 등록정보의 영속 저장은 Gravitino가 PostgreSQL(`iceberg_rest56`)로 처리한다. 이것은 **JDBC가 전혀 없는 구조**가 아니라, *클라이언트가 DB 스키마에 직접 의존하지 않는* 설계다. REST 서버 버전/API/백엔드 호환성은 실제 테이블 생성과 SQL 교차 조회로 확인한다.

### 3.4 Spark SQL이 있는데 Trino를 설치한 이유

Spark도 SQL 실행·조회가 가능하다. Trino는 SQL 사용자와 BI 요청을 ETL용 Spark/YARN 실행과 **운영·자원 측면에서 분리**하고, 필요할 경우 다른 DB 커넥터와 연합 쿼리·반복 대화형 SQL 서비스를 제공하기 위해 설치했다. 현재는 **metadata 한 VM의 단일 노드**이므로 Trino의 다중 Worker 성능이나 높은 동시성까지 검증했다고 주장하지 않는다. 이번에 확인한 것은 REST Iceberg/HDFS를 통한 **실제 Spark 작성 데이터의 Trino 읽기 성공**이다.

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
│   ├── iceberg_spark_lab/, iceberg_transactions_lab/     # 53~54 lab
│   ├── iceberg_rest_catalog/                         # 56 Gravitino REST
│   ├── trino_iceberg_rest/                           # 57 Trino
│   ├── olist_iceberg_materialization/                 # 58 Silver/Gold
│   ├── olist_iceberg_wap/                             # 59 Gold WAP
│   ├── olist_iceberg_release60/                       # 60 one-shot
│   ├── olist_iceberg_observability61/                 # 61 Iceberg catalog / metrics
│   ├── olist_iceberg_maintenance62/                   # 62 compaction/rollback lab
│   ├── olist_iceberg_incremental63/                   # 63 incremental CDC lab
│   ├── kafka_kraft64/                                 # 64-A Kafka KRaft
│   ├── kafka_platform64b/                            # 64-B metrics/OpenMetadata
│   ├── kafka_ui64c/                                  # 64-C Kafbat
│   ├── olist_iceberg_kafka64/                         # 64 Kafka/AvailableNow CDC
│   ├── olist_kafka65/                                 # 65 continuous + lightweight verification
│   ├── olist_kafka66/                                 # 66 watchdog opt-in
│   ├── olist_kafka67/                                 # 67 Kafka/Spark progress collection
│   ├── olist_kafka68/                                 # 68 Prometheus Rule/Grafana
│   └── olist_kafka69/                                 # 69 recovery evidence
├── vendor/iceberg/, vendor/iceberg-rest/, vendor/trino57/, vendor/kafka64/, vendor/kafka-broker64/ # 다운로드 바이너리, Git 제외
├── artifacts/                        # 실행 JSON 증빙/보고서, Git 제외
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

`39`의 기본 UI 바로가기는 **NameNode, ResourceManager, Spark History, Prometheus, Grafana, Alertmanager 총 6개**다. DataNode/NodeManager 4개 링크는 사용자 요청으로 제거됐다. 당시 UI 링크는 YARN `18088`, Grafana `23000`, Alertmanager `19093`으로 변경됐다. **그러나 2026-11-07 기준 Kafka KRaft Controller가 `19093`을 사용하게 되었으므로 Alertmanager 바로가기는 재확인·수정이 필요하다.** 이 Playbook은 링크 URL을 바꿀 뿐 서비스 데몬 포트를 바꾸지는 않는다.

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

### 5.8 기존 Olist HDFS/Parquet 및 Iceberg Lab: 45~54

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

### 5.9 Iceberg REST Catalog·Trino·Silver/Gold·Gold WAP 및 릴리스: 55~60

| Playbook / 실행점 | 노드·핵심 구현 | 실제 확인된 결과 |
|---|---|---|
| `55-iceberg-trino-preflight.yml` | 5대 VM 읽기 전용 점검, `scripts/55-probe-node.py`/`55-review-preflight.py` | CPU/RAM/Java/포트/HDFS Lab PASS, `metadata` 경로 PermissionError 예외 처리 후 보고서 생성 |
| `56-iceberg-rest-catalog.yml` | worker3 Gravitino REST 1.3.1/Java17, PostgreSQL 전용 DB, HDFS Warehouse | REST API READY, Spark REST 테이블 `shared.olist56.catalog_smoke` 생성·조회 성공 |
| `57-trino-iceberg-rest.yml` | metadata Trino 484 + Java25, 독립 systemd, REST Connector/HDFS | Trino ACTIVE, `[[56, 'spark']]`, COUNT=1, Spark/Trino 교차 조회 PASS |
| `58-olist-iceberg-materialization.yml` | master Spark YARN이 승인 Parquet v3 → REST Iceberg 7개 테이블 변환 | 사용자 완료 확인; 컬럼·건수·양방향 `exceptAll`, Trino 비교. `warehouse` 옵션 오류 수정 이력 |
| `59-olist-iceberg-gold-wap.yml` | master Spark YARN `stage`/`publish`로 Gold 1개 `audit_v3` Branch, 29 DQ | Stage/Publish 완료 확인, Trino 55,877행 확인 |
| `60-olist-iceberg-release.yml` | worker3 수동 One-shot service, Python runner `plan/verify/stage/publish/run` | 2026-11-01 systemd 로그 `STEP60_PIPELINE_PASS: mode=run`, 기존 공개 v3 검증·정상 종료 |

**적용 Role:** `roles/iceberg_rest_catalog`, `roles/trino_iceberg_rest`, `roles/olist_iceberg_materialization`, `roles/olist_iceberg_wap`, `roles/olist_iceberg_release60`. **실행 자료:** `vendor/`(다운로드 바이너리), `artifacts/`(런타임 JSON 증빙)은 소스가 아니므로 기본 Git 제외. 고정 버전, JDK 아카이브 SHA-256, REST/RBAC·메모리 주의사항은 각 `docs/55*`~`docs/60*` 문서를 참고한다.


### 5.10 Iceberg 관리(61~63), Kafka 플랫폼/CDC(64-A/B/C, 64), 상시 운영·관측(65~69)

| Playbook | 대상 | 주요 실행 모드 / 역할 | 경계·현재 상태 |
|---|---|---|---|
| `61-olist-iceberg-observability.yml` | worker3 | Iceberg 8 자산/8 계보, Grafana 릴리스 지표, 별도 One-shot | 사용자 완료. Timer 자동 실행은 별도 확인 |
| `62-olist-iceberg-maintenance.yml` | master Spark/Trino 확인 | 격리된 maintenance_smoke compaction, rollback, 옵션 expire-lab | 사용자 완료; 58/59 공개 테이블 수정 금지 |
| `63-olist-iceberg-incremental.yml` | master Spark/Trino 확인 | seq/soft delete CDC `MERGE` | 사용자 완료, 결과 516행 |
| `64a-kafka-kraft-cluster.yml` | worker1/2/3 | `plan`, `preflight`, `install`, `verify` | Smoke PASS, Controller 19093, Broker 9092; 저장된 Cluster ID 재생성 금지 |
| `64b-kafka-platform-integration.yml` | worker3 | 기본 `plan`, `apply`, `verify` | Read-only Kafka 수집기, Grafana, OpenMetadata 명시 등록; Broker 무재시작 |
| `64c-kafka-web-ui.yml` | metadata | 기본 `plan`, `apply`, `verify` | Kafbat UI Docker, 읽기 전용. 사용자 화면 언급, `verify` 성공 로그 별도 확인 |
| `64-olist-kafka-iceberg-cdc.yml` | master (+ broker 검사) | `preflight`, `seed`, `produce`, `ingest`, `verify` | `AvailableNow` 단발성 CDC, 사용자 PASS(520행) |
| `65-kafka-streaming-operations.yml` | master/worker3 CLI | `preflight`, `upgrade`, `start/status/stop`, `produce/replay/reconcile/verify` | **Step65 경량 Verify PASS:** 521/512/9, Kafka/Checkpoint=24, Snapshot 불변. Replay 2회차 Guard는 발행 전 차단됨 |
| `66-kafka-streaming-watchdog.yml` | master, worker3 Dashboard | `plan/preflight/apply/status/verify/enable/disable` | 지정 Step65 앱만 감시, 자동 복구는 Opt-in. 설치/enable·실장애 자동 복구 결과 별도 확인 |
| `67-kafka-spark-progress.yml` | master, worker3 | `plan/preflight/apply/verify/enable/disable` | 양쪽 수집 서비스·60초 Timer 활성 및 Prometheus 양쪽 Target 실제 관측 PASS |
| `68-streaming-health.yml` | worker3 | `plan/preflight/apply/verify` | 사용자 배포·검증 확인, Prometheus recording/alert rules 및 Grafana, 실제 알림 전달 미검증 |
| `69-streaming-recovery-proof.yml` | worker3 (HDFS는 master 위임) | `plan/preflight/baseline/after/compare` | 증빙 저장/비교 PASS; 실제 stop/start와 결합한 체크포인트 안정성 확인, 미완료 Batch 복구 미검증 |

각 새 단계는 `roles/<role>/defaults/main.yml`에서 설정과 전용 경로를 명시한다. `64-A/B`는 설치 오류 수정 패치가 적용된 최종 파일을 기준으로 보고, 구버전 ZIP의 9093 Controller 포트/`when` Undefined 조건을 다시 덮어쓰지 않는다. 64-C 전역 바이너리 탐지 단위 테스트 문제도 수정본으로 관리한다.


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
                              55 VM Preflight
                                   ↓
                              56 Gravitino REST worker3 + PostgreSQL DB
                                   ↓
                              57 Trino metadata / Spark↔Trino shared table
                                   ↓
                              58 approved Parquet v3 → Iceberg Silver/Gold 7 tables
                                   ↓
                              59 Gold 1 table real WAP + 29 DQ + Trino
                                   ↓
                              60 Manual one-shot guarded release + verify
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

### 6.5 45~54번 기존 HDFS·Parquet·Iceberg Lab 실행 의존성 및 재현 명령

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

### 6.6 55~60번 배포·검증·릴리스 명령 (worker3 기준)

**기본 원칙:** 먼저 `make lint`와 각 단계 `test_*.py`, Plan 모드를 확인하고, **변경을 수반하는 옵션만 명시적으로 opt-in**한다. 56/57번 서비스가 이미 존재하면 재설치를 반복하지 않는다. 58/59번의 HDFS/REST 테이블은 삭제하거나 덮어쓰지 않는다.

```bash
cd ~/bigtop-cluster-ansible
make lint

# 55: 5 VM 진단 / 보고서
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/55-iceberg-trino-preflight.yml
python3 scripts/55-review-preflight.py artifacts/55-iceberg-trino-preflight.json

# 56: REST 서버 (기본 plan, 실제 적용 시 명시적 true)
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/56-iceberg-rest-catalog.yml
# .venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/56-iceberg-rest-catalog.yml -e iceberg_rest_apply=true
# Spark Smoke는 이미 통과: 필요 시에만 -e iceberg_rest_spark_smoke=true
curl -fsS http://192.158.2.103:9001/iceberg/v1/namespaces | python3 -m json.tool

# 57: Trino(기본 plan) / 실제 SQL 검증
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/57-trino-iceberg-rest.yml
curl -fsS http://192.158.2.104:8080/v1/info | python3 -m json.tool
python3 scripts/57-verify-trino.py --url http://192.158.2.104:8080

# 58: 승인 v3 Iceberg 물질화 (이미 완료; 재실행 시 동일성 검증)
# .venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/58-olist-iceberg-materialization.yml -e olist_iceberg_materialization_apply=true
python3 scripts/58-verify-trino.py --url http://192.158.2.104:8080 --proof artifacts/58-iceberg-v3-proof.json

# 59: Gold stage/publish (이미 완료, 불필요한 재공개를 피할 것)
python3 scripts/59-verify-trino.py --mode publish --url http://192.158.2.104:8080

# 60: 현재 공개 상태 읽기 전용 검증 및 마지막 실행 기록
python3 scripts/60-run-iceberg-release.py --mode verify
cat artifacts/60-iceberg-release-last-attempt.json
sudo journalctl -u bigtop-olist-iceberg-release60.service -n 100 --no-pager
```

### 6.7 실제 릴리스 재실행에 대한 안전 규칙

- `58`은 기존 7개 테이블을 **재생성·덮어쓰기 하지 않고 재검증**한다. 새 v4 데이터가 들어오면 v3 고정 Namespace를 다시 쓰지 말고 별도 버전 계약을 설계해야 한다.
- `59` Stage는 **실제 DQ 29개** 및 Snapshot parity/원본 승인 증빙을 재검증한다. FAIL 증빙을 삭제하고 재시도하지 않는다. Publish는 Gold 1개 테이블 `main`에 대한 **명시적** 변경이다.
- `60 --mode verify`는 로컬 증빙·Trino SQL 기반의 읽기 전용 확인이며 HDFS 전체 증빙을 별도로 직접 재조회하는 것은 아니다. `--mode run --allow-publish`는 Spark/HDFS 강검증과 함께 승인된 v3의 멱등 공개 검사를 진행한다.
- `60`의 `flock`은 **worker3 실행자 간의 잠금**이다. 운영자가 59번 Playbook을 별도 프로세스로 직접 실행하면 같은 잠금이 적용되지 않으므로 동시 실행을 피한다.
- `bigtop-olist-iceberg-release60.service`는 **수동 one-shot**이고 Timer가 없다. 실행 종료 후 `inactive (dead)` / `Deactivated successfully`는 정상일 수 있다. 판단 기준은 `STEP60_PIPELINE_PASS`와 상태 코드다.
- 그 외 단계의 원격 실행에서 `-e ansible_connection=local`을 전체 호스트에 강제하지 않는다.


### 6.8 64-A~69 명령 및 단계별 검증 (worker3 기준)

> 이미 동작하는 Kafka Broker, HDFS 체크포인트 및 Iceberg 데이터를 재초기화하지 않는다. `plan/preflight/verify/status/compare`는 확인용; `start/stop/produce/replay/enable/apply`는 대상 변경이 발생할 수 있는 별도 실행이다.

```bash
cd ~/bigtop-cluster-ansible

# Kafka KRaft 설치 상태 (읽기 전용)
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/64a-kafka-kraft-cluster.yml -e kafka_kraft64_mode=verify

# 65: App 상태 + Trino/HDFS commit/offset + snapshot (경량 Verify)
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/65-kafka-streaming-operations.yml -e olist_kafka65_mode=status
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/65-kafka-streaming-operations.yml -e olist_kafka65_mode=verify

# 66: Watchdog 상태; enable은 옵트인, 자동 복구 테스트를 일상 점검으로 수행하지 않음
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/66-kafka-streaming-watchdog.yml -e olist_kafka66_mode=status

# 67: Kafka End & Spark Committed Offset 수동 수집/검증
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/67-kafka-spark-progress.yml -e olist_kafka67_mode=verify

# 68: Recording/Alert Rules + Grafana 배포 상태 검증
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/68-streaming-health.yml -e olist_kafka68_mode=verify

# 69: 기존 불변 JSON 비교. 새 실험에는 별도의 evidence_dir을 지정한다.
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/69-streaming-recovery-proof.yml \
  -e olist_kafka69_mode=compare \
  -e "olist_kafka69_evidence_dir=$PWD/artifacts/step69/restart-20261108-181312"
```

#### 65-B 경량 검증 및 정상 종료·재기동 실제 관찰 (2026-11-08)

- 최초 520행 Seed → Step65 CDC 반영 521행(512 Live, 9 Tombstones). `STEP65_LIGHT_VERIFY_PASS`, checkpoint=24, kafka_end=24, snapshot=`7676611440228973561` 확인. HDFS `_SEED_REPORT/_PRODUCE_REPORT/_APPLY_REPORT` 기존 증빙을 읽었다.
- Step65 `stop`: `application_1794117087936_0002` 정상 중지와 `STEP65_CHECKPOINT_PRESERVED` 출력. Step65 `start`: `application_1794117087936_0003` 제출 성공. 직후 `status`는 **ACCEPTED**였으므로 *당시 `RUNNING` 전환은 로그에 미포함*. 후속 Step65 Verify, Step67 수집, Step69 비교가 PASS였다는 사실과 구분한다.
- Replay 실행은 `STEP65_LIGHT_FAILED: Replay permitted only once from initial 12 offsets; current end=24`로 **보호 조건에서 거부**. 추가 Replay를 증명하기 위해 guard 조건을 제거하거나 토픽/HDFS checkpoint를 초기화하지 않는다. 초기 12개 이후 12개 기록은 존재하지만, 그 이유를 Offset 숫자만으로 확정하지 않는다.
- Step69 독립 `baseline/after` 증빙 저장 및 `COMPARE_PASS`: `kafka=24→24`, `spark=24→24`, HDFS `offsets/commits` Batch `2→2`. 입력 증가 없이 실행한 검증으로, 결과는 **관찰된 무회귀/안정성**이지 *강제 장애 중 in-flight Batch 복구* 증명이 아니다.
- Step69 파일은 기본 `artifacts/step69/after-progress.json` 경로의 덮어쓰기를 거부한다. 이미 해당 파일이 존재할 경우 `-e "olist_kafka69_evidence_dir=$PWD/artifacts/step69/run-<unique>"`로 새 디렉터리 지정 후 Baseline/After를 새로 수집한다. Git에는 어떤 증빙 JSON도 커밋하지 않는다.

```bash
# master/worker3 메트릭·타이머 간격 확인
ssh master 'systemctl list-timers --all "bigtop-olist-step67-*" --no-pager'
systemctl list-timers --all 'bigtop-olist-step67-*' --no-pager
curl -fsSG http://127.0.0.1:9090/api/v1/query --data-urlencode 'query=bigtop_olist_step68_offset_lag'
```

**주기 실행과 실패 처리는 구분:** Step66은 YARN 앱 중단 감시·통제된 자동 복구용이지만 실제 활성/복구 시험 결과는 미확인이다. Step67 Timer는 60초 간격 실제 실행, Step68은 기록·경고 규칙 로드 확인, Step69는 사용자 입력에 따른 읽기 전용 증빙 저장만 수행한다.

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
| Alertmanager | 이전 콘솔 링크 `http://worker3:19093` **재확인 필요** | 현재 Kafka Controller `:19093`과 충돌하는 이전 링크. Alertmanager 실제 Listen은 `ss -lntp`로 확인 후 갱신 |
| OpenMetadata | `http://192.158.2.104:8585` | 거버넌스 전용 VM |
| OpenMetadata health | `http://127.0.0.1:8586/healthcheck` | metadata VM **내부** |
| Gravitino Iceberg REST | `http://192.158.2.103:9001/iceberg/v1/config` | worker3 REST API, 일반적인 Web UI 아님 |
| Trino 484 API | `http://192.158.2.104:8080/v1/info` | metadata 단일 노드 SQL 엔진; API 상태와 실제 SQL 조회를 구분 |

**주의:** `23000`은 호스트 PC에서 사용하는 VirtualBox 포트포워딩 주소이고 Grafana가 worker3 내부에서 listen하는 포트는 `3000`이다. `18088`, `19093` 등 나머지 링크는 *39번 Console 바로가기 기본값*을 확인한 것이다. 실제 데몬 listen 포트, NAT 포트포워딩, 리버스 프록시 방식은 `ss -lntp`, OS 설정, VirtualBox 설정으로 따로 확인해야 한다. `all.yml` 등에서 링크 목록을 override했다면 그 값이 기본값보다 우선한다.

### 7.3 Kafka 통합 화면과 API (2026-11-07)

| Component | endpoint/metric | 의미/주의 |
|---|---|---|
| Kafka Brokers | worker1/2/3 `:9092` | Client bootstrap. JVM Kafka 버전 검사에는 전용 Java17 사용 |
| Kafka KRaft Controllers | worker1/2/3 `:19093` | 브로커 클라이언트 포트가 아니며 외부 UI에 연결하지 않음 |
| Kafka 64-B Prometheus | `bigtop_kafka64_collector_success`, `kafka_platform64b.prom` | CLI 관측/Quorum/ISR/토픽 상태. Consumer Lag/JMX 통계 아님 |
| Kafka 64-B Grafana | UID `bigtop-kafka-kraft64` | Broker·Partition 가용성/리더십 대시보드 |
| OpenMetadata Messaging | service `bigtop_kafka_kraft64` | 별도 `sudo python3 scripts/64b-kafka-platform.py --mode metadata` 시 등록. 자동 스키마/컬럼 계보는 아님 |
| Kafbat UI | metadata `:18081` | Topic/Message/Consumer Group 조회. `read-only` UI는 Kafka ACL을 대신하지 않음 |
| Alertmanager | **worker3 `:9093` 실제 리스너 확인 필요** | 옛 문서의 `:19093` Console 링크는 Kafka Controller와 중복됨. `ss -lntp`로 확인 전 링크 단정 금지 |

**운영상 중요한 차이:** Kafka Consumer Group이 없는 Spark Streaming의 데이터 처리 진도는 Kafbat Lag에 그대로 반영되지 않을 수 있다. Kafka 체크포인트는 Spark `checkpoint/offsets`, `checkpoint/commits`, micro-batch 로그와 비교한다. Consumer Lag 증가 자동 알림은 **아직 추가하지 않았다**.


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
| Gravitino Catalog PostgreSQL | worker3 DB `iceberg_rest56`(기존 PostgreSQL 서버 내 별도 DB) | 56번 전용 계정/비밀번호, REST 서버 내부 JDBC | **이 DB 백업·복원은 별도 검증 필요**. Web Console DB 백업에 자동 포함된다고 가정 금지 |
| Iceberg REST Warehouse | HDFS `/data/olist/iceberg-rest` | Iceberg metadata/manifest/Parquet, HDFS 복제 | 53~54 HadoopCatalog `/iceberg-lab`와 별도. Metadata와 데이터 파일 모두 보존해야 함 |
| Olist v3 Silver/Gold | Iceberg REST `olist58_v3`, 7개 테이블 | 58번 Parquet 원본과 `exceptAll` 일치 증빙 | 기존 v3 Parquet 자체의 대체·삭제는 수행하지 않음 |
| Gold WAP 공개 | Iceberg REST `olist59_v3.gold_daily_category_sales` | 59번 `_STAGE_REPORT.json`, `_PUBLISH_REPORT.json` | Gold 1개 Snapshot만 `main` 공개; 다중 테이블 원자성 아님 |
| Iceberg 검증 증빙 | HDFS `/data/olist/iceberg-validation/step58/v3` 및 `step59/v3`; worker3 `artifacts/` | PASS·Snapshot·Trino 교차 검증 확인 | 로컬 artifacts만으로 HDFS Proof 완전성을 대체하지 않음 |
| Trino 설치/실행 상태 | metadata `/opt/bigtop-trino57`, `/var/lib/bigtop-trino57`, `/etc/bigtop-trino57` | 독립 Java 25 + systemd, 메모리 제한 | 비HA 단일 노드. HDFS/REST 장애 시 쿼리 영향 |

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

### 9.5 Quality Gate(52), 샘플 WAP(54), 실제 Gold WAP(59) 비교

| 영역 | 52 Parquet v3 품질 승인 | 54 Iceberg HadoopCatalog 샘플 | 59 REST Iceberg 실제 Gold WAP |
|---|---|---|---|
| 대상 | 46번 Parquet Warehouse v3 | `lab.olist.iceberg_wap_step54` 소규모 행 | `shared.olist59_v3.gold_daily_category_sales` **55,877행** |
| 검증 | 최신 PASS 29 DQ·Manifest SHA·7 `_SUCCESS`·기본 168h | 합성 데이터/스냅샷/Branch 확인 | **29 DQ + Step58 7개 입력 Snapshot + 전체 Gold parity + 승인 증빙** |
| 공개 방식 | HDFS `_APPROVAL.json` 기록(읽기 차단 없음) | 샘플 `audit_step54` → `main` | 실제 Gold `audit_v3` → `main` |
| 범위 | 승인 기록만 발급 | Lab 및 SQL 변경 실습 | Gold 하나의 테이블 버전 Publish |
| 멀티테이블 원자성 | 미구현 | 미구현 | **미구현** |

59번의 기술적 main(처음 1행)을 실제 데이터가 공개된 것으로 소비자에게 제공하면 안 된다. Branch를 숨기는 ACL·별도 Serving View 없이 내부 사용자가 직접 네임스페이스/브랜치에 접근할 수 있는 권한을 갖는다면 WAP의 관례만으로 공개/미공개를 강제하지 못한다.

### 9.6 Gravitino REST Catalog와 OpenMetadata 역할 구분

| 컴포넌트 | 다루는 메타데이터 | 사용자 요청/운영 |
|---|---|---|
| Gravitino standalone REST | Iceberg 테이블 식별자, 현재 metadata file location, REST 커밋 | Spark/Trino가 SQL 실행 시 테이블을 찾고 커밋하는 데 필수 |
| Iceberg Metadata(JSON/Manifest) | Schema·Snapshot·Partition·데이터 파일·통계 | HDFS에 저장된 테이블 상태, REST 서버가 Parquet를 대체하지 않음 |
| OpenMetadata | 사업 용어·오너·태그·스키마·Lineage·거버넌스 | 50~51번 **기존 Olist HDFS 경로** 중심 등록; 58~60번 Iceberg 신규 테이블 등록은 61번 미구현 |
| PostgreSQL(Gravitino backend) | Catalog 내부 등록정보와 metadata 위치 | worker3 `iceberg_rest56`, Olist 레코드 미저장 |

### 9.7 60번 One-shot 릴리스의 의미

현재 60번은 **v3 고정된 불변 승인 데이터**를 대상으로 순차 검증 및 Publish를 수행하는 수동 One-shot 작업이다. 사용자 실행 로그에서 Stage 증빙 PASS, 기존 공개 감지, Gold 재검증, Trino 55,877행, `STEP60_PIPELINE_PASS: mode=run` 및 systemd 정상 종료가 확인됐다. **새 날짜/새 원본을 주기적으로 수집하는 Airflow DAG나 변경분 CDC 파이프라인은 아니다.** 새 버전에 대한 예약/재처리/릴리스 레지스트리·7개 테이블의 일관된 snapshot view는 추후 설계가 필요하다.

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
9. Gravitino REST(:9001)·PostgreSQL Catalog DB, Trino(:8080) 상태 및 Iceberg SQL 샘플 조회.
10. v3 최신 Quality/Approval, 58~59번 HDFS 증빙, 60번 One-shot 실행 이력(`STEP60_PIPELINE_PASS`) 확인.
11. Snapshot 파일 수·retention 계획 및 metadata/Gravitino DB 백업 상태 별도 확인.
12. Kafka/Spark Step67 `collector_success`, Step68 Offset Lag/Rule Health, Step65 YARN 상태, Step66 Watchdog opt-in 여부 확인.
13. 장애 복구 훈련 중 수집한 Step69 증빙은 `artifacts/step69/`에 격리해 보관하며, 실행 로그로 검증 범위를 명시한다.

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

**추가 확인(REST Catalog/Trino/Gold):**

```bash
# worker3
systemctl status bigtop-iceberg-rest --no-pager
curl -fsS http://192.158.2.103:9001/iceberg/v1/namespaces | python3 -m json.tool
curl -fsS http://192.158.2.104:8080/v1/info | python3 -m json.tool
python3 scripts/57-verify-trino.py --url http://192.158.2.104:8080
python3 scripts/60-run-iceberg-release.py --mode verify
sudo journalctl -u bigtop-olist-iceberg-release60.service -n 100 --no-pager

# metadata: Trino 자원과 서버 로그
systemctl status bigtop-trino57 --no-pager
systemctl show bigtop-trino57 -p MemoryCurrent -p MemoryMax
sudo journalctl -u bigtop-trino57 -n 100 --no-pager
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
| REST API는 정상인데 Spark 테이블 생성 실패 | Gravitino `:9001` 로그, REST `warehouse` 옵션 사용 여부, Hadoop HDFS 설정 | 시스템 Java 변경·기존 Lab 삭제 |
| Trino API는 ACTIVE인데 SQL 조회 실패 | Catalog URI, `fs.hadoop.enabled=true`, NameNode/DataNode 네트워크·HDFS 클라이언트 XML, SQL Query failure | API 200만으로 데이터 읽기 PASS 선언 |
| 58번 재실행 Parity/스키마 불일치 | 승인 v3 Manifest와 7개 Iceberg Snapshot, `exceptAll` 결과, YARN Driver Traceback | 기존 REST 테이블 무조건 DROP |
| 59번 Gold Stage/Publish 불일치 | `_STAGE_REPORT`의 29 DQ·Snapshot ID, `_PUBLISH_REPORT`, Gold main/audit ref | FAIL 증빙 삭제 후 강제 Publish |
| 60번 One-shot 종료/실패 | `journalctl -u bigtop-olist-iceberg-release60`, `artifacts/60-iceberg-release-last-attempt.json`, `STEP60_PIPELINE_PASS` | 정상 종료된 inactive 상태를 장애로 오인 |

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
| 55번 `PermissionError` | metadata의 보호된 OpenMetadata 경로에 `Path.exists()` 실행 | 권한이 없으면 `permission_denied`로 기록하고 전체 진단을 중단하지 않음 |
| 56번 Vendor 다운로드 404 | Maven Central PostgreSQL `.jar.sha256` 파일 조회가 404 | 검증된 SHA-256 고정값으로 JAR 다운로드 후 검사; 부분 다운로드 시 배포 중단 |
| 56번 Role Lint 32건 | 변수/`register`의 role-prefix 규칙 미준수 | `iceberg_rest_catalog_*` 접두사로 통일; `vendor/` 기존 존재는 오류가 아님 |
| 58번 `Couldn't find Iceberg configuration for catalog hdfs://...` | Spark REST Catalog에 물리 `warehouse` URI를 전달해 Gravitino가 Catalog 식별자로 해석 | Spark의 `spark.sql.catalog.shared.warehouse`를 제거하고 서버 관리 Warehouse 사용; YARN Driver 로그 확인 |

### 10.4 2026-11-07~08 Kafka/Streaming 실제 오류와 교정

| 이슈 | 확인된 원인 | 안전한 대응 / 재발 방지 |
|---|---|---|
| 64-B `Kafka 64A binary missing` | 64-A의 다운로드/Preflight만 완료되고 실제 설치 전 | 64-A `install/verify/smoke` 먼저. 바이너리만 수동 복사하거나 64-B 사전 검사 제거 금지 |
| 64-A worker3 `9093 already in use` | worker3 기존 서비스와 Kafka controller listener 충돌 | Controller **19093**으로 3노드 설정/Quorum Voters 모두 변경. 기존 서비스 강제 중지 금지 |
| 64-A `Missing stable cluster id` | worker3 preflight 실패로 Cluster ID 생성 Play 생략 | 선행 오류 해결. 다른 노드 임의 KRaft format/Cluster ID 생성 금지 |
| 64-A `meta.properties Permission denied` | 서비스 전용 소유자 `0750` data dir의 Ansible stat task 권한 부족 | **읽는 Task만 `become: true`**; data-dir chmod 777 또는 삭제 금지 |
| 64-A Java `class file 61.0 vs 52.0` | Kafka CLI Smoke가 시스템 Java8 사용 | Smoke에 Kafka 전용 `/opt/bigtop-kafka-kraft64/jdk17`를 강제 지정. Hadoop 전역 Java 업그레이드 금지 |
| 64-B `kafka_platform64b_mode undefined` | `include_role`의 default가 Playbook 외부 `when`에 노출되지 않음 | `(kafka_platform64b_mode | default('plan'))` 적용; default Plan 무변경 유지 |
| 64-C `test_not_committing_binaries FAIL` | `ROOT.rglob('*')`가 기존 vendor JAR를 탐지해 오탐 | 새 UI Role 내부만 검사하도록 단위 테스트 수정; 기존 vendor 삭제 금지 |
| 65 Topic does not exist | `bigtop_olist_step65_cdc_v1` 생성 전 start | worker3에서 `bash scripts/65-kafka-topic.sh create/describe`, RF3/minISR2/ISR3 확인 |
| 65 `produce`/`verify` YARN `ACCEPTED` | RUNNING Streaming이 자원을 점유한 2-worker 소형 YARN 클러스터에 추가 Driver/AM/Executor를 할당할 자원이 부족 | **65-B 경량 Kafka CLI/Trino/HDFS Verifier 적용·검증 완료**, 별도 Spark 제출 회피 |
| 67 `hdfsTimeoutStartSec=120` 실행 실패 | Jinja2 Service `ExecStart` 끝의 조건문과 다음 Unit 설정 사이 줄바꿈 소실 | Template 원본의 `TimeoutStartSec`을 독립 줄로 두고 `--hdfs-bin` 인자를 inline 조건식으로 렌더링; `apply→verify` 양 노드 PASS |
| 69 After 재수집 `File exists` | 증빙 JSON 불변 저장 정책으로 동일 `after-progress.json` 경로 덮어쓰기 거부 | 기존 파일 보존; 실행별 새 `olist_kafka69_evidence_dir` 사용 |
| 65 Replay 재실행 `current end=24` 거부 | 최초 12 Offset 이후 한 번만 Replay 허용하는 안전 Guard 조건 | 조건 제거/Offset 초기화 금지. 이전 Replay 증빙 검토 또는 새 격리 Step70 시나리오 사용 |


## 11. 보안·운영 경계

- **Ansible SSH 키:** worker3의 공개키를 제어 대상의 `authorized_keys`에 등록한다. metadata의 개인키를 전체 Hadoop 노드에 복사할 필요는 없다. *개인키 배포 금지*.
- **비밀번호 없는 sudo:** 개인 실습 편의 설정이지만 실운영에서는 sudo 명령 허용 범위를 제한해야 한다.
- **Nginx Basic Auth:** 콘솔 접근 제어의 출발점이다. 사용자별 RBAC, 작업 승인, 정밀 감사 추적을 대신하지 않는다.
- **내부망 목표:** 기업용 On-Premise/격리된 내부 데이터망이 전제다. 모든 개발 단계를 외부 공개 SaaS 수준의 TLS·SSO 선구축에 묶지 않는다. **그러나 내부 다중 사용자 환경에서도 계정 분리·최소 권한·감사·네트워크 경계는 필요**하다.
- **OpenMetadata·Gravitino·Trino HTTP:** 현재 VM 간 평문 내부 REST/HTTP를 사용한다. 내부 접근만 허용하고, 향후 내부 사용자 인증/TLS, 데이터 정책 및 프록시 배치를 단계적으로 검증한다. Gravitino Standalone에는 완성된 테이블 단위 인증·권한 체계가 없다.
- **PostgreSQL reader:** 메타데이터 수집 용도로만 사용하되 DB `SELECT` 권한은 행 데이터까지 읽을 수 있으므로 비밀번호를 강하게 보호한다.
- **Data Classification:** 태그나 용어를 생성했다고 해서 자동으로 민감정보 탐지·마스킹·DB 접근제어가 이뤄지는 것은 아니다.
- **Git 제외 대상:** `inventory/hosts.ini`, `inventory/group_vars/all.yml`, SSH 개인키, 실제 `.env`, JWT, DB 비밀번호, DB 덤프/백업, `vendor/` (JDK/JAR/서버 아카이브), `artifacts/` (Step69 포함 모든 실행 증빙), Olist 원본, 실행 로그, 임시 ZIP/패치 번들, `__pycache__/`·`*.pyc`, `.venv/`, 캐시·테스트 결과. `git status --short` 및 `git diff --cached`를 확인한다.
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
| Iceberg DML/Schema/샘플 WAP(54) | **stage·publish 사용자 확인** | 샘플 테이블만 변경 |
| 5 VM Preflight(55) | **진단 완료** | Java·메모리/포트·HDFS Lab 확인; 권한 오류 처리 후 전체 결과 수집 |
| Gravitino REST + PostgreSQL + HDFS(56) | **배포 및 Spark 실데이터 검증** | worker3:9001, Java17, `iceberg_rest56`, Spark Iceberg REST Smoke 성공 |
| Trino 484 / Spark 교차 SQL(57) | **PASS** | metadata:8080, Java25, `[[56, 'spark']]`, COUNT=1 |
| Olist v3 Iceberg Silver/Gold(58) | **완료(사용자 확인)** | Iceberg 7개, 원본 Parquet 행수/스키마/전체 레코드 parity 및 Trino |
| Olist 실제 Gold WAP(59) | **Stage·Publish 완료(사용자 확인)** | 29 DQ, `audit_v3` → Gold `main`, 공개 데이터 55,877행 |
| Iceberg 릴리스 자동화(60) | **One-shot 실행 PASS** | 2026-11-01 systemd 로그, v3 기존 공개 감지·검증, Trino PASS 및 정상 종료 |
| Iceberg 유지보수 격리 실습(62) | **사용자 완료 확인** | compaction, snapshot rollback, 별도 테이블, Trino 검증 |
| Iceberg CDC 증분 MERGE(63) | **사용자 완료 확인** | 48 이벤트/16 키, 516행(유효512/삭제4) |
| Kafka KRaft 3-node(64-A) | **실측 Smoke PASS** | broker 9092, controller 19093, 3 ISR, minISR2, 전용 Java17 |
| Kafka 메트릭·OpenMetadata(64-B) | **Preflight 실측, 후속 사용자 확인** | CLI collector/dashboard 및 명시적 Messaging Service 등록; timer/실제 등록 상태 별도 확인 |
| Kafka Kafbat Web UI(64-C) | **사용자 UI 언급, 최종 검증 로그 미제공** | Docker :18081, 읽기 전용; 최초 8/9 테스트 실패는 잘못된 전역 바이너리 검사로 수정본 동봉 |
| Kafka AvailableNow Iceberg CDC(64) | **사용자 통과 확인** | 520행(유효512/삭제8), 체크포인트 유지, Gold와 분리 |
| Kafka Spark Iceberg CDC(65/65-B) | **경량 Verify PASS; 재기동 후 데이터 불변 확인** | 521/512/9, Kafka End=Checkpoint=24, Snapshot 7676611440228973561; 별도 YARN 배치 없이 확인. 새 App RUNNING 최종 확인은 별도 |
| Spark/YARN Watchdog(66) | **구현·테스트 준비; 실제 동작 미확인** | 의도 상태/중복 submit 방지/타이머 opt-in 설계. enable 여부·자동 복구 테스트 기록 미제공 |
| Kafka/Spark Progress(67) | **설치·Timer·Prometheus PASS** | master/worker3 oneshot PASS, 60초 `active/waiting`, 양쪽 collector_success=1 |
| Streaming Lag/Alerts(68) | **사용자 Apply/Verify 확인** | Recording/Alert Rule + Grafana 설치 보고, Offset Lag=0. Alertmanager 전달 실증 별도 |
| Recovery Evidence(69) | **baseline/after/compare PASS** | stop/start 후 24→24, Batch 2→2, Iceberg 521/512/9 Snapshot 불변. 미완료 Batch 재처리 미검증 |

| 범용 Spark OpenLineage Listener / 컬럼 계보 | **미구축** | 51번 파일 단위 계보와 구분 |
| 내부 사용자별 Catalog/Trino RBAC·강제 승인 후 조회 | **미구축** | REST/Trino는 구축됐지만 BI 소비자 접근 강제, multi-table 공개 정책은 아직 없음 |
| 48 Scheduler/51 Reconcile Timer 주기 실행 | **상태 별도 확인** | 기본 비활성; 설치·수동 실행과 주기 자동 실행은 다름 |
| OpenMetadata 전용 백업·복원 | 미구축 | 41번 Docker 볼륨 영속화만 확인 |
| REST Iceberg OpenMetadata/Grafana 통합(61) | **사용자 완료 확인** | 8 Iceberg 자산·8 계보 관계 및 Gold 릴리스 관측; Timer 주기 실행은 별도 점검 |
| Gravitino Catalog DB·Iceberg HDFS Metadata 백업·복구 | **운영 검증 필요** | Web Console PostgreSQL 논리 백업 대상과 구분 |
| 실운영 인증·TLS·RBAC | 미구축 또는 보강 필요 | 실습용 Basic Auth·Bot 토큰 중심 |

### 12.2 다음 세션 우선순위 (69번 읽기 전용 복구 증빙 이후)

1. **65/66 상태 확정:** 새 YARN App `application_1794117087936_0003`이 `RUNNING`에 전환되었는지, Step66 Watchdog Timer가 명시적으로 enable됐는지 상태 재조회. 타이머 설치·자동 복구 성공을 혼동하지 않는다.
2. **Step70 격리된 CDC 재처리·멱등성 검증:** 기존 65 Guard 우회 없이 **별도 Topic, Checkpoint, Iceberg 테이블**과 재현 가능한 Insert/Update/Delete·중복·역순 CDC 이벤트. 미완료 Batch의 장애 후 재실행, Kafka End/Committed Offset 변화, Iceberg PK+Version 최종 상태와 Snapshot 검증을 분리한다.
3. **68 Alertmanager 전달 시험:** Prometheus Rule이 존재하는 것과 실제 Alertmanager Receiver 전달은 다르다. 누락 수집, 지속 Lag 조건을 통제된 테스트로 검증하고 운영 경보 임계값을 조정한다.
4. **운영 리소스/복구 정책:** YARN Queue·AM/Executor 할당, Gravitino/PostgreSQL/OpenMetadata 및 Iceberg Metadata 백업/복구, 내부 계정별 SQL 권한·감사, Gold 다중 테이블 릴리스 일관성.
5. **미검증 초기 기능:** 38번 원격 백업, 43 분류, 44 View 계보, 48/51 Timer 등 기존 미완료 항목의 실제 VM 검증.

## 13. 문서 유지보수 규칙

새 기능을 추가할 때 기존 README 말미에 작업 로그를 계속 누적하지 않는다.

1. **`README.md`:** 현재 구조·실제 동작·대표 기능·빠른 명령만 유지한다. 단계 번호를 홍보성 이력으로 나열하지 않는다.
2. **`docs/PROJECT_GUIDE.md`:** 아키텍처, 컴포넌트 선정 근거, Playbook별 역할·의존성·운영 흐름을 **현재 상태 기준으로 갱신**한다.
3. **`docs/<기능>.md`:** 해당 기능의 변수, 신규 API, systemd unit, 세부 실행/복구 명령을 보관한다.
4. **`docs/runbooks/`:** 장애 시 상황/탐지/확인/완화/복구/재발 방지 절차로 구성한다.
5. **미확인 기능은 미확인으로 기재:** 패키지를 생성했다는 사실과 VM에서 실제 배포·검증되었다는 사실을 혼동하지 않는다.
6. **프로젝트 구조 변경 시 둘 다 수정:** 신규 VM/서비스가 생기면 README 구성표, 본 문서 아키텍처, Playbook 매핑을 같이 고친다.
7. **실제 소스 기준 검증:** `find playbooks -maxdepth 1 -name '*.yml' | sort`와 `find roles -maxdepth 1 -mindepth 1 -type d | sort`로 현재 작업 트리를 비교한다.
8. **하루 작업 단일 커밋:** 같은 날 적용한 **Playbook/Role/Script/Test/Docs/README/PROJECT_GUIDE** 변경을 모두 함께 검토해 커밋한다. 문서만 별도 커밋하지 않으며 다운로드/비밀정보/실행 산출물은 제외한다.

### 관련 기존 문서

- `docs/runbooks/` — 재부팅·모니터링 알림·NodeManager 디스크·Spark History/YARN logs·애플리케이션 실패.
- `docs/web_console_postgres.md`, `docs/web_console_metadata.md` — 31·32번.
- `docs/web_console_pg_backup.md`, `docs/web_console_pg_backup_observability.md`, `docs/web_console_pg_backup_alerts.md` — 34~36번.
- `docs/web_console_pg_remote_replica.md`, `docs/web_console_service_ui_links.md` — 38·39번.
- `docs/40-governance-bootstrap.md`, `docs/41-governance-openmetadata.md`, `docs/42-governance-postgresql-catalog.md`, `docs/43-governance-catalog-enrichment.md`, `docs/44-governance-lineage.md` — 기존 거버넌스 개별 문서.
- `docs/45-olist-*.md` ~ `docs/52-olist-*.md` — 실데이터 적재·ETL·품질·운영·거버넌스·승인 상세 문서.
- `docs/53-iceberg-apply.md`, `docs/53-iceberg-architecture.md`, `docs/54-iceberg-apply.md`, `docs/54-iceberg-architecture.md` — Iceberg HadoopCatalog Lab 문서.
- `docs/55-iceberg-trino-preflight.md`, `docs/55-shared-catalog-trino-design.md` — 공유 Catalog 진단/선정 문서.
- `docs/56-iceberg-rest-apply.md`, `docs/56-iceberg-rest-architecture.md` — Gravitino REST + PostgreSQL + HDFS.
- `docs/57-trino-iceberg-rest-apply.md`, `docs/57-trino-iceberg-rest-architecture.md` — Trino 484 + REST/HDFS SQL.
- `docs/58-olist-iceberg-apply.md`, `docs/58-olist-iceberg-architecture.md` — Iceberg Silver/Gold 7개와 Parquet 동등성.
- `docs/59-olist-iceberg-apply.md`, `docs/59-olist-iceberg-architecture.md` — 실제 Gold WAP/29 DQ.
- `docs/60-olist-iceberg-release-apply.md`, `docs/60-olist-iceberg-release-architecture.md` — 멱등 릴리스 One-shot 및 운영 증빙.
- `docs/61-olist-iceberg-{apply,architecture}.md` — Iceberg OpenMetadata/관측.
- `docs/62-olist-iceberg-maintenance-{apply,architecture}.md` — 격리된 compaction/snapshot rollback.
- `docs/63-olist-iceberg-incremental-{apply,architecture}.md` — 합성 CDC MERGE.
- `docs/64a-kafka-kraft-{apply,architecture}.md`, `docs/64b-kafka-platform-{apply,architecture}.md`, `docs/64c-kafka-web-ui-{apply,architecture}.md` — Kafka 설치/운영/UI.
- `docs/64-olist-kafka-iceberg-{apply,architecture}.md`, `docs/65-kafka-streaming-{apply,architecture}.md` — Kafka CDC·상시 스트리밍/복구.
- `docs/65-resource-and-recovery-notes.md` — 2026-11-07 당시 YARN 자원 경합 및 미완료 이력을 기록한 과거 문서(현재 상태는 이 가이드·65-B 문서 기준).
- `docs/65-low-resource-producer-verifier.md`, `docs/66-kafka-streaming-watchdog.md`, `docs/68-streaming-health.md`, `docs/69-streaming-recovery-proof.md` — 오늘 추가·확인한 Streaming 경량화, Watchdog, 관측, 복구 증빙 상세 문서.

---

**요약(2026-11-08):** 5대 VM 내부망 기반 HDFS/YARN/Spark, Iceberg/Gravitino/Trino, Gold WAP, OpenMetadata/Prometheus/Grafana, Kafka 4.2.2 KRaft를 통합하고 Step65 경량 Kafka 발행/검증 및 데이터 정합성(521/512/9)을 확인했다. Step67 수집기 60초 Timer/Prometheus, Step68 Lag/Alert Rule 사용자 적용 확인, Step69 정상 종료·재시작 후 Offset/Checkpoint/Trino 안정성 비교까지 진행했다. **새 App `RUNNING` 상태, Step66 자동 복구와 Step68 실제 알림 전달, 미완료 Batch 재처리 및 재차 Replay 멱등성은 다음 단계 확인 대상**이다.
