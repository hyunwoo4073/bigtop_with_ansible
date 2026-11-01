# Bigtop Data Platform — Ansible 기반 실습·운영 플랫폼

> **프로젝트 한눈에 보기 · 2026-11-01 기준**  
> 상세 아키텍처, 컴포넌트 선정 이유, 모든 Playbook의 역할, 배포 순서 및 운영 방법: **[docs/PROJECT_GUIDE.md](docs/PROJECT_GUIDE.md)**

## 이 프로젝트는 무엇인가?

**5대 Ubuntu VM에서 기업 내부망용 On-Premise 데이터 플랫폼을 재현하는 Ansible 기반 실습 프로젝트**다. Hadoop(HDFS/YARN/Spark), 중앙 운영 및 관측(Prometheus/Grafana), OpenMetadata를 구축하고, Olist CSV 9개 적재부터 데이터 품질·승인·계보를 구현했다. 2026-11-01에는 **Gravitino Iceberg REST Catalog + PostgreSQL + HDFS + Spark + Trino** 간 공유 테이블을 검증하고, 승인된 Olist v3의 **Iceberg Silver/Gold 7개 테이블, 실제 Gold WAP(29개 품질 검사), 수동 릴리스 One-shot 자동화**까지 완료했다. Ambari 없이 역할·설정·실행 증빙을 코드로 관리한다.

목적은 다음 세 가지로 나뉜다.

1. **데이터 파이프라인:** Olist CSV 9개를 HDFS Raw에 보존하고 Spark on YARN으로 정제(5)·Quarantine(1)·Mart(1) Parquet를 생성, 29개 품질 검사와 v3 승인을 수행한다.
2. **Lakehouse 및 SQL:** Iceberg REST Catalog에서 승인된 v3 데이터를 Silver/Gold로 물질화하고, Spark와 Trino가 **동일한 Iceberg 테이블**을 사용하도록 한다. Gold 1개 테이블은 품질 검증 후 WAP로 공개한다.
3. **운영 플랫폼:** Prometheus/Grafana/Alertmanager, Web Console, Ansible/systemd로 상태·로그·장애·백업과 릴리스 작업을 관리한다.
4. **데이터 거버넌스:** OpenMetadata에서 데이터 카탈로그와 Olist 파일 입출력 계보를 조회한다. 새 Iceberg REST 테이블의 OpenMetadata 연동은 61번 예정으로 구분한다.

## 시스템 구성

| VM | 역할 | 주요 구성 |
|---|---|---|
| `master` | 데이터 플랫폼 제어 | HDFS NameNode, YARN ResourceManager, Spark History Server |
| `worker1` | 분산 처리 | HDFS DataNode, YARN NodeManager |
| `worker2` | 분산 처리 및 백업 수신 후보 | DataNode, NodeManager, PostgreSQL 원격 백업 수신(38번 실전송 확인 필요) |
| `worker3` | 운영·자동화 및 Catalog | Ansible Controller, Web Console, PostgreSQL, **Gravitino Iceberg REST Server(Java 17)**, Prometheus, Grafana, Alertmanager |
| `metadata` | 메타데이터·SQL 분석 | Docker Compose, OpenMetadata, MySQL, Elasticsearch, **Trino 484(Java 25, 단일 노드)** |

- 실습 VM 자원: 기존 노드 각각 **2 vCPU/4GB**, `metadata` **4 vCPU/8GB**; 물리 PC **6코어/12스레드·32GB RAM**.
- Ansible Inventory: `[hadoop_cluster]` = master + worker1 + worker2, `[ops]` = worker3, `[governance_nodes]` = metadata. **metadata는 Hadoop 설치 대상이 아니다.**
- **NameNode·ResourceManager HA는 미구성.** 백업과 복구 절차가 HA를 대체하지는 않는다.

```text
                                 ┌───────────────────────────────────┐
                                 │ worker3: Ansible + Web Console     │
                                 │ PostgreSQL + Gravitino REST :9001  │
                                 │ Prometheus / Grafana / Alertmanager│
                                 └───────┬───────────────┬───────────┘
                           SSH/Ansible   │               │ OpenMetadata API
                 ┌───────────────────────┘               ▼
                 ▼                           ┌─────────────────────────┐
 master: NameNode / RM / Spark History / ETL submit       │ metadata: OpenMetadata  │
       ├── worker1: DN / NM                  │ MySQL + Elasticsearch   │
       └── worker2: DN / NM                  │ Trino 484 :8080         │
                   HDFS + YARN / Spark       └──────────────┬──────────┘
                           │                           Iceberg REST API
                           └──────── Iceberg HDFS ◄─────────┘
                  (REST metadata registry: worker3 PostgreSQL)
```

## 컴포넌트를 넣은 이유

| 컴포넌트 | 역할과 도입 이유 |
|---|---|
| **Apache Bigtop + Ansible** | Hadoop 계열 패키지 설치와 반복 가능한 설정·배포 관리 |
| **HDFS** | 여러 노드에 데이터를 분산 저장하는 파일 시스템 |
| **YARN** | Spark 등 작업의 CPU·메모리 자원 배분 및 실행 관리 |
| **Spark / History Server** | YARN Cluster 모드 배치 ETL·Parquet 출력과 완료된 애플리케이션 실행 이력 분석 |
| **Node Exporter + JMX Exporter** | OS·디스크 지표와 JVM/Hadoop 내부 지표 수집 |
| **Prometheus + Grafana + Alertmanager** | 메트릭 저장·평가, 시각화, 장애 알림 |
| **FastAPI + React + Nginx** | 서비스 상태·로그·작업 이력·백업 상태를 통합하는 Web Console. Nginx는 접근 인증/프록시 |
| **PostgreSQL** | Web Console Action History와 클러스터 운영 메타데이터의 영속 저장 |
| **systemd Timer + pg_dump + rsync/SSH** | 정기 백업, 무결성 검사, 원격 보관 자동화 |
| **Docker Compose + MySQL + Elasticsearch** | OpenMetadata 본체·메타데이터 DB·검색 기능을 별도 VM에서 구동 |
| **OpenMetadata + PostgreSQL Ingestion CLI** | 테이블/컬럼 카탈로그와 데이터 거버넌스 메타데이터 관리. Airflow는 현재 수집에 사용하지 않음 |
| **Olist CSV + Spark ETL** | 공개 주문 데이터를 HDFS Raw(9 CSV)에 보존하고 4개 핵심 테이블로 정제·오류 격리·상품 단위 Fact·일별 Mart 생성 |
| **Olist Quality / Scheduler / Metrics** | 독립 품질 검사, systemd 서비스·선택적 타이머, Node Exporter/Prometheus/Grafana 운영 관측 |
| **Olist Catalog / Runtime Lineage / Approval** | OpenMetadata Container·계보, 실제 Spark 파일 I/O 근거, 품질 합격 스냅샷 승인 기록(접근제어 자체는 미구현) |
| **Apache Iceberg 1.10.1** | Spark 3.5.6/Scala 2.12/Java 11과 HDFS 기반 Snapshot·Time Travel·DML·Branch WAP, Olist Silver/Gold 테이블 포맷 |
| **Gravitino Iceberg REST Server 1.3.1** | worker3 `:9001`, 전용 Java 17. Spark/Trino 공통 테이블 등록·조회·커밋 API; PostgreSQL `iceberg_rest56`을 내부 Catalog 저장소로 사용 |
| **Trino 484** | metadata `:8080`, 전용 Java 25. Iceberg REST Connector와 HDFS 클라이언트로 실제 Parquet SQL 조회; 현재 Coordinator+Worker 단일 노드 |
| **Olist Iceberg 물질화·Gold WAP·릴리스 자동화(58~60)** | 승인된 v3 → Iceberg 테이블 7개(정확한 다중집합 비교) → 29 DQ 검사 → Gold 단일 테이블 `audit_v3` → `main` 공개 → Trino 검증, 수동 one-shot 운영 |

## 주요 사용자 화면

| 화면 | 접근 주소/설명 |
|---|---|
| **Bigtop Web Console** | `http://worker3:1337` — Nginx Basic Auth; 내부 FastAPI `127.0.0.1:18090` |
| NameNode | `http://master:9870` |
| YARN RM | `http://master:18088` — 콘솔 바로가기 설정값 |
| Spark History | `http://master:18080` |
| Prometheus | `http://worker3:9090` |
| Grafana | **worker3 내부** `http://127.0.0.1:3000` / **VirtualBox 포트포워딩 PC 브라우저** `http://127.0.0.1:23000` |
| Alertmanager | `http://worker3:19093` — 콘솔 바로가기 설정값 |
| **OpenMetadata** | `http://<metadata-host>:8585` — metadata VM, 실습 환경 접속 주소 |
| **Iceberg REST Catalog API** | `http://worker3:9001/iceberg/v1/config` — Gravitino API(일반적인 웹 대시보드가 아님) |
| **Trino API** | `http://metadata:8080/v1/info` — Trino 484 정보; SQL 조회는 HTTP Client/CLI/검증 스크립트 사용 |

> 위 이름·주소는 실습 환경 기준이다. 특히 Grafana의 경우 worker3 내부 포트 3000과 호스트 브라우저 포트포워딩 23000을 구분한다. **다른 바로가기 URL이 실제 데몬의 listen 포트 또는 VirtualBox 포트포워딩과 동일하다는 뜻은 아니다.** 브라우저가 `master`/`worker3`를 해석하지 못하면 Ansible 변수로 브라우저에서 접근 가능한 주소를 지정한다. 실제 IP와 포트포워딩 설정은 로컬 Inventory에서 관리하고 공개 README에는 기재하지 않는다.

## 지금까지 구축한 기능

| 단계 | 범위 | 결과 |
|---|---|---|
| **01–12** | 서버 공통 설정, Java, Bigtop, HDFS/YARN/Spark 설치·기동 | 분산 저장·처리 환경 |
| **13–19** | Exporter, Prometheus/Grafana/Alertmanager, NameNode 백업·복제 | 관측 및 HDFS 메타데이터 보호 |
| **20–27** | Health/Recovery/Smoke/Diagnostics, YARN 로그, Spark History, NM 디스크 | 운영 자동화 |
| **30–33** | Web Console, PostgreSQL Action History, Ansible 운영 메타데이터, Inventory 화면 | 중앙 운영·영속 저장 |
| **34–37** | PostgreSQL 백업, 복원 검증, 백업 메트릭·알림·Admin 대시보드 | 운영 DB 백업 감시 |
| **38–39** | DB 원격 백업 복제, 서비스 Web UI 바로가기 | 38번 실제 전송 확인 필요 / 39번 적용 확인 |
| **40–42** | metadata VM Docker, OpenMetadata Core, PostgreSQL 카탈로그 수집 | 설치·수집 성공 확인 |
| **43–44** | 분류/용어집/설명 정책, SQL View 기반 Lineage PoC | 43번 실제 적용 로그 미제공 / **44번 미배포·미확인** |
| **45** | Olist CSV 9개 원본 HDFS Raw 적재, Manifest/SHA-256, 재실행 불변성 | **실제 적재 성공 확인**, 단위 테스트 14개·Lint 통과 |
| **46** | Spark on YARN **Cluster Mode** 주문 배치 ETL, Curated/Quarantine/Mart Parquet | v1 성공, 재실행 SKIP 오류 수정, v3 실행 계보 생성 확인 |
| **47** | Olist 데이터 품질 검사(29개) 및 결과 지표 | v1 검증, v3 품질 검사 및 52번 승인 근거 |
| **48** | systemd ETL → Quality 순차 실행·실행 메트릭 | 서비스 재실행 오류 수정 후 수동 실행 성공; 타이머 기본 비활성 |
| **49** | Grafana Olist Pipeline & Data Quality(16패널) | Dashboard API 등록 및 날짜 표시 수정 확인, 기본 버전 v1 |
| **50** | HDFS Container 카탈로그와 변환 관계 계보 | OpenMetadata 최초 1 service + 16 containers + 13 edges, 개별 Container UI 확인, v3 등록 진행 |
| **51** | Spark 실행 시 파일 입력·출력 기록 및 계보 Import | v3 `_RUNTIME_LINEAGE.json` 생성 및 OpenMetadata 연계 완료 |
| **52** | 최신 PASS 품질 리포트 기반 Warehouse 승인 | v3 승인 성공(사용자 확인), `_APPROVAL.json` 기록 |
| **53** | Iceberg Hadoop Catalog / Snapshot / Time Travel / Mart 복사 | Java 11 호환 Iceberg 1.10.1로 PASS, 55,877행 일치·스냅샷 2개 확인 |
| **54** | Iceberg INSERT/UPDATE/DELETE/MERGE, Schema Evolution, Branch WAP | stage와 publish까지 실습 완료(사용자 확인), 별도 실습 테이블만 변경 |
| **55** | 5대 VM Iceberg/Trino 읽기 전용 사전 진단 | 각 노드 CPU/메모리/Java·포트·HDFS 실습 증빙 확인, metadata 경로 권한 예외 처리 후 성공 |
| **56** | Gravitino 1.3.1 Iceberg REST Server + PostgreSQL + HDFS | worker3 `:9001`, 전용 Java 17, 새 Warehouse 및 Spark REST 테이블 생성/조회 검증 성공 |
| **57** | Trino 484 독립 설치·Spark/Trino 교차 조회 | metadata `:8080`, 전용 Java 25, `[[56, 'spark']]` / 1행 SQL 검증 **PASS** |
| **58** | 승인된 Olist v3 → Iceberg Silver(5)·Quarantine(1)·Gold(1) | REST Catalog `olist58_v3` 7개 테이블·스키마/행수/양방향 `exceptAll`·Trino 검증 완료(사용자 확인) |
| **59** | 실제 Olist Gold 테이블에 WAP + 29개 DQ | `olist59_v3.gold_daily_category_sales`의 `audit_v3` 검증 후 Gold `main` 공개 완료 |
| **60** | Iceberg Gold 릴리스 수동 One-shot 워크플로우 | Step58/59 재검증·멱등 Publish·Trino Gold **55,877행** 확인, systemd 정상 종료(`STEP60_PIPELINE_PASS: mode=run`) |

> 번호 **28·29는 현재 확인한 Playbook 목록에 없다.** 44번은 패키지 제공 단계까지 진행했으며 DB View 및 OpenMetadata 계보가 실제 생성됐다는 근거는 없다.

<!-- BEGIN OLIST PIPELINE 45-60 -->
## Olist 데이터 파이프라인과 Iceberg Lakehouse (45~60, 2026-11-01)

**상태:** 45~52번 기존 Olist HDFS/Parquet 파이프라인, 53~54번 독립 Iceberg Lab, 55~57번 **공유 REST Catalog + Trino 교차 조회**, 58~60번 **승인된 v3의 Iceberg Silver/Gold·Gold WAP·수동 릴리스 오케스트레이션**을 확인했다. **주의:** 48번 Timer는 기본 비활성, 49번 Grafana 기본 질의 버전은 v1, 60번은 **일회성/수동 시스템 서비스**이며 자동 신규 데이터 유입·주기적 Bronze→Silver→Gold 갱신은 아직 미구축이다.

### 데이터 흐름

```text
Kaggle — Brazilian E-Commerce Public Dataset by Olist (9 CSV)
    │  원본 ZIP/CSV는 worker3 로컬에 보관 (Git 제외)
    ▼
[45] Ansible → master(hdfs CLI) → HDFS Raw (CSV / immutable snapshot)
    /data/olist/raw/v1/     9 CSV + _MANIFEST.json
    │
    ▼
[46] Spark on YARN — cluster deploy mode / batch queue
    master에서 spark-submit; Driver(AM)와 Executor는 YARN 컨테이너
    ├── 입력 4개: orders, order_items, customers, products
    ├── 명시적 스키마·행 수 검사·필수값 검증·중복키/참조 검사
    ├── Curated: 정제 테이블 및 주문상품 단위 Fact
    ├── Quarantine: 오류 행과 사유 분리
    └── Mart: 배송 완료 주문의 일자/카테고리/고객 주별 매출 집계
    ▼
HDFS Warehouse (Parquet / immutable snapshot)
    /data/olist/warehouse/v1/    _ETL_MANIFEST.json + 7개 _SUCCESS
    /data/olist/warehouse/v3/    위 항목 + _RUNTIME_LINEAGE.json
    │
    ├── [47] Data Quality → /data/olist/quality/<version>/runs/<run_id>/
    │                       └── _QUALITY_REPORT.json
    ├── [50~51] OpenMetadata Container & 실행 근거 Lineage
    └── [52] PASS 검사 + Manifest digest 검증 → /data/olist/published/v3/_APPROVAL.json

[48] systemd 오케스트레이션(46→47) · [49] Prometheus/Grafana 관측

[53~54] 별도 Iceberg HadoopCatalog Lab /data/olist/iceberg-lab/
     Spark Java 11 + Iceberg 1.10.1: Snapshot/DML/audit Branch (기존 Warehouse 미변경)

[55] 5 VM Preflight → [56] worker3 Gravitino REST(:9001) + PostgreSQL Catalog DB
                                      │
                         Iceberg HDFS /data/olist/iceberg-rest/
                                      │
[57] metadata Trino(:8080) ◄──── 동일 Iceberg 테이블 ──── Spark on YARN
                                      │
[58] 승인 Parquet v3 → REST Iceberg 7개 (Silver 5 / Quarantine 1 / Gold 1)
                                      │  exact exceptAll parity + Trino
[59] 실제 Gold 1개 대상: audit_v3 → 29 DQ PASS → 명시적 fast_forward → main
                                      │
[60] worker3 One-shot Runner: 기존 공개 v3 재검증 + Trino 55,877행 확인
     (신규 Raw 수집/정기 스케줄, 여러 테이블 원자적 공개는 미구축)
```

**데이터 원본:** [Kaggle Olist Brazilian E-Commerce Public Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce). Olist 원본은 9개의 CSV이며, `olist/source/`에 압축 해제했다. **9개 전부 Raw에 저장**하고, 46번 첫 ETL은 4개만 읽는다. 나머지 결제·리뷰·판매자·지역·카테고리 번역은 Raw에 보존하여 후속 변환에 사용한다.

| Raw 데이터셋 | 파일명 | 로컬 CSV 레코드 수 (헤더 제외) |
|---|---|---:|
| customers | `olist_customers_dataset.csv` | 99,441 |
| geolocation | `olist_geolocation_dataset.csv` | 1,000,163 |
| order_items | `olist_order_items_dataset.csv` | 112,650 |
| payments | `olist_order_payments_dataset.csv` | 103,886 |
| reviews | `olist_order_reviews_dataset.csv` | 99,224 |
| orders | `olist_orders_dataset.csv` | 99,441 |
| products | `olist_products_dataset.csv` | 32,951 |
| sellers | `olist_sellers_dataset.csv` | 3,095 |
| category_translation | `product_category_name_translation.csv` | 71 |
| **합계** | **9개 CSV** | **1,550,922** |

> 9개 CSV의 원본 총 바이트 수는 **126,186,995 bytes**. 위 카운트는 45번 로컬 `scan` 결과이며, 서로 다른 테이블의 행 수를 합산한 값이지 중복을 제거한 주문 수가 아니다. SHA-256은 HDFS Raw의 `_MANIFEST.json`으로 검증한다.

### 45. HDFS Raw Zone 적재

- Playbook: `playbooks/45-olist-hdfs-raw.yml`
- Role: `roles/data_pipeline_olist_raw/`
- 문서: [`docs/45-olist-hdfs-raw.md`](docs/45-olist-hdfs-raw.md)
- 소스 기본값: worker3의 `olist/source/` (Git 제외)
- 결과: `/data/olist/raw/v1/` 아래 9개 CSV와 `_MANIFEST.json`
- 무결성: 레코드 수, 파일 크기, SHA-256. **같은 버전·같은 내용 재실행 시 건너뛰고**, 내용 변경 시 덮어쓰지 않고 실패하도록 구현.
- 검증: **Python 단위 테스트 14개 통과**, `make lint` 통과, 실 HDFS 적재 성공(사용자 확인).

```bash
cd ~/bigtop-cluster-ansible
python3 -m unittest discover -s tests -p 'test_olist_raw.py' -v
python3 roles/data_pipeline_olist_raw/files/olist_raw.py scan --source "$PWD/olist/source"
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/45-olist-hdfs-raw.yml

# HDFS 확인 (worker3 → master SSH)
.venv/bin/ansible master -b --become-user hdfs -m shell \
  -a 'hdfs dfs -ls -R /data/olist/raw/v1'
.venv/bin/ansible master -b --become-user hdfs -m shell \
  -a 'hdfs dfs -cat /data/olist/raw/v1/_MANIFEST.json'
```

**주의:** `v1`은 Raw 입력 스냅샷 버전이며 날짜 파티션이 아니다. 이미 게시한 데이터를 변경해야 한다면 기존 `v1`을 지우지 말고 새 버전을 사용한다.

### 46. Spark Batch ETL — YARN Cluster Mode

- Playbook: `playbooks/46-olist-spark-etl.yml`
- Role: `roles/data_pipeline_olist_etl/`
- 애플리케이션: `roles/data_pipeline_olist_etl/files/olist_etl.py` → master의 `/usr/local/lib/bigtop-olist/etl/olist_etl.py`
- 실행: master에서 `hadoop` 계정으로 `spark-submit --master yarn --deploy-mode cluster --queue batch`.
- 기본 리소스: Driver 512 MiB, Executor **1개** × 512 MiB/1 core, shuffle partitions 4, 출력 partitions 2. YARN 컨테이너 오버헤드를 포함한 실제 필요 메모리는 별도 확인.
- 입력: `/data/olist/raw/v1/`의 `orders`, `order_items`, `customers`, `products`. Raw Manifest도 함께 확인.
- 결과: `/data/olist/warehouse/v1/`에 **Curated 5개 + Quarantine 1개 + Mart 1개** Parquet 경로.
- 정합성: 명시적 스키마, CSV 행 수 비교, 잘못된 키·타임스탬프·금액 격리, 중복 비즈니스 키 검증, 미참조 데이터 격리, 집계 시 주문상품 단위 기준 적용.
- 스냅샷: 실행별 `_staging/`에 작성한 뒤 최종 경로로 게시. 이미 완료된 동일 버전은 SKIP, 불일치/불완전한 버전은 ERROR 처리. **동일 출력 버전 동시 실행은 지원하지 않음.**
- 검증: 수정본 **Cluster 모드 실행 성공(사용자 확인)**. 정적·단위 테스트는 실행 로그와 별개이며, 정확한 처리 건수는 각 버전 Manifest로 확인한다. 불변 버전 재실행 시 Driver가 SparkContext 초기화 전에 종료되던 YARN `exitCode 13` 문제를 수정해, 기존 정상 스냅샷은 Spark 제출 전에 검증/SKIP하도록 보완했다.

```text
/data/olist/warehouse/v1/
├── _ETL_MANIFEST.json
├── curated/
│   ├── orders/
│   ├── order_items/
│   ├── customers/
│   ├── products/
│   └── order_item_fact/
├── quarantine/
│   └── rows/
└── mart/
    └── daily_category_sales/
```

```bash
cd ~/bigtop-cluster-ansible
python3 -m unittest discover -s tests -p 'test_olist_etl.py' -v
make lint
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/46-olist-spark-etl.yml

# 게시 결과 확인 (Manifest 및 Parquet _SUCCESS)
.venv/bin/ansible master -b --become-user hdfs -m shell \
  -a 'hdfs dfs -cat /data/olist/warehouse/v1/_ETL_MANIFEST.json'
.venv/bin/ansible master -b --become-user hdfs -m shell \
  -a 'hdfs dfs -ls -R /data/olist/warehouse/v1'

# Driver 로그는 spark-submit 표준출력 대신 YARN에서 조회
# master에서 실행: yarn application -list -appStates ALL
# ID 확인 후: yarn logs -applicationId application_XXXXXXXXXXXXX_XXXX
```

**집계 해석:** `daily_category_sales`는 배송 완료 주문 중 *주문상품 가격*을 합산한 값이며, 실제 결제 정산/세금/환불이 반영된 매출을 의미하지 않는다. 결제 데이터와 단순 1:N JOIN하면 중복 집계 위험이 있으므로 후속 별도 모델로 처리한다.

**운영 원칙:** cluster 모드에서는 Driver가 YARN 컨테이너에서 실행된다. Playbook은 제출 종료 코드와 `_ETL_MANIFEST.json` 및 **7개** Parquet `_SUCCESS` 마커를 확인한다. Driver 로그는 `yarn logs` 또는 Spark History Server에서 확인한다. `-e ansible_connection=local`을 사용하지 않는다.

### 47~49. 데이터 품질·스케줄러·관측

| 단계 | 입력 → 결과 | 핵심 조건 / 확인 위치 |
|---|---|---|
| **47** | Warehouse `<version>` → `/data/olist/quality/<version>/runs/<run_id>/_QUALITY_REPORT.json` | 총 29개 검사, PASS/FAIL, orphan=0, Quarantine 비율 허용 상한 5%; worker3 `olist_quality.prom` |
| **48** | `bigtop-olist-pipeline.service` → 46번 ETL 성공 후 47번 Quality | `bigtop-olist-pipeline.timer` 기본 **disabled**; 기본 입력 Raw v1·출력 Warehouse v1; `olist_pipeline.prom` 기록 |
| **49** | Prometheus → Grafana 16개 패널 | Grafana UID `bigtop-olist-pipeline`; timestamp(초→밀리초) 수정 완료; 기본 Warehouse v1 |

```bash
# worker3에서 실행. 원격 master를 대상으로 하므로 47번에는 전역 local 연결 금지
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/47-olist-data-quality.yml \
  -e data_pipeline_olist_quality_warehouse_version=v3
sudo systemctl status bigtop-olist-pipeline.service bigtop-olist-pipeline.timer --no-pager
sudo cat /var/lib/prometheus/node-exporter/olist_pipeline.prom
curl -fsS http://127.0.0.1:9100/metrics | grep -E '^olist_(pipeline|quality)_'
```

**Grafana:** Windows/호스트 브라우저 VirtualBox 포트포워딩 `http://127.0.0.1:23000/d/bigtop-olist-pipeline/olist-pipeline-and-data-quality` (worker3 내부 Grafana `:3000`). **No data는 0이나 PASS가 아니다.** 48번 실행 메트릭이 없으면 일부 패널이 비어 보인다. 49번은 기본값이 v1이므로 v3 승인을 모니터링한다고 해석하면 안 된다.

### 50~52. 카탈로그·실행 기반 계보·품질 승인

| 단계 | 실제 결과 | 구현 범위 / 한계 |
|---|---|---|
| **50** | OpenMetadata Storage Service `bigtop_hdfs_olist`, Raw CSV 9개·Warehouse Parquet 7개 Container; 최초 v1에 계보 13개 | ETL 코드에 근거한 **수동 관계**. Container 상세 화면의 Lineage 탭에서 조회. |
| **51** | Warehouse v3의 `_RUNTIME_LINEAGE.json` → OpenMetadata API | Spark `DataFrame.inputFiles()`와 실제 완료된 출력 경로를 기록하는 **Olist 전용 실행 계보**. 범용 OpenLineage Listener나 컬럼 계보 아님. |
| **52** | `/data/olist/published/v3/_APPROVAL.json` | 최신 PASS 품질 리포트, 모든 검사 PASS, Manifest SHA-256 일치, `_SUCCESS` 및 리포트 유효기간(기본 168시간) 확인 후 승인 증빙. **HDFS ACL/소비자 접근 통제는 아님.** |

```bash
# 50: Raw v1 및 Warehouse v3 Container 등록 — 여기서는 명시적으로 APPLY 실행
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/50-olist-openmetadata.yml \
  -e data_pipeline_olist_catalog_version=v1 \
  -e data_pipeline_olist_catalog_warehouse_version=v3 -e data_pipeline_olist_catalog_apply=true

# 51: v3 실행 기록 Import (기본 plan, apply는 별도 opt-in)
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/51-olist-spark-runtime-lineage.yml \
  -e data_pipeline_olist_runtime_lineage_warehouse_version=v3 \
  -e data_pipeline_olist_runtime_lineage_apply=true

# 52: v3 승인 증빙 확인
ssh master 'sudo -u hadoop hdfs dfs -cat /data/olist/published/v3/_APPROVAL.json' \
  | python3 -m json.tool
```

**버전 이력:** Raw는 v1을 재사용했다. Warehouse **v1**은 최초 ETL, **v2**는 발행됐으나 런타임 계보 파일이 없었던 스냅샷, **v3**는 실행 계보·품질 검증·승인에 사용했다. 버전 번호가 새로운 원본 유입을 뜻하지 않는다. 48/49는 기본 v1이며 50/51/52는 적용 버전을 명시해야 한다.

### 53~54. Apache Iceberg 별도 실습 (Java 11 호환)

- **실제 실행 환경:** Bigtop Spark **3.5.6**, Scala **2.12.18**, Java **11.0.32.1**; Iceberg **1.10.1** (`iceberg-spark-runtime-3.5_2.12-1.10.1.jar`). Iceberg 1.12.0은 Java 17 바이트코드(61)로 컴파일돼 기존 Java 11(55)에서 `UnsupportedClassVersionError`가 발생하여 1.10.1로 교체했다.
- **배포:** worker3 `vendor/iceberg/` → master `/opt/bigtop-iceberg/`; `spark-submit --jars`가 HDFS Staging/YARN Distributed Cache를 통해 Driver·Executor에 전달한다. 모든 Worker의 전역 Spark JAR 디렉터리에 수동 복사하지 않는다.
- **Hadoop Catalog:** `lab`, Warehouse `hdfs://master:9000/data/olist/iceberg-lab`. 53번은 Snapshot 2개, 과거 1건/현재 2건, v3 Mart 복사 **55,877행 ↔ 55,877행**, `status=PASS`를 확인했다. 이 복사본은 **새로 승인된 릴리스가 아니다**.
- **54번:** 별도 `lab.olist.iceberg_mutations_step54`에서 INSERT/UPDATE/DELETE/MERGE/ADD COLUMN/Time Travel. `lab.olist.iceberg_wap_step54`에서는 `stage`가 audit 브랜치의 후보만 생성(main 1행/audit 2행), `publish`가 검증 후 `fast_forward`로 main을 갱신. stage·publish 모두 사용자 완료 확인.

```bash
# worker3에서 /opt JAR, 실행 애플리케이션을 관리하는 53번 (실행 opt-in)
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/53-iceberg-hdfs-lab.yml \
  -e iceberg_spark_lab_run_smoke=true
ssh master "sudo -u hadoop hdfs dfs -text '/data/olist/iceberg-validation/step53/part-*'"

# 54번은 deploy_only가 기본, stage와 publish는 각각 명시적으로 실행
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/54-iceberg-transactions-wap.yml \
  -e iceberg_transactions_lab_mode=stage
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/54-iceberg-transactions-wap.yml \
  -e iceberg_transactions_lab_mode=publish
ssh master "sudo -u hadoop hdfs dfs -text '/data/olist/iceberg-validation/step54/publish/part-*'"
```

**실습과 실제 Gold WAP의 구분:** 54번은 소규모 샘플 WAP이고, **59번은 29개 실제 Olist 품질 검사와 Silver/Gold 스냅샷 검증을 연결한 Gold 단일 테이블 WAP**다. 53~54번 HadoopCatalog와 56~60번 공유 REST Catalog는 별도로 유지된다. **멀티테이블 원자적 공개·사용자별 접근제어·새 버전의 정기 데이터 수집은 아직 구현하지 않았다.**

### 55~57. 공유 Iceberg REST Catalog와 Trino SQL 엔진

| 단계 | 배치 / 실제 구성 | 검증된 결과 |
|---|---|---|
| **55 사전 점검** | 5대 VM 상태를 읽기 전용으로 조사; `artifacts/55-iceberg-trino-preflight.json` | Spark Java 11 유지, `metadata` 4 vCPU/8GB; HDFS Step53 증빙 PASS. 경로 권한 제한을 `permission_denied`로 처리하는 패치 반영 |
| **56 REST Catalog** | worker3 **Gravitino Iceberg REST Server 1.3.1**, `:9001`, 독립 JDK **17**, PostgreSQL `iceberg_rest56`(`127.0.0.1:5432`), HDFS `/data/olist/iceberg-rest` | `/iceberg/v1/config`와 Namespace API 성공, Spark YARN에서 `shared.olist56.catalog_smoke` 생성/조회 성공 |
| **57 Trino** | metadata **Trino 484**, `:8080`, 독립 JDK **25**, 단일 노드 Coordinator+Worker; Iceberg REST Connector + `fs.hadoop.enabled=true` | SQL `iceberg_rest.olist56.catalog_smoke`에서 **`[[56, 'spark']]`, COUNT=1, `STEP57_TRINO_SPARK_CROSS_QUERY_PASS`** |

```text
Spark/YARN(Java 11) ──┐
                      ├─ Iceberg REST API ── Gravitino(worker3:9001) ── PostgreSQL(Catalog DB)
Trino(Java 25) ───────┘          │
     │                          └── Iceberg metadata pointer
     └──────────────────── HDFS Iceberg metadata + Manifest + Parquet files
```

**중요:** REST Catalog가 Parquet 데이터를 전달하거나 SQL을 실행하지 않는다. Catalog는 등록·조회·커밋을 처리하고, Spark/Trino는 Iceberg Metadata/Manifest를 해석해 **HDFS의 데이터 파일을 직접 읽고 쓴다**. PostgreSQL `iceberg_rest56`은 실제 판매 데이터가 아닌 Catalog 등록 정보를 보관한다. Trino는 **YARN 작업이 아니며**, 현재 metadata에서 Coordinator와 Worker를 함께 실행한다.

```bash
# worker3: 서비스/API 확인
systemctl status bigtop-iceberg-rest --no-pager
curl -fsS http://192.158.2.103:9001/iceberg/v1/namespaces | python3 -m json.tool
curl -fsS http://192.158.2.104:8080/v1/info | python3 -m json.tool
python3 scripts/57-verify-trino.py --url http://192.158.2.104:8080

# metadata: Trino 노드 확인 및 메모리
ssh metadata 'systemctl status bigtop-trino57 --no-pager'
ssh metadata 'systemctl show bigtop-trino57 -p MemoryCurrent -p MemoryMax'
```

- **Java 분리:** Spark/HDFS/YARN은 Java 11, Gravitino는 전용 Java 17, Trino는 전용 Java 25. 시스템의 전역 `java`를 변경하지 않는다.
- **오프라인 재현:** 다운로드 스크립트 `scripts/56-fetch-iceberg-rest-assets.py`, `scripts/57-fetch-trino-assets.py`와 검증 스크립트 사용. `vendor/`의 JAR/JDK/아카이브는 Git 제외. 실제 구축 중 PostgreSQL JDBC의 `.sha256` 404 문제를 다운로드 코드에서 해결했다.
- **과거 오류:** 58번 Spark에서 `spark.sql.catalog.shared.warehouse=hdfs://...`를 전달하면 REST 서버가 이를 Catalog 식별자로 오해해 `Couldn't find Iceberg configuration` 오류가 발생했다. **58번 수정본에서는 해당 Spark 옵션을 제거**했으며 Gravitino가 HDFS Warehouse를 관리한다.

### 58. 승인된 Olist v3 → Iceberg Silver/Gold (7개)

- Playbook: `playbooks/58-olist-iceberg-materialization.yml` / Role: `roles/olist_iceberg_materialization/`.
- **소스:** 52번 승인된 `/data/olist/warehouse/v3`의 기존 Parquet(변경하지 않음).
- **대상:** REST Catalog `shared.olist58_v3` — Silver 5개, Quarantine 1개, Gold `daily_category_sales` 1개.
- 검증: 스키마·행 수·전체 중복을 포함하는 **양방향 `exceptAll` 다중집합 일치**, Trino 7개 테이블과 Gold 집계 조회, HDFS `step58/v3` 증빙.
- 동일한 원본 v3에 대한 재실행은 기존 테이블을 덮어쓰지 않고 출처/내용을 재검증한다. 부분 성공 시 이미 생성된 테이블은 삭제하지 않는다.

```bash
.venv/bin/ansible-playbook -i inventory/hosts.ini \
  playbooks/58-olist-iceberg-materialization.yml \
  -e olist_iceberg_materialization_apply=true
python3 scripts/58-verify-trino.py --url http://192.158.2.104:8080 \
  --proof artifacts/58-iceberg-v3-proof.json
```

**범위:** Raw CSV에서 Iceberg 테이블로 직접 ETL하는 새 파이프라인이 아니라 **이미 승인된 Parquet v3의 Iceberg 물질화**다. Bronze는 기존 Raw CSV이며 Silver/Gold는 Iceberg를 사용하는 혼합 메달리온 구조다.

### 59. Olist Gold 단일 테이블 WAP (Write–Audit–Publish)

- Playbook: `playbooks/59-olist-iceberg-gold-wap.yml` / Role: `roles/olist_iceberg_wap/`.
- 58번의 Silver 5개/Quarantine 1개/Gold 1개는 **읽기 전용 입력**; 새로운 `shared.olist59_v3.gold_daily_category_sales`에 별도 `audit_v3` Branch를 만든다.
- `stage`: 실제 Olist **29개 DQ** + 스키마 및 Gold 전체 레코드 동등성 + 소스/Branch Snapshot 검사, `/data/olist/iceberg-validation/step59/v3/_STAGE_REPORT.json`.
- `publish`: PASS 증빙과 Snapshot 불변성을 다시 확인한 뒤 **Gold 1개 테이블만** `fast_forward(main ← audit_v3)`, `_PUBLISH_REPORT.json` 기록.
- Trino `iceberg_rest.olist59_v3.gold_daily_category_sales` 공개 결과는 **55,877행**. Publish 전 기술적 main(1행)은 BI에 노출할 대상이 아니다.

```bash
# stage → 독립 Trino 점검 → 명시적 publish → 독립 Trino 점검
.venv/bin/ansible-playbook -i inventory/hosts.ini \
  playbooks/59-olist-iceberg-gold-wap.yml -e olist_iceberg_wap_mode=stage
python3 scripts/59-verify-trino.py --mode stage --url http://192.158.2.104:8080
.venv/bin/ansible-playbook -i inventory/hosts.ini \
  playbooks/59-olist-iceberg-gold-wap.yml -e olist_iceberg_wap_mode=publish
python3 scripts/59-verify-trino.py --mode publish --url http://192.158.2.104:8080
```

**제약:** Branch는 테이블별 Snapshot이고 **7개 Silver/Gold 테이블을 하나의 트랜잭션으로 Publish하지 않는다**. 접근 제어/승인 강제 소비자 게이트는 별도 과제다.

### 60. 릴리스 검증·Publish 수동 One-shot 오케스트레이션

- Runner: `scripts/60-run-iceberg-release.py` / Playbook `playbooks/60-olist-iceberg-release.yml` / Role `roles/olist_iceberg_release60/`.
- **실행 체인:** 58번 Spark/Trino 검증 → 59번 stage·29개 DQ/증빙 확인 → 명시적 Gold Publish → Trino 최종 검증.
- 이미 Publish된 불변 **v3**는 새로 덮어쓰지 않고 **동일 Snapshot과 증빙을 재검증**한다. 실행 중복을 `flock`으로 제한하고 실패 시 후속 단계를 차단한다.
- systemd `bigtop-olist-iceberg-release60.service`는 **수동 One-shot**. 매일 실행하는 Timer나 새 Raw 파일을 수집하는 자동화는 없다.
- **2026-11-01 실제 로그:** `STEP60_ALREADY_PUBLISHED_VERIFIED`, `STEP60_GOLD_RELEASE_PASS`, `STEP60_PIPELINE_PASS: mode=run`, Trino 55,877행, `Deactivated successfully`.

```bash
# worker3: 안전한 상태 확인(읽기 전용)
python3 scripts/60-run-iceberg-release.py --mode plan
python3 scripts/60-run-iceberg-release.py --mode verify
cat artifacts/60-iceberg-release-last-attempt.json

# 설치된 systemd One-shot 결과와 로그
sudo systemctl status bigtop-olist-iceberg-release60.service --no-pager -l
sudo journalctl -u bigtop-olist-iceberg-release60.service -n 120 --no-pager

# 새로운 릴리스 발행이 필요할 때만 명시적으로 검토한 뒤 실행
# python3 scripts/60-run-iceberg-release.py --mode run --allow-publish
```

**Proof 구분:** 58/59 증빙 원본은 HDFS `iceberg-validation/step58|step59/v3`이고, worker3 `artifacts/58-iceberg-v3-proof.json`, `59-olist-wap-stage.json`, `59-olist-wap-publish.json`, `60-iceberg-release-last-attempt.json`는 로컬 실행·검증 용도다. `--mode verify`는 로컬 증빙과 Trino를 읽어 검증하며, `--mode run`/`publish`는 Spark 경유로 HDFS Snapshot 조건까지 재확인한다.


### 향후 작업 / 검증 필요

1. **61번:** 신규 REST Iceberg Silver/Gold 및 Gold WAP 릴리스를 **OpenMetadata Container/Lineage + Prometheus/Grafana**에 연계한다. 기존 49번 v1 대시보드와 별도로 모델링한다.
2. **62번 이후:** 새 데이터 버전의 Raw → Iceberg ETL, 실행별 DQ, 리텐션/스냅샷 정리, 배치 재실행/장애 주입, 멀티테이블 일관된 소비 버전 전략을 설계한다.
3. **내부 플랫폼 접근정책:** 사용자별 Trino/Catalog/HDFS 권한·감사, 메타데이터 DB 백업, 오프라인 배포 자산 관리. 실습 네트워크의 HTTP 자체를 장애로 간주하지 않되 인증·승인 강제를 과장하지 않는다.
4. 48번 Timer/49번 대시보드의 v1 기본값, 51번 Timer, 38/43/44번 미확인 작업 및 나머지 Olist CSV 모델링 여부를 점검한다.

**단계별 상세 실행법:** [`docs/47-olist-apply.md`](docs/47-olist-apply.md) ~ [`docs/54-iceberg-apply.md`](docs/54-iceberg-apply.md) · [`docs/55-iceberg-trino-preflight.md`](docs/55-iceberg-trino-preflight.md) · [`docs/56-iceberg-rest-apply.md`](docs/56-iceberg-rest-apply.md) · [`docs/57-trino-iceberg-rest-apply.md`](docs/57-trino-iceberg-rest-apply.md) · [`docs/58-olist-iceberg-apply.md`](docs/58-olist-iceberg-apply.md) · [`docs/59-olist-iceberg-apply.md`](docs/59-olist-iceberg-apply.md) · [`docs/60-olist-iceberg-release-apply.md`](docs/60-olist-iceberg-release-apply.md).

<!-- END OLIST PIPELINE 45-60 -->

## 시작하기 / 일상 점검

worker3의 프로젝트 루트 `~/bigtop-cluster-ansible`에서:

```bash
# Inventory 대상 확인: 5대 VM, 세 그룹 구분
.venv/bin/ansible-inventory -i inventory/hosts.ini --graph

# 원격 접근과 정적 검사
.venv/bin/ansible -i inventory/hosts.ini all -m ansible.builtin.ping
make lint
make validate

# 구축된 데이터 플랫폼 상태/복구/기능 점검
make health
make smoke
# make recover  # 서비스 복구가 필요한 경우에만 실행

# Web Console API / PostgreSQL / OpenMetadata
curl -fsS http://127.0.0.1:18090/api/health
curl -fsS http://127.0.0.1:18090/api/storage/health
METADATA_HOST=metadata  # 실제 접근 가능한 hostname/IP로 변경
curl -fsS "http://${METADATA_HOST}:8585/" -o /dev/null
```

`make validate`는 로컬 개발환경/Collection 준비 상태에 의존할 수 있다. 복구·중지·재시작 명령은 상태 조회와 다르게 서비스에 영향을 준다.

## 소스 코드 안내

```text
inventory/           호스트 그룹·환경별 설정 예제 (실제 IP/비밀정보는 Git 제외)
playbooks/           배포·재배포·점검·복구의 진입점
roles/               서비스별 Ansible 설정, 템플릿, systemd 단위
roles/web_console/   FastAPI·React/Vite·Nginx 중앙 콘솔
roles/governance_*/  OpenMetadata 관련 Docker, 수집, 정책·계보
roles/data_pipeline_olist_raw/  45번 Raw 적재 및 무결성 검증
roles/data_pipeline_olist_etl/  46번 Spark on YARN Cluster 배치 ETL / 51번 실행 계보 후킹
roles/data_pipeline_olist_quality/  47번 품질 검증
roles/data_pipeline_olist_scheduler/  48번 systemd 서비스·타이머
roles/data_pipeline_olist_observability/  49번 Grafana Olist 대시보드
roles/data_pipeline_olist_catalog/  50번 OpenMetadata Container·코드 기반 계보
roles/data_pipeline_olist_runtime_lineage/  51번 실행 계보 API 브리지
roles/data_pipeline_olist_promotion/  52번 HDFS 승인 게이트
roles/iceberg_spark_lab/  53번 Iceberg Hadoop Catalog 스모크
roles/iceberg_transactions_lab/  54번 Iceberg SQL/WAP 실습
roles/iceberg_rest_catalog/      56번 Gravitino REST Server·PostgreSQL·HDFS
roles/trino_iceberg_rest/        57번 단일 노드 Trino 484 및 REST/HDFS 커넥터
roles/olist_iceberg_materialization/ 58번 v3 → Iceberg Silver/Gold 7개
roles/olist_iceberg_wap/         59번 Gold 1개 테이블 audit/publish
roles/olist_iceberg_release60/   60번 수동 One-shot 릴리스 서비스
collections/         필요한 Ansible Collection 선언
scripts/, tests/     검증·운영 스크립트·테스트
Makefile             자주 쓰는 명령을 표준화
docs/PROJECT_GUIDE.md 01~60번 구조·의존성·운영·검증 가이드
docs/45-olist-*.md ~ docs/60-olist-*.md  데이터 파이프라인·Iceberg·Trino 단계별 운영 문서
```

**문서 읽는 순서:** 이 README → [상세 프로젝트 가이드](docs/PROJECT_GUIDE.md) → 해당 단계의 `docs/*-apply.md` 및 설계 문서 → 필요 시 `docs/runbooks/`.

## 운영 안전 원칙

- SSH **개인키**, JWT, DB 비밀번호, `.env`, 실제 Inventory, DB 백업 파일을 Git에 넣지 않는다.
- Olist 원본 CSV·ZIP 및 HDFS/Parquet/Iceberg 산출물, Java/Trino/Gravitino 바이너리, Iceberg Runtime JAR는 Git에 올리지 않는다. `.gitignore`에 `/olist/`, `/vendor/`, `/artifacts/` 등이 반영됐는지 확인한다. HDFS 결과는 Git에 Export/Commit하지 않는다.
- 원격 다중 노드 Playbook(특히 **38, 40, 41**)에는 전역 `-e ansible_connection=local`을 사용하지 않는다.
- OpenMetadata의 Docker 볼륨 삭제(`docker compose down -v`) 금지. PostgreSQL 복원 검증은 운영 DB가 아닌 임시 DB에서 수행한다.
- **Web Console의 운영 Inventory(PostgreSQL)**와 **OpenMetadata의 데이터 카탈로그(MySQL)**는 목적이 다른 시스템이다.

**문서 기준:** 2026-11-01. 54번까지의 기존 문서와 55~60번 ZIP의 Playbook/Role/운영 문서, 사용자가 제공한 56~57·60번 실제 성공 로그 및 58~59번 완료 확인을 반영했다. **worker3의 현재 Git 작업 트리 자체를 직접 열람한 것은 아니므로** 적용 전 `git diff`로 개인 수정사항을 검토한다. 38·43·44번 기존 미검증 표기는 유지한다.
