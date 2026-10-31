# Bigtop Data Platform — Ansible 기반 실습·운영 플랫폼

> **프로젝트 한눈에 보기 · 2026-10-31 기준**  
> 상세 아키텍처, 컴포넌트 선정 이유, 모든 Playbook의 역할, 배포 순서 및 운영 방법: **[docs/PROJECT_GUIDE.md](docs/PROJECT_GUIDE.md)**

## 이 프로젝트는 무엇인가?

**5대 Ubuntu VM에 Hadoop 기반 데이터 처리 플랫폼과 중앙 운영 콘솔, 관측·백업 체계, OpenMetadata 데이터 거버넌스 환경을 Ansible로 구축한 개인 실습 프로젝트**다. 2026-10-31 현재 Kaggle Olist 데이터를 HDFS에 적재하고 Spark on YARN으로 배치 ETL·품질 검사·실행 관측·OpenMetadata 계보·품질 승인까지 연결했다. 별도 Iceberg 실습에서는 Hadoop Catalog, Snapshot/Time Travel, 행 단위 SQL 및 WAP(검증 후 공개)까지 검증했다. Ambari 없이 인프라와 서비스 설정을 코드로 관리하는 것이 핵심이다.

목적은 다음 세 가지로 나뉜다.

1. **데이터 플랫폼:** Olist CSV 9개를 HDFS Raw에 보존하고 Spark on YARN으로 Curated/Quarantine/Mart를 생성하며, 47~52번에서 검증·운영·승인·계보를 연결한다. 53~54번은 Iceberg 기반 테이블 운영의 독립 실습이다.
2. **운영 플랫폼:** Prometheus/Grafana/Alertmanager로 상태를 감시하고, Web Console에서 서비스를 조회·관리하며, 장애 진단과 백업·복원을 자동화한다.
3. **데이터 거버넌스:** OpenMetadata에 데이터베이스·테이블·컬럼을 수집하고 설명, 분류, 용어집, 데이터 계보를 관리한다.

## 시스템 구성

| VM | 역할 | 주요 구성 |
|---|---|---|
| `master` | 데이터 플랫폼 제어 | HDFS NameNode, YARN ResourceManager, Spark History Server |
| `worker1` | 분산 처리 | HDFS DataNode, YARN NodeManager |
| `worker2` | 분산 처리 및 백업 수신 후보 | DataNode, NodeManager, PostgreSQL 원격 백업 수신(38번 실전송 확인 필요) |
| `worker3` | 운영·자동화 중심 | Ansible Controller, Web Console(Nginx/FastAPI/React), PostgreSQL, Prometheus, Grafana, Alertmanager |
| `metadata` | 데이터 거버넌스 전용 | Docker Compose, OpenMetadata, MySQL, Elasticsearch |

- 실습 VM 자원: 기존 노드 각각 **2 vCPU/4GB**, `metadata` **4 vCPU/8GB**; 물리 PC **6코어/12스레드·32GB RAM**.
- Ansible Inventory: `[hadoop_cluster]` = master + worker1 + worker2, `[ops]` = worker3, `[governance_nodes]` = metadata. **metadata는 Hadoop 설치 대상이 아니다.**
- **NameNode·ResourceManager HA는 미구성.** 백업과 복구 절차가 HA를 대체하지는 않는다.

```text
                                 ┌───────────────────────────────────┐
                                 │ worker3: Ansible + Web Console     │
                                 │ PostgreSQL / Prometheus / Grafana  │
                                 │ Alertmanager / Ingestion CLI       │
                                 └───────┬───────────────┬───────────┘
                           SSH/Ansible   │               │ OpenMetadata API
                 ┌───────────────────────┘               ▼
                 ▼                           ┌─────────────────────────┐
 master: NameNode / RM / Spark History / ETL submit       │ metadata: OpenMetadata  │
       ├── worker1: DN / NM                  │ MySQL + Elasticsearch   │
       └── worker2: DN / NM                  └─────────────────────────┘
                   HDFS + YARN / Spark
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
| **Apache Iceberg 1.10.1** | Spark 3.5.6/Scala 2.12/Java 11과 HDFS Hadoop Catalog 기반 Snapshot·Time Travel·변경 SQL·Branch WAP 실습 |

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

> 번호 **28·29는 현재 확인한 Playbook 목록에 없다.** 44번은 패키지 제공 단계까지 진행했으며 DB View 및 OpenMetadata 계보가 실제 생성됐다는 근거는 없다.

<!-- BEGIN OLIST PIPELINE 45-54 -->
## Olist 데이터 파이프라인 및 Iceberg 실습 (45~54, 2026-10-31)

**상태:** 45~52번 Olist 파이프라인 구축과 v3 품질 승인, 53~54번 독립 Iceberg 스모크/트랜잭션/WAP 실습을 사용자 확인으로 완료했다. **주의:** 48번 주기적 Timer는 기본 비활성이고, 49번 대시보드의 기본 질의 버전은 v1이며, v3를 항상 자동 처리·승인하는 통합 파이프라인은 아니다. 실제 결과의 수치·상태는 HDFS 증빙과 운영 메트릭으로 구분해 확인한다.

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

[53~54] Iceberg 별도 실습: /data/olist/iceberg-lab/ (기존 Warehouse 미변경)
    Iceberg Hadoop Catalog → Snapshot/Time Travel → SQL 변경 → audit Branch → Publish
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

**WAP 실습의 경계:** 54번 검사는 소규모 샘플 행 검사로, 47번의 29개 실제 Olist 품질 검사 및 52번 승인을 대체하지 않는다. 53~54번은 기존 Raw/Warehouse/Quality/Approval/OpenMetadata를 변경하지 않는다. 공유 Catalog·Trino·운영용 권한정책·멀티테이블 원자적 공개는 아직 구축하지 않았다.

### 향후 작업 / 검증 필요

1. Iceberg 테이블과 47번 품질 검사를 연결한 **실제 Olist** WAP 설계. 54번 샘플 WAP를 운영 승인 정책과 혼동하지 않는다.
2. Spark와 Trino가 함께 사용할 Catalog를 별도 선정·검증하고 SQL 소비자가 품질 승인 상태를 강제하도록 구현.
3. 48번 Timer·Alert 실제 활성화 여부 및 v3 최신성 모니터링, 51번 Reconcile Timer 상태 확인.
4. 결제·리뷰·판매자·지역·카테고리 번역 CSV 정제/모델링, 38/43/44번 미검증 기능 확인.

**단계별 상세 실행법:** [`docs/47-olist-apply.md`](docs/47-olist-apply.md) · [`docs/48-olist-apply.md`](docs/48-olist-apply.md) · [`docs/49-olist-apply.md`](docs/49-olist-apply.md) · [`docs/50-olist-apply.md`](docs/50-olist-apply.md) · [`docs/51-olist-apply.md`](docs/51-olist-apply.md) · [`docs/52-olist-apply.md`](docs/52-olist-apply.md) · [`docs/53-iceberg-apply.md`](docs/53-iceberg-apply.md) · [`docs/54-iceberg-apply.md`](docs/54-iceberg-apply.md).

<!-- END OLIST PIPELINE 45-54 -->

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
collections/         필요한 Ansible Collection 선언
scripts/, tests/     검증·운영 스크립트·테스트
Makefile             자주 쓰는 명령을 표준화
docs/PROJECT_GUIDE.md 01~54번 구조·의존성·운영·검증 가이드
docs/45-olist-*.md ~ docs/54-iceberg-*.md  실데이터 파이프라인·Iceberg 단계별 운영 문서
```

**문서 읽는 순서:** 이 README → [상세 프로젝트 가이드](docs/PROJECT_GUIDE.md) → 해당 단계의 `docs/*-apply.md` 및 설계 문서 → 필요 시 `docs/runbooks/`.

## 운영 안전 원칙

- SSH **개인키**, JWT, DB 비밀번호, `.env`, 실제 Inventory, DB 백업 파일을 Git에 넣지 않는다.
- Olist 원본 CSV·ZIP 및 HDFS/Parquet/Iceberg 산출물, Iceberg Runtime JAR는 Git에 올리지 않는다. 프로젝트 `.gitignore`에 `/olist/`를 포함한다. HDFS 결과는 파일로 Export해서 커밋하지 않는다.
- 원격 다중 노드 Playbook(특히 **38, 40, 41**)에는 전역 `-e ansible_connection=local`을 사용하지 않는다.
- OpenMetadata의 Docker 볼륨 삭제(`docker compose down -v`) 금지. PostgreSQL 복원 검증은 운영 DB가 아닌 임시 DB에서 수행한다.
- **Web Console의 운영 Inventory(PostgreSQL)**와 **OpenMetadata의 데이터 카탈로그(MySQL)**는 목적이 다른 시스템이다.

**문서 기준:** 2026-10-31. 기존 README(46번까지)와 47~54번 패키지·대화의 실제 결과를 종합해 갱신했다. 실제 worker3 Git 작업 트리를 직접 읽은 것은 아니므로 적용 전 `git diff`로 비교한다. 38·43·44번의 기존 미검증 표기는 유지한다.
