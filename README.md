# Bigtop Data Platform — Ansible 기반 실습·운영 플랫폼

> **프로젝트 한눈에 보기 · 2026-10-30 기준**  
> 상세 아키텍처, 컴포넌트 선정 이유, 모든 Playbook의 역할, 배포 순서 및 운영 방법: **[docs/PROJECT_GUIDE.md](docs/PROJECT_GUIDE.md)**

## 이 프로젝트는 무엇인가?

**5대 Ubuntu VM에 Hadoop 기반 데이터 처리 플랫폼과 중앙 운영 콘솔, 관측·백업 체계, OpenMetadata 데이터 거버넌스 환경을 Ansible로 구축한 개인 실습 프로젝트**다. Ambari 없이 인프라와 서비스 설정을 코드로 관리하는 것이 핵심이다.

목적은 다음 세 가지로 나뉜다.

1. **데이터 플랫폼:** HDFS에 데이터를 저장하고 YARN에서 Spark를 실행한다.
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
 master: NameNode / RM / Spark History       │ metadata: OpenMetadata  │
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
| **Spark / History Server** | 분산 처리와 완료된 애플리케이션 실행 이력 분석 |
| **Node Exporter + JMX Exporter** | OS·디스크 지표와 JVM/Hadoop 내부 지표 수집 |
| **Prometheus + Grafana + Alertmanager** | 메트릭 저장·평가, 시각화, 장애 알림 |
| **FastAPI + React + Nginx** | 서비스 상태·로그·작업 이력·백업 상태를 통합하는 Web Console. Nginx는 접근 인증/프록시 |
| **PostgreSQL** | Web Console Action History와 클러스터 운영 메타데이터의 영속 저장 |
| **systemd Timer + pg_dump + rsync/SSH** | 정기 백업, 무결성 검사, 원격 보관 자동화 |
| **Docker Compose + MySQL + Elasticsearch** | OpenMetadata 본체·메타데이터 DB·검색 기능을 별도 VM에서 구동 |
| **OpenMetadata + PostgreSQL Ingestion CLI** | 테이블/컬럼 카탈로그와 데이터 거버넌스 메타데이터 관리. Airflow는 현재 수집에 사용하지 않음 |

## 주요 사용자 화면

| 화면 | 접근 주소/설명 |
|---|---|
| **Bigtop Web Console** | `http://worker3:1337` — Nginx Basic Auth; 내부 FastAPI `127.0.0.1:18090` |
| NameNode | `http://master:9870` |
| YARN RM | `http://master:18088` — 콘솔 바로가기 설정값 |
| Spark History | `http://master:18080` |
| Prometheus | `http://worker3:9090` |
| Grafana | `http://worker3:23000` — 콘솔 바로가기 설정값 |
| Alertmanager | `http://worker3:19093` — 콘솔 바로가기 설정값 |
| **OpenMetadata** | `http://192.158.2.104:8585` — metadata VM, 실습 환경 주소 |

> 위 이름·주소는 실습 환경 기준이다. **바로가기 URL이 실제 데몬의 listen 포트 또는 VirtualBox 포트포워딩과 동일하다는 뜻은 아니다.** 브라우저가 `master`/`worker3`를 해석하지 못하면 Ansible 변수로 브라우저에서 접근 가능한 주소를 지정한다. `192.158.2.x`는 RFC1918 사설 IP 대역이 아니므로 네트워크 격리를 확인한다.

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
| **46** | Spark on YARN **Cluster Mode** 주문 배치 ETL, Curated/Quarantine/Mart Parquet | **실제 실행 성공(사용자 확인)**, 출력·재실행 상세값 별도 점검 |

> 번호 **28·29는 현재 확인한 Playbook 목록에 없다.** 44번은 패키지 제공 단계까지 진행했으며 DB View 및 OpenMetadata 계보가 실제 생성됐다는 근거는 없다.

<!-- BEGIN OLIST PIPELINE 45-46 -->
## Olist 실데이터 파이프라인 (45~46, 2026-10-30)

**상태:** 45번 HDFS Raw 적재 완료, 46번 Spark on YARN **Cluster 모드 실행 성공(사용자 확인)**. 자동 정기 실행·OpenMetadata 계보 연동·재실행 SKIP 검증은 별도 후속 작업이다. 실제 배치 처리 건수는 이 문서에 임의로 기재하지 않는다. 로그와 매니페스트를 기준으로 점검한다.

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
- 검증: 수정본 **Cluster 모드 실행 성공(사용자 확인)**. 정적·단위 테스트는 실행 로그와 별개이며, 정확한 처리 건수와 재실행 SKIP 증빙은 추후 확인.

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

### 향후 작업

1. **47번 후보:** Parquet 정합성·행 수·합계 검증과 품질 지표/기준(중복, null, orphan, quarantine) 대시보드화.
2. 배치 스케줄링과 실패 재시도/재처리 정책(Airflow 등) 및 idempotent 운영 검증.
3. OpenMetadata에 HDFS/Spark 결과 자산 등록과 **실제 Spark 처리 계보** 수집. SQL View Lineage(44)와는 별도.
4. 결제·리뷰·판매자·지역·카테고리 번역 CSV를 단계적으로 정제/모델링.
5. 기존 **38번 원격 백업 복제**, **43번 정책**, **44번 SQL View 계보**의 실제 적용 상태는 별도로 점검한다.

<!-- END OLIST PIPELINE 45-46 -->

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
curl -fsS http://192.158.2.104:8585/ -o /dev/null
```

`make validate`는 로컬 개발환경/Collection 준비 상태에 의존할 수 있다. 복구·중지·재시작 명령은 상태 조회와 다르게 서비스에 영향을 준다.

## 소스 코드 안내

```text
inventory/           호스트 그룹·환경별 설정 예제 (실제 IP/비밀정보는 Git 제외)
playbooks/           배포·재배포·점검·복구의 진입점
roles/               서비스별 Ansible 설정, 템플릿, systemd 단위
roles/web_console/   FastAPI·React/Vite·Nginx 중앙 콘솔
roles/governance_*/  OpenMetadata 관련 Docker, 수집, 정책·계보
collections/         필요한 Ansible Collection 선언
scripts/, tests/     검증·운영 스크립트·테스트
Makefile             자주 쓰는 명령을 표준화
 docs/PROJECT_GUIDE.md 전체 구조, 컴포넌트 선정 배경, Playbook별 상세 설명
```

**문서 읽는 순서:** 이 README → [상세 프로젝트 가이드](docs/PROJECT_GUIDE.md) → 필요한 영역의 `docs/` 운영 문서 및 `docs/runbooks/`.

## 운영 안전 원칙

- SSH **개인키**, JWT, DB 비밀번호, `.env`, 실제 Inventory, DB 백업 파일을 Git에 넣지 않는다.
- 원격 다중 노드 Playbook(특히 **38, 40, 41**)에는 전역 `-e ansible_connection=local`을 사용하지 않는다.
- OpenMetadata의 Docker 볼륨 삭제(`docker compose down -v`) 금지. PostgreSQL 복원 검증은 운영 DB가 아닌 임시 DB에서 수행한다.
- **Web Console의 운영 Inventory(PostgreSQL)**와 **OpenMetadata의 데이터 카탈로그(MySQL)**는 목적이 다른 시스템이다.

**문서 기준:** 이전 README 스냅샷과 31~44번 Ansible 배포 패키지, 대화 중 확인된 실제 작업 로그를 재구성했다. 현재 worker3 Git 작업 트리를 직접 읽은 결과는 아니므로 상세 가이드의 `확인 필요` 항목은 실제 리포지터리와 대조해야 한다.
