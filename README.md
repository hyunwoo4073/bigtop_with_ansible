# Bigtop Data Platform — 온프레미스 데이터 플랫폼 실습

> **현황: 2026-11-14 · 5대 VM · Ansible 중심 관리**
> 상세 구조·전체 Playbook·운영/복구 절차: [docs/PROJECT_GUIDE.md](docs/PROJECT_GUIDE.md)  
> **현재 중점:** Olist Lakehouse(45~63) → Kafka KRaft·CDC(64~65) → Watchdog·진행 관측·복구 증빙(66~69) → **Web Console의 Services 안에 Kafka 통합(70)** → **격리 CDC 중복·역순·Replay 정합성 검증(71)** → **Iceberg MERGE 직후 장애 복구 시험 패키지(72, 실제 실행 미확인)**. 5-VM 자원 제약으로 Spark 애플리케이션은 순차 실행한다.

## 1. 프로젝트 소개와 현재 방향

Apache Bigtop과 Ansible로 Hadoop(HDFS·YARN·Spark), Iceberg Lakehouse, Kafka, SQL 엔진, 거버넌스, 모니터링을 **내부망의 작은 5-VM 클러스터에 직접 통합**하는 실습 프로젝트다. Ambari에 의존하지 않고 설치·설정·데이터 흐름·운영 검증을 Playbook/Role/스크립트와 Git으로 관리한다.

현재 목표는 기능을 많이 설치하는 데서 끝나지 않고 **데이터가 유입·변환·검증·공개·조회되는 경로와 장애 시 복구 가능성**까지 확인하는 것이다.

- **배치 데이터:** Olist 9개 원본 CSV → HDFS Raw → Spark/Parquet → 29개 DQ → v3 승인 → Iceberg Silver/Gold → Gold WAP → Trino.
- **증분 데이터:** 별도 실습 테이블에서 Iceberg `MERGE`, Kafka 이벤트 소비, 시퀀스 기반 멱등 갱신과 HDFS 체크포인트 검증.
- **운영:** Prometheus/Grafana/Alertmanager, Web Console **Services > Kafka (Step70)**, OpenMetadata, Kafbat UI, systemd, YARN·Trino·Kafka CLI로 진단과 검증 분리.
- **자원 효율:** 상시 Spark 한 개가 YARN 자원을 점유하므로 Step65의 `produce/replay/reconcile`은 worker3 경량 Kafka CLI, `verify`는 Trino/HDFS·Kafka End Offset 검증으로 분리했다. 추가 YARN Spark 배치 제출 없이 검증한다.
- **진행 관측·복구:** Step67은 Kafka 최신 Offset(worker3)과 Spark 커밋 완료 Offset(master)를 60초 주기로 수집한다. Step68은 Prometheus Lag/Health Rule과 Grafana를 추가하고, Step69는 읽기 전용 복구 전·후 증빙을 비교한다. Step71은 독립 Topic·테이블·Checkpoint에서 중복 및 역순 CDC의 멱등성을 검증했으며, Step72는 `foreachBatch`의 Iceberg 커밋 이후 Spark 체크포인트 커밋 전 실패를 재현하도록 준비했다.

## 2. 인프라·런타임 배치

| 노드 | 역할 | 실행 서비스 |
|---|---|---|
| `master` | 제어·Spark submit | NameNode, ResourceManager, Spark History Server, Spark 3.5.6/YARN cluster submit, Step66 Watchdog(자동 복구 활성 여부 별도), Step67 Spark Progress Exporter |
| `worker1` | 처리·메시징 | DataNode, NodeManager, Kafka Broker+Controller(node 1) |
| `worker2` | 처리·메시징 | DataNode, NodeManager, Kafka Broker+Controller(node 2), DB 원격 백업 후보(검증 필요) |
| `worker3` | 운영·카탈로그·메시징 | Ansible, Web Console, PostgreSQL, Prometheus/Grafana/Alertmanager, Gravitino REST `:9001`, Kafka Broker+Controller(node 3), Step67 Kafka Offset Exporter, Step68 Prometheus/Grafana, Step69 로컬 증빙 수집/비교, Step70 Web Console Kafka Services·API |
| `metadata` | SQL·거버넌스·UI | OpenMetadata 2.0.6, MySQL/Elasticsearch, Trino 484 `:8080`, Docker Kafbat UI `:18081`(설치/접속 언급; 최종 `verify` 로그 별도 확인) |

- **Spark:** Java 11 / Spark 3.5.6 / Scala 2.12 / Iceberg runtime 1.10.1. Kafka source JAR는 Spark 3.5.6에 맞춘 `vendor/kafka64` 의존성 집합.
- **REST Catalog:** Gravitino Iceberg REST Server 1.3.1(전용 Java 17), Catalog 메타데이터 PostgreSQL, Warehouse는 HDFS. Trino는 전용 Java 25.
- **Kafka:** 4.2.2, KRaft 3노드 **combined broker+controller**, broker `:9092`, controller **`:19093`**, RF=3/minISR=2. Kafka 전용 Java 17 및 systemd 서비스 `bigtop-kafka-kraft64`.
- **노드 자원:** 기존 소형 VM(worker1/2/3 약 2 vCPU/4GiB)에서 Kafka와 Spark/YARN이 공유. 메모리 부족과 Queue `ACCEPTED` 대기는 실제 운영 제약으로 확인됨.
- **HA 범위:** HDFS NameNode/ResourceManager HA **미구성**. Kafka 3 voters는 Kafka Metadata Quorum이며 HDFS/YARN HA를 의미하지 않는다. Kafka combined mode는 실습용이며 운영 대규모 배치 권장 구조와 구분한다.

```text
Olist CSV(9) ── HDFS Raw ── Spark on YARN ── Parquet v3 ── 29 DQ / 승인
                                                          │
                                                  Iceberg Silver/Gold
                                                  Gold WAP + Trino
                                                          │
                                         OpenMetadata / Prometheus / Grafana

Iceberg CDC seed ──(합성 CDC 이벤트)──► Kafka worker1/2/3 :9092
                                           │
                                           ▼
                              Spark Structured Streaming on YARN
                              foreachBatch + 멱등 Iceberg MERGE
                                           │
                              Iceberg CDC table + HDFS checkpoint
                                           │
                                   Trino / 증빙 검증

Kafka ── CLI collector → Prometheus/Grafana  |  Kafka ── Kafbat UI(토픽/메시지/그룹)
                     ↓ Step67 (Kafka End Offset + Spark committed checkpoint)
             Step68 Prometheus Lag/Health/Alerts + Grafana
                     ↓
             Step69 baseline/after JSON + HDFS offsets/commits 비교 (worker3)

Web Console Services ── Kafka 3개 Broker 서비스 상태/Topic/Consumer/Streaming 현황
                 └── 상세 조회 Kafbat / 시계열 Grafana

[격리 테스트] Step71: Kafka Topic71 → Spark availableNow → Iceberg71 + Checkpoint71
             (9개 이벤트 + 동일 9개 재발행 → 최종 3행, Snapshot 유지)
[미실행]     Step72: 별도 Topic72/Checkpoint72/Iceberg72; MERGE 후 의도된 실패 → 복구
```

## 3. 구현 범위와 실제 확인 상태

| 단계 | 구현 기능 | 2026-11-14 기준 |
|---|---|---|
| 01~44 | Hadoop 기반/관측/Web Console/백업/OpenMetadata | 기본 플랫폼 운영. 38/43/44 등 일부 최종 실검증 별도 필요 |
| 45~52 | Raw 9 CSV, Spark Parquet, 29 DQ, 배치 스케줄러, 모니터링, 데이터 계보, v3 승인 | 구현 및 사용자 검증. 정기 Timer 활성화 여부 별도 |
| 53~54 | Iceberg HadoopCatalog, Snapshot/Time Travel, DML·샘플 WAP | 완료 |
| 55~57 | 5 VM Preflight, Gravitino REST, Spark↔Trino 공통 Iceberg 테이블 | 완료, 교차 SQL PASS |
| 58~60 | 승인 v3의 Iceberg 7개 테이블, Gold 29 DQ·WAP, 수동 One-shot 릴리스 | 완료. Gold 공개 55,877행; 멀티테이블 원자적 공개는 아님 |
| 61 | Iceberg 8개 자산·8개 계보 관계 OpenMetadata 등록, 운영 대시보드/메트릭 | 사용자 완료 확인 |
| 62 | **별도 실습 테이블만** 작은 파일 Compaction, Snapshot 롤백, Trino 검증 | 사용자 완료 확인 |
| 63 | Iceberg 합성 CDC `MERGE`, 최신 시퀀스·중복·Soft Delete 처리 | 사용자 완료 확인. 최종 516행(유효 512/삭제 4) |
| **64-A** | Kafka 4.2.2 3-node KRaft 설치·토픽 송수신 | **실제 PASS 로그 확인**. Topic RF3/minISR2/ISR3, Java17 Smoke PASS |
| **64-B** | Kafka Quorum/ISR/Broker CLI 메트릭, Grafana, OpenMetadata Messaging Service | Kafka 사전 점검 PASS; 수정본 이후 사용자 확인. 수집 Timer/메타데이터 최종 상태는 개별 점검 |
| **64-C** | Docker Kafbat UI v1.5.0, 읽기 전용 Kafka 탐색 | 사용자 UI 사용 언급. 최초 단위 테스트 8/9(전역 바이너리 탐색 오탐), 수정 포함; `verify` 결과 미제공 |
| **64** | Kafka → Spark Structured Streaming `AvailableNow` → Iceberg CDC → Trino | 사용자 **통과 확인**. 최종 520행(유효 512/삭제 8) |
| **65 / 65-B** | 상시 Streaming + 경량 Kafka Producer/Replay·Trino/HDFS Verifier | **데이터 정합성 PASS:** 521행(유효 512/삭제 9), Kafka End/Checkpoint 24/24, Iceberg Snapshot `7676611440228973561` 유지. 과거 YARN `ACCEPTED` 자원 문제는 경량 검증으로 우회. |
| **66** | Step65 단일 App 대상 보호형 Watchdog, 의도된 중지/복구 제한, systemd Timer | **코드/테스트 마련:** 자동 재시작 여부, Timer 활성화·실장애 복구 검증은 별도 확인. `enable` 명시적 opt-in. |
| **67** | master Spark Commit, worker3 Kafka End Offset, Node Exporter Textfile 수집 | **적용·검증 PASS:** 서비스 템플릿 줄바꿈 오류 수정 후 양 노드 검증, 60초 Timer Active/Waiting, Prometheus 두 Target 수집 확인. |
| **68** | Prometheus Offset Lag/Health Recording·Alert Rules + Grafana | **사용자 적용·검증 완료 보고:** `Offset Lag=0` 확인. 실제 Alertmanager 알림 전달 및 경고 발화 시나리오는 별도 미검증. |
| **69** | Before/After Kafka·Spark Progress 및 HDFS offsets/commits 읽기 전용 증빙 | **baseline/after/compare PASS:** Offset `24→24`, Batch `2→2`, Lag `0`. Step65 정상 stop/start와 Snapshot/행수 불변 확인. 실패 중 미완료 Batch replay는 미검증. |
| **70** | 기존 Web Console Services에 Kafka 3 Broker 등록, 조회 전용 Broker/Topic/Consumer/Streaming 화면, Kafbat·Grafana 연결 | **배포 후 사용자 UI 정상 반영 확인.** 별도 Kafka 최상위 메뉴 대신 기존 Services 그룹에 통합. 초기 apply 시 핸들러 SSH 단절/API 포트 응답 실패 후 사용자 정상 복구 확인. Broker 조작 기능은 포함하지 않음. |
| **71** | 독립 Topic/Checkpoint/Iceberg, I/U/D·중복·역순 처리 및 동일 메시지 Replay | **사용자가 전체 검증 및 Step65 복구 완료 확인.** 초기 9건 발행 `offset=0→9` 실제 PASS, Replay 후 최종 3행(유효2/삭제1)·Snapshot 불변 검증을 사용자 완료 보고. `py_compile` 캐시 권한 이슈는 수정. |
| **72** | Iceberg MERGE 직후, Spark `commits/0` 이전 고의 실패 후 동일 체크포인트 복구 실험 | **패키지 및 실행 SH 준비, VM 실행/검증 미확인.** `fault → inspect_fault → recover → verify`가 별도 Topic·테이블·Checkpoint를 사용하도록 설계. Exactly-once 전체 보증 아님. |

**실행 범위 주의:** 61~65는 별도 네임스페이스·토픽·실습 테이블에 적용된다. 59번 Gold 공개 테이블과 기존 승인 데이터는 변경 대상으로 삼지 않는다.

## 4. 접속 화면 및 역할

| 서비스 | 실습 접근 경로 | 용도 |
|---|---|---|
| Web Console | `http://worker3:1337` | 내부 서비스·운영 액션, **Services > Kafka** (Step70 브로커 상태·토픽·그룹·스트리밍 조회) |
| HDFS NameNode | `http://master:9870` | HDFS 상태 |
| YARN | `http://master:18088`(기존 콘솔 설정; 실제 리스너 확인) | 앱 `RUNNING`·`ACCEPTED`, Queue/AM 리소스 |
| Spark History | `http://master:18080` | 종료된 Spark 앱 실행 기록 |
| Prometheus / Grafana | worker3 `:9090` / `:3000`, Windows 호스트 포워딩 `http://127.0.0.1:23000` | 시계열·Alert Rule·Kafka/Gold/Step67·68 대시보드; 알림 전달 경로는 별도 점검 |
| Alertmanager | **worker3 `:9093` 추정 — `ss`로 서비스·바로가기 검증 필요** | 경보 라우팅. Kafka Controller는 별도 `:19093` |
| OpenMetadata | metadata `:8585` | 데이터셋·Iceberg 계보·Kafka Messaging Service |
| Gravitino REST | worker3 `:9001/iceberg/` | 공유 Iceberg Catalog API |
| Trino | metadata `:8080` | SQL 검증 및 Iceberg 조회 |
| Kafbat UI | metadata `:18081` (설치 후 내부망 접근) | Kafka 토픽·메시지·Consumer Group 탐색. Step68 지연 경보는 별도 Prometheus 기반 |

Kafbat UI는 운영 탐색용이다. Step67/68은 **Kafka End Offset과 Spark 커밋 완료 Offset의 차이(스트리밍 진행 Lag)**를 별도 Prometheus Rule로 계산하며, Kafka 일반 Consumer Group Lag와 동일한 의미로 보지 않는다. Spark 체크포인트의 `offsets`·`commits` 및 YARN 로그, Iceberg 최종 상태를 함께 검증한다.

## 5. 사용법 — 변경 전에 상태부터 확인

명령 실행 기준: `worker3`의 `~/bigtop-cluster-ansible`.

```bash
.venv/bin/ansible-inventory -i inventory/hosts.ini --graph
make lint
make health

# Kafka 서비스·Quorum(64-A)
.venv/bin/ansible-playbook -i inventory/hosts.ini \
  playbooks/64a-kafka-kraft-cluster.yml -e kafka_kraft64_mode=verify

# Kafka/Grafana 구성(64-B), Kafbat UI(64-C) 읽기 전용 검증
.venv/bin/ansible-playbook -i inventory/hosts.ini \
  playbooks/64b-kafka-platform-integration.yml -e kafka_platform64b_mode=verify
.venv/bin/ansible-playbook -i inventory/hosts.ini \
  playbooks/64c-kafka-web-ui.yml -e kafka_ui64c_mode=verify

# 65번 상시 스트리밍 앱 상태 — 시작 전 반드시 실행
.venv/bin/ansible-playbook -i inventory/hosts.ini \
  playbooks/65-kafka-streaming-operations.yml -e olist_kafka65_mode=status
```

### 65~69번 주요 운영 검증 (2026-11-08)

```bash
# 65: 상시 Spark 애플리케이션 상태 및 Kafka/Trino/HDFS 정합성 (변경 없음)
.venv/bin/ansible-playbook -i inventory/hosts.ini \
  playbooks/65-kafka-streaming-operations.yml -e olist_kafka65_mode=status
.venv/bin/ansible-playbook -i inventory/hosts.ini \
  playbooks/65-kafka-streaming-operations.yml -e olist_kafka65_mode=verify

# 66: watchdog 상태 (자동 복구 enable은 별도 의도적 작업)
.venv/bin/ansible-playbook -i inventory/hosts.ini \
  playbooks/66-kafka-streaming-watchdog.yml -e olist_kafka66_mode=status

# 67: 양 노드 메트릭 수집 (기존 60초 Timer가 따로 동작)
.venv/bin/ansible-playbook -i inventory/hosts.ini \
  playbooks/67-kafka-spark-progress.yml -e olist_kafka67_mode=verify

# 68: 기존 Prometheus/Grafana에 설치된 Rule 검증
.venv/bin/ansible-playbook -i inventory/hosts.ini \
  playbooks/68-streaming-health.yml -e olist_kafka68_mode=verify

# 69: 관측 비교는 기존 증빙 디렉터리 선택(기본 after 파일 덮어쓰기 금지)
.venv/bin/ansible-playbook -i inventory/hosts.ini \
  playbooks/69-streaming-recovery-proof.yml -e olist_kafka69_mode=compare \
  -e "olist_kafka69_evidence_dir=$PWD/artifacts/step69/restart-20261108-181312"
```

- **65 증빙:** Topic `bigtop_olist_step65_cdc_v1`; target `shared.olist65_stream.fact_cdc_running_v1`; `521/512/9`, End Offset=`24`, Spark Checkpoint=`24`, Snapshot=`7676611440228973561`이 2026-11-08 검증 시점에서 유지되었다. `verify`는 별도 Spark/YARN Batch를 제출하지 않는다.
- **정상 종료·재기동:** `application_1794117087936_0002`를 Step65 `stop`으로 종료하며 체크포인트 보존, `start`로 `application_1794117087936_0003` 제출. 그 직후 `status`는 `ACCEPTED`였으므로 **새 앱의 `RUNNING` 전환은 별도 `status` 확인**해야 한다. 이어서 Kafka/Trino/HDFS `verify`와 Step69 비교는 통과했다.
- **Replay 보호:** Step65 `replay` 재실행은 Kafka End Offset=24라 `Replay permitted only once from initial 12 offsets` 보호 조건으로 차단되었다. 체크포인트/오프셋을 강제로 초기화하지 않는다. 마지막 Replay 재실행으로 **중복 반영 방지를 새로 실증한 것은 아니다.**
- **관측:** Step67 수집기 60초 Timer `active/waiting`, Prometheus에서 `source=spark/kafka` 모두 `collector_success=1`, Lag=0 확인. Step68은 규칙 로드·배포 사용자 확인이며 실제 Alertmanager 알림 전달은 미확인.
- **Step69:** `artifacts/step69/`에 매 실행마다 별도 디렉터리 사용. `STEP69_COMPARE_PASS`는 해당 시점의 Offset/Checkpoint **안정성 증명**이며 미완료 Batch 재처리 또는 Exactly Once 증명은 아니다.
- **재기동·실습 변경은 상시 명령이 아니다:** 새 입력·명시적 테스트 없이 `65 stop/start/replay/produce`를 반복하지 않는다. Kafka 토픽, HDFS 체크포인트, 기존 Iceberg Snapshot을 삭제하지 않는다.

상세 구현·안전 경계: [65 경량 Producer/Verifier](docs/65-low-resource-producer-verifier.md), [66 Watchdog](docs/66-kafka-streaming-watchdog.md), [68 관측·경고](docs/68-streaming-health.md), [69 복구 증빙](docs/69-streaming-recovery-proof.md), [프로젝트 가이드](docs/PROJECT_GUIDE.md).

### 70~72번 운영 UI 및 CDC 검증 (2026-11-14)

```bash
# 70: Web Console Kafka 조회 전용 API/UI (배포 및 사용자 화면 확인)
.venv/bin/ansible-playbook -i inventory/hosts.ini \
  playbooks/70-kafka-web-console.yml -e web_console_kafka_mode=verify

# 71: 격리 CDC 검증 (검증 완료; 불변 증빙/이벤트 때문에 전체 재실행하지 말 것)
# .venv/bin/ansible-playbook -i inventory/hosts.ini \
#   playbooks/71-cdc-consistency.yml -e olist_kafka71_mode=verify_replay

# 72: 아직 실행하지 않은 장애 주입 실험; Step65 중지 및 Spark 앱 부재 확인 후에만
# .venv/bin/ansible-playbook -i inventory/hosts.ini \
#   playbooks/65-kafka-streaming-operations.yml -e olist_kafka65_mode=stop
# bash scripts/run_step72_once.sh
# Step72를 완료하고 YARN 앱이 종료된 것을 확인한 뒤 Step65를 start/status/verify
```

- **Step70:** `web_console_kafka` Role이 기존 React/FastAPI 콘솔에 Kafka 조회 화면·API를 추가했다. Kafka 3개 브로커는 독립 최상위 메뉴가 아닌 **Services**에 일반 서비스처럼 등록하고, 운영 조작은 비활성(조회 전용)으로 둔다. Kafbat UI는 별도 Docker UI 바로가기이며 systemd Broker로 표시하지 않는다. 서비스 API 최초 재시작에서 SSH 연결 오류/18090 거부가 발생했지만 사용자는 복구 및 화면 반영을 확인했다.
- **Step71:** Topic `bigtop_olist_step71_cdc_v1`(1 partition, RF3), Iceberg `shared.olist71_cdc.fact_cdc_consistency_v1`, HDFS Checkpoint `/data/olist/iceberg-checkpoints/step71/v1`. 9개 이벤트 발행 `STEP71_PUBLISH_PASS: offsets=0->9`, `run→verify→baseline→replay→run→verify_replay` 완료를 사용자 확인. Replay 후 Offset 18, 최종 3행(유효2/삭제1), Snapshot 불변이 검증 설계상 성공 조건이다. 사용자 보고와 상세 최종 실행 로그는 구분한다. Spark 초기화 시 YARN `ACCEPTED`는 가용 자원 부족으로 대기한 것(최종 `FINISHED/SUCCEEDED` 확인). `py_compile`에서 `__pycache__` 권한 오류가 있어 문법 검사 방식을 수정했다.
- **Step72:** 별도 Topic `bigtop_olist_step72_crash_v1`, Iceberg `shared.olist72_recovery.fact_cdc_crash_v1`, Checkpoint `/data/olist/iceberg-checkpoints/step72/v1`. `scripts/run_step72_once.sh`는 `preflight→apply→publish→fault→inspect_fault→recover→verify` 순서이며 **장애 주입과 복구 결과의 실제 VM 실행은 미확인**. `fault`의 Spark `FAILED`는 Iceberg MERGE 직후 의도된 실패 근거가 있을 때만 정상이다. 중간 실패 시 스크립트 전체를 다시 실행하거나 Topic/Checkpoint를 지우지 않는다.
- **VM 자원:** YARN의 동시 Spark App 실행 공간이 사실상 1개이므로 Step71/72 작업 때 기존 Step65를 **명시적으로 중지**하고 모든 일회성 Spark 작업을 순차 처리한다. 작업 후 기존 체크포인트로 Step65를 `start→status(RUNNING)→verify` 복구해야 한다.

상세: [Step70 Web Console](docs/70-kafka-web-console.md) · [Step71 CDC Consistency](docs/71-cdc-consistency.md) · [Step72 Crash Recovery](docs/72-cdc-crash-recovery.md).

## 6. 코드 구조

```text
inventory/                            실제 인증정보·호스트 값은 Git 관리에서 분리
playbooks/                            인프라/서비스/데이터 파이프라인 Ansible 진입점
roles/                                Hadoop·운영·거버넌스·데이터 작업 Role
  olist_iceberg_observability61/       Iceberg OpenMetadata/Grafana 연계
  olist_iceberg_maintenance62/         Compaction/rollback 격리 실습
  olist_iceberg_incremental63/         Iceberg CDC MERGE 실습
  kafka_kraft64/                       Kafka KRaft 3-broker 설치
  kafka_platform64b/                  Kafka 메트릭·OpenMetadata/대시보드
  kafka_ui64c/                         Kafbat Docker UI
  olist_iceberg_kafka64/              Kafka CDC AvailableNow
  olist_kafka65/                       CDC 상시 스트리밍·경량 Producer/Verifier·YARN 제어
  olist_kafka66/                       보호형 단일 앱 Watchdog(systemd opt-in)
  olist_kafka67/                       Kafka End / Spark committed progress collectors
  olist_kafka68/                       Prometheus Lag/Alert Rule + Grafana
  olist_kafka69/                       읽기 전용 baseline/after 증빙 수집 및 비교
  web_console_kafka/                   Kafka Services UI/API 조회 전용 통합
  olist_kafka71/                       격리 CDC 중복/역순/Replay 정합성 실습
  olist_kafka72/                       커밋 경계 장애 주입 및 Checkpoint 복구 실습(미실행)
scripts/                              데이터 점검, Kafka 토픽 생성, 운영 CLI
  64a-smoke-kafka.sh                  Kafka Produce/Consume Smoke
  64b-kafka-platform.py               Kafka Quorum/ISR 수집 및 메타데이터 등록
  65-kafka-topic.sh                   65번 전용 토픽 관리
  run_step72_once.sh                   Step72 실패 주입/복구 단발성 실행(미실행)
tests/                                역할/스크립트 단위 테스트
docs/                                 설계/적용 문서, 운영·복구 Runbook
  PROJECT_GUIDE.md                    전체 아키텍처·Playbook 매핑·현재 운영 이슈
vendor/, artifacts/                   오프라인 의존성·실행 증빙: Git 제외
```

## 7. 데이터 정확성·보안·Git 운영 원칙

- `Gold WAP`는 **59번 Gold 1개 테이블**에 적용한 릴리스이다. 7개 Iceberg 테이블 전체를 원자적으로 공개한 것은 아니다.
- Spark `foreachBatch`는 장애 시 **같은 배치를 재실행**할 수 있다. Kafka와 Iceberg 사이에 분산 Exactly-once 트랜잭션이 있는 것은 아니며, 시퀀스 조건을 이용한 **최종 상태 멱등성**을 검증한다.
- HDFS 체크포인트/`meta.properties`/KRaft Cluster ID/기존 Iceberg Snapshot을 오류 해결 목적으로 삭제하거나 재생성하지 않는다. Kafka Controller는 `:19093`, Alertmanager와 포트가 겹치지 않도록 유지한다.
- 내부망 Kafbat UI의 `read-only`는 UI 변경을 제한할 뿐 Kafka ACL이나 사용자 인증을 대신하지 않는다. 서비스는 외부 인터넷에 노출하지 않는다.
- `vendor/` JAR/JDK, `artifacts/`(Step69 evidence 포함), `__pycache__/`/`*.pyc`, `.venv/`, 실행 로그/CSV/DB 백업, 운영 `inventory/hosts.ini`, 비밀번호/토큰/개인키/`.env`, 다운로드 ZIP은 Git 커밋에서 제외한다.
- **하루 작업 종료 시 한 번만 커밋:** 당일 생성·수정한 **Playbook + Role + Script + Test + Docs + README + PROJECT_GUIDE** 전체를 함께 스테이징하고, 변경 파일과 민감정보를 검토한 뒤 **하나의 Git 커밋**으로 묶는다. 문서만 따로 커밋하지 않는다.

## 8. 다음 단계 및 남은 검증

1. **Step72 실제 실행:** Step65의 현재 `RUNNING` 상태 확인 → 정상 중지 및 YARN 자원 확보 → 격리된 Topic/Checkpoint/Iceberg로 `fault/inspect_fault/recover/verify` 진행 → 성공 후 Step65 재기동. **2026-11-14 기준 아직 배포·실행 결과 없음.**
2. **Step70 운영 UI 보강:** Web Console Services의 Kafka 항목과 Topic/Consumer/Streaming 화면은 반영됐으나, Broker TCP 상태와 Kafka 자체 Health/ISR/Controller Quorum은 별개다. 읽기 전용 정책과 기존 인증/서비스 상태 API의 일관성을 유지하고 실제 운용 점검을 추가한다.
3. **Step68/66 실운영 안전 검증:** Alertmanager 수신/전달 경로, Watchdog Timer 활성화 및 통제된 장애 복구 시험은 별도 검증한다.
4. **자원·거버넌스/백업:** 작은 VM의 Spark 동시 실행 한계를 고려한 YARN Queue와 자원 설정, Gravitino/PostgreSQL/OpenMetadata·Iceberg metadata 백업/복원, 내부 계정별 SQL 권한/감사, 미확인 초기 기능(38/43/44/48/51) 정리.

**문서 기준:** 2026-11-14. 11월 8일 갱신본을 유지하면서 70(사용자 UI 정상 반영), 71(사용자 전체 PASS 및 Step65 복구), 72(패키지 준비·미실행) 상태를 반영했다. `git status`로 실제 VM의 모든 소스 변경과 설치/검증 결과를 다시 확인한다. **하루 작업은 Playbook·Role·Script·Test·Docs·README/PROJECT_GUIDE를 함께 한 번의 커밋으로 올리고, ZIP/벤더 파일/증빙/인증정보는 제외한다.**
