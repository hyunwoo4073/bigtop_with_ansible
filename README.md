# Bigtop Data Platform — 온프레미스 데이터 플랫폼 실습

> **현황: 2026-11-07 · 5대 VM · Ansible 중심 관리**  
> 상세 구조·전체 Playbook·운영/복구 절차: [docs/PROJECT_GUIDE.md](docs/PROJECT_GUIDE.md)  
> **현재 중점:** 배치 Olist Lakehouse(45~63) → Kafka KRaft 기반 CDC(64) → 상시 Structured Streaming(65). **65번 최종 검증은 YARN 자원 부족으로 미완료.**

## 1. 프로젝트 소개와 현재 방향

Apache Bigtop과 Ansible로 Hadoop(HDFS·YARN·Spark), Iceberg Lakehouse, Kafka, SQL 엔진, 거버넌스, 모니터링을 **내부망의 작은 5-VM 클러스터에 직접 통합**하는 실습 프로젝트다. Ambari에 의존하지 않고 설치·설정·데이터 흐름·운영 검증을 Playbook/Role/스크립트와 Git으로 관리한다.

현재 목표는 기능을 많이 설치하는 데서 끝나지 않고 **데이터가 유입·변환·검증·공개·조회되는 경로와 장애 시 복구 가능성**까지 확인하는 것이다.

- **배치 데이터:** Olist 9개 원본 CSV → HDFS Raw → Spark/Parquet → 29개 DQ → v3 승인 → Iceberg Silver/Gold → Gold WAP → Trino.
- **증분 데이터:** 별도 실습 테이블에서 Iceberg `MERGE`, Kafka 이벤트 소비, 시퀀스 기반 멱등 갱신과 HDFS 체크포인트 검증.
- **운영:** Prometheus/Grafana/Alertmanager, Web Console, OpenMetadata, Kafbat UI, systemd, YARN·Trino·Kafka CLI로 진단과 검증 분리.
- **자원 효율:** 상시 Spark 한 개가 YARN 자원을 점유하는 소형 클러스터이므로, 향후 CDC `produce`와 `verify`는 Spark 배치 대신 경량 Kafka 클라이언트·Trino/HDFS 검사로 분리한다.

## 2. 인프라·런타임 배치

| 노드 | 역할 | 실행 서비스 |
|---|---|---|
| `master` | 제어·Spark submit | NameNode, ResourceManager, Spark History Server, Spark 3.5.6/YARN cluster submit |
| `worker1` | 처리·메시징 | DataNode, NodeManager, Kafka Broker+Controller(node 1) |
| `worker2` | 처리·메시징 | DataNode, NodeManager, Kafka Broker+Controller(node 2), DB 원격 백업 후보(검증 필요) |
| `worker3` | 운영·카탈로그·메시징 | Ansible, Web Console, PostgreSQL, Prometheus/Grafana/Alertmanager, Gravitino REST `:9001`, Kafka Broker+Controller(node 3), Kafka 메트릭 수집기 |
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
```

## 3. 구현 범위와 실제 확인 상태

| 단계 | 구현 기능 | 2026-11-07 기준 |
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
| **65** | 상시 스트리밍(YARN), 전용 CDC 테이블·토픽·HDFS 체크포인트 | **진행 중**. Seed/Start 및 YARN `RUNNING` 관찰. `produce`·`verify` 별도 Spark 작업이 `ACCEPTED`로 대기해 전체 CDC/복구 검증 미완료 |

**실행 범위 주의:** 61~65는 별도 네임스페이스·토픽·실습 테이블에 적용된다. 59번 Gold 공개 테이블과 기존 승인 데이터는 변경 대상으로 삼지 않는다.

## 4. 접속 화면 및 역할

| 서비스 | 실습 접근 경로 | 용도 |
|---|---|---|
| Web Console | `http://worker3:1337` | 내부 서비스·운영 액션 |
| HDFS NameNode | `http://master:9870` | HDFS 상태 |
| YARN | `http://master:18088`(기존 콘솔 설정; 실제 리스너 확인) | 앱 `RUNNING`·`ACCEPTED`, Queue/AM 리소스 |
| Spark History | `http://master:18080` | 종료된 Spark 앱 실행 기록 |
| Prometheus / Grafana | worker3 `:9090` / `:3000`, Windows 호스트 포워딩 `http://127.0.0.1:23000` | 시계열·알림·Kafka/Gold 대시보드 |
| Alertmanager | **worker3 `:9093` 추정 — `ss`로 서비스·바로가기 검증 필요** | 경보 라우팅. Kafka Controller는 별도 `:19093` |
| OpenMetadata | metadata `:8585` | 데이터셋·Iceberg 계보·Kafka Messaging Service |
| Gravitino REST | worker3 `:9001/iceberg/` | 공유 Iceberg Catalog API |
| Trino | metadata `:8080` | SQL 검증 및 Iceberg 조회 |
| Kafbat UI | metadata `:18081` (설치 후 내부망 접근) | Kafka 토픽·메시지·Consumer Group; **Lag 자동 알림은 미구성** |

Kafbat UI는 운영 탐색용이고 Consumer Lag 시계열/임계치 알림은 아직 구현하지 않았다. Spark Structured Streaming의 소비 진도는 일반 Kafka Consumer Group Lag만으로 판단하지 않고 **Spark 체크포인트의 `offsets`·`commits` 및 YARN 로그**로 검증한다.

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

### 65번 실행·복구 제약 (다음 세션에서 이어갈 일)

```bash
# 현재 YARN에 실행/대기 중인 65번 작업을 먼저 조사 (master 또는 Hadoop CLI 노드)
yarn application -list -appStates RUNNING,ACCEPTED
yarn queue -status batch
# 앱 ID를 확인하고 해당 앱만 상태 조사; 무차별 kill 금지
yarn application -status <APPLICATION_ID>
```

- **실제 관찰:** `olist-iceberg-kafka65-continuous`가 `RUNNING`인 동안 별도 `olist-kafka65-produce`, `...-verify`가 Queue의 자원 부족으로 `ACCEPTED` 대기. 이는 `Kafka Topic` 부재 문제가 아니라 추가 Spark Driver/AM/Executor 자원 확보 실패다.
- **우선 해결 방향:** 65번 `produce/replay`는 worker3의 경량 Kafka Producer로, `verify`는 Trino + HDFS 커밋 완료 Offset 검사로 분리하고 **YARN Spark 작업은 상시 스트리밍 1개만 유지**한다. 아직 구현되지 않은 개선 계획이다.
- **임시 실습:** 실행 중이던 `produce`의 완료/중복 제출 여부를 먼저 확인하고, 필요한 경우 기존 `stop` 명령으로 **65번 앱만** 중지해 배치 처리를 끝낸 뒤 동일 HDFS 체크포인트로 재시작한다. `ACCEPTED` 앱을 중복 제출하지 않는다.
- **미검증:** 65번 최종 521행(유효 512/삭제 9), Offset 추적, 재시작·동일 이벤트 replay 후 Snapshot 불변성은 **기대 기준이지 완료 사실이 아니다.**

```bash
# 65번 전용 상시 앱만 정지; Kafka와 HDFS Checkpoint는 보존
.venv/bin/ansible-playbook -i inventory/hosts.ini \
  playbooks/65-kafka-streaming-operations.yml -e olist_kafka65_mode=stop

# 점검 후 재기동 — 먼저 중복 RUNNING 앱이 없는지 확인
.venv/bin/ansible-playbook -i inventory/hosts.ini \
  playbooks/65-kafka-streaming-operations.yml -e olist_kafka65_mode=start
```

자세한 순서 및 실패/복구 진단은 [65번 운영 문서](docs/65-kafka-streaming-apply.md), [프로젝트 가이드](docs/PROJECT_GUIDE.md)를 참고한다.

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
  olist_kafka65/                       CDC 상시 스트리밍과 YARN 제어
scripts/                              데이터 점검, Kafka 토픽 생성, 운영 CLI
  64a-smoke-kafka.sh                  Kafka Produce/Consume Smoke
  64b-kafka-platform.py               Kafka Quorum/ISR 수집 및 메타데이터 등록
  65-kafka-topic.sh                   65번 전용 토픽 관리
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
- `vendor/` JAR/JDK, `artifacts/`, 실행 로그/CSV/DB 백업, 운영 `inventory/hosts.ini`, 비밀번호/토큰/개인키/`.env`, 다운로드 ZIP은 Git 커밋에서 제외한다.
- **하루 작업 종료 시 한 번만 커밋:** 당일 생성·수정한 **Playbook + Role + Script + Test + Docs + README + PROJECT_GUIDE** 전체를 함께 스테이징하고, 변경 파일과 민감정보를 검토한 뒤 **하나의 Git 커밋**으로 묶는다. 문서만 따로 커밋하지 않는다.

## 8. 다음 단계

1. **65번 자원 효율화:** Kafka Producer/Replay를 Spark/YARN에서 worker3의 경량 클라이언트로 분리; Verifier를 Trino+HDFS Offset/Checkpoint 검사로 전환. 기존 `ACCEPTED` 앱 및 중복 이벤트 점검 후 적용.
2. **65번 복구 검증 완료:** 스트리밍 동작 중 이벤트 주입, 최종 521행·증빙, stop/start·체크포인트 재사용, replay 후 Snapshot 불변성 검증. 실습 시 불필요한 앱 동시 실행 방지.
3. **Kafka 운영 메트릭 보강:** Consumer Lag 수집과 증가 추세/Alertmanager 규칙, JMX Heap/GC·처리량(별도 단계). Kafbat UI의 조회 기능과 알림을 구분.
4. **내부망 운영 강화:** 실제 메모리/Queue 자원 계획, OpenMetadata·Gravitino 데이터 보존/복구, Kafka 접근제어, 다중 테이블 공개 정책, 미검증 초기 단계(38/43/44) 확인.

**문서 기준:** 2026-11-07. 마지막 확보한 60번 문서와 61~65번 Ansible 패키지, 사용자가 공유한 Kafka/KRaft Smoke·64-B Preflight·65번 YARN UI/실패 로그 및 단계별 완료 답변을 교차 반영했다. **worker3의 실제 Git HEAD/작업 트리를 직접 읽지는 못했으므로** ZIP 적용 전 기존 개인 수정사항과 파일 차이를 확인해야 한다.
