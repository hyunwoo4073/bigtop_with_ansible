# 64번 적용 — 기존 Kafka 브로커를 통한 Iceberg CDC 증분 MERGE

> **선행 조건:** 63번 Seed/Apply/Trino PASS. **64-A Kafka KRaft 4.2.2 3노드 설치/Produce-Consume PASS** 및 64-B 모니터링 통합. 64-C Web UI는 선택 사항이다. Kafka 3개 브로커 주소는 defaults/main.yml에 고정했다. **64번은 CDC 파이프라인 실습이며 Gold 릴리스는 변경하지 않는다.**

## 1. worker3 — 압축 해제·검증

```bash
cd ~/bigtop-cluster-ansible
unzip -o 64_kafka_cdc_integrated_ansible.zip -d .
python3 -m unittest discover -s tests -p 'test_olist_iceberg_kafka64.py' -v
make lint
```

## 2. Plan / Kafka 환경 확인 (읽기 전용)

```bash
.venv/bin/ansible-playbook -i inventory/hosts.ini \
  playbooks/64-olist-kafka-iceberg-cdc.yml

# 실제 64-A Kafka 3개 브로커의 9092 포트를 master/worker1/worker2에서 검사
.venv/bin/ansible-playbook -i inventory/hosts.ini \
  playbooks/64-olist-kafka-iceberg-cdc.yml \
  -e olist_iceberg_kafka64_mode=preflight
```

**Preflight TCP PASS는 포트 연결 확인일 뿐 토픽과 Kafka 프로토콜 검증을 보장하지 않는다.** 아래 Kafka CLI로 별도 확인한다. Spark Driver/Executor가 쓰는 브로커 Metadata의 advertised.listeners도 실제 IP인지 확인한다.

## 3. Step64 전용 Kafka Topic 생성/검증 — worker3

```bash
# worker3에는 64-A Kafka CLI와 전용 Java 17이 설치되어 있다.
# 전용 토픽을 처음 한 번만 생성. 실행할 때마다 현재 RF/ISR/minISR 점검.
bash scripts/64-kafka-lab-topic.sh create
bash scripts/64-kafka-lab-topic.sh describe
```

- Topic: `bigtop_olist_step64_cdc_v1` (1 partition, RF=3, minISR=2).
- 기존 Kafka 내부 토픽/Consumer Group은 수정하지 않음.
- `retention.ms=604800000` (7일)로 이벤트가 실습 중 너무 빨리 만료되는 것을 방지. **7일이 지나면 Checkpoint를 재생성해도 메시지가 복구되지 않는다.**
- `STEP64_TOPIC_READY`가 출력되고 ISR 3개를 확인한 후에만 Produce 수행.

## 4. Spark Kafka dependency 다운로드(인터넷 가능 PC) → worker3에 배포

```bash
# worker3에서 인터넷 가능하면 직접 실행
python3 scripts/64-fetch-spark-kafka-jars.py
python3 scripts/64-verify-kafka-jars.py vendor/kafka64
# 폐쇄망이면 검증된 vendor/kafka64 파일을 반입; Git에 vendor/를 추가하지 않음
```

`--packages`는 사용하지 않음. 검증된 JAR를 master로만 복사하고, `spark-submit --jars`가 YARN Driver/Executor로 배포한다. **vendor/는 Git에 포함하지 않음.**

## 5. Step63 검증 테이블을 64번 전용 테이블로 복사

```bash
.venv/bin/ansible-playbook -i inventory/hosts.ini \
  playbooks/64-olist-kafka-iceberg-cdc.yml \
  -e olist_iceberg_kafka64_mode=seed
```

- 생성: `shared.olist64_kafka.fact_cdc_stream_v1`
- 기준: 516행 (512 live + 4 tombstones); 기존 Step63/Step59 테이블은 읽기만 함
- 증빙: `artifacts/64-kafka-cdc-seed.json`

## 6. 실제 Kafka에 48개 실습 이벤트 Produce

```bash
.venv/bin/ansible-playbook -i inventory/hosts.ini \
  playbooks/64-olist-kafka-iceberg-cdc.yml \
  -e olist_iceberg_kafka64_mode=produce
```

전송 이후 `_PRODUCE_REPORT.json`이 존재하면 기본 `produce` 재실행은 **전송하지 않고 검증 후 SKIP**한다. 임의로 다른 데이터나 일반 운영 토픽을 사용하지 않는다.

## 7. Structured Streaming AvailableNow → Iceberg MERGE

```bash
.venv/bin/ansible-playbook -i inventory/hosts.ini \
  playbooks/64-olist-kafka-iceberg-cdc.yml \
  -e olist_iceberg_kafka64_mode=ingest
```

HDFS Checkpoint: `/data/olist/iceberg-checkpoints/step64/v1`. **초기화 금지.** Kafka retention 이전으로 지워진 offset은 복구할 수 없을 수 있다.

## 8. Spark/HDFS 읽기 검증 및 Trino 교차 조회

```bash
.venv/bin/ansible-playbook -i inventory/hosts.ini \
  playbooks/64-olist-kafka-iceberg-cdc.yml \
  -e olist_iceberg_kafka64_mode=verify

python3 scripts/64-verify-trino.py \
  --url http://192.158.2.104:8080 \
  --seed-proof artifacts/64-kafka-cdc-seed.json \
  --produce-proof artifacts/64-kafka-cdc-produce.json \
  --ingest-proof artifacts/64-kafka-cdc-ingest.json
```

기대 출력:

```text
STEP64_TRINO_KAFKA_CDC_ROWS_PASS: total=520 live=512 tombstones=8
STEP64_SPARK_KAFKA_ICEBERG_TRINO_PASS
```

## 9. 선택: 동일 이벤트를 다시 Produce한 뒤 멱등성 확인

```bash
.venv/bin/ansible-playbook -i inventory/hosts.ini \
  playbooks/64-olist-kafka-iceberg-cdc.yml \
  -e olist_iceberg_kafka64_mode=replay

# 같은 체크포인트에서 새 offset들을 처리하되 Iceberg Snapshot이 변하면 실패
.venv/bin/ansible-playbook -i inventory/hosts.ini \
  playbooks/64-olist-kafka-iceberg-cdc.yml \
  -e olist_iceberg_kafka64_mode=ingest
```

## 10. Kafbat UI 및 Consumer Lag 이해

- Kafbat UI는 `bigtop_olist_step64_cdc_v1`의 메시지/파티션 점검에 사용 가능 (64-C 설치했다면 `http://192.158.2.104:18081`).
- **Spark Structured Streaming은 Kafka Consumer Group의 committed offset을 기반으로 진행률을 추적하는 것이 아니라 HDFS Checkpoint에 Kafka offset을 저장**한다. 따라서 Kafbat UI에서 "Spark Consumer Group Lag"이 일반 Kafka consumer처럼 정확하게 보이지 않을 수 있다. Iceberg CDC 진행률은 Spark Checkpoint, YARN 로그, `_INGEST_REPORT.json`로 확인한다.
- 이번 작업은 `AvailableNow` 유한 실행이다. 지속적인 Lag 알림/모니터링은 64-B의 기본 Broker 메트릭과 별도의 Streaming Progress Collector로 확장해야 한다.

## 11. YARN 로그 / 장애 분석

```bash
# master
sudo -u hadoop hdfs dfs -cat /data/olist/iceberg-validation/step64/v1/_INGEST_REPORT.json

yarn application -list -appStates ALL | grep olist-iceberg-kafka64
# yarn logs -applicationId application_<ID> | grep -E 'STEP64_|Traceback|Caused by:|Exception'
```

- Kafka 접속 불가: 64-A의 3개 Kafka systemd 및 64-B 모니터링부터 확인. Step64는 Broker를 재설치하지 않음.
- 토픽 없음: `bash scripts/64-kafka-lab-topic.sh create`로 전용 토픽만 생성; Kafka auto-create에 의존하지 않음.
- Kafka 연결 오류: Broker `advertised.listeners`가 Driver/Executor에서 해석·접근되는지 확인 (64-A IP:9092).
- Kafka 커넥터 클래스 없음: 오프라인 JAR 검증과 `--jars` 로그 확인.
- 이벤트 중복: MERGE에 `event_seq` 조건 적용, 재처리된 Kafka offset은 Checkpoint로 관리.
- Kafka 쓰기 성공 직후 증빙 저장 실패: **무작정 produce 반복 금지**. 실제 토픽 offset과 기존 증빙 비교 후 재개.
- 59번 Gold 공개 상태: 변경 금지. Kafka CDC를 Gold로 Publish하는 것은 아직 64번 범위 밖.
- **Git 정책**: 오늘 작업을 종료하며 README/PROJECT_GUIDE 업데이트 요청 시 해당 날짜의 모든 코드·문서를 함께 `git add`부터 일괄 커밋.
