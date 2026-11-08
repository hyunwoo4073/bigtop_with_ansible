# Step 65-B — Kafka CDC 운영 검증을 YARN 외부로 분리

## 현황 및 목적

- 기존 65번에서는 `olist-iceberg-kafka65-continuous`가 YARN에서 계속 실행되고, `olist-kafka65-produce` / `olist-kafka65-verify` / `olist-kafka65-replay`도 별도의 Spark-on-YARN 앱으로 제출되어 가용 메모리가 부족해 `ACCEPTED`로 대기함.
- **이번 수정은 위 세 배치 작업만 교체**한다. 상시 Spark 애플리케이션의 코드, HDFS 체크포인트, Kafka 토픽, Iceberg 64/65 테이블, 기존 증빙 파일은 초기화·재포맷하지 않는다.
- worker3 Kafka 4.2.2 CLI(Java 17)로 12개 이벤트 전송/재전송, master에서 Trino 484와 HDFS 체크포인트 읽기로 결과 검증. **추가 YARN Spark 앱 0개**. 최초 65번 `apply` 실행은 더 이상 필요 없다.
- 65번 기존 Seed 520행, 변경 후 521행(유효 512, tombstone 9)을 그대로 검증한다.

## 0. 기존 대기 Spark 작업 확인(중요)

master에서 실행:

```bash
yarn application -list -appStates RUNNING,ACCEPTED
```

기존 `olist-kafka65-produce`, `olist-kafka65-replay`, `olist-kafka65-verify`가 아직 `ACCEPTED` 또는 `RUNNING`이면 **해당 앱 ID와 사용자(hadoop)를 확인**하고, 오래된 배치 작업만 수동으로 종료한다. 스트리밍 `olist-iceberg-kafka65-continuous`는 종료하지 않는다.

```bash
# 예시: 실제 조회한 정확한 ID만 대입. 스트리밍 앱 ID는 절대로 중지하지 않는다.
yarn application -kill application_실제ID
```

새 `produce/replay/reconcile/verify` 모드는 실행 전 오래된 Spark 배치 이름과 상태를 검사해 실행을 거부한다. 자동으로 YARN 앱을 종료하지 않는다.

## 1. 패키지 적용 (worker3)

```bash
cd ~/bigtop-cluster-ansible
unzip -o 65b_kafka_yarnless_producer_verify_ansible.zip -d .
python3 -m unittest discover -s tests -p 'test_olist_kafka65*.py' -v
make lint

# 처음에는 계획 확인. 이미 Seed가 있으므로 apply는 재실행하지 않는다.
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/65-kafka-streaming-operations.yml
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/65-kafka-streaming-operations.yml \
  -e olist_kafka65_mode=upgrade
```

`upgrade`는 master와 worker3에 경량 Python 스크립트를 복사할 뿐 Spark를 제출하거나 Kafka/HDFS/Iceberg 상태를 변경하지 않는다.

## 2. 게시 상태 복구 또는 최초 전송

Kafka 토픽 `bigtop_olist_step65_cdc_v1`은 이미 만든 토픽을 그대로 쓴다.

```bash
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/65-kafka-streaming-operations.yml \
  -e olist_kafka65_mode=produce
```

- HDFS `_PRODUCE_REPORT.json`이 이미 존재하고 유효하면 **중복 전송 없이 재사용**한다.
- 토픽 Kafka offset이 **0**이고 기존 proof가 없으면 Java17 Kafka CLI로 12개 전송 후 HDFS에 immutable proof 저장.
- 이전 YARN producer가 Kafka 메시지 12개를 전송한 직후 증빙 저장 전에 중단되었다면, 기존 Kafka 0..11 메시지 값이 모두 12개 fixture와 정확히 일치하는지 비교하고 **재전송 없이 proof만 복원**한다. 이 작업은 `mode=reconcile`로도 명시적으로 실행 가능하다.
- Kafka 메시지가 1~11개만 있거나 다른 데이터가 존재하면 **자동 재발행하지 않고 중단**한다. 수동 원인 분석 후 결정한다.

## 3. 실시간 상태에서 추가 YARN 앱 없이 검증

```bash
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/65-kafka-streaming-operations.yml \
  -e olist_kafka65_mode=verify
python3 scripts/65-verify-trino.py --url http://192.158.2.104:8080
```

`verify`는 다음을 확인한다:

1. Trino 484에서 Step64 원본 520행과 고정된 Iceberg `main` snapshot/digest를 확인한다.
2. Step65 타깃이 521행 / live 512 / tombstones 9이고 예상 CDC 데이터 digest와 동일한지 검사한다.
3. Kafka 전용 토픽의 최신 Log End Offset과 **Spark checkpoint `commits/`에 대응하는 `offsets/`**를 비교한다. `offsets/`만 존재하는 미완료 배치는 통과하지 못한다.
4. 첫 성공시 HDFS `_APPLY_REPORT.json`을 한 번만 생성한다. 이후 재검증에서는 snapshot이 변경되면 실패한다.
5. 기존 Trino 검증용 `artifacts/65-kafka-cdc-{seed,produce,apply}.json`만 로컬 파일로 내보낸다.

스파크 마이크로배치가 아직 완료되지 않았다면 `verify`가 실패할 수 있다. **produce를 반복하지 말고**, YARN 스트리밍 상태를 보고 `verify`만 다시 실행한다.

## 4. 체크포인트 재기동 + 동일 이벤트 재전송

```bash
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/65-kafka-streaming-operations.yml -e olist_kafka65_mode=stop
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/65-kafka-streaming-operations.yml -e olist_kafka65_mode=start
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/65-kafka-streaming-operations.yml -e olist_kafka65_mode=replay
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/65-kafka-streaming-operations.yml -e olist_kafka65_mode=verify
python3 scripts/65-verify-trino.py --url http://192.158.2.104:8080
```

`replay`는 Kafka에 **새 Offset 12개**를 추가하지만, 기존 Iceberg event sequence가 더 크거나 같으므로 멱등 MERGE가 수행되지 않아 최종 Iceberg `main` Snapshot을 그대로 유지해야 한다. checkpoint가 새로운 Kafka 끝 Offset을 따라잡아야 검증 통과한다.

## 안전 범위와 주의점

- 64번/59번 테이블 변경, 토픽 삭제, Offset 초기화, HDFS Checkpoint 삭제, Broker 재시작, YARN 글로벌 Java 수정 없음.
- Kafka CLI는 **전용 Java17**을 사용하며 Apache Kafka 4.2의 `--reader-property`, `--command-property` 옵션을 사용한다.
- Kafka 이벤트와 HDFS 증빙은 단일 분산 트랜잭션이 아니다. 실패 후 이전 메시지 증빙이 누락되면 `produce/reconcile` 검증을 통해 복원; 부분 게시의 자동 복구는 지원하지 않는다.
- Verify가 Trino와 HDFS에서 읽는 데이터는 외부 변동이 없다는 실습 전제에 따른다. 운영 수준의 원자적 CDC / 완전한 Exactly Once 보장으로 해석하지 않는다.
- 자동 재시작·Alertmanager Consumer Lag 알림은 미구축. 이 작업의 범위가 아니다.
- 실제 worker3, master, Kafka 4.2.2, Trino 484 환경의 최종 실행 검증은 사용자가 Playbook을 실행한 후 판정한다.
