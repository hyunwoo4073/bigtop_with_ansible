# Step 65 — YARN 자원 경합 및 스트리밍 복구 실습 인계

> **작업 정리: 2026-11-07 · 최종 단계 미완료**  
> 관련 문서: [`65-kafka-streaming-apply.md`](65-kafka-streaming-apply.md), [`65-kafka-streaming-architecture.md`](65-kafka-streaming-architecture.md), [`PROJECT_GUIDE.md`](PROJECT_GUIDE.md)

## 결론

Kafka 4.2.2 KRaft 3노드, 64번 유한 Spark CDC(520행)는 검증됐다. 65번 전용 Kafka 토픽을 생성하고 `olist-iceberg-kafka65-continuous` 스트리밍 앱도 YARN `RUNNING`으로 확인했다. 그러나 65번의 별도 `produce` 및 `verify` Spark 작업이 **실제 YARN 자원 부족으로 `ACCEPTED`에 머물렀다**. 따라서 65번 처리 완료/장애 복구/멱등 재전송은 **아직 검증되지 않았다**.

## 현재 아키텍처 (변경 전)

| 프로세스 | 제출 방식 | YARN 추가 메모리/AM/Executor |
|---|---|---|
| `olist-iceberg-kafka65-continuous` | Spark `readStream`, `foreachBatch`, YARN cluster | 상시 점유 |
| `olist-kafka65-produce` | Spark batch `spark-submit` | 추가 컨테이너 필요; `ACCEPTED` 발생 |
| `olist-kafka65-verify` | Spark batch `spark-submit` | 추가 컨테이너 필요; `ACCEPTED` 발생 |
| `olist-kafka65-replay` | Spark batch `spark-submit` | 추가 컨테이너 필요; 같은 문제 잠재 |

각 Spark 앱은 Driver 768m / Executor 768m 외에도 컨테이너 오버헤드와 AM 자원을 필요로 한다. 기존 worker1/worker2의 YARN 자원은 Kafka Broker와 HDFS DataNode 등과 경쟁한다. 65번 현재 `run.yml`은 `produce/replay/verify`도 Spark job으로 제출한다.

## 관찰된 정확한 진행 상태

1. 65번 `apply`/Seed 이후 `start` 진행. 첫 시도 때 토픽 `bigtop_olist_step65_cdc_v1`이 없다는 사전 점검 실패가 발생했다.
2. 토픽 생성 후 YARN UI에서 `olist-iceberg-kafka65-continuous=RUNNING`, `olist-kafka65-produce=ACCEPTED`가 관찰됐다.
3. 사용자가 `verify` 역시 RUNNING 스트리밍 중 `ACCEPTED`라고 확인했고 자원 부족 원인도 확인했다.
4. **아직 제공되지 않은 것:** 65번 CDC 521행 결과, `_APPLY_REPORT.json`, Spark Kafka Offset 커밋 완료 증빙, 중지/재기동·재전송 후 Iceberg Snapshot 불변성 PASS.

## 다음 세션에 우선 수행할 일

1. **현재 YARN 앱 조사:** 별도 `produce`가 여전히 `ACCEPTED`인지, 종료/실패/완료됐는지와 Streaming 앱의 상태 확인. 완료 여부가 불명확하면 이벤트 재전송 금지.
2. **65번 클라이언트 역할 분리:** Kafka `produce`/`replay`는 worker3의 전용 Java17 Kafka CLI 또는 경량 Python 클라이언트가 토픽으로 12개 CDC 메시지를 전송하도록 전환. YARN을 사용하지 않음.
3. **65번 검증 분리:** Trino로 `shared.olist65_stream.fact_cdc_running_v1`에서 `total/live/tombstones`·최종 이벤트 시퀀스를 읽고, Kafka end offset와 Spark Checkpoint `commits`/`offsets`의 **커밋 완료 Offset**을 비교하도록 구현. 검증 자체가 Spark/YARN 앱을 추가 제출하지 않아야 함.
4. **자원 계획:** `batch` Queue의 AM 비율, 사용 가능한 메모리/vCore, NM별 할당량, Spark 앱 실제 컨테이너 오버헤드를 조사. `RUNNING`이라는 이유만으로 정상 처리 중이라고 판단하지 말고 스트리밍 처리량/진도 확인.
5. **복구 실습:** 65번 스트리밍만 중지 → 동일 Checkpoint 재기동 → 새 이벤트 소모 → 동일 이벤트 Replay → 최종 상태·Iceberg Snapshot 불변성 검증.

## 안전한 확인 명령

```bash
# master에서. ID는 현재 UI/CLI로 확인한 실제 값으로 대체
yarn application -list -appStates RUNNING,ACCEPTED
yarn queue -status batch
yarn node -list -showDetails
yarn application -status <APPLICATION_ID>

# worker3 (Ansible controller)에서
.venv/bin/ansible-playbook -i inventory/hosts.ini \
  playbooks/65-kafka-streaming-operations.yml -e olist_kafka65_mode=status

# 필요한 경우에만, 대상 애플리케이션 정확히 확인 후
.venv/bin/ansible-playbook -i inventory/hosts.ini \
  playbooks/65-kafka-streaming-operations.yml -e olist_kafka65_mode=stop
```

**금지:** 65번 이벤트를 무심코 반복 Produce, KRaft 재포맷/Cluster ID 재생성, Kafka 토픽 삭제, HDFS Checkpoint 제거, Iceberg Gold 테이블 수정, YARN의 타 앱 무차별 kill.

## 완료 판정

- 전용 Kafka topic: `bigtop_olist_step65_cdc_v1`, RF3/minISR2/ISR3.
- 65번 초기 Table: `shared.olist65_stream.fact_cdc_running_v1`, 520행.
- 12개의 CDC 메시지/4개 변경 키를 처리한 뒤 **521행 / live 512 / tombstones 9**. 이는 아직 기대값이다.
- Trino와 Iceberg 결과 동일성 및 Kafka End Offset 대비 체크포인트의 최신 Commit Offset이 뒤처지지 않음을 확인.
- 동일 Checkpoint 재기동 후 중복/오래된 이벤트 재전송에도 **결과와 최초 검증된 Snapshot이 그대로 유지**되는지 확인.

## Git 운영

2026-11-07에 수정·추가한 Kafka/Streaming Playbook, Role, Script, Tests, docs, README, PROJECT_GUIDE는 **전부 하나의 커밋**에 포함한다. `vendor/`, `artifacts/`, 토큰·키·로그·다운로드 ZIP은 제외한다. 이 문서는 완료 보고가 아니라 **미완료 사항과 다음 작업의 인계 기록**이다.
