# Step74 — Step66 Watchdog 안전한 자동 복구 실증

## 목표와 범위

기존 Step66 Systemd Watchdog를 재구현하지 않고, Step65 Spark/YARN 상시 스트리밍 앱 하나의 **실제 종료 후 자동 복구**를 검증한다.

- 실행 대상: master의 Spark/YARN 앱 `olist-iceberg-kafka65-continuous` (owner `hadoop`)
- 스크립트: `roles/olist_kafka74/files/step74_proof.py`
- 체크포인트: `hdfs://master:9000/data/olist/iceberg-checkpoints/step65/v1`
- 기존 Step66 Lock/Desired/Recovery 파일을 **읽기만** 하며, 장애 시험 증빙은 별도 `step74-proof.json` 파일로 생성한다.
- Kafka, Iceberg, HDFS 메타데이터, Step65 컨트롤러, Step66 Timer/Service 구성 변경 없음.
- 기존 5-VM 환경에서 YARN에 단 하나의 활성 앱만 존재할 때 장애 주입을 허용한다.
- **Step74 verify 통과는 YARN 자동 복구와 체크포인트 경로 보존의 증명이지, 데이터 무결성 전체 증명은 아니다.** Step65 verify를 따로 실행한다.

## 경계 조건

**기본 모드는 inspect이며, 읽기 전용이다.** `setup`은 독립 스크립트를 설치하고 inspect만 수행한다. Step66 enable, Step65 stop, YARN kill은 자동 수행하지 않는다.

실제 장애 실험은 정확한 기존 앱 ID와 `STEP74_ALLOW_FAULT=YES`를 명시적으로 전달해야 수행할 수 있다. Step66 Timer 활성, `desired=running`, 실행 중인 Step65 앱, 다른 활성 앱 없음, Checkpoint 디렉터리 존재, 최근 1시간의 시도 횟수 3회 미만 조건을 모두 충족해야 한다.

Step65 `mode=stop`은 `desired=stopped`를 기록하므로 장애 실험용으로 쓰면 안 된다. Step74 `fault`는 기존 Step65와 동일한 `control.lock`을 잡아 현재 ID 및 의도 상태를 다시 확인하고 **해당 YARN 앱 ID만** `yarn application -kill`로 종료한다. Step66이 120초 유예 후 새로운 앱을 시작하는지 관찰한다.

`step74-proof.json`이 이미 있으면 다시 장애를 주입할 수 없다. 이전 실험 성공/실패 증빙을 임의 삭제하거나 바꾸지 않는다. 장애 주입과 검증 중간에 문제가 생기면 재주입하지 말고 Step66 journal, YARN 상태, 증빙 파일을 분석한다.

## 실행

```bash
cd ~/bigtop-cluster-ansible
unzip -n step74_watchdog_recovery_proof.zip -d .
make validate

# Step66이 설치돼 있고 Step65가 RUNNING인지 확인. Step74 코드는 독립 설치.
bash scripts/run_step74_watchdog_proof.sh setup

# 읽기 전용 상태 재확인
bash scripts/run_step74_watchdog_proof.sh inspect
```

`ready_for_fault: false`이면 장애를 실행하지 않는다. Step66 Timer만 비활성화돼 있고 Step65가 RUNNING이라면, Step66을 활성화할 때만 아래 별도 명령을 명시적으로 실행한다.

```bash
.venv/bin/ansible-playbook -i inventory/hosts.ini \
  playbooks/66-kafka-streaming-watchdog.yml -e olist_kafka66_mode=enable
bash scripts/run_step74_watchdog_proof.sh inspect
```

`ready_for_fault: true`이고 운영자가 잠시 Step65 앱이 종료되는 테스트에 동의한 경우에만 **inspect 출력에 표시된 정확한 앱 ID**를 사용한다.

```bash
read -r -p 'inspect에서 확인한 app_id를 입력: ' STEP74_APP_ID
STEP74_ALLOW_FAULT=YES STEP74_APP_ID="$STEP74_APP_ID" \
  bash scripts/run_step74_watchdog_proof.sh fault

# 자동 복구를 기다려 최대 540초 범위에서 검증 (반복 fault 실행 금지)
bash scripts/run_step74_watchdog_proof.sh verify

# 복구된 앱의 실제 데이터 정합성은 기존 Step65에서 검증
.venv/bin/ansible-playbook -i inventory/hosts.ini \
  playbooks/65-kafka-streaming-operations.yml -e olist_kafka65_mode=verify
```

**주의**: 프롬프트에 현재 `inspect` 출력의 `app_id`를 정확히 입력한다. 이 시험은 실제 Step65 스트리밍을 잠시 종료한다.

Step74 검증 종료 후 자동 복구를 계속 유지하지 않으려면, 기존 Step66을 아래 명령으로 **의도적으로 해제**한다. 이 작업은 현재 Step65 앱을 종료하지 않는다.

```bash
.venv/bin/ansible-playbook -i inventory/hosts.ini \
  playbooks/66-kafka-streaming-watchdog.yml -e olist_kafka66_mode=disable
```

## 기대 결과

- `STEP74_INSPECT_PASS` — 읽기 전용 상태 점검
- `STEP74_FAULT_INJECTED: application_...` — 정확한 앱 하나만 종료 요청
- `STEP74_WATCHDOG_RECOVERY_PASS` — 새로운 앱 ID가 `RUNNING`이고 Step66 회복 시도 1회 증가
- `STEP74_SINGLE_ACTIVE_APP_PASS` — 활성 앱 하나만 존재
- `STEP74_CHECKPOINT_PATH_PRESERVED_PASS` — 기존 HDFS 체크포인트 경로 존재

단일 실험으로 **Step66 재시도 폭주 제한의 모든 경우나 Exactly-once 전체를 입증하지 않는다.** 기존 Step66의 단위 테스트가 수동 중지·ACCEPTED·3회 상한 등에 대한 모의 테스트를 제공한다.

## 장애 대응

1. `fault` 단계가 실패했다면 YARN 상태와 `/var/lib/bigtop-olist-step65/step74-proof.json`을 확인한다. 바로 다시 `fault`를 실행하지 않는다.
2. `verify` 실패 시 `ssh master 'sudo systemctl status bigtop-olist-step66-watchdog.timer --no-pager'`, `ssh master 'sudo journalctl -u bigtop-olist-step66-watchdog.service -n 100 --no-pager'`를 확인한다.
3. YARN 앱이 `ACCEPTED`라면 현재 VM 리소스 제약 때문일 수 있으므로 재기동 명령을 반복하지 않는다.
4. 자동 복구를 해제하려면 `olist_kafka66_mode=disable`을 실행한다. Step65 앱 복구가 아직 필요하면 기존 `mode=start`를 이용해 체크포인트를 유지한 채 운영자가 복구한다.

Step73은 API/Route 경로 점검만 확인했으므로 실제 Slack Firing/Resolved 수신을 별도로 확인해야 한다.
