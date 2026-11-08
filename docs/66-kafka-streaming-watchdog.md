# Step66 — Spark/YARN 상시 CDC watchdog (Opt-in)

## 기존 운영 상태와 목표

Step65의 Spark Structured Streaming은 master에서 YARN `cluster` 모드로 실행한다. Step65-B부터 `produce`, `replay`, `verify`는 경량 CLI/Trino/HDFS로 분리되어 추가 Spark 배치 앱을 제출하지 않는다. Step65-C에서는 기존 HDFS 체크포인트로 재기동하고 같은 이벤트를 다시 전송해도 최종 Iceberg Snapshot이 변경되지 않는 것을 확인했다.

**Step66은 65-C를 변경하지 않고, 정확히 `olist-iceberg-kafka65-continuous` + user `hadoop` 애플리케이션 하나만 주기적으로 감시하고 장애 복구를 수행한다.** 데이터/Checkpoint 재생성 없음. Broker, Kafka 토픽, 59/63/64/65번 Iceberg 테이블, Grafana 기존 대시보드, 기존 Java 설정 변경 없음.

## 자동 재기동 안전 장치

- `apply`는 watchdog와 비활성화된 timer만 설치. Spark/YARN 프로세스 자동 재기동 없음.
- `enable`을 명시해야 시작: Step65 앱이 **RUNNING**이어야만 감시 상태를 `running`으로 기록하고 timer 활성화.
- `65 ... mode=stop`을 호출하면 **YARN kill 전에** 원하는 상태를 `stopped`로 설정. timer가 돌아도 재시작하지 않음.
- `66 mode=disable`은 timer/자동 복구만 중지하고 **현재 스트리밍 앱은 살려둠**.
- `YARN` 조회 오류, 알 수 없는 출력, 동일 이름/사용자 활성 앱 중복은 **안전 중단**; 새 앱 제출 금지.
- `ACCEPTED`/`SUBMITTED`는 기존 앱으로 인정, 추가 제출 금지. YARN 자원 부족에서는 재기동 반복하지 않음.
- 앱이 목록에서 사라진 상태가 **최소 120초** 유지되어야 복구 시작.
- **최대 1시간에 3회** 자동 제출, 추가로 최소 120초 간격. 제출 전에 시도 기록을 남겨 프로세스 실패/재시작 반복 시에도 상한 유지.
- 기존 Step65 lifecycle controller의 파일 잠금(`control.lock`)을 자동 복구도 공유: `65 start/stop`과 경합 시 직렬화.
- 유한 스트리밍 `AvailableNow`로 변경하지 않음. Spark, Kafka Bootstrap, REST Catalog, HDFS Checkpoint 변경 없음.
- 이 watchdog는 **YARN 앱의 존재/상태만 검증**한다. `RUNNING`이어도 마이크로배치가 정체되었는지/Consumer Lag이 쌓이는지는 별도 지표가 필요하다.

## 파일/메트릭

master:

- `/opt/bigtop-iceberg/step65/step65_yarn_control.py` — Step65 시작/중지의 의도 상태 + lock 기반 복구
- `/opt/bigtop-iceberg/step65/step66_watchdog.py` — 시스템의 `runuser`로 hadoop 권한의 controller 실행 후 root가 textfile 메트릭만 기록
- `/var/lib/bigtop-olist-step65/desired66.json` — `running`/`stopped` 의도 상태
- `/var/lib/bigtop-olist-step65/recovery66.json` — 자동 시도 기록. **삭제하지 않음**
- `/etc/systemd/system/bigtop-olist-step66-watchdog.service`
- `/etc/systemd/system/bigtop-olist-step66-watchdog.timer`
- `/var/lib/prometheus/node-exporter/olist_step66_watchdog.prom`

worker3:

- `/var/lib/grafana/dashboards/olist-step66-streaming-watchdog.json`
- Grafana dashboard UID `bigtop-olist-step66`.

Node Exporter가 **master**의 textfile 디렉터리를 읽고, Prometheus가 **master** 노드를 scrape해야 Grafana에 데이터가 표시된다. `apply`만으로 Prometheus scrape target이나 Node Exporter 설정을 변경하지 않는다. 이 조건이 충족되지 않았다면 메트릭 파일은 생겨도 Grafana에 `No data`가 표시될 수 있다.

## worker3 실행 순서

```bash
cd ~/bigtop-cluster-ansible
unzip -o 66_kafka_streaming_watchdog_ansible.zip -d .
python3 -m unittest discover -s tests -p 'test_olist_kafka66.py' -v
python3 -m unittest discover -s tests -p 'test_olist_kafka65.py' -v
make lint

.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/66-kafka-streaming-watchdog.yml
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/66-kafka-streaming-watchdog.yml -e olist_kafka66_mode=preflight
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/66-kafka-streaming-watchdog.yml -e olist_kafka66_mode=apply

# 설치 후 먼저 수동 검사. 자동 복구는 여전히 OFF.
ssh master 'sudo systemctl start bigtop-olist-step66-watchdog.service'
ssh master 'sudo journalctl -u bigtop-olist-step66-watchdog.service -n 80 --no-pager'
ssh master 'cat /var/lib/prometheus/node-exporter/olist_step66_watchdog.prom'

# 65번 앱이 RUNNING임을 확인한 뒤, 별도 승인/명시적 실행으로 timer 활성화.
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/66-kafka-streaming-watchdog.yml -e olist_kafka66_mode=enable
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/66-kafka-streaming-watchdog.yml -e olist_kafka66_mode=status
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/66-kafka-streaming-watchdog.yml -e olist_kafka66_mode=verify
```

Grafana: `http://127.0.0.1:23000/d/bigtop-olist-step66/` (기존 Windows 포트 포워딩 조건).

Prometheus: `bigtop_olist_step66_watchdog_success`, `bigtop_olist_step66_running`, `bigtop_olist_step66_desired_running`, `bigtop_olist_step66_total_attempts`, `bigtop_olist_step66_attempts_window`, `bigtop_olist_step66_last_run_timestamp_seconds`.

Prometheus가 실제 scrape하는지 worker3에서 조회:

```bash
curl -fsSG http://127.0.0.1:9090/api/v1/query \
  --data-urlencode 'query=bigtop_olist_step66_watchdog_success' \
  | python3 -m json.tool
```

## 안전한 중지 및 실습 검증

의도적으로 스트리밍 중지 시 **기존 65번 Stop 명령**을 그대로 사용:

```bash
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/65-kafka-streaming-operations.yml -e olist_kafka65_mode=stop
```

`desired66.json`이 `stopped`가 된 뒤 YARN 앱이 중지되므로 자동 복구되지 않아야 한다. 재시작하려면 기존 65번 `start`를 호출한다. 단, Step66 `disable` 상태에서 65번을 시작한 경우 timer는 자동 활성화되지 않는다.

**자동 장애 복구 실험은 선택**이고, 기존 운영 중인 Spark 앱 종료가 수반되므로 의도하지 않은 테스트는 하지 말 것. 명시적으로 감시를 활성화하고, 현재 앱의 정확한 ID·사용자·이름을 확인한 뒤에만 제한된 테스트를 수행한다. 장애 유발 `kill -9` 같은 방식은 권장하지 않는다.

완전히 자동 복구를 끄려면:

```bash
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/66-kafka-streaming-watchdog.yml -e olist_kafka66_mode=disable
```

이 명령은 timer와 자동 재기동 의도를 끄지만 **실행 중인 스트리밍 앱은 중지하지 않는다**.

## 운영 한계

- master 프로세스/OS가 정지하면 해당 노드의 watchdog 역시 동작하지 않는다. HA 외부 컨트롤러나 관측 노드 분리가 다음 단계.
- master Node Exporter textfile collector 경로가 실제 활성화돼 있어야 Prometheus/Grafana에서 메트릭을 볼 수 있다.
- 비정상 종료 이후 앱이 `RUNNING`에 오래 남는 경우 별도 YARN/배치 활동 지표로 점검해야 한다.
- 이 코드는 **운영 환경에서 인증된 배포용 런북이 아니라 POC**로, 65번 실습 앱 하나에만 적용하고 점진적으로 검증한다.
