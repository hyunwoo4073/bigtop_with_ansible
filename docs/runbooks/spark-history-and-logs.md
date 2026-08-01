# Spark History and YARN Logs Runbook

Spark History Server, Spark event log, YARN log aggregation 상태를 점검하는 절차입니다.

## 1. 대상 상황

```text
Spark History Server UI에 application이 보이지 않음
YARN UI에서 logs unavailable 표시
yarn logs 명령으로 로그 조회 불가
Spark event log directory 사용량 증가
Spark History cleaner 동작 여부 확인 필요
```

## 2. Spark History Server 상태 확인

```bash
ansible master -b -m command -a "systemctl is-active spark-history-server"
ansible master -m shell -a "curl -fsS http://127.0.0.1:18080/api/v1/applications | head"
ansible master -m shell -a "curl -fsS http://127.0.0.1:19105/metrics | head"
```

## 3. Spark event log directory 확인

```bash
ansible master -b -m shell -a "sudo -u hdfs hdfs dfs -test -d hdfs:///spark-history"
ansible master -b -m shell -a "sudo -u hdfs hdfs dfs -du -h -s hdfs:///spark-history"
ansible master -b -m shell -a "sudo -u hdfs hdfs dfs -ls -t hdfs:///spark-history | head -20"
ansible master -b -m shell -a "sudo -u hdfs hdfs dfs -count hdfs:///spark-history"
```

## 4. Spark History cleaner 설정 확인

```bash
ansible master -m shell -a "grep -E 'spark.history.fs.cleaner|spark.eventLog' /etc/spark/conf/spark-defaults.conf"
```

기대 설정:

```text
spark.eventLog.enabled true
spark.eventLog.dir hdfs:///spark-history
spark.history.fs.cleaner.enabled true
spark.history.fs.cleaner.interval 1d
spark.history.fs.cleaner.maxAge 7d
```

## 5. maintenance playbook 실행

```bash
ansible-playbook playbooks/25-spark-history-maintenance.yml
```

## 6. YARN log aggregation 설정 확인

```bash
ansible master -m shell -a "grep -A2 'yarn.log-aggregation-enable' /etc/hadoop/conf/yarn-site.xml"
ansible master -b -m shell -a "sudo -u hdfs hdfs dfs -ls -d /tmp/logs"
```

기대 설정:

```text
yarn.log-aggregation-enable = true
yarn.nodemanager.remote-app-log-dir = /tmp/logs
```

## 7. 특정 application log 확인

```bash
ansible master -b -m shell -a "sudo -u yarn yarn application -status application_XXXXXXXXXXXX_XXXX | grep -E 'Application-Id|State|Final-State|Log Aggregation Status'"
ansible master -b -m shell -a "sudo -u hadoop yarn logs -applicationId application_XXXXXXXXXXXX_XXXX | head -100"
```

## 8. YARN UI2 logs unavailable 메시지 해석

YARN UI2에서 다음 메시지가 보일 수 있습니다.

```text
Logs are unavailable because Application Timeline Service seems unhealthy and could not connect to the JobHistory server.
```

이 메시지가 있어도 Spark on YARN 운영에서 반드시 ATSv2를 먼저 구성해야 하는 것은 아닙니다.

현재 프로젝트 기준 우선순위:

```text
1. YARN log aggregation
2. yarn logs -applicationId
3. Spark History Server
4. diagnostics playbook
5. 필요 시 ATS/JobHistory Server 검토
```

## 9. 복구 후 검증

```bash
ansible-playbook playbooks/20-cluster-health-check.yml
ansible-playbook playbooks/22-cluster-smoke-test.yml
```

## 10. 완료 기준

```text
Spark History Server active
/api/v1/applications 응답
hdfs:///spark-history 존재
Spark cleaner 설정 존재
/tmp/logs 존재
yarn logs -applicationId 조회 가능
```
