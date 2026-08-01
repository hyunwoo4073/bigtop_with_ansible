# YARN and Spark Application Failure Runbook

Spark on YARN application 실패 시 application id 기준으로 진단 정보를 수집하고 원인을 분류하는 절차입니다.

## 1. 대상 상황

```text
- Spark job failed
- YARN application FAILED/KILLED
- executor container launch failed
- yarn logs 확인 필요
- Spark History Server에는 application이 보이지만 원인 파악이 어려움
```

## 2. 최근 application 확인

```bash
ansible master -b -m shell -a "sudo -u yarn yarn application -list -appStates ALL | head -30"
```

application id 예시:

```text
application_1784287310550_0001
```

## 3. application status 확인

```bash
ansible master -b -m shell -a "sudo -u yarn yarn application -status application_XXXXXXXXXXXX_XXXX"
```

확인할 항목:

```text
Application-Id
Application-Name
Application-Type
User
Queue
State
Final-State
Tracking-URL
Log Aggregation Status
```

## 4. diagnostics playbook 실행

```bash
ansible-playbook playbooks/23-yarn-app-diagnostics.yml -e yarn_app_id=application_XXXXXXXXXXXX_XXXX
```

수집 항목:

```text
YARN application status
ResourceManager application API JSON
ResourceManager scheduler snapshot
YARN aggregated logs
suspicious error lines
Spark-related log lines
YARN node list
HDFS report
Prometheus queue metrics
summary.txt
```

결과 경로:

```text
artifacts/yarn-app-diagnostics/<application_id>.tgz
```

## 5. 압축 해제 후 확인

```bash
mkdir -p /tmp/yarn-diagnostic-check

tar xzf ~/bigtop-cluster-ansible/artifacts/yarn-app-diagnostics/application_XXXXXXXXXXXX_XXXX.tgz \
  -C /tmp/yarn-diagnostic-check

find /tmp/yarn-diagnostic-check -type f
```

## 6. 주요 원인 분류

### Queue 리소스 부족

확인:

```bash
ansible ops -m shell -a "curl -s 'http://127.0.0.1:9090/api/v1/query?query=yarn_queue_appspending' | python3 -m json.tool | head -80"
ansible ops -m shell -a "curl -s 'http://127.0.0.1:9090/api/v1/query?query=yarn_queue_pendingmb' | python3 -m json.tool | head -80"
```

의심 증상:

```text
AppsPending 증가
PendingMB 증가
application이 ACCEPTED 상태에서 오래 대기
```

### NodeManager local/log directory 문제

확인:

```bash
ansible workers -b -m shell -a "df -h /data/hadoop/yarn/local /data/hadoop/yarn/logs"
ansible workers -m shell -a "curl -s http://127.0.0.1:9100/metrics | grep yarn_nodemanager_storage"
```

의심 증상:

```text
No space left on device
Container launch failed
Failed to localize resource
DiskErrorException
```

### Spark resource profile 문제

확인:

```bash
ansible hadoop_cluster -m shell -a "grep -E 'spark.yarn.queue|spark.driver.memory|spark.executor.memory|spark.executor.cores|spark.executor.instances' /etc/spark/conf/spark-defaults.conf"
ansible hadoop_cluster -m shell -a "which spark-submit-profile && spark-submit-profile || true"
```

### Log aggregation 미설정 또는 미완료

확인:

```bash
ansible master -b -m shell -a "sudo -u yarn yarn application -status application_XXXXXXXXXXXX_XXXX | grep -E 'Application-Id|State|Final-State|Log Aggregation Status'"
ansible master -b -m shell -a "sudo -u hdfs hdfs dfs -ls -R /tmp/logs | grep application_XXXXXXXXXXXX_XXXX | head"
ansible master -b -m shell -a "sudo -u hadoop yarn logs -applicationId application_XXXXXXXXXXXX_XXXX | head -100"
```

## 7. 복구 후 검증

```bash
ansible-playbook playbooks/20-cluster-health-check.yml
ansible-playbook playbooks/22-cluster-smoke-test.yml
```

## 8. 완료 기준

```text
application 원인 분류 완료
필요 로그 archive 확보
YARN/Spark 기본 smoke test 성공
같은 유형의 장애가 alert 또는 dashboard로 확인 가능
```
