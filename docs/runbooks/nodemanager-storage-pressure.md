# NodeManager Storage Pressure Runbook

YARN NodeManager local/log directory 사용률 증가 또는 디스크 부족 alert 발생 시 대응 절차입니다.

## 1. 대상 상황

```text
YARNNodeManagerStorageUsageHigh
YARNNodeManagerStorageUsageCritical
YARNNodeManagerStorageAvailableLow
YARNNodeManagerStoragePathMissing
Spark container launch failed
No space left on device
```

## 2. 현재 metric 확인

Prometheus query:

```bash
ansible ops -m shell -a "curl -s 'http://127.0.0.1:9090/api/v1/query?query=yarn_nodemanager_storage_used_percent' | python3 -m json.tool | head -80"
ansible ops -m shell -a "curl -s 'http://127.0.0.1:9090/api/v1/query?query=yarn_nodemanager_storage_available_bytes' | python3 -m json.tool | head -80"
```

worker 직접 확인:

```bash
ansible workers -b -m shell -a "df -h /data/hadoop/yarn/local /data/hadoop/yarn/logs"
ansible workers -b -m shell -a "du -sh /data/hadoop/yarn/local /data/hadoop/yarn/logs"
```

## 3. timer와 metric 파일 확인

```bash
ansible workers -b -m command -a "systemctl is-active yarn-nodemanager-storage-metrics.timer"
ansible workers -b -m shell -a "cat /var/lib/prometheus/node-exporter/yarn_nodemanager_storage.prom"
```

## 4. 어떤 경로가 문제인지 확인

```bash
ansible workers -b -m shell -a "find /data/hadoop/yarn/local -maxdepth 3 -type d | head -50"
ansible workers -b -m shell -a "find /data/hadoop/yarn/logs -maxdepth 3 -type d | head -50"
```

## 5. 실행 중인 application 확인

```bash
ansible master -b -m shell -a "sudo -u yarn yarn application -list"
```

실행 중인 application이 있으면 local-dir 정리는 신중하게 수행합니다.

## 6. 오래된 NodeManager local cache 확인

```bash
ansible workers -b -m shell -a "find /data/hadoop/yarn/local -mindepth 1 -maxdepth 3 -mtime +1 -print | head -100"
```

## 7. NodeManager log directory 확인

```bash
ansible workers -b -m shell -a "find /data/hadoop/yarn/logs -type f -mtime +1 -print | head -100"
```

YARN log aggregation이 정상이라면 완료된 application log는 HDFS `/tmp/logs`로 aggregation됩니다.

```bash
ansible master -b -m shell -a "sudo -u hdfs hdfs dfs -ls -R /tmp/logs | head -80"
```

## 8. 복구 방향

### 디렉터리 누락

```bash
ansible-playbook playbooks/26-yarn-nodemanager-storage.yml
ansible workers -b -m service -a "name=hadoop-yarn-nodemanager state=restarted enabled=true"
```

### metric 누락

```bash
ansible-playbook playbooks/27-yarn-nodemanager-storage-metrics.yml
```

### 사용률 증가

1. 실행 중인 application 확인
2. 완료된 application log aggregation 확인
3. 불필요한 오래된 local/log 파일 확인
4. 필요한 경우 NodeManager 재시작으로 local cache 정리 유도
5. 재발 시 local/log directory 용량 확장 또는 경로 추가 검토

## 9. 복구 후 검증

```bash
ansible-playbook playbooks/20-cluster-health-check.yml
ansible-playbook playbooks/22-cluster-smoke-test.yml
```

Prometheus alert 확인:

```bash
ansible ops -m shell -a "curl -s http://127.0.0.1:9090/api/v1/alerts | grep -E 'YARNNodeManagerStorage' || true"
```

## 10. 완료 기준

```text
NodeManager storage path exists = 1
storage used percent warning/critical threshold 이하
available bytes 기준 이상
health check failed=0
SparkPi smoke test 성공
```
