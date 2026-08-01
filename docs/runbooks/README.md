# Bigtop Cluster Runbooks

이 디렉터리는 Bigtop 기반 미니 데이터 플랫폼 운영 중 자주 발생할 수 있는 장애와 점검 절차를 정리한 runbook 모음입니다.

README.md는 프로젝트 전체 개요와 빠른 실행 흐름을 설명하고, 상세 장애 대응 절차는 이 디렉터리의 문서로 분리합니다.

## Runbook 목록

```text
cluster-reboot-recovery.md
- VM 재기동 이후 전체 클러스터 복구 및 검증 절차

yarn-spark-application-failure.md
- Spark on YARN application 실패 시 진단 절차

nodemanager-storage-pressure.md
- NodeManager local/log directory 디스크 사용률 증가 대응 절차

spark-history-and-logs.md
- Spark History Server, YARN log aggregation, event log retention 점검 절차

monitoring-alert-response.md
- Prometheus/Grafana/Alertmanager alert 발생 시 기본 대응 절차
```

## 기본 운영 순서

VM 재기동 후에는 다음 순서로 확인합니다.

```bash
cd ~/bigtop-cluster-ansible

ansible all -m ping
ansible-playbook playbooks/21-cluster-recover.yml
ansible-playbook playbooks/20-cluster-health-check.yml
ansible-playbook playbooks/22-cluster-smoke-test.yml
```

## 장애 대응 기본 원칙

```text
1. 서비스 상태 확인
2. 포트/JMX/metric 확인
3. health check playbook 실행
4. application id 또는 alert 이름 기준으로 상세 진단
5. 복구 후 smoke test 실행
6. 필요한 경우 README 또는 runbook 갱신
```
