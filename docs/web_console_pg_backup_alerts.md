# PostgreSQL 백업 장애 알림 (Ansible 36)

## 목적

34번 PostgreSQL 자동 백업, 35번 Node Exporter 기반 백업 메트릭에 대해 Prometheus Rule을 등록합니다.
기존 Alertmanager 라우팅과 수신 채널을 그대로 사용합니다. **새 Slack 설정을 생성하지 않습니다.**

## 적용

```bash
cd ~/bigtop-cluster-ansible
unzip -n web_console_pg_backup_alerts_ansible.zip -d .
make lint && .venv/bin/ansible-playbook playbooks/36-web-console-pg-backup-alerts.yml   -e ansible_connection=local
```

검증 전제: 35번 메트릭 파일이 생성돼 있어야 하고, `/etc/prometheus/prometheus.yml`의 `rule_files`가
`/etc/prometheus/rules/web-console-pg-backup.yml`을 포함해야 합니다. 일반적으로 `- /etc/prometheus/rules/*.yml`을 사용합니다.
다른 설정이면 36번은 **규칙을 배포하기 전 안전하게 중단**합니다. 기존 Prometheus 역할의 원본
`roles/prometheus/templates/prometheus.yml.j2`을 먼저 수정해 영속적으로 반영해야 합니다.

## 알림 종류

| Alert | 조건 | 대기 |
|---|---|---|
| BigtopPGBackupNoArchives | 백업 파일 0개 | 15m |
| BigtopPGBackupStale | 최신 백업이 26시간 초과 | 15m |
| BigtopPGBackupLatestArchiveInvalid | 최신 백업이 유효하지 않음 | 10m |
| BigtopPGBackupTimerInactive | 백업 systemd timer가 꺼짐 | 10m |
| BigtopPGBackupCollectorFailed | 수집기 실패 | 10m |
| BigtopPGBackupObserverStale | 수집 데이터가 30분 넘게 갱신되지 않음 | 10m |
| BigtopPGBackupMetricsMissing | Prometheus에서 핵심 메트릭 미수집 | 10m |

`for` 대기 후 Prometheus가 알림을 firing 상태로 평가합니다. 정상 상태의 알림은 inactive로 보입니다.
Prometheus 알림이 firing 상태가 되어야 Alertmanager에 전달되며, Slack 등 외부 채널은 기존 설정에 따라 달라집니다.

## 확인

```bash
sudo promtool check rules /etc/prometheus/rules/web-console-pg-backup.yml
sudo promtool check config /etc/prometheus/prometheus.yml
curl -fsS http://127.0.0.1:9090/api/v1/rules?type=alert | python3 -m json.tool
curl -fsS http://127.0.0.1:9090/api/v1/alertmanagers | python3 -m json.tool
curl -fsS http://127.0.0.1:9093/-/ready
```

**중요:** 이 역할은 DB, PostgreSQL 백업 파일, 기존 Alertmanager 설정을 수정하지 않습니다. 기존 Prometheus 역할을 다시 실행한 뒤 36번 역할을 재실행하면 규칙 로드를 다시 검증할 수 있습니다.
