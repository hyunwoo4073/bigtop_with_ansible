# Web Console PostgreSQL 백업 상태 모니터링 (Ansible 35)

## 구성

- **기존** 34번 Role: DB를 매일 백업하고 14일 보관. DB와 원본 백업 스크립트는 변경하지 않음
- **추가** 35번 Role: worker3에 있는 백업 파일을 읽어서 Node Exporter의 textfile collector에 메트릭 게시
- 10분마다 systemd timer로 최신 백업 검증 (`pg_restore --list`)
- 처음 배포한 직후 한 번 게시하며, 이후 변경이 없으면 반복 Playbook 실행에 불필요한 서비스 재시작 없음

## 배포

```bash
cd ~/bigtop-cluster-ansible
unzip -n web_console_pg_observability_ansible.zip -d .
make lint && .venv/bin/ansible-playbook playbooks/35-web-console-pg-backup-observability.yml -e ansible_connection=local
```

`roles/web_console_pg_backup/`과 Node Exporter textfile collector (기본 경로: `/var/lib/prometheus/node-exporter`) 가 있어야 함.

## 점검

```bash
systemctl is-active bigtop-web-console-pg-backup-observer.timer
sudo systemctl status bigtop-web-console-pg-backup-observer.service --no-pager
sudo cat /var/lib/prometheus/node-exporter/web_console_pg_backup.prom
curl -fsS http://127.0.0.1:9100/metrics | grep bigtop_web_console_pg_backup_
```

예시 PromQL:

```promql
bigtop_web_console_pg_backup_latest_archive_valid == 0
bigtop_web_console_pg_backup_timer_active == 0
time() - bigtop_web_console_pg_backup_last_valid_timestamp_seconds > 93600
absent(bigtop_web_console_pg_backup_collector_success)
```

메트릭 의미:

- `collector_success`: 감시 코드가 실행 성공했는지
- `last_check_timestamp_seconds`: 감시 코드 최종 실행 시각
- `archive_count`: 보관 중인 백업 수
- `latest_archive_size_bytes`: 최신 백업의 크기
- `latest_archive_timestamp_seconds`: 최신 백업 수정 시각
- `latest_archive_valid`: 최신 백업이 `pg_restore --list` 검증을 통과했는지
- `last_valid_timestamp_seconds`: 검증된 최신 백업 수정 시각 (아니면 0)
- `timer_active`: 백업 스케줄러 타이머 실행 여부

**주의:** `pg_restore --list`는 백업 아카이브 구조를 읽는 검사이지, **실제 복원 성공의 증거가 아님**. 34번 복원 검증 도구를 별도로 주기적으로 실행해야 함.

**주의:** 이번 Role은 메트릭만 게시함. Prometheus Alert Rule과 Alertmanager 통지는 구성하지 않음. 실제 알림 전송은 기존 Prometheus rule_file 경로 확인 후 추가해야 함.

**주의:** worker3 VM과 백업 파일이 같은 디스크에 있으면 VM 손상 시 동시 유실될 수 있음. 다음 단계에서 별도 노드/스토리지로 백업을 복제해야 함.
