# Web Console PostgreSQL 런타임 계정 자동 탐지 수정

이 패키지는 앞서 설치한 `web_console_postgres` Role의 일부만 교체합니다. PostgreSQL DB, 비밀번호, 이력 및 기존 FastAPI 패치는 변경하지 않습니다.

문제: `roles/web_console_postgres/tasks/integration.yml`이 `bigtop-web-console` 계정이 존재한다고 가정하고 `getent passwd`를 조회하여 실패했습니다.

수정: 이미 배포된 `bigtop-web-console` systemd 서비스의 `User`와 `Group` 속성을 읽고, 설정이 비어 있으면 root 사용자와 해당 기본 그룹을 사용합니다. 실제 시스템 계정이 없으면 검증 단계에서 중단합니다. 동기화 서비스 사용자 및 환경변수 파일 권한도 같은 계정을 따릅니다.

## 적용

```bash
cd ~/bigtop-cluster-ansible
unzip -o web_console_postgres_runtime_identity_fix.zip -d .
make lint && .venv/bin/ansible-playbook playbooks/31-web-console-postgres.yml -e ansible_connection=local
```

재실행 시 기존 비밀번호와 DB를 유지합니다. 원본 역할 백업은 `artifacts/backups/`에 보관하면 좋습니다. 이전에 적용한 PostgreSQL 백엔드 패치는 다시 실행하지 마세요.

## 확인

```bash
curl -fsS http://127.0.0.1:18090/api/storage/health | python3 -m json.tool
sudo systemctl status bigtop-web-console-history-sync.timer --no-pager
```
