# Web Console PostgreSQL — Ansible 반복 배포

기존 일회성 `setup_web_console_postgres.sh`는 실행하지 마세요. 이 패키지는 PostgreSQL 설치·계정·DB·DDL·systemd 환경변수·기존 로그 이관·정기 동기화를 Ansible Role/Playbook으로 관리합니다.

## 최초 소스 반영

worker3의 `~/bigtop-cluster-ansible`에서 아래 절차를 수행하세요.

```bash
cd ~/bigtop-cluster-ansible
unzip -n ~/web_console_postgres_ansible.zip -d .

# 아직 DB 관련 FastAPI 코드를 반영하지 않았다면:
git apply --check patches/web_console_pg_backend.patch
git apply patches/web_console_pg_backend.patch

# Python 문법, diff 검토
python3 -m py_compile roles/web_console/templates/app.py.j2
git diff -- roles/web_console/templates/app.py.j2
```

> **중요:** 실제 현재 `app.py.j2`가 위 패치와 다르면 `git apply --check`가 실패합니다. 이때 `git apply --reject`나 강제 덮어쓰기를 하지 말고 실제 소스와 패치를 대조해 수정하세요. 이전 일회성 코드 수정 스크립트로 이미 PostgreSQL 훅을 넣었다면 중복 패치하지 않습니다.

## 컨트롤 노드에 PostgreSQL 컬렉션 설치

```bash
.venv/bin/ansible-galaxy collection install -r collections/requirements-web-console-postgres.yml
```

이미 `make collections`를 사용하고 있다면 `community.postgresql`을 프로젝트의 `collections/requirements.yml`에도 추가해서 팀/신규 환경 설치 표준에 포함하세요.

## 반복 가능한 단일 배포 명령

worker3에서 실행 (지난번 worker3 → worker3 SSH 불안정 우회):

```bash
.venv/bin/ansible-playbook playbooks/31-web-console-postgres.yml -e ansible_connection=local
```

**2회 이상 반복해도 기존 DB, 계정, 비밀번호, 기록을 초기화하지 않습니다.** DB는 로컬 worker3에 설치되고 외부 포트 개방을 수행하지 않습니다.

## 검증

```bash
curl -fsS http://127.0.0.1:18090/api/storage/health | python3 -m json.tool
curl -fsS http://127.0.0.1:18090/api/actions/history | python3 -m json.tool
sudo systemctl status bigtop-web-console-history-sync.timer --no-pager
```

`/api/storage/health`에 `postgresql=connected`, `ok=true`가 나와야 합니다. PostgreSQL 장애 시 Web Console의 기존 `actions.log` 기록은 계속 유지하며, 복구 후 5분 주기 동기화 작업이 누락 이력을 DB로 옮깁니다.

## Git 커밋 대상

새로운 `roles/web_console_postgres/`, `playbooks/31-web-console-postgres.yml`, `collections/requirements-web-console-postgres.yml`, `docs/web_console_postgres.md`, 변경된 `roles/web_console/templates/app.py.j2`만 커밋하세요. `db.env`, `postgres.password` 등 실제 비밀번호는 서버 `/etc/bigtop-web-console/`에만 저장됩니다.

실제 PostgreSQL 설치/연결 테스트는 **worker3에서 실행해야** 검증할 수 있습니다. 파일 생성 단계에서는 YAML 구조·Python 구문·기준 소스 패치 적용만 검사했습니다.

## 핫픽스 (2026-10-25)

초기 배포에서 발생했던 Role 변수 접두어 `var-naming` 린트 오류와 `become_user: postgres` 임시 파일 ACL 오류를 수정했습니다. 기존 PostgreSQL DB/계정/비밀번호는 재생성하지 않습니다. 관리 대상 노드에 `acl`을 Ansible로 설치하며, PostgreSQL collection 모듈은 배포판 `/usr/bin/python3`를 사용합니다.
