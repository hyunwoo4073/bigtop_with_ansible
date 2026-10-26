#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="${1:-$PWD}"
TARGET="${PROJECT_ROOT}/roles/web_console_postgres/tasks/integration.yml"

if [[ ! -f "$TARGET" ]]; then
  echo "ERROR: Missing $TARGET" >&2
  exit 1
fi

if grep -q 'Check whether the running Web Console loaded DB settings' "$TARGET"; then
  echo "SKIP: Database runtime reconciliation has already been applied."
  exit 0
fi

BACKUP_DIR="${PROJECT_ROOT}/artifacts/backups"
mkdir -p "$BACKUP_DIR"
BACKUP="${BACKUP_DIR}/web_console_postgres_integration_$(date +%Y%m%d_%H%M%S).yml"
cp -p -- "$TARGET" "$BACKUP"

if ! python3 - "$TARGET" <<'PY'
from pathlib import Path
import sys

path = Path(sys.argv[1])
src = path.read_text(encoding="utf-8")
anchor = "- name: Import historical JSONL records idempotently\n"
if src.count(anchor) != 1:
    raise SystemExit("ERROR: Expected a single backfill task anchor. Original file left unchanged.")

insert = r'''- name: Apply any pending Web Console DB configuration restart before testing runtime
  ansible.builtin.meta: flush_handlers

- name: Check whether the running Web Console loaded DB settings
  ansible.builtin.shell:
    cmd: |
      pid="$(systemctl show --property=MainPID --value {{ web_console_postgres_service | quote }})"
      if [ "$pid" -gt 0 ] 2>/dev/null && [ -r "/proc/$pid/environ" ] &&
         grep -azq '^WEB_CONSOLE_DB_DSN=' "/proc/$pid/environ"; then
        printf 'configured\n'
      else
        printf 'missing\n'
      fi
    executable: /bin/bash
  register: web_console_postgres_running_env
  changed_when: false
  no_log: true

- name: Recover runtime PostgreSQL settings after an earlier partial deployment
  ansible.builtin.systemd_service:
    name: "{{ web_console_postgres_service }}"
    daemon_reload: true
    state: restarted
  when: web_console_postgres_running_env.stdout != 'configured'

'''

path.write_text(src.replace(anchor, insert + anchor, 1), encoding="utf-8")
print("PASS: Added retry-safe runtime environment reconciliation to Ansible role")
PY
then
  cp -p -- "$BACKUP" "$TARGET"
  exit 1
fi

if ! (cd "$PROJECT_ROOT" && make lint); then
  echo "ERROR: Ansible lint failed; restoring the role file." >&2
  cp -p -- "$BACKUP" "$TARGET"
  exit 1
fi

echo "PASS: Ansible lint"
echo "Backup: $BACKUP"
echo "Next: .venv/bin/ansible-playbook playbooks/31-web-console-postgres.yml -e ansible_connection=local"
