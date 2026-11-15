#!/usr/bin/env bash
set -Eeuo pipefail
umask 077
cd "$(dirname "${BASH_SOURCE[0]}")/.."
ID_FILE='artifacts/step76/latest_backup_id'
[[ -f "$ID_FILE" ]] || { echo "Missing $ID_FILE: Phase B backup ID required" >&2; exit 2; }
ID="$(cat "$ID_FILE")"
[[ "$ID" =~ ^[0-9]{8}T[0-9]{6}Z-[a-f0-9]{8}$ ]] || { echo 'Malformed Step76 backup ID' >&2; exit 2; }
TARGET="${STEP76_DR_TARGET_HOST:-worker1}"
case "$TARGET" in worker1|worker2) ;; *) echo 'Only worker1 or worker2 may be a DR candidate' >&2; exit 2;; esac
printf 'Step76 Phase D read-only discovery: backup=%s target=%s\n' "$ID" "$TARGET"
.venv/bin/ansible-playbook -i inventory/hosts.ini \
  playbooks/76d-metadata-dr-preflight.yml \
  -e "olist_metadata76_dr_backup_id=$ID" \
  -e "olist_metadata76_dr_target_host=$TARGET"
