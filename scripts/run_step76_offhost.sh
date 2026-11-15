#!/usr/bin/env bash
set -Eeuo pipefail
umask 077
cd "$(dirname "${BASH_SOURCE[0]}")/.."
MODE="${1:-}"
case "$MODE" in preflight|copy|verify) ;; *) echo "Usage: $0 preflight|copy|verify" >&2; exit 2;; esac
ID_FILE='artifacts/step76/latest_backup_id'
[[ -f "$ID_FILE" ]] || { echo "Missing $ID_FILE" >&2; exit 2; }
BACKUP_ID="$(cat "$ID_FILE")"
[[ "$BACKUP_ID" =~ ^[0-9]{8}T[0-9]{6}Z-[a-f0-9]{8}$ ]] || { echo 'Invalid Step76 backup ID' >&2; exit 2; }
TARGET="${STEP76_DR_TARGET_HOST:-worker1}"
case "$TARGET" in worker1|worker2) ;; *) echo 'Offhost must be worker1 or worker2' >&2; exit 2;; esac
if [[ "$MODE" == copy && "${STEP76_ALLOW_OFFHOST_COPY:-}" != YES ]]; then
  echo 'Use STEP76_ALLOW_OFFHOST_COPY=YES for the non-destructive offhost copy' >&2
  exit 2
fi
echo "Step76 D2: $MODE backup=$BACKUP_ID target=$TARGET"
.venv/bin/ansible-playbook -i inventory/hosts.ini \
  playbooks/76d-offhost-copy.yml \
  -e "olist_metadata76_offhost_mode=$MODE" \
  -e "olist_metadata76_offhost_backup_id=$BACKUP_ID" \
  -e "olist_metadata76_offhost_target_host=$TARGET"
