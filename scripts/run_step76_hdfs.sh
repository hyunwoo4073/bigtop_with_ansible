#!/usr/bin/env bash
set -Eeuo pipefail
umask 077
cd "$(dirname "${BASH_SOURCE[0]}")/.."
MODE="${1:-}"
case "$MODE" in preflight|capture|copy|restore|verify) ;; *) echo "Usage: $0 preflight|capture|copy|restore|verify" >&2; exit 2 ;; esac
ID_FILE='artifacts/step76/latest_backup_id'
[[ -r "$ID_FILE" ]] || { echo "Missing $ID_FILE" >&2; exit 2; }
BACKUP_ID="$(<"$ID_FILE")"
[[ "$BACKUP_ID" =~ ^[0-9]{8}T[0-9]{6}Z-[a-f0-9]{8}$ ]] || { echo 'Invalid backup identity' >&2; exit 2; }
case "$MODE" in
  capture) [[ "${STEP76_ALLOW_HDFS_CAPTURE:-}" == YES ]] || { echo 'Set STEP76_ALLOW_HDFS_CAPTURE=YES' >&2; exit 2; } ;;
  copy) [[ "${STEP76_ALLOW_HDFS_COPY:-}" == YES ]] || { echo 'Set STEP76_ALLOW_HDFS_COPY=YES' >&2; exit 2; } ;;
  restore) [[ "${STEP76_ALLOW_HDFS_RESTORE:-}" == YES ]] || { echo 'Set STEP76_ALLOW_HDFS_RESTORE=YES' >&2; exit 2; } ;;
esac
printf 'Step76 D3: mode=%s backup=%s table=catalog_smoke source=master target=worker1\n' "$MODE" "$BACKUP_ID"
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/76d-hdfs-metadata-proof.yml \
  -e "olist_metadata76_hdfs_mode=$MODE" \
  -e "olist_metadata76_hdfs_backup_id=$BACKUP_ID"
