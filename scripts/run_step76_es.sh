#!/usr/bin/env bash
set -Eeuo pipefail
umask 077
cd "$(dirname "${BASH_SOURCE[0]}")/.."
mode="${1:-}"
case "$mode" in
  preflight|configure|snapshot|restore|verify) ;;
  *) echo "Usage: $0 preflight|configure|snapshot|restore|verify" >&2; exit 2 ;;
esac
id_file=artifacts/step76/latest_backup_id
[[ -r "$id_file" ]] || { echo "Missing $id_file" >&2; exit 2; }
backup_id="$(<"$id_file")"
[[ "$backup_id" =~ ^[0-9]{8}T[0-9]{6}Z-[a-f0-9]{8}$ ]] || { echo "Invalid backup ID" >&2; exit 2; }
allow_restart=false
allow_restore=false
if [[ "$mode" == configure ]]; then
  [[ "${STEP76_ALLOW_ES_RESTART:-}" == YES ]] || { echo "STEP76_ALLOW_ES_RESTART=YES is required" >&2; exit 2; }
  allow_restart=true
fi
if [[ "$mode" == restore ]]; then
  [[ "${STEP76_ALLOW_ES_RESTORE:-}" == YES ]] || { echo "STEP76_ALLOW_ES_RESTORE=YES is required" >&2; exit 2; }
  allow_restore=true
fi
.venv/bin/ansible-playbook -i inventory/hosts.ini \
  playbooks/76d-openmetadata-es-snapshot.yml \
  -e "olist_metadata76_es_mode=$mode" \
  -e "olist_metadata76_es_backup_id=$backup_id" \
  -e "olist_metadata76_es_allow_restart=$allow_restart" \
  -e "olist_metadata76_es_allow_restore=$allow_restore"
if [[ "$mode" == configure ]]; then
  python3 scripts/step76_d4_patch_template.py
fi
