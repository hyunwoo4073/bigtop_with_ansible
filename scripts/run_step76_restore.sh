#!/usr/bin/env bash
set -Eeuo pipefail
umask 077
cd "$(dirname "${BASH_SOURCE[0]}")/.."
MODE="${1:-}"
COMPONENT="${2:-both}"
[[ "$MODE" == preflight || "$MODE" == restore || "$MODE" == verify ]] || {
  echo "Usage: $0 preflight|restore|verify [both|pg|mysql] [BACKUP_ID]" >&2
  exit 2
}
[[ "$COMPONENT" == both || "$COMPONENT" == pg || "$COMPONENT" == mysql ]] || {
  echo 'Invalid component (both|pg|mysql)' >&2; exit 2;
}
ID="${3:-}"
if [[ -z "$ID" ]]; then
  LATEST=artifacts/step76/latest_backup_id
  [[ -f "$LATEST" ]] || { echo "Missing $LATEST; provide backup ID" >&2; exit 2; }
  ID="$(cat "$LATEST")"
fi
[[ "$ID" =~ ^[0-9]{8}T[0-9]{6}Z-[a-f0-9]{8}$ ]] || {
  echo 'Invalid backup ID' >&2; exit 2;
}
if [[ "$MODE" == restore ]]; then
  [[ "${STEP76_ALLOW_SCRATCH_RESTORE:-}" == YES ]] || {
    echo 'Explicit consent required: STEP76_ALLOW_SCRATCH_RESTORE=YES' >&2; exit 2;
  }
fi
printf 'Step76 C: mode=%s component=%s backup_id=%s\n' "$MODE" "$COMPONENT" "$ID"
.venv/bin/ansible-playbook \
  -i inventory/hosts.ini \
  playbooks/76c-metadata-isolated-restore.yml \
  -e "olist_metadata76_restore_mode=$MODE" \
  -e "olist_metadata76_restore_component=$COMPONENT" \
  -e "olist_metadata76_restore_id=$ID"
echo "STEP76_ISOLATED_${MODE^^}_PASS: $COMPONENT $ID"
