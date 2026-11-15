#!/usr/bin/env bash
set -Eeuo pipefail
umask 077
cd "$(dirname "${BASH_SOURCE[0]}")/.."

MODE="${1:-}"
[[ "$MODE" == backup || "$MODE" == verify ]] || {
  echo "Usage: $0 backup|verify [BACKUP_ID]" >&2
  exit 2
}

ARTIFACT_DIR="artifacts/step76"
mkdir -p "$ARTIFACT_DIR"
chmod 700 "$ARTIFACT_DIR"
LATEST="$ARTIFACT_DIR/latest_backup_id"

if [[ "$MODE" == backup ]]; then
  ID="$(python3 - <<'PY'
import datetime, secrets
print(datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ-') + secrets.token_hex(4))
PY
)"
  [[ -z "${2:-}" ]] || { echo 'backup mode does not accept a manually selected backup ID' >&2; exit 2; }
else
  if [[ -n "${2:-}" ]]; then
    ID="$2"
  else
    [[ -f "$LATEST" ]] || { echo "Missing $LATEST; provide an explicit BACKUP_ID" >&2; exit 2; }
    ID="$(cat "$LATEST")"
  fi
fi
[[ "$ID" =~ ^[0-9]{8}T[0-9]{6}Z-[a-f0-9]{8}$ ]] || { echo 'Invalid BACKUP_ID' >&2; exit 2; }

echo "Step76 phase B: ${MODE} ID=${ID} (root-only backups on worker3 and metadata)"
.venv/bin/ansible-playbook \
  -i inventory/hosts.ini \
  playbooks/76b-metadata-logical-backup.yml \
  -e "olist_metadata76_backup_mode=$MODE" \
  -e "olist_metadata76_backup_id=$ID"

if [[ "$MODE" == backup ]]; then
  printf '%s\n' "$ID" > "${LATEST}.new"
  chmod 600 "${LATEST}.new"
  mv "${LATEST}.new" "$LATEST"
  echo "STEP76_BACKUP_ID_SAVED: $LATEST"
  echo 'To verify immediately, run: bash scripts/run_step76_backup.sh verify'
else
  echo "STEP76_LOGICAL_VERIFY_PASS: $ID"
fi
