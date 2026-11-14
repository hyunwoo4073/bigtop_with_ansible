#!/usr/bin/env bash
# Execute Step72 phases once after Step65 is manually stopped.
# Do not auto-restart Step65 while a Step72 Spark app may still be running.
set -Eeuo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_DIR"
PLAYBOOK="$PROJECT_DIR/playbooks/72-cdc-crash-recovery.yml"
ANSIBLE="$PROJECT_DIR/.venv/bin/ansible-playbook"
INV="$PROJECT_DIR/inventory/hosts.ini"

if [[ ! -x "$ANSIBLE" ]]; then
  echo "Step72: missing Ansible venv executable: $ANSIBLE" >&2
  exit 2
fi

$ANSIBLE -i "$INV" "$PLAYBOOK" --syntax-check

for MODE in preflight apply publish fault inspect_fault recover verify; do
  echo "===== STEP72 MODE: $MODE ====="
  if ! "$ANSIBLE" -i "$INV" "$PLAYBOOK" -e "olist_kafka72_mode=$MODE"; then
    echo "STEP72_STOPPED_AT: $MODE" >&2
    echo 'Inspect the last error and YARN/HDFS state. Do NOT rerun the full script.' >&2
    echo 'Restore Step65 only after checking no Step72 Spark app remains active.' >&2
    exit 1
  fi
  echo "STEP72_MODE_PASS: $MODE"
done

echo 'STEP72_ALL_PHASES_PASS: controlled failure and same-checkpoint recovery verified'
echo 'Now restore Step65 via original start, status, verify playbook modes.'
