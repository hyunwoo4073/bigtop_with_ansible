#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
MODE="${1:-}"
PY=".venv/bin/ansible-playbook"
PLAY="playbooks/74-watchdog-recovery-proof.yml"
INV="inventory/hosts.ini"
case "$MODE" in
  setup)
    "$PY" -i "$INV" "$PLAY" -e olist_kafka74_mode=apply
    "$PY" -i "$INV" "$PLAY" -e olist_kafka74_mode=inspect
    ;;
  inspect)
    "$PY" -i "$INV" "$PLAY" -e olist_kafka74_mode=inspect
    ;;
  fault)
    if [[ "${STEP74_ALLOW_FAULT:-NO}" != YES || ! "${STEP74_APP_ID:-}" =~ ^application_[0-9]+_[0-9]+$ ]]; then
      echo 'Refused: STEP74_ALLOW_FAULT=YES and exact STEP74_APP_ID are required' >&2
      exit 2
    fi
    "$PY" -i "$INV" "$PLAY" -e olist_kafka74_mode=fault \
      -e "olist_kafka74_confirm_fault=YES" \
      -e "olist_kafka74_expected_app_id=$STEP74_APP_ID"
    ;;
  verify)
    "$PY" -i "$INV" "$PLAY" -e olist_kafka74_mode=verify
    ;;
  *)
    echo 'Usage: bash scripts/run_step74_watchdog_proof.sh {setup|inspect|fault|verify}' >&2
    exit 2
    ;;
esac
