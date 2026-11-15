#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
MODE="${1:-preflight}"
case "$MODE" in
  preflight|apply|verify) ;;
  *) echo 'Usage: bash scripts/run_step75.sh preflight|apply|verify' >&2; exit 2 ;;
esac
.venv/bin/ansible-playbook \
  -i inventory/hosts.ini \
  playbooks/75-kafka-operations-ui.yml \
  -e "web_console_kafka75_mode=${MODE}"
