#!/usr/bin/env bash
set -Eeuo pipefail
cd "$(dirname "$0")/.."
[[ -f playbooks/76-metadata-backup-dr.yml ]] || { echo 'Run from installed Step76 package' >&2; exit 1; }
.venv/bin/ansible-playbook \
  -i inventory/hosts.ini \
  playbooks/76-metadata-backup-dr.yml \
  -e olist_metadata76_mode=preflight
