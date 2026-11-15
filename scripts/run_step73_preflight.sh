#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/73-alert-delivery.yml -e olist_kafka73_mode=preflight
