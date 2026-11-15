#!/usr/bin/env bash
# Run exclusively on worker3. No production Spark/Kafka operations.
set -euo pipefail
cd "$(dirname "$0")/.."

MODE="${1:-inspect}"
PROM="${STEP73_PROMETHEUS_URL:-http://127.0.0.1:9090}"
AM="${STEP73_ALERTMANAGER_URL:-http://127.0.0.1:9093}"
EXPECT="${STEP73_EXPECT_RECEIVER:-}"
LABELS="${STEP73_PROBE_LABELS_JSON:-{\"severity\":\"warning\",\"component\":\"step73\",\"service\":\"kafka-streaming\"}}"
ANSIBLE=.venv/bin/ansible-playbook
PB=playbooks/73-alert-delivery.yml

run_playbook() {
  "$ANSIBLE" -i inventory/hosts.ini "$PB" \
    -e "olist_kafka73_mode=$1" \
    -e "olist_kafka73_prometheus_url=$PROM" \
    -e "olist_kafka73_alertmanager_url=$AM" "${@:2}"
}

cleanup() {
  if [[ "${NEED_CLEANUP:-0}" == 1 ]]; then
    echo 'Step73: removing dedicated synthetic alert file...'
    if ! run_playbook remove_probe; then
      echo 'CLEANUP_FAILED: run bash scripts/run_step73_delivery.sh cleanup immediately' >&2
      return 2
    fi
    NEED_CLEANUP=0
  fi
}

case "$MODE" in
  inspect)
    run_playbook preflight
    run_playbook inspect_route
    ;;
  cleanup)
    run_playbook remove_probe
    ;;
  test)
    if [[ "${STEP73_ENABLE_LIVE_ALERT:-}" != YES ]]; then
      echo 'Refusing test: set STEP73_ENABLE_LIVE_ALERT=YES to acknowledge a real test notification.' >&2
      exit 2
    fi
    # Validate labels before making any production-visible change.
    EXTRAVARS="$(python3 - "$LABELS" <<'PY'
import json, re, sys
labels = json.loads(sys.argv[1])
if not isinstance(labels, dict):
    raise SystemExit('Probe labels must be a JSON object')
if any(not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*', k) or k.startswith('__')
       or k in {'alertname', 'step73_probe', 'step73_probe_id'} or not isinstance(v, str)
       or len(v) > 120 for k, v in labels.items()):
    raise SystemExit('Invalid probe label; only safe string label values are allowed')
if labels.get('component') != 'step73':
    raise SystemExit('component must remain step73')
if len(labels) > 12:
    raise SystemExit('Too many labels')
print(json.dumps({'olist_kafka73_probe_labels': labels}, separators=(',', ':')))
PY
)"
    PID="p$(date +%s)"
    EXPIRY="$(( $(date +%s) + 600 ))"
    VARS="$(python3 - "$PID" "$EXPIRY" <<'PY'
import json,sys
print(json.dumps({'olist_kafka73_probe_id': sys.argv[1],
                  'olist_kafka73_probe_expires_at': int(sys.argv[2])}, separators=(',', ':')))
PY
)"
    run_playbook preflight
    run_playbook inspect_route
    NEED_CLEANUP=1
    trap cleanup EXIT
    # Start cleanup trap before any file can be created.
    run_playbook install_probe -e "$VARS" -e "$EXTRAVARS"
    python3 roles/olist_kafka73/files/step73_observe.py \
      --phase firing --prometheus "$PROM" --alertmanager "$AM" \
      --probe-id "$PID" --expect-receiver "$EXPECT" --timeout 240
    HOLD="${STEP73_HOLD_SECONDS:-75}"
    if ! [[ "$HOLD" =~ ^[0-9]+$ ]] || (( HOLD < 30 || HOLD > 180 )); then
      echo 'STEP73_HOLD_SECONDS must be between 30 and 180' >&2
      exit 2
    fi
    echo "Holding alert for ${HOLD}s to allow Alertmanager group_wait and delivery..."
    sleep "$HOLD"
    run_playbook resolve_probe
    python3 roles/olist_kafka73/files/step73_observe.py \
      --phase resolved --prometheus "$PROM" --alertmanager "$AM" \
      --probe-id "$PID" --timeout 240
    cleanup
    echo 'STEP73_ROUTE_AND_RESOLUTION_PASS'
    echo 'NOTE: Slack/operator delivery must be confirmed from destination or integration-specific evidence.'
    ;;
  *) echo 'Usage: run_step73_delivery.sh {inspect|test|cleanup}' >&2; exit 2 ;;
esac
