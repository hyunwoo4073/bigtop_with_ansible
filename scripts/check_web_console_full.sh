#!/usr/bin/env bash
set -euo pipefail

ANSIBLE_BIN="${ANSIBLE_BIN:-.venv/bin/ansible}"
OPS_GROUP="${OPS_GROUP:-ops}"
INTERNAL_BASE_URL="${INTERNAL_BASE_URL:-http://127.0.0.1:18090}"
PUBLIC_BASE_URL="${PUBLIC_BASE_URL:-http://127.0.0.1:1337}"
LOGROTATE_CONFIG="${LOGROTATE_CONFIG:-/etc/logrotate.d/bigtop-web-console-actions}"
AUDIT_LOG="${AUDIT_LOG:-/var/log/bigtop-web-console/actions.log}"

if [ ! -x "$ANSIBLE_BIN" ]; then
  echo "ERROR: ansible binary not found or not executable: $ANSIBLE_BIN" >&2
  exit 1
fi

section() {
  echo
  echo "================================================================================"
  echo "$1"
  echo "================================================================================"
}

remote_root() {
  "$ANSIBLE_BIN" "$OPS_GROUP" -b -m shell -a "$1"
}

remote_user() {
  "$ANSIBLE_BIN" "$OPS_GROUP" -m shell -a "$1"
}

check_http_code() {
  local name="$1"
  local url="$2"
  local expected="$3"

  local cmd
  cmd="code=\$(curl -s -o /dev/null -w '%{http_code}' '$url' || true); echo \"\$code\"; test \"\$code\" = '$expected'"

  echo "- $name: $url expected=$expected"
  remote_user "$cmd"
}

section "1. systemd service status"
remote_root "systemctl is-active bigtop-web-console && systemctl is-active nginx"

section "2. listening ports"
remote_root "ss -lntp | grep -E '1337|18090|uvicorn|nginx' || true"

section "3. HTTP endpoint status"
check_http_code "internal health" "$INTERNAL_BASE_URL/api/health" "200"
check_http_code "internal self-check" "$INTERNAL_BASE_URL/api/self-check" "200"
check_http_code "internal action history json" "$INTERNAL_BASE_URL/api/actions/history?limit=5" "200"
check_http_code "internal action history page" "$INTERNAL_BASE_URL/actions/history" "200"
check_http_code "public nginx basic auth" "$PUBLIC_BASE_URL" "401"

section "4. self-check payload"
remote_user "curl -s '$INTERNAL_BASE_URL/api/self-check' | python3 -m json.tool"

section "5. action history payload"
remote_user "curl -s '$INTERNAL_BASE_URL/api/actions/history?limit=5' | python3 -m json.tool"

section "6. audit log file"
remote_root "ls -ld \$(dirname '$AUDIT_LOG') && ls -lh '$AUDIT_LOG' || true"
remote_root "tail -n 20 '$AUDIT_LOG' || true"

section "7. logrotate config"
remote_root "test -f '$LOGROTATE_CONFIG' && cat '$LOGROTATE_CONFIG'"

section "8. logrotate dry-run"
remote_root "test -f '$LOGROTATE_CONFIG' && logrotate -d '$LOGROTATE_CONFIG'"

section "9. nginx config test"
remote_root "nginx -t"

section "10. recent Web Console journal"
remote_root "journalctl -u bigtop-web-console -n 80 --no-pager"

section "DONE"
echo "Web Console full check completed."
