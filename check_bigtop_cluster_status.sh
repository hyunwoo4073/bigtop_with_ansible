#!/usr/bin/env bash
set -uo pipefail

# Read-only checks against the cluster. Writes only a local diagnostics report.
PROJECT_ROOT="${PROJECT_ROOT:-$(pwd)}"
cd "$PROJECT_ROOT" || { echo "Cannot open project: $PROJECT_ROOT" >&2; exit 1; }

if [ -x .venv/bin/ansible ]; then
  ANSIBLE_BIN=.venv/bin/ansible
elif command -v ansible >/dev/null 2>&1; then
  ANSIBLE_BIN="$(command -v ansible)"
else
  echo "Ansible not found (.venv/bin/ansible or PATH). Run this from ~/bigtop-cluster-ansible." >&2
  exit 1
fi

if command -v timeout >/dev/null 2>&1; then
  TIMER=(timeout 55s)
else
  TIMER=()
fi

REPORT_DIR="$PROJECT_ROOT/artifacts/diagnostics"
mkdir -p "$REPORT_DIR"
REPORT="$REPORT_DIR/cluster-status-$(date +%Y%m%d-%H%M%S).log"
exec > >(tee "$REPORT") 2>&1

export ANSIBLE_NOCOLOR=1
export ANSIBLE_TIMEOUT=8
export ANSIBLE_HOST_KEY_CHECKING="${ANSIBLE_HOST_KEY_CHECKING:-True}"

PASS=0
FAIL=0

section() { printf '\n========== %s ==========\n' "$1"; }
run_local() {
  local label="$1"; shift
  printf '\n--- %s ---\n' "$label"
  "$@"
  local rc=$?
  if (( rc == 0 )); then printf '[PASS] %s\n' "$label"; PASS=$((PASS+1));
  else printf '[CHECK] %s (rc=%s)\n' "$label" "$rc"; FAIL=$((FAIL+1)); fi
}
run_remote() {
  local host="$1" label="$2" command="$3"
  printf '\n--- %s [%s] ---\n' "$label" "$host"
  "${TIMER[@]}" "$ANSIBLE_BIN" "$host" -T 8 -o -b -m shell -a "$command"
  local rc=$?
  if (( rc == 0 )); then printf '[PASS] %s\n' "$label"; PASS=$((PASS+1));
  else printf '[CHECK] %s (rc=%s)\n' "$label" "$rc"; FAIL=$((FAIL+1)); fi
}

section '0. Local environment & Git'
printf 'Project: %s\nReport: %s\n' "$PROJECT_ROOT" "$REPORT"
run_local 'Git branch' git branch --show-current
run_local 'Working tree changes' git status --short
run_local 'Last commit' git log -1 --oneline

section '1. VM / Ansible connectivity'
run_local 'Ansible ping all nodes' "${TIMER[@]}" "$ANSIBLE_BIN" all -T 8 -o -m ping

section '2. Hadoop / Spark systemd services'
run_remote master 'master: NameNode / ResourceManager / Spark History Server' \
  'failed=0; for svc in hadoop-hdfs-namenode hadoop-yarn-resourcemanager spark-history-server; do state=$(systemctl is-active "$svc" 2>/dev/null || true); printf "%s: %s\n" "$svc" "${state:-unknown}"; [ "$state" = active ] || failed=1; done; exit "$failed"'
run_remote workers 'workers: DataNode / NodeManager' \
  'failed=0; for svc in hadoop-hdfs-datanode hadoop-yarn-nodemanager; do state=$(systemctl is-active "$svc" 2>/dev/null || true); printf "%s: %s\n" "$svc" "${state:-unknown}"; [ "$state" = active ] || failed=1; done; exit "$failed"'

section '3. Hadoop / Spark HTTP endpoints'
run_remote master 'master: HDFS 9870 / YARN 8088 / Spark 18080' \
  'failed=0; for port in 9870 8088 18080; do code=$(curl -s -m 5 -o /dev/null -w "%{http_code}" "http://127.0.0.1:${port}/" || true); printf "port %s: HTTP %s\n" "$port" "${code:-unreachable}"; [ "$code" = 200 ] || [ "$code" = 302 ] || failed=1; done; exit "$failed"'

section '4. Monitoring / Web Console services'
run_remote ops 'worker3: monitoring and Web Console systemd' \
  'failed=0; for svc in prometheus grafana-server prometheus-alertmanager bigtop-web-console nginx; do state=$(systemctl is-active "$svc" 2>/dev/null || true); printf "%s: %s\n" "$svc" "${state:-unknown}"; [ "$state" = active ] || failed=1; done; exit "$failed"'
run_remote ops 'worker3: listening ports' \
  'ss -lnt | grep -E ":(9090|9093|3000|18090|1337)[[:space:]]" || true'

section '5. Monitoring / Web Console HTTP endpoints'
run_remote ops 'worker3: HTTP endpoints and Nginx authentication' \
  'failed=0; check() { label="$1"; url="$2"; expected="$3"; code=$(curl -s -m 6 -o /dev/null -w "%{http_code}" "$url" || true); printf "%s: HTTP %s (expected %s)\n" "$label" "${code:-unreachable}" "$expected"; [ "$code" = "$expected" ] || failed=1; }; check prometheus http://127.0.0.1:9090/-/healthy 200; check alertmanager http://127.0.0.1:9093/-/healthy 200; check grafana http://127.0.0.1:3000/api/health 200; check console-health http://127.0.0.1:18090/api/health 200; check console-self-check http://127.0.0.1:18090/api/self-check 200; check action-history http://127.0.0.1:18090/api/actions/history?limit=2 200; check runbooks http://127.0.0.1:18090/runbooks 200; check nginx-basic-auth http://127.0.0.1:1337/ 401; exit "$failed"'
run_remote ops 'worker3: Self-check warning details' \
  'curl -fsS --max-time 8 http://127.0.0.1:18090/api/self-check | python3 -c '\''import json,sys; d=json.load(sys.stdin); print("status:", d.get("status")); print("warnings:", d.get("warnings", []))'\'''

section '6. Completed (no changes or restart performed)'
printf 'Passed steps: %s\nNeeds checking: %s\n' "$PASS" "$FAIL"
printf 'Report: %s\n' "$REPORT"
printf 'Share the [CHECK] steps and their output (or the full report) for diagnosis.\n'

