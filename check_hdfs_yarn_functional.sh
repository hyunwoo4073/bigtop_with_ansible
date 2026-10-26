#!/usr/bin/env bash
# HDFS/YARN functional health checks for bigtop-cluster-ansible.
# Default: read-only against the cluster (writes a report locally).
# Optional --hdfs-io: creates, verifies, then deletes a small HDFS test file.
set -uo pipefail

usage() {
  cat <<'USAGE'
Usage: ./check_hdfs_yarn_functional.sh [--hdfs-io] [--help]

Default: read-only checks of HDFS and YARN through Ansible on master.
--hdfs-io  Additionally write/read/delete a small test file under HDFS /tmp.

Environment:
  EXPECTED_LIVE_DATANODES=2
  EXPECTED_ACTIVE_NODEMANAGERS=2
  PROJECT_ROOT=/path/to/bigtop-cluster-ansible

No Hadoop or Web Console services are started, stopped or restarted.
USAGE
}

HDFS_IO=0
while (( $# )); do
  case "$1" in
    --hdfs-io) HDFS_IO=1 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown option: $1" >&2; usage >&2; exit 2 ;;
  esac
  shift
done

PROJECT_ROOT="${PROJECT_ROOT:-$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)}"
cd -- "$PROJECT_ROOT" || exit 2
if [[ ! -d playbooks || ! -d inventory ]]; then
  echo "ERROR: Run this script from the bigtop-cluster-ansible project root (or set PROJECT_ROOT)." >&2
  exit 2
fi

EXPECTED_LIVE_DATANODES="${EXPECTED_LIVE_DATANODES:-2}"
EXPECTED_ACTIVE_NODEMANAGERS="${EXPECTED_ACTIVE_NODEMANAGERS:-2}"
for val in "$EXPECTED_LIVE_DATANODES" "$EXPECTED_ACTIVE_NODEMANAGERS"; do
  if ! [[ "$val" =~ ^[0-9]+$ ]] || (( 10#$val < 1 )); then
    echo "ERROR: Expected node counts must be positive integers." >&2
    exit 2
  fi
done
# Strip leading zeros before passing numeric expectations into remote shell.
EXPECTED_LIVE_DATANODES="$((10#$EXPECTED_LIVE_DATANODES))"
EXPECTED_ACTIVE_NODEMANAGERS="$((10#$EXPECTED_ACTIVE_NODEMANAGERS))"

if [[ -x .venv/bin/ansible ]]; then
  ANSIBLE_BIN=.venv/bin/ansible
elif command -v ansible >/dev/null 2>&1; then
  ANSIBLE_BIN="$(command -v ansible)"
else
  echo "ERROR: Ansible executable not found." >&2
  exit 2
fi
if ! command -v timeout >/dev/null 2>&1; then
  echo "ERROR: 'timeout' command is required." >&2
  exit 2
fi

REPORT_DIR="$PROJECT_ROOT/artifacts/diagnostics"
mkdir -p -- "$REPORT_DIR" || exit 2
REPORT="$REPORT_DIR/hdfs-yarn-functional-$(date +%Y%m%d-%H%M%S)-$$.log"
exec > >(tee "$REPORT") 2>&1
export ANSIBLE_NOCOLOR=1
export ANSIBLE_TIMEOUT=10

PASS=0
FAIL=0
section() { printf '\n========== %s ==========\n' "$1"; }
run_check() {
  local label="$1"; shift
  printf '\n--- %s ---\n' "$label"
  if timeout 100s "$@"; then
    printf '[PASS] %s\n' "$label"
    PASS=$((PASS + 1))
  else
    local rc=$?
    printf '[FAIL] %s (rc=%s)\n' "$label" "$rc"
    FAIL=$((FAIL + 1))
  fi
}
remote_check() {
  local label="$1" shell_cmd="$2"
  run_check "$label" "$ANSIBLE_BIN" master -T 10 -b -m shell -a "$shell_cmd"
}

printf 'Project: %s\n' "$PROJECT_ROOT"
printf 'Report: %s\n' "$REPORT"
printf 'Mode: %s\n' "$(if (( HDFS_IO )); then echo 'read-only + HDFS I/O smoke'; else echo 'read-only'; fi)"
printf 'Expected live DataNodes: %s\n' "$EXPECTED_LIVE_DATANODES"
printf 'Expected active NodeManagers: %s\n' "$EXPECTED_ACTIVE_NODEMANAGERS"

section '0. Inventory connectivity'
run_check 'Ansible ping: all VMs' "$ANSIBLE_BIN" all -T 10 -m ping

section '1. HDFS: NameNode and DataNodes'
remote_check 'HDFS RPC: list root directory' '
  set -eu
  sudo -u hdfs hdfs dfs -ls /
'
remote_check 'HDFS Safe Mode: must be OFF' '
  set -eu
  status=$(sudo -u hdfs hdfs dfsadmin -safemode get 2>&1)
  printf "%s\n" "$status"
  printf "%s\n" "$status" | grep -Eqi "Safe mode is OFF"
'
remote_check 'HDFS DataNode registrations and dead node count' "
  set -eu
  output=\$(sudo -u hdfs hdfs dfsadmin -report 2>&1) || { printf '%s\\n' \"\$output\"; exit 1; }
  live=\$(printf '%s\\n' \"\$output\" | sed -n 's/.*Live datanodes (\\([0-9][0-9]*\\)).*/\\1/p' | head -n 1)
  dead=\$(printf '%s\\n' \"\$output\" | sed -n 's/.*Dead datanodes (\\([0-9][0-9]*\\)).*/\\1/p' | head -n 1)
  dead=\${dead:-0}
  printf 'Live DataNodes: %s (expected >= %s)\\n' \"\${live:-unknown}\" $EXPECTED_LIVE_DATANODES
  printf 'Dead DataNodes: %s (expected 0)\\n' \"\${dead:-unknown}\"
  test -n \"\$live\" && test -n \"\$dead\"
  test \"\$live\" -ge $EXPECTED_LIVE_DATANODES && test \"\$dead\" -eq 0
"

section '2. YARN: ResourceManager and NodeManagers'
remote_check 'YARN CLI: RUNNING NodeManager count' "
  set -eu
  output=\$(sudo -u yarn yarn node -list -states RUNNING 2>&1) || { printf '%s\\n' \"\$output\"; exit 1; }
  count=\$(printf '%s\\n' \"\$output\" | sed -n 's/.*Total Nodes:[[:space:]]*\\([0-9][0-9]*\\).*/\\1/p' | head -n 1)
  printf 'RUNNING NodeManagers: %s (expected >= %s)\\n' \"\${count:-unknown}\" $EXPECTED_ACTIVE_NODEMANAGERS
  test -n \"\$count\" && test \"\$count\" -ge $EXPECTED_ACTIVE_NODEMANAGERS
"
remote_check 'YARN ResourceManager REST: active/lost/unhealthy nodes' "
  set -eu
  curl -fsS --max-time 15 http://master:8088/ws/v1/cluster/metrics | python3 -c '
import json, sys
expected = int(sys.argv[1])
metrics = json.load(sys.stdin)[\"clusterMetrics\"]
active = int(metrics[\"activeNodes\"])
lost = int(metrics[\"lostNodes\"])
unhealthy = int(metrics[\"unhealthyNodes\"])
print(f\"Active NodeManagers: {active} (expected >= {expected})\")
print(f\"Lost NodeManagers: {lost} (expected 0)\")
print(f\"Unhealthy NodeManagers: {unhealthy} (expected 0)\")
sys.exit(0 if active >= expected and lost == 0 and unhealthy == 0 else 1)
' $EXPECTED_ACTIVE_NODEMANAGERS
"

if (( HDFS_IO )); then
  section '3. Optional HDFS end-to-end I/O smoke'
  remote_check 'HDFS create/put/cat/compare/cleanup under /tmp' '
    set -eu
    base="/tmp/bigtop-hdfs-health-$(date +%Y%m%d%H%M%S)-$$"
    file="$base/payload.txt"
    payload="bigtop-hdfs-health-$base"
    cleanup() {
      if sudo -u hdfs hdfs dfs -test -e "$base"; then
        sudo -u hdfs hdfs dfs -rm -r -skipTrash "$base" >/dev/null || return 1
      fi
    }
    trap '\''if ! cleanup; then echo "ERROR: HDFS cleanup failed: $base" >&2; exit 1; fi'\'' EXIT
    sudo -u hdfs hdfs dfs -mkdir -p "$base"
    printf "%s\n" "$payload" | sudo -u hdfs hdfs dfs -put - "$file"
    actual=$(sudo -u hdfs hdfs dfs -cat "$file")
    test "$actual" = "$payload" || { echo "ERROR: HDFS payload mismatch" >&2; exit 1; }
    printf "HDFS write/read verified: %s\n" "$file"
    cleanup
    trap - EXIT
    echo "HDFS cleanup verified"
  '
else
  section '3. HDFS I/O smoke: skipped by default'
  printf 'Use --hdfs-io to test HDFS create/put/read/delete in a temporary directory.\n'
fi

section '4. Summary'
printf 'PASS: %s | FAIL: %s\n' "$PASS" "$FAIL"
printf 'Report: %s\n' "$REPORT"
if (( FAIL > 0 )); then
  printf 'Some checks failed. Review the [FAIL] steps; no services were restarted.\n' >&2
  exit 1
fi
printf 'Read-only checks passed. This is not a replacement for an actual YARN/Spark job smoke test.\n'
