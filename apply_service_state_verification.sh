#!/usr/bin/env bash
set -Eeuo pipefail

# Verify service state after start/stop/restart; no cluster service is touched by the tests.
# Usage: ./apply_service_state_verification.sh [project_root]
PROJECT_DIR="${1:-$PWD}"
BACKEND="$PROJECT_DIR/roles/web_console/templates/app.py.j2"
FRONTEND="$PROJECT_DIR/roles/web_console/files/frontend"
MAIN="$FRONTEND/src/main.jsx"
HISTORY="$FRONTEND/src/ActionHistoryView.jsx"

for required in python3 npm make; do
  command -v "$required" >/dev/null || { echo "ERROR: missing command: $required" >&2; exit 1; }
done
for path in "$BACKEND" "$MAIN" "$HISTORY" "$FRONTEND/package.json" "$FRONTEND/index.html" "$PROJECT_DIR/Makefile"; do
  [[ -f "$path" ]] || { echo "ERROR: missing $path" >&2; exit 1; }
done

# Never attempt partial changes on an unexpected or already-modified target.
python3 - "$BACKEND" "$MAIN" "$HISTORY" <<'PY'
from pathlib import Path
import sys
backend, main, history = (Path(p).read_text(encoding='utf-8') for p in sys.argv[1:])
conditions = {
    'original lifecycle action handler': 'def run_component_lifecycle(component_id_value: str, action: str) -> Dict:' in backend,
    'old return-code-only success condition': '"ok": result.returncode == 0' in backend,
    'known audit function': 'def append_action_audit(record: Dict) -> None:' in backend,
    'no state-verification patch applied': 'def verify_lifecycle_state(' not in backend and 'verification_status' not in backend,
    'known frontend action views': 'async function runLifecycleAction(component, action)' in main and 'async function restart(component)' in main,
    'existing incident demo retained': 'INCIDENT_DEMO_ALLOWED' in main,
    'existing action history': 'function actionResult(entry)' in history,
    'existing runbooks': 'RunbooksView' in main and 'SelfCheckPanel' in main,
}
for description, passed in conditions.items():
    if not passed:
        raise SystemExit(f'ERROR: source mismatch: {description}. No files changed.')
print('PASS: expected source version and existing UI integrations verified')
PY

cd "$PROJECT_DIR"
STAGE="$(mktemp -d)"
trap 'rm -rf "$STAGE"' EXIT
mkdir -p "$STAGE/frontend"
cp "$BACKEND" "$STAGE/app.py.j2"
cp -a "$FRONTEND/src" "$FRONTEND/package.json" "$FRONTEND/index.html" "$STAGE/frontend/"
for config in "$FRONTEND"/vite.config.* "$FRONTEND"/postcss.config.*; do
  [[ ! -f "$config" ]] || cp -a "$config" "$STAGE/frontend/"
done
[[ ! -d "$FRONTEND/public" ]] || cp -a "$FRONTEND/public" "$STAGE/frontend/"
[[ ! -f "$FRONTEND/package-lock.json" ]] || cp -a "$FRONTEND/package-lock.json" "$STAGE/frontend/"

mkdir -p "$PROJECT_DIR/artifacts/backups"
BACKUP_DIR="$(mktemp -d "$PROJECT_DIR/artifacts/backups/web-console-state-verify-$(date +%Y%m%d-%H%M%S)-XXXXXX")"
cp -p "$BACKEND" "$BACKUP_DIR/app.py.j2"
cp -p "$MAIN" "$BACKUP_DIR/main.jsx"
cp -p "$HISTORY" "$BACKUP_DIR/ActionHistoryView.jsx"
echo "Source backup: $BACKUP_DIR"

echo '==> Patch only the staging copy'
python3 - "$STAGE/app.py.j2" "$STAGE/frontend/src/main.jsx" "$STAGE/frontend/src/ActionHistoryView.jsx" <<'PY'
from pathlib import Path
import sys
backend_file, main_file, history_file = map(Path, sys.argv[1:])
backend = backend_file.read_text(encoding='utf-8')
main = main_file.read_text(encoding='utf-8')
history = history_file.read_text(encoding='utf-8')

def once(text, original, replacement, label):
    n = text.count(original)
    if n != 1:
        raise SystemExit(f'ERROR: {label} expected once, found {n}. Source unchanged.')
    return text.replace(original, replacement, 1)

backend = once(backend, 'import subprocess\n', 'import subprocess\nimport time\n', 'time import')
start = backend.index('def run_component_lifecycle(component_id_value: str, action: str) -> Dict:\n')
end = backend.index('@app.post("/api/component/{component_id_value:path}/start")\n', start)
original_handler = backend[start:end]
if original_handler.count('append_action_audit({') != 2 or 'f"systemctl is-active {quoted_service}; "' not in original_handler:
    raise SystemExit('ERROR: lifecycle handler changed unexpectedly. Source unchanged.')

replacement_handler = '''def verify_lifecycle_state(host: str, service: str, expected_state: str) -> Dict:
    """Best-effort, bounded post-action systemd verification via Ansible."""
    attempts = 3
    interval_seconds = 2
    command = (
        f"systemctl show {shlex.quote(service)} "
        "--property=ActiveState --property=SubState "
        "--property=MainPID --property=LoadState --no-page"
    )

    last_state = "unknown"
    last_error = ""
    checked = False

    for attempt in range(1, attempts + 1):
        if attempt > 1:
            time.sleep(interval_seconds)

        try:
            result = ansible_shell(host, command, become=True, timeout=STATUS_TIMEOUT)
        except (subprocess.TimeoutExpired, OSError) as exc:
            last_error = compact_text(str(exc), 400)
            continue

        if result.returncode != 0 or "ActiveState=" not in (result.stdout or ""):
            last_error = compact_text((result.stderr or result.stdout or "systemctl show returned incomplete data"), 400)
            continue

        state = parse_systemd_output(result.stdout)
        last_state = state["active"]
        checked = True
        load_state = state["load"]
        if load_state != "loaded":
            last_error = f"Unexpected unit LoadState: {load_state}"
            continue

        if last_state == expected_state:
            return {
                "verification_ok": True,
                "verification_status": "verified",
                "observed_state": last_state,
                "verification_attempts": attempt,
                "verification_error": "",
            }

        last_error = f"Expected ActiveState={expected_state}, observed={last_state}"

    return {
        "verification_ok": False,
        "verification_status": "mismatch" if checked else "unavailable",
        "observed_state": last_state,
        "verification_attempts": attempts,
        "verification_error": last_error or "No verifiable systemd state received",
    }


def run_component_lifecycle(component_id_value: str, action: str) -> Dict:
    if action not in {"start", "stop", "restart"}:
        raise HTTPException(status_code=400, detail=f"Unsupported action: {action}")

    component = find_component(component_id_value)
    service = component["service"]
    host = component["host"]
    expected_state = "inactive" if action == "stop" else "active"

    base_record = {
        "action": action,
        "host": host,
        "service": service,
        "component_id": component_id(component),
        "component": component.get("component"),
        "group": component.get("group"),
    }

    if action == "stop" and service in STOP_PROTECTED_SERVICES:
        append_action_audit({
            **base_record,
            "blocked": True,
            "ok": False,
            "command_ok": False,
            "verification_ok": False,
            "verification_status": "not_run",
            "expected_state": expected_state,
            "observed_state": "unknown",
            "verification_attempts": 0,
            "returncode": None,
            "reason": "protected_stop",
            "stdout_summary": "",
            "stderr_summary": f"Stop is blocked for protected service: {service}",
        })
        raise HTTPException(status_code=400, detail=f"Stop is blocked for protected service: {service}")

    command = f"systemctl {action} {shlex.quote(service)}"
    try:
        result = ansible_shell(host, command, become=True, timeout=REQUEST_TIMEOUT)
        command_ok = result.returncode == 0
        returncode = result.returncode
        stdout = result.stdout or ""
        stderr = result.stderr or ""
    except (subprocess.TimeoutExpired, OSError) as exc:
        command_ok = False
        returncode = None
        stdout = ""
        stderr = compact_text(str(exc))

    verification = {
        "verification_ok": False,
        "verification_status": "not_run",
        "observed_state": "unknown",
        "verification_attempts": 0,
        "verification_error": "Command failed or timed out; state verification was not attempted",
    }
    if command_ok:
        verification = verify_lifecycle_state(host, service, expected_state)

    ok = command_ok and verification["verification_ok"]
    reason = "verified" if ok else (
        "command_failed" if not command_ok else verification["verification_status"]
    )
    audit = {
        **base_record,
        "blocked": False,
        "ok": ok,
        "command_ok": command_ok,
        "expected_state": expected_state,
        **verification,
        "returncode": returncode,
        "reason": reason,
        "stdout_summary": compact_text(stdout),
        "stderr_summary": compact_text(stderr),
    }
    append_action_audit(audit)

    return {
        "component": component,
        "action": action,
        "returncode": returncode,
        "stdout": stdout,
        "stderr": stderr,
        "ok": ok,
        "command_ok": command_ok,
        "expected_state": expected_state,
        **verification,
    }


'''
backend = backend[:start] + replacement_handler + backend[end:]

# Modify ONLY the lifecycle and restart action panels; leave Incident Summary untouched.
old_info = '''        `OK: ${result.ok}\\n` +
        `Return code: ${result.returncode}\\n\\n` +'''
new_info = '''        `Result: ${result.ok ? 'VERIFIED SUCCESS' : 'FAILED / UNVERIFIED'}\\n` +
        `Command succeeded: ${result.command_ok}\\n` +
        `Service state verified: ${result.verification_ok}\\n` +
        `Expected ActiveState: ${result.expected_state}\\n` +
        `Observed ActiveState: ${result.observed_state || 'unknown'}\\n` +
        `Verification: ${result.verification_status || 'not_run'}\\n` +
        `Verification attempts: ${result.verification_attempts ?? 0}\\n` +
        `Verification error: ${result.verification_error || '-'}\\n` +
        `Command return code: ${result.returncode}\\n\\n` +'''
for fname, ending in [
    ('runLifecycleAction', '  async function start(component)'),
    ('restart', '  async function incidentSummary()'),
]:
    boundary = '  async function runLifecycleAction(component, action)' if fname == 'runLifecycleAction' else '  async function restart(component)'
    start = main.index(boundary)
    stop = main.index(ending, start)
    snippet = main[start:stop]
    snippet = once(snippet, old_info, new_info, f'{fname} result display')
    main = main[:start] + snippet + main[stop:]

history = once(
    history,
    "              const details = [stderr && `[stderr]\\n${stderr}`, stdout && `[stdout]\\n${stdout}`].filter(Boolean).join('\\n\\n')",
    "              const verification = item.expected_state ? `State verification: ${item.verification_status || 'unknown'}\\nExpected: ${item.expected_state}\\nObserved: ${item.observed_state || 'unknown'}\\nAttempts: ${item.verification_attempts ?? 0}${item.verification_error ? `\\nError: ${item.verification_error}` : ''}` : ''\n              const details = [verification, stderr && `[stderr]\\n${stderr}`, stdout && `[stdout]\\n${stdout}`].filter(Boolean).join('\\n\\n')",
    'Action History verification details',
)

backend_file.write_text(backend, encoding='utf-8')
main_file.write_text(main, encoding='utf-8')
history_file.write_text(history, encoding='utf-8')
print('PASS: lifecycle state verification, Actions view, and Action History integrated')
PY

# Python AST-level unit tests: import only pure functions from the templated backend.
# Mock Ansible and time.sleep so tests NEVER execute systemctl or alter a real service.
echo '==> Run mocked lifecycle tests (NO service commands executed)'
python3 - "$STAGE/app.py.j2" <<'PY'
import ast
import subprocess
import shlex
import sys
from types import SimpleNamespace
from pathlib import Path
from unittest.mock import Mock

source = Path(sys.argv[1]).read_text(encoding='utf-8')
tree = ast.parse(source)
compile(source, sys.argv[1], 'exec')
functions = {n.name:n for n in tree.body if isinstance(n, ast.FunctionDef)}
needed = ('compact_text', 'component_id', 'parse_systemd_output', 'verify_lifecycle_state', 'run_component_lifecycle')
assert all(f in functions for f in needed), 'missing function after patch'
class HTTPException(Exception):
    def __init__(self, status_code, detail):
        self.status_code=status_code; self.detail=detail
component = {'host':'master', 'service':'spark-history-server', 'component':'History Server', 'group':'Spark'}
audit = []
ns = dict(
    Dict=dict, shlex=shlex, subprocess=subprocess,
    time=SimpleNamespace(sleep=Mock()),
    HTTPException=HTTPException,
    STATUS_TIMEOUT=8, REQUEST_TIMEOUT=60, STOP_PROTECTED_SERVICES={'nginx','bigtop-web-console'},
    find_component=lambda _:component, append_action_audit=audit.append,
)
module = ast.Module(body=[functions[n] for n in needed], type_ignores=[])
exec(compile(ast.fix_missing_locations(module), '<mocked-lifecycle-test>', 'exec'), ns)

def cproc(rc=0, state=None, load='loaded', stderr=''):
    out = (f'ActiveState={state}\nSubState=running\nMainPID=777\nLoadState={load}\n' if state else '')
    return subprocess.CompletedProcess('mock', rc, stdout=out, stderr=stderr)

def scenario(action, calls, exp_ok, exp_reason, exp_state):
    audit.clear()
    stub = Mock(side_effect=calls)
    ns['ansible_shell'] = stub
    output = ns['run_component_lifecycle']('master::spark-history-server', action)
    assert output['ok'] is exp_ok, (action, output)
    assert output['verification_status'] == exp_reason, (action, output)
    assert output['observed_state'] == exp_state, (action, output)
    assert audit[-1]['ok'] == exp_ok and audit[-1]['returncode'] == output['returncode']
    assert audit[-1]['verification_status'] == exp_reason
    return output

scenario('start', [cproc(), cproc(state='active')], True, 'verified', 'active')
scenario('stop', [cproc(), cproc(state='inactive')], True, 'verified', 'inactive')
scenario('restart', [cproc(), cproc(state='failed'), cproc(state='failed'), cproc(state='failed')], False, 'mismatch', 'failed')
scenario('start', [cproc(), cproc(state='activating'), cproc(state='active')], True, 'verified', 'active')
scenario('start', [cproc(rc=1)], False, 'not_run', 'unknown')
scenario('start', [cproc(), cproc(rc=2, stderr='host unreachable')]*1 + [cproc(rc=2, stderr='host unreachable')]*2, False, 'unavailable', 'unknown')
scenario('start', [subprocess.TimeoutExpired('ansible', 60)], False, 'not_run', 'unknown')
scenario('start', [cproc(), cproc(state='active', load='not-found'), cproc(state='active', load='not-found'), cproc(state='active', load='not-found')], False, 'mismatch', 'active')
component['service'] = 'nginx'
audit.clear()
try:
    ns['run_component_lifecycle']('worker3::nginx', 'stop')
    raise AssertionError('protected stop not blocked')
except HTTPException as exc:
    assert exc.status_code == 400 and audit[-1]['blocked']
print('PASS: start/stop/restart, transitioning states, failures, timeouts, missing units, protected stop')
PY

echo '==> Validate staged frontend build'
if [[ -x "$FRONTEND/node_modules/.bin/vite" ]]; then
  ln -s "$(readlink -f "$FRONTEND/node_modules")" "$STAGE/frontend/node_modules"
elif [[ -x /opt/bigtop-web-console/frontend-src/node_modules/.bin/vite ]]; then
  ln -s /opt/bigtop-web-console/frontend-src/node_modules "$STAGE/frontend/node_modules"
else
  (cd "$STAGE/frontend" && npm install --no-audit --no-fund --prefer-offline)
fi
(cd "$STAGE/frontend" && npm run build)
echo 'PASS: Vite build'

echo '==> Validate existing Ansible code'
make lint

echo '==> Copy tested sources into project'
cp "$STAGE/app.py.j2" "$BACKEND"
cp "$STAGE/frontend/src/main.jsx" "$MAIN"
cp "$STAGE/frontend/src/ActionHistoryView.jsx" "$HISTORY"

echo '==> Deploy Web Console only (does not start/stop Hadoop services)'
echo "NOTE: if deployment fails, modified sources remain for inspection. Restore from: $BACKUP_DIR"
make web-console

echo '==> Read-only health check'
python3 - <<'PY'
import json
from urllib.request import urlopen
with urlopen('http://127.0.0.1:18090/api/health', timeout=10) as r:
    data = json.load(r)
    assert r.status == 200 and data.get('status') == 'ok', data
print('PASS: Web Console health HTTP 200')
PY

echo "DONE: post-action systemd verification deployed. Backup: $BACKUP_DIR"
echo 'Verification was tested using mocks only; NO actual Hadoop services were restarted.'
