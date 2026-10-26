#!/usr/bin/env bash
set -Eeuo pipefail

# Add read-only HDFS/YARN JMX/REST health summary to Admin, without changing Hadoop services.
# Usage: ./apply_web_console_functional_health.sh [project_root]
PROJECT_DIR="${1:-$PWD}"
BACKEND="$PROJECT_DIR/roles/web_console/templates/app.py.j2"
FRONTEND="$PROJECT_DIR/roles/web_console/files/frontend"
MAIN="$FRONTEND/src/main.jsx"
HEALTH_COMPONENT="$FRONTEND/src/FunctionalHealthPanel.jsx"
HEALTH_CSS="$FRONTEND/src/functional-health.css"

for required in python3 npm make; do
  command -v "$required" >/dev/null || { echo "ERROR: missing $required" >&2; exit 1; }
done
for file in "$BACKEND" "$MAIN" "$FRONTEND/package.json" "$FRONTEND/index.html" "$PROJECT_DIR/Makefile"; do
  [[ -f "$file" ]] || { echo "ERROR: missing $file" >&2; exit 1; }
done

python3 - "$BACKEND" "$MAIN" "$HEALTH_COMPONENT" "$HEALTH_CSS" <<'PY'
from pathlib import Path
import sys
back, main, component, css = (Path(p) for p in sys.argv[1:])
a = back.read_text(encoding='utf-8')
b = main.read_text(encoding='utf-8')
checks = {
    'existing FastAPI route': '@app.get("/api/self-check")' in a,
    'lifecycle state verification retained': 'def verify_lifecycle_state(' in a,
    'React Admin panel anchor': b.count('      <SelfCheckPanel />') == 1,
    'React import anchor': b.count("import SelfCheckPanel from './SelfCheckPanel.jsx'") == 1,
    'incident demo retained': 'INCIDENT_DEMO_ALLOWED' in b,
    'not already applied': 'def build_functional_health(' not in a and 'FunctionalHealthPanel' not in b and not component.exists() and not css.exists(),
}
for label, ok in checks.items():
    if not ok:
        raise SystemExit(f'ERROR: {label} check failed. No source changed.')
print('PASS: current source markers and existing integrations verified')
PY

cd "$PROJECT_DIR"
mkdir -p "$PROJECT_DIR/artifacts/backups"
BACKUP_DIR="$(mktemp -d "$PROJECT_DIR/artifacts/backups/web-console-functional-health-$(date +%Y%m%d-%H%M%S)-XXXXXX")"
cp -p "$BACKEND" "$BACKUP_DIR/app.py.j2"
cp -p "$MAIN" "$BACKUP_DIR/main.jsx"
echo "Source backup: $BACKUP_DIR"

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

echo '==> Patch isolated staging source'
python3 - "$STAGE/app.py.j2" "$STAGE/frontend/src/main.jsx" <<'PY'
from pathlib import Path
import sys
backend_file, main_file = map(Path, sys.argv[1:])
backend = backend_file.read_text(encoding='utf-8')
main = main_file.read_text(encoding='utf-8')

def once(s, old, new, label):
    if s.count(old) != 1:
        raise SystemExit(f'ERROR: {label}: expected exactly one anchor, found {s.count(old)}. Nothing deployed.')
    return s.replace(old, new, 1)

backend = once(backend, 'import subprocess\n', 'import subprocess\nimport urllib.request\n', 'backend urllib import')

health_code = '''# Read-only overview only: this does NOT execute Hadoop CLI, write HDFS files,
# run YARN jobs, or change service states. Fixed internal endpoints prevent SSRF.
FUNC_EXPECTED_DATANODES = 2
FUNC_EXPECTED_NODEMANAGERS = 2


def _functional_json(url: str) -> Dict:
    request = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(request, timeout=5) as response:
        return json.loads(response.read(1024 * 1024).decode("utf-8"))


def _functional_count(source: Dict, name: str) -> int:
    if name not in source or isinstance(source[name], bool):
        raise ValueError(f"Metric missing or invalid: {name}")
    value = int(source[name])
    if value < 0:
        raise ValueError(f"Negative metric: {name}")
    return value


def _functional_hdfs() -> Dict:
    try:
        data = _functional_json(
            "http://master:9870/jmx?qry=Hadoop%3Aservice%3DNameNode%2Cname%3DFSNamesystemState"
        )
        bean = next(
            item for item in data.get("beans", [])
            if isinstance(item, dict) and "NumLiveDataNodes" in item
        )
        live = _functional_count(bean, "NumLiveDataNodes")
        dead = _functional_count(bean, "NumDeadDataNodes")
        status = "HEALTHY" if live >= FUNC_EXPECTED_DATANODES and dead == 0 else "DEGRADED"
        return {
            "status": status,
            "live_datanodes": live,
            "dead_datanodes": dead,
            "expected_live": FUNC_EXPECTED_DATANODES,
            "source": "NameNode JMX",
            "error": "",
        }
    except Exception as exc:
        return {"status": "UNKNOWN", "source": "NameNode JMX", "error": str(exc)[:240]}


def _functional_yarn() -> Dict:
    try:
        data = _functional_json("http://master:8088/ws/v1/cluster/metrics")
        metrics = data["clusterMetrics"]
        active = _functional_count(metrics, "activeNodes")
        lost = _functional_count(metrics, "lostNodes")
        unhealthy = _functional_count(metrics, "unhealthyNodes")
        status = (
            "HEALTHY" if active >= FUNC_EXPECTED_NODEMANAGERS and lost == 0 and unhealthy == 0
            else "DEGRADED"
        )
        return {
            "status": status,
            "active_nodemanagers": active,
            "lost_nodemanagers": lost,
            "unhealthy_nodemanagers": unhealthy,
            "expected_active": FUNC_EXPECTED_NODEMANAGERS,
            "source": "YARN ResourceManager REST",
            "error": "",
        }
    except Exception as exc:
        return {"status": "UNKNOWN", "source": "YARN ResourceManager REST", "error": str(exc)[:240]}


def build_functional_health() -> Dict:
    hdfs = _functional_hdfs()
    yarn = _functional_yarn()
    states = [hdfs["status"], yarn["status"]]
    overall = "UNKNOWN" if "UNKNOWN" in states else (
        "DEGRADED" if "DEGRADED" in states else "HEALTHY"
    )
    return {
        "checked_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "status": overall,
        "hdfs": hdfs,
        "yarn": yarn,
        "note": "JMX/REST node registration metrics only; not HDFS I/O or YARN job smoke testing",
    }


@app.get("/api/functional-health")
def api_functional_health():
    return build_functional_health()


'''
backend = once(backend, '@app.get("/api/self-check")\n', health_code + '@app.get("/api/self-check")\n', 'backend functional health API insertion')

main = once(main,
    "import SelfCheckPanel from './SelfCheckPanel.jsx'\n",
    "import SelfCheckPanel from './SelfCheckPanel.jsx'\nimport FunctionalHealthPanel from './FunctionalHealthPanel.jsx'\n",
    'React import')
main = once(main,
    '      <SelfCheckPanel />',
    '      <SelfCheckPanel />\n      <FunctionalHealthPanel />',
    'React Admin integration')

backend_file.write_text(backend, encoding='utf-8')
main_file.write_text(main, encoding='utf-8')
print('PASS: FastAPI endpoint and React Admin integration staged')
PY

cat > "$STAGE/frontend/src/FunctionalHealthPanel.jsx" <<'JSX'
import React, { useEffect, useState } from 'react'
import './functional-health.css'

function Metric({ label, value }) {
  return (
    <div className="functional-health-metric">
      <span>{label}</span>
      <strong>{value ?? '—'}</strong>
    </div>
  )
}

function HealthCard({ title, data, children }) {
  const state = data?.status || 'UNKNOWN'
  return (
    <div className="functional-health-card">
      <div className="functional-health-card-header">
        <strong>{title}</strong>
        <span className={`functional-health-badge ${state.toLowerCase()}`}>{state}</span>
      </div>
      <div className="functional-health-source">{data?.source || '—'}</div>
      {data?.error ? (
        <p className="functional-health-error">조회 실패: {data.error}</p>
      ) : (
        <div className="functional-health-metrics">{children}</div>
      )}
    </div>
  )
}

export default function FunctionalHealthPanel() {
  const [data, setData] = useState(null)
  const [error, setError] = useState('')
  const [loading, setLoading] = useState(true)
  const [reloadKey, setReloadKey] = useState(0)

  useEffect(() => {
    let active = true
    let controller = null
    const load = async () => {
      if (controller) controller.abort()
      controller = new AbortController()
      const current = controller
      setLoading(true)
      try {
        const response = await fetch('/api/functional-health', {
          cache: 'no-store', signal: current.signal
        })
        if (!response.ok) throw new Error(`HTTP ${response.status}`)
        const result = await response.json()
        if (!result || !result.hdfs || !result.yarn || !result.checked_at) {
          throw new Error('잘못된 Functional Health 응답')
        }
        if (active && !current.signal.aborted) {
          setData(result)
          setError('')
        }
      } catch (err) {
        if (active && !current.signal.aborted) {
          setError(err instanceof Error ? err.message : String(err))
          setData(null)
        }
      } finally {
        if (active && !current.signal.aborted) setLoading(false)
      }
    }
    load()
    const interval = window.setInterval(load, 60000)
    return () => {
      active = false
      window.clearInterval(interval)
      if (controller) controller.abort()
    }
  }, [reloadKey])

  const overall = error ? 'UNKNOWN' : data?.status || 'UNKNOWN'
  return (
    <section className="functional-health-panel" aria-label="HDFS and YARN functional health">
      <div className="functional-health-heading">
        <div>
          <h3>HDFS / YARN Health</h3>
          <p>노드 등록 상태를 JMX / REST로 조회합니다. 실제 HDFS I/O 및 YARN 작업 검증과는 별개입니다.</p>
        </div>
        <div className="functional-health-actions">
          <span className={`functional-health-badge ${overall.toLowerCase()}`}>{overall}</span>
          <button type="button" className="btn tiny" onClick={() => setReloadKey((n) => n + 1)} disabled={loading}>Refresh</button>
        </div>
      </div>
      <div className="functional-health-meta">{loading ? 'Checking...' : `Checked: ${data?.checked_at || '—'}`} · 자동 갱신: 60초</div>
      {error && <p className="functional-health-error" role="alert">API 오류: {error}</p>}
      <div className="functional-health-cards">
        <HealthCard title="HDFS" data={data?.hdfs}>
          <Metric label="Live DataNodes" value={data?.hdfs?.live_datanodes} />
          <Metric label="Dead DataNodes" value={data?.hdfs?.dead_datanodes} />
          <Metric label="Minimum Live" value={data?.hdfs?.expected_live} />
        </HealthCard>
        <HealthCard title="YARN" data={data?.yarn}>
          <Metric label="Active NodeManagers" value={data?.yarn?.active_nodemanagers} />
          <Metric label="Lost NodeManagers" value={data?.yarn?.lost_nodemanagers} />
          <Metric label="Unhealthy NodeManagers" value={data?.yarn?.unhealthy_nodemanagers} />
        </HealthCard>
      </div>
      <p className="functional-health-note">HDFS Safe Mode·RPC·파일 읽기/쓰기는 기존 check_hdfs_yarn_functional.sh에서 확인하세요.</p>
    </section>
  )
}
JSX

cat > "$STAGE/frontend/src/functional-health.css" <<'CSS'
.functional-health-panel { margin: 20px 0; padding: 18px; border: 1px solid var(--border); border-radius: 12px; background: var(--surface-muted); min-width: 0; }
.functional-health-heading { display: flex; justify-content: space-between; align-items: flex-start; gap: 12px; flex-wrap: wrap; }
.functional-health-heading h3 { margin: 0; font-size: 16px; }
.functional-health-heading p { margin: 6px 0 0; color: var(--muted); font-size: 12px; }
.functional-health-actions { display: flex; gap: 8px; align-items: center; }
.functional-health-badge { padding: 5px 9px; border-radius: 999px; font-size: 11px; font-weight: 800; background: #e2e8f0; color: #475569; }
.functional-health-badge.healthy { background: #dcfce7; color: #166534; }
.functional-health-badge.degraded { background: #fef3c7; color: #92400e; }
.functional-health-badge.unknown { background: #fee2e2; color: #991b1b; }
.functional-health-meta { margin: 12px 0; color: var(--muted); font-size: 12px; }
.functional-health-cards { display: grid; grid-template-columns: repeat(2, minmax(0,1fr)); gap: 12px; }
.functional-health-card { padding: 14px; border: 1px solid var(--border); border-radius: 10px; background: var(--surface); min-width: 0; }
.functional-health-card-header { display: flex; justify-content: space-between; align-items: center; gap: 8px; }
.functional-health-source { font-size: 11px; color: var(--muted); margin: 6px 0 12px; }
.functional-health-metrics { display: grid; grid-template-columns: repeat(2, minmax(0,1fr)); gap: 8px; }
.functional-health-metric { display: flex; flex-direction: column; padding: 10px; gap: 4px; border: 1px solid var(--border); border-radius: 8px; min-width: 0; }
.functional-health-metric span { color: var(--muted); font-size: 11px; }
.functional-health-metric strong { font-size: 18px; }
.functional-health-error { color: #991b1b; border: 1px solid #fecaca; background: #fef2f2; padding: 10px; font-size: 12px; border-radius: 8px; overflow-wrap: anywhere; }
.functional-health-note { color: var(--muted); font-size: 12px; margin: 12px 0 0; }
@media (max-width: 760px) { .functional-health-cards { grid-template-columns: 1fr; } }
CSS

echo '==> Validate Python syntax and deterministic health evaluation (no network calls)'
python3 - "$STAGE/app.py.j2" <<'PY'
from pathlib import Path
import ast, sys
from datetime import datetime
path = Path(sys.argv[1])
code = path.read_text(encoding='utf-8')
tree = ast.parse(code)
compile(code, str(path), 'exec')
functions = {item.name:item for item in tree.body if isinstance(item, ast.FunctionDef)}
wanted = ['_functional_count', '_functional_hdfs', '_functional_yarn', 'build_functional_health']
assert all(item in functions for item in wanted)
module = ast.Module(body=[functions[item] for item in wanted], type_ignores=[])
ns = {'Dict': dict, 'datetime': datetime, 'FUNC_EXPECTED_DATANODES': 2, 'FUNC_EXPECTED_NODEMANAGERS': 2}
exec(compile(ast.fix_missing_locations(module), '<functional-health-test>', 'exec'), ns)

def check(hdfs, yarn, expected):
    ns['_functional_json'] = lambda url: hdfs if '/jmx?' in url else yarn
    result = ns['build_functional_health']()
    assert result['status'] == expected, result
    return result

h_ok = {'beans':[{'NumLiveDataNodes':2, 'NumDeadDataNodes':0}]}
y_ok = {'clusterMetrics':{'activeNodes':2,'lostNodes':0,'unhealthyNodes':0}}
assert check(h_ok, y_ok, 'HEALTHY')['hdfs']['dead_datanodes'] == 0
assert check({'beans':[{'NumLiveDataNodes':1,'NumDeadDataNodes':1}]},y_ok,'DEGRADED')['hdfs']['status']=='DEGRADED'
assert check(h_ok,{'clusterMetrics':{'activeNodes':2,'lostNodes':1,'unhealthyNodes':0}},'DEGRADED')['yarn']['status']=='DEGRADED'
assert check({'beans':[{'NumLiveDataNodes':2}]},y_ok,'UNKNOWN')['hdfs']['status']=='UNKNOWN'
assert check(h_ok,{'invalid':'payload'},'UNKNOWN')['yarn']['status']=='UNKNOWN'
print('PASS: Python syntax; HEALTHY, DEGRADED, UNKNOWN; missing metrics are never treated as zero')
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

echo '==> Validate Ansible code'
make lint

echo '==> Copy validated source into project'
cp "$STAGE/app.py.j2" "$BACKEND"
cp "$STAGE/frontend/src/main.jsx" "$MAIN"
cp "$STAGE/frontend/src/FunctionalHealthPanel.jsx" "$HEALTH_COMPONENT"
cp "$STAGE/frontend/src/functional-health.css" "$HEALTH_CSS"

echo '==> Deploy Web Console (NO Hadoop service commands)'
echo "NOTE: Deployment errors leave source changes in place. Restore from $BACKUP_DIR if needed."
make web-console

echo '==> Read-only endpoint check on worker3'
python3 - <<'PY'
import json
from urllib.request import urlopen
with urlopen('http://127.0.0.1:18090/api/functional-health',timeout=15) as response:
    data=json.load(response)
assert data['status'] in ('HEALTHY','DEGRADED','UNKNOWN'), data
print('API status:', data['status'])
for name in ('hdfs','yarn'):
    print(name, json.dumps(data[name], ensure_ascii=False))
PY

echo 'DONE: Admin -> HDFS / YARN Health. No Hadoop services were modified.'
echo "Source backup: $BACKUP_DIR"
