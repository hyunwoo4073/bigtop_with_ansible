#!/usr/bin/env bash
set -Eeuo pipefail

# React-only incident preview. Does not change backend APIs or Hadoop services.
PROJECT_DIR="${1:-$PWD}"
FRONTEND="$PROJECT_DIR/roles/web_console/files/frontend"
SRC="$FRONTEND/src"
MAIN="$SRC/main.jsx"
RUNBOOKS="$SRC/RunbooksView.jsx"
DEMO="$SRC/IncidentDemoPanel.jsx"
CSS="$SRC/incident-demo.css"

for name in python3 npm make; do
  command -v "$name" >/dev/null || { echo "ERROR: required command missing: $name" >&2; exit 1; }
done
[[ -f "$MAIN" && -f "$RUNBOOKS" && -f "$SRC/incident-runbook.css" && -f "$FRONTEND/index.html" && -f "$FRONTEND/package.json" && -f "$PROJECT_DIR/Makefile" ]] || {
  echo 'ERROR: run from ~/bigtop-cluster-ansible on worker3, after the Incidents -> Runbooks patch.' >&2
  exit 1
}

# Only apply to the known structure. Never overwrite a partially modified checkout.
python3 - "$MAIN" "$RUNBOOKS" "$DEMO" "$CSS" <<'PY'
from pathlib import Path
import sys
main, runbooks, demo, css = [Path(p) for p in sys.argv[1:]]
a = main.read_text(encoding='utf-8')
b = runbooks.read_text(encoding='utf-8')
requirements = [
    ('latest incident-to-runbook integration', 'function openIncidentRunbook(issue)' in a),
    ('Runbooks context integration', 'requestedRunbook={runbookRequest}' in a and 'activeIncident &&' in b),
    ('sidebar page structure', "{view === 'incidents' && (" in a),
    ('Action History retained', "{view === 'action-history' && (" in a),
    ('Self Check retained', 'SelfCheckPanel' in a),
]
for title, ok in requirements:
    if not ok:
        raise SystemExit(f'ERROR: expected source missing: {title}. No files changed.')
if demo.exists() or css.exists() or 'IncidentDemoPanel' in a or 'incident_demo' in a:
    raise SystemExit('ERROR: demo patch already exists or partially applied. No files changed.')
print('PASS: target source verified; previous UI integrations present')
PY

cd "$PROJECT_DIR"
mkdir -p "$PROJECT_DIR/artifacts/backups"
BACKUP_DIR="$(mktemp -d "$PROJECT_DIR/artifacts/backups/web-console-incident-demo-$(date +%Y%m%d-%H%M%S)-XXXXXX")"
cp -p "$MAIN" "$BACKUP_DIR/main.jsx"
cp -p "$RUNBOOKS" "$BACKUP_DIR/RunbooksView.jsx"
echo "Source backup: $BACKUP_DIR"
STAGE="$(mktemp -d)"
trap 'rm -rf "$STAGE"' EXIT
mkdir -p "$STAGE/frontend"
cp -a "$FRONTEND/src" "$FRONTEND/package.json" "$FRONTEND/index.html" "$STAGE/frontend/"
for config in "$FRONTEND"/vite.config.* "$FRONTEND"/postcss.config.*; do
  [[ ! -f "$config" ]] || cp -a "$config" "$STAGE/frontend/"
done
[[ ! -d "$FRONTEND/public" ]] || cp -a "$FRONTEND/public" "$STAGE/frontend/"
[[ ! -f "$FRONTEND/package-lock.json" ]] || cp -a "$FRONTEND/package-lock.json" "$STAGE/frontend/"

echo '==> Patch isolated frontend copy'
python3 - "$STAGE/frontend/src/main.jsx" "$STAGE/frontend/src/RunbooksView.jsx" <<'PY'
from pathlib import Path
import sys
main_path, runbooks_path = [Path(p) for p in sys.argv[1:]]
main = main_path.read_text(encoding='utf-8')
runbooks = runbooks_path.read_text(encoding='utf-8')

def replace_once(source, old, new, label):
    count = source.count(old)
    if count != 1:
        raise SystemExit(f'ERROR: {label}: expected one exact match, found {count}. No source deployed.')
    return source.replace(old, new, 1)

main = replace_once(
    main,
    "import './incident-runbook.css'\n",
    "import './incident-runbook.css'\nimport IncidentDemoPanel from './IncidentDemoPanel.jsx'\nimport './incident-demo.css'\n",
    'imports',
)
main = replace_once(
    main,
    "const DETAIL_TABS = ['Overview', 'Logs', 'Actions', 'Details']\n",
    "const DETAIL_TABS = ['Overview', 'Logs', 'Actions', 'Details']\n\n// The preview toggle is hidden unless the operator explicitly opts in by URL.\nconst INCIDENT_DEMO_ALLOWED = new URLSearchParams(window.location.search).get('incident_demo') === '1'\n",
    'preview URL gating',
)
main = replace_once(
    main,
    "  const [runbookRequest, setRunbookRequest] = useState(null)\n",
    "  const [runbookRequest, setRunbookRequest] = useState(null)\n  const [demoEnabled, setDemoEnabled] = useState(false)\n",
    'demo state',
)
main = replace_once(
    main,
    "        status: component?.status || 'UNKNOWN'\n",
    "        status: issue.simulated ? 'DOWN (SIMULATED)' : (component?.status || 'UNKNOWN'),\n        simulated: Boolean(issue.simulated)\n",
    'incident runbook demo context',
)
main = replace_once(
    main,
    "            {view === 'incidents' && (\n              <IncidentsView\n",
    '''            {view === 'incidents' && (
              <>
                {INCIDENT_DEMO_ALLOWED && (
                  <IncidentDemoPanel
                    enabled={demoEnabled}
                    onToggle={() => setDemoEnabled((current) => !current)}
                    onOpenRunbook={openIncidentRunbook}
                  />
                )}
                <IncidentsView
''',
    'incident page demo panel',
)
main = replace_once(
    main,
    "                onViewRunbook={openIncidentRunbook}\n              />\n            )}\n\n            {view === 'action-history'",
    "                onViewRunbook={openIncidentRunbook}\n                />\n              </>\n            )}\n\n            {view === 'action-history'",
    'incident page close fragment',
)
main = replace_once(
    main,
    "                onBackToIncidents={() => runbookRequest?.issue && openIssue(runbookRequest.issue)}\n",
    '''                onBackToIncidents={() => {
                  setView('incidents')
                  setDetailVisible(false)
                  if (runbookRequest?.issue && !runbookRequest.issue.simulated) {
                    openIssue(runbookRequest.issue)
                  }
                }}
''',
    'back to simulated incident',
)
runbooks = replace_once(
    runbooks,
    '        <div className="runbook-incident-context">',
    '        <div className={activeIncident.simulated ? "runbook-incident-context incident-demo-context" : "runbook-incident-context"}>',
    'runbook simulated visual distinction',
)
runbooks = replace_once(
    runbooks,
    '<span className="runbook-context-label">FROM INCIDENTS</span>',
    '<span className="runbook-context-label">{activeIncident.simulated ? "SIMULATION — NOT A REAL INCIDENT" : "FROM INCIDENTS"}</span>',
    'runbook simulation label',
)
runbooks = replace_once(
    runbooks,
    "            <small>연결된 권장 문서: {selected?.title || requestedRunbook?.title || 'Runbook'} (서비스별 추천이며 원인 확정이 아닙니다.)</small>",
    "            <small>연결된 권장 문서: {selected?.title || requestedRunbook?.title || 'Runbook'} (서비스별 추천이며 원인 확정이 아닙니다.)</small>\n            {activeIncident.simulated && <p className=\"incident-demo-notice\">브라우저 UI에서만 생성한 가상 장애입니다. 실제 NameNode 상태·클러스터 지표·작업 기록은 변경되지 않았습니다.</p>}",
    'runbook demo notice',
)
main_path.write_text(main, encoding='utf-8')
runbooks_path.write_text(runbooks, encoding='utf-8')
print('PASS: main.jsx and RunbooksView.jsx patched')
PY

cat > "$STAGE/frontend/src/IncidentDemoPanel.jsx" <<'JSX'
import React from 'react'

// Client-only fixture: never sent to /api/status and never associated with lifecycle API actions.
const EXAMPLE_NAMENODE_INCIDENT = Object.freeze({
  simulated: true,
  component_id: 'demo::hadoop-hdfs-namenode',
  group: 'HDFS',
  component: 'NameNode',
  host: 'master',
  service: 'hadoop-hdfs-namenode',
  severity: 'CRITICAL',
  message: '[SIMULATION] HDFS NameNode on master is DOWN',
  runbook: {
    title: 'Cluster reboot and HDFS recovery',
    path: 'docs/runbooks/cluster-reboot-recovery.md',
    url: '/runbooks/cluster-reboot-recovery.md',
    reason: 'NameNode or HDFS core service issue'
  }
})

export default function IncidentDemoPanel({ enabled, onToggle, onOpenRunbook }) {
  return (
    <section className="card incident-demo-panel" aria-label="Incident UI simulation">
      <div className="incident-demo-heading">
        <div>
          <span className="incident-demo-tag">SIMULATION · CLIENT SIDE ONLY</span>
          <h2>Incident → Runbook 테스트</h2>
          <p>클러스터를 중지하지 않고 화면 이동만 시험합니다. 실제 장애·서비스 상태·이력에는 영향을 주지 않습니다.</p>
        </div>
        <button type="button" className="btn" onClick={onToggle}>
          {enabled ? 'End simulation' : 'Show test incident'}
        </button>
      </div>

      {enabled && (
        <div className="incident-demo-example">
          <div className="incident-demo-example-body">
            <span className="incident-demo-tag">TEST DATA — NOT A REAL FAILURE</span>
            <strong>{EXAMPLE_NAMENODE_INCIDENT.message}</strong>
            <span>Host: master · Service: hadoop-hdfs-namenode · Simulated state: DOWN</span>
            <small>Recommended Runbook: {EXAMPLE_NAMENODE_INCIDENT.runbook.title}</small>
          </div>
          <button type="button" className="btn primary" onClick={() => onOpenRunbook(EXAMPLE_NAMENODE_INCIDENT)}>
            Open Runbook
          </button>
        </div>
      )}
    </section>
  )
}
JSX

cat > "$STAGE/frontend/src/incident-demo.css" <<'CSS'
.incident-demo-panel { border: 2px dashed #d97706; background: #fffbeb; margin-bottom: 16px; }
.incident-demo-heading { display: flex; justify-content: space-between; align-items: center; gap: 18px; }
.incident-demo-heading h2 { margin: 7px 0; font-size: 17px; color: #92400e; }
.incident-demo-heading p { font-size: 12px; line-height: 1.6; color: #92400e; margin: 0; }
.incident-demo-tag { color: #92400e; letter-spacing: .04em; font-size: 10px; font-weight: 800; }
.incident-demo-heading > button, .incident-demo-example > button { flex: 0 0 auto; }
.incident-demo-example { display: flex; justify-content: space-between; align-items: center; gap: 18px; padding: 16px; background: #fff; margin-top: 15px; border: 1px solid #f59e0b; border-radius: 9px; }
.incident-demo-example-body { display: grid; gap: 7px; min-width: 0; }
.incident-demo-example strong { color: #92400e; overflow-wrap: anywhere; }
.incident-demo-example span:not(.incident-demo-tag), .incident-demo-example small { color: #78350f; }
.runbook-incident-context.incident-demo-context { background: #fffbeb; border-color: #f59e0b; }
.runbook-incident-context.incident-demo-context .runbook-context-label { color: #92400e; }
.runbook-incident-context .incident-demo-notice { font-weight: 700; color: #92400e; }
@media (max-width: 860px) {
  .incident-demo-heading, .incident-demo-example { flex-direction: column; align-items: stretch; }
  .incident-demo-heading > button, .incident-demo-example > button { flex: 0 1 auto; }
}
CSS

python3 - "$STAGE/frontend/src" <<'PY'
from pathlib import Path
import sys
src = Path(sys.argv[1])
a = (src / 'main.jsx').read_text()
b = (src / 'RunbooksView.jsx').read_text()
c = (src / 'IncidentDemoPanel.jsx').read_text()
assert a.count('<IncidentDemoPanel') == 1
assert a.count('INCIDENT_DEMO_ALLOWED &&') == 1
assert a.count('openIncidentRunbook(issue)') == 1
assert "{view === 'action-history' && (" in a
assert "{view === 'admin' && (" in a
assert 'activeIncident.simulated' in b
assert 'demo::hadoop-hdfs-namenode' in c
assert 'api/component' not in c and 'fetch(' not in c
print('PASS: React component, URL gating, existing pages, and client-only fixture checked')
PY

# Validate in staging before touching the actual project source.
echo '==> Isolated Vite build'
if [[ -x "$FRONTEND/node_modules/.bin/vite" ]]; then
  ln -s "$(readlink -f "$FRONTEND/node_modules")" "$STAGE/frontend/node_modules"
elif [[ -x /opt/bigtop-web-console/frontend-src/node_modules/.bin/vite ]]; then
  ln -s /opt/bigtop-web-console/frontend-src/node_modules "$STAGE/frontend/node_modules"
else
  echo '==> Install staging dependencies (project frontend modules unavailable)'
  (cd "$STAGE/frontend" && npm install --no-audit --no-fund --prefer-offline)
fi
(cd "$STAGE/frontend" && npm run build)
echo 'PASS: Vite build'

echo '==> Ansible validation'
make lint

echo '==> Copy verified frontend source'
cp "$STAGE/frontend/src/main.jsx" "$MAIN"
cp "$STAGE/frontend/src/RunbooksView.jsx" "$RUNBOOKS"
cp "$STAGE/frontend/src/IncidentDemoPanel.jsx" "$DEMO"
cp "$STAGE/frontend/src/incident-demo.css" "$CSS"

echo '==> Deploy Web Console'
echo 'NOTE: if deployment fails after this step, source files remain modified; see the printed backup directory.'
make web-console

echo '==> Check Web Console service and API'
python3 - <<'PY'
import json
from urllib.request import urlopen
base = 'http://127.0.0.1:18090'
for path in ('/api/health', '/api/runbooks', '/api/status'):
    with urlopen(base + path, timeout=180) as response:
        data = json.load(response)
        print(f'PASS: {path} HTTP {response.status}')
        if path == '/api/status':
            print(f'  Actual incidents: {len(data.get("issues", []))}; test data is not inserted into this API.')
PY

echo "DONE: Incident demo preview deployed. Backup: $BACKUP_DIR"
echo 'Use your Web Console address with ?incident_demo=1, then Incidents -> Show test incident -> Open Runbook.'
