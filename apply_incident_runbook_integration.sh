#!/usr/bin/env bash
set -Eeuo pipefail

# Apply against the worker3 Ansible checkout. Never touch the backend template.
PROJECT_DIR="${1:-$PWD}"
FRONTEND="$PROJECT_DIR/roles/web_console/files/frontend"
SRC="$FRONTEND/src"
MAIN="$SRC/main.jsx"
RUNBOOKS="$SRC/RunbooksView.jsx"
STYLE="$SRC/incident-runbook.css"

for name in python3 npm make; do
  command -v "$name" >/dev/null || { echo "ERROR: missing tool: $name" >&2; exit 1; }
done
[[ -f "$MAIN" && -f "$RUNBOOKS" && -f "$FRONTEND/package.json" && -f "$FRONTEND/index.html" && -f "$PROJECT_DIR/Makefile" ]] || {
  echo "ERROR: run this script in ~/bigtop-cluster-ansible on worker3." >&2
  exit 1
}

python3 - "$MAIN" "$RUNBOOKS" "$STYLE" <<'PY'
import pathlib, sys
main, runbooks, style = (pathlib.Path(p) for p in sys.argv[1:])
a, b = main.read_text(), runbooks.read_text()
required = [
    ('Action History', 'ActionHistoryView' in a),
    ('Runbooks', 'RunbooksView' in a and 'function IncidentsView' in a),
    ('backend runbook URL selection', 'selectedUrl' in b and '/api/runbooks' in b),
    ('incident metadata', 'issue.component_id' in a),
]
for label, ok in required:
    if not ok:
        sys.exit(f'ERROR: source incompatible: missing {label}. No changes made.')
if 'function openIncidentRunbook(' in a or 'requestedRunbook = null' in b or style.exists():
    sys.exit('ERROR: patch seems already applied or partly applied. No changes made; inspect files first.')
print('PASS: current React integration points verified')
PY

cd "$PROJECT_DIR"
mkdir -p "$PROJECT_DIR/artifacts/backups"
BACKUP_DIR="$(mktemp -d "$PROJECT_DIR/artifacts/backups/web-console-incident-runbook-$(date +%Y%m%d-%H%M%S)-XXXXXX")"
cp -p "$MAIN" "$BACKUP_DIR/main.jsx"
cp -p "$RUNBOOKS" "$BACKUP_DIR/RunbooksView.jsx"
echo "Backup: $BACKUP_DIR"
STAGE="$(mktemp -d)"
cleanup() { rm -rf "$STAGE"; }
trap cleanup EXIT
mkdir -p "$STAGE/frontend"
cp -a "$FRONTEND/src" "$FRONTEND/package.json" "$FRONTEND/index.html" "$STAGE/frontend/"
for config in "$FRONTEND"/vite.config.* "$FRONTEND"/postcss.config.*; do
  if [[ -f "$config" ]]; then cp -a "$config" "$STAGE/frontend/"; fi
done
if [[ -d "$FRONTEND/public" ]]; then cp -a "$FRONTEND/public" "$STAGE/frontend/"; fi
if [[ -f "$FRONTEND/package-lock.json" ]]; then cp -a "$FRONTEND/package-lock.json" "$STAGE/frontend/"; fi

python3 - "$STAGE/frontend/src/main.jsx" "$STAGE/frontend/src/RunbooksView.jsx" <<'PY'
from pathlib import Path
import sys

main_path = Path(sys.argv[1])
runbooks_path = Path(sys.argv[2])
main = main_path.read_text(encoding='utf-8')
runbooks = runbooks_path.read_text(encoding='utf-8')

def replace_once(source, before, after, label):
    count = source.count(before)
    if count != 1:
        raise SystemExit(f'ERROR: {label}: expected exactly one match, found {count}; nothing deployed.')
    return source.replace(before, after, 1)

main = replace_once(
    main,
    "import SelfCheckPanel from './SelfCheckPanel.jsx'\n",
    "import SelfCheckPanel from './SelfCheckPanel.jsx'\nimport './incident-runbook.css'\n",
    'stylesheet import',
)
main = replace_once(
    main,
    "  const [error, setError] = useState(null)\n",
    "  const [error, setError] = useState(null)\n  const [runbookRequest, setRunbookRequest] = useState(null)\n",
    'runbook request state',
)
main = replace_once(
    main,
    "  function navigateView(nextView) {\n    setView(nextView)\n",
    "  function navigateView(nextView) {\n    if (nextView === 'runbooks') setRunbookRequest(null)\n    setView(nextView)\n",
    'sidebar navigation',
)
main = replace_once(
    main,
    "  return (\n    <div className=\"airflow-shell\">",
    '''  function openIncidentRunbook(issue) {
    const runbook = issue?.runbook
    if (!runbook?.url || !runbook.url.startsWith('/runbooks/')) return

    const component = components.find((item) => item.id === issue.component_id)
    setRunbookRequest({
      url: runbook.url,
      title: runbook.title,
      path: runbook.path,
      reason: runbook.reason,
      issue: {
        component_id: issue.component_id,
        message: issue.message,
        severity: issue.severity,
        host: issue.host,
        service: issue.service,
        status: component?.status || 'UNKNOWN'
      }
    })
    setDetailVisible(false)
    setView('runbooks')
  }

  return (
    <div className="airflow-shell">''',
    'incident runbook navigation',
)
main = replace_once(
    main,
    "                onRestart={restart}\n              />\n            )}\n\n            {view === 'action-history'",
    "                onRestart={restart}\n                onViewRunbook={openIncidentRunbook}\n              />\n            )}\n\n            {view === 'action-history'",
    'incident handler',
)
main = replace_once(
    main,
    "            {view === 'runbooks' && (\n              <RunbooksView />\n            )}",
    '''            {view === 'runbooks' && (
              <RunbooksView
                requestedRunbook={runbookRequest}
                onBackToIncidents={() => runbookRequest?.issue && openIssue(runbookRequest.issue)}
              />
            )}''',
    'runbooks view props',
)
old_begin = 'function IncidentsView({ issues, components, onSelect, onLogs, onRestart }) {'
old_end = '\nfunction AdminView({'
if main.count(old_begin) != 1 or main.count(old_end) != 1:
    raise SystemExit('ERROR: IncidentsView boundaries changed; nothing deployed.')
start = main.index(old_begin)
end = main.index(old_end, start)
new_incidents = '''function IncidentsView({ issues, components, onSelect, onLogs, onRestart, onViewRunbook }) {
  if (issues.length === 0) {
    return (
      <section className="card">
        <CardHeader title="Incidents" description="현재 감지된 장애 또는 비정상 서비스입니다." />
        <EmptyState title="No active incidents" description="현재 등록된 서비스는 모두 정상입니다. 장애가 감지되면 해당 서비스의 권장 Runbook 링크가 표시됩니다." />
      </section>
    )
  }

  return (
    <section className="card">
      <CardHeader title="Incidents" description="비정상 서비스의 상태와 관련 운영 문서를 바로 확인합니다. Runbook은 서비스별 권장 문서이며 자동 원인 진단은 아닙니다." />
      <div className="incident-table">
        {issues.map((issue) => {
          const component = components.find((item) => item.id === issue.component_id)
          const runbook = issue.runbook
          const hasRunbook = Boolean(runbook?.url?.startsWith('/runbooks/'))
          return (
            <div key={issue.component_id} className="incident-card incident-runbook-card">
              <div className="incident-card-main">
                <button type="button" className="incident-title-link" disabled={!hasRunbook} onClick={() => onViewRunbook(issue)} title="관련 Runbook 보기">
                  {issue.message}
                </button>
                <span>{issue.host} · {issue.service}</span>
                <div className="incident-metadata">
                  <span>상태: <strong>{component?.status || 'UNKNOWN'}</strong></span>
                  <span>그룹: <strong>{issue.group || component?.group || '-'}</strong></span>
                  <span>PID: <strong>{component?.pid || '-'}</strong></span>
                </div>
                <div className="incident-runbook-hint">
                  관련 문서: <strong>{runbook?.title || '등록된 Runbook 없음'}</strong>
                  {runbook?.reason && <small>{runbook.reason}</small>}
                </div>
              </div>
              <div className="row-actions incident-runbook-actions">
                <button className="btn tiny primary" type="button" disabled={!hasRunbook} onClick={() => onViewRunbook(issue)}>Runbook</button>
                <button className="btn tiny" type="button" disabled={!component} onClick={() => onSelect(component, 'Overview')}>Details</button>
                <button className="btn tiny" type="button" disabled={!component} onClick={() => onLogs(component)}>Logs</button>
                <button className="btn tiny danger" type="button" disabled={!component} onClick={() => onRestart(component)}>Restart</button>
              </div>
            </div>
          )
        })}
      </div>
    </section>
  )
}
'''
main = main[:start] + new_incidents + main[end:]

runbooks = replace_once(
    runbooks,
    'export default function RunbooksView() {',
    'export default function RunbooksView({ requestedRunbook = null, onBackToIncidents }) {',
    'RunbooksView props',
)
runbooks = replace_once(
    runbooks,
    "  const [reloadDocument, setReloadDocument] = useState(0)\n\n  useEffect(() => {",
    '''  const [reloadDocument, setReloadDocument] = useState(0)

  useEffect(() => {
    if (!requestedRunbook?.url) return
    setKeyword('')
    setSelectedUrl(requestedRunbook.url)
  }, [requestedRunbook])

  useEffect(() => {''',
    'set incident-selected document',
)
runbooks = replace_once(
    runbooks,
    "        setSelectedUrl((previous) => data.items.find((entry) => entry.url === previous)?.url || data.items[0]?.url || '')",
    "        setSelectedUrl((previous) => requestedRunbook?.url || data.items.find((entry) => entry.url === previous)?.url || data.items[0]?.url || '')",
    'preserve incident-selected document after list fetch',
)
runbooks = replace_once(
    runbooks,
    "  }, [reloadList])\n\n  useEffect(() => {",
    "  }, [reloadList, requestedRunbook?.url])\n\n  useEffect(() => {",
    'runbook list effect dependencies',
)
runbooks = replace_once(
    runbooks,
    "  const selected = items.find((entry) => entry.url === selectedUrl)\n",
    "  const selected = items.find((entry) => entry.url === selectedUrl) || (requestedRunbook?.url === selectedUrl ? requestedRunbook : null)\n  const activeIncident = requestedRunbook?.url === selectedUrl ? requestedRunbook.issue : null\n",
    'incident context selection',
)
runbooks = replace_once(
    runbooks,
    '''      {listError && <div className="runbook-error" role="alert">Runbook 목록 조회 실패: {listError}</div>}''',
    '''      {activeIncident && (
        <div className="runbook-incident-context">
          <div>
            <span className="runbook-context-label">FROM INCIDENTS</span>
            <h3>{activeIncident.message}</h3>
            <p>{activeIncident.host} / {activeIncident.service} · 상태 {activeIncident.status || 'UNKNOWN'} · 심각도 {activeIncident.severity || '-'}</p>
            <small>연결된 권장 문서: {selected?.title || requestedRunbook?.title || 'Runbook'} (서비스별 추천이며 원인 확정이 아닙니다.)</small>
          </div>
          <button type="button" className="btn" onClick={onBackToIncidents}>Back to Incident</button>
        </div>
      )}
      {listError && <div className="runbook-error" role="alert">Runbook 목록 조회 실패: {listError}</div>}''',
    'incident context card',
)
for label, text in [('main.jsx', main), ('RunbooksView.jsx', runbooks)]:
    if 'openIncidentRunbook' not in main or 'activeIncident' not in runbooks:
        raise SystemExit(f'ERROR: invalid patched {label}')
main_path.write_text(main, encoding='utf-8')
runbooks_path.write_text(runbooks, encoding='utf-8')
print('PASS: Incidents → Runbooks navigation, metadata and incident context integrated')
PY

cat > "$STAGE/frontend/src/incident-runbook.css" <<'CSS'
/* Incident -> Runbook navigation, scoped to existing Web Console screens. */
.incident-card-main { flex: 1 1 280px; min-width: 0; }
.incident-title-link {
  display: block; appearance: none; border: 0; padding: 0; margin: 0;
  background: transparent; color: var(--text); font: inherit; font-weight: 750;
  text-align: left; cursor: pointer; overflow-wrap: anywhere;
}
.incident-title-link:hover:not(:disabled) { color: var(--primary); text-decoration: underline; }
.incident-title-link:disabled { cursor: default; }
.incident-title-link:focus-visible { outline: 2px solid var(--primary); outline-offset: 3px; }
.incident-metadata { display: flex; flex-wrap: wrap; gap: 4px 14px; margin-top: 9px; }
.incident-metadata span { margin: 0; font-size: 11px; }
.incident-metadata strong { color: var(--text); font-weight: 650; }
.incident-runbook-hint { margin-top: 10px; font-size: 12px; color: var(--muted); overflow-wrap: anywhere; }
.incident-runbook-hint strong { color: var(--text); }
.incident-runbook-hint small { display: block; font-size: 11px; margin-top: 3px; }
.incident-runbook-actions { flex-wrap: wrap; justify-content: flex-end; }
.runbook-incident-context {
  display: flex; align-items: center; justify-content: space-between; gap: 14px;
  padding: 15px 16px; margin-bottom: 16px; border: 1px solid #bfdbfe;
  background: #eff6ff; border-radius: 10px;
}
.runbook-incident-context > div { min-width: 0; }
.runbook-context-label { color: #1d4ed8; font-size: 10px; letter-spacing: .08em; font-weight: 800; }
.runbook-incident-context h3 { margin: 6px 0; font-size: 15px; line-height: 1.45; overflow-wrap: anywhere; }
.runbook-incident-context p { margin: 3px 0 6px; font-size: 12px; color: #475569; overflow-wrap: anywhere; }
.runbook-incident-context small { color: #475569; line-height: 1.5; }
.runbook-incident-context > button { flex: 0 0 auto; }
@media (max-width: 860px) {
  .incident-runbook-actions { justify-content: flex-start; }
  .runbook-incident-context { flex-direction: column; align-items: stretch; }
}
CSS

python3 - "$STAGE/frontend/src" <<'PY'
from pathlib import Path
import sys
src = Path(sys.argv[1])
a = (src/'main.jsx').read_text()
b = (src/'RunbooksView.jsx').read_text()
assert a.count('function openIncidentRunbook(') == 1
assert a.count('onViewRunbook={openIncidentRunbook}') == 1
assert a.count('requestedRunbook={runbookRequest}') == 1
assert a.count("import './incident-runbook.css'") == 1
assert b.count('activeIncident &&') == 1
assert b.count('requestedRunbook?.url || data.items.find') == 1
print('PASS: source markers and runbook data flow')
PY

# Test the modified sources without altering running service or checked-out source.
echo '==> Isolated Vite frontend build'
if [[ -x "$FRONTEND/node_modules/.bin/vite" ]]; then
  ln -s "$(readlink -f "$FRONTEND/node_modules")" "$STAGE/frontend/node_modules"
elif [[ -x /opt/bigtop-web-console/frontend-src/node_modules/.bin/vite ]]; then
  ln -s /opt/bigtop-web-console/frontend-src/node_modules "$STAGE/frontend/node_modules"
else
  echo '==> Installing dependencies inside temporary staging directory'
  (cd "$STAGE/frontend" && npm install --no-audit --no-fund --prefer-offline)
fi
(cd "$STAGE/frontend" && npm run build)
echo 'PASS: Vite build'

echo '==> Ansible lint (source still unchanged)'
make lint

echo '==> Copy validated sources to project'
cp "$STAGE/frontend/src/main.jsx" "$MAIN"
cp "$STAGE/frontend/src/RunbooksView.jsx" "$RUNBOOKS"
cp "$STAGE/frontend/src/incident-runbook.css" "$STYLE"

echo '==> Deploy Web Console via existing Ansible playbook'
echo 'NOTE: if deployment fails, source files remain modified; consult backup before rolling back.'
make web-console

echo '==> Verify internal Web Console APIs on worker3'
python3 - <<'PY'
import json, urllib.request
base = 'http://127.0.0.1:18090'
for suffix in ['/api/health', '/api/runbooks']:
    with urllib.request.urlopen(base + suffix, timeout=15) as response:
        data = json.load(response)
        print(f'PASS: {suffix} HTTP {response.status}')
with urllib.request.urlopen(base + '/api/status', timeout=150) as response:
    data = json.load(response)
    issues = data.get('issues', [])
    missing = [x.get('component_id') for x in issues if not x.get('runbook', {}).get('url', '').startswith('/runbooks/')]
    if missing:
        raise SystemExit(f'ERROR: issues missing runbook mapping: {missing}')
    print(f'PASS: /api/status: {len(issues)} active incident(s); runbook mappings checked')
PY

echo "DONE: Incidents → Runbook integrated. Backup: $BACKUP_DIR"
echo 'If the cluster is healthy, the Incidents page will show no active incidents; no service restart is needed to test deployment.'
