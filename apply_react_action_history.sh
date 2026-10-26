#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="${PROJECT_ROOT:-$(pwd)}"
cd "$PROJECT_ROOT"
PROJECT_ROOT="$(pwd -P)"

FRONTEND="roles/web_console/files/frontend"
MAIN="$FRONTEND/src/main.jsx"
COMPONENT="$FRONTEND/src/ActionHistoryView.jsx"
STYLE="$FRONTEND/src/action-history.css"

for file in "$MAIN" "$FRONTEND/package.json" "$FRONTEND/index.html"; do
  if [ ! -f "$file" ]; then
    echo "ERROR: required frontend file not found: $file" >&2
    exit 1
  fi
done

if grep -q "ActionHistoryView" "$MAIN"; then
  if [ -f "$COMPONENT" ] && [ -f "$STYLE" ]; then
    echo "Action History integration already exists; no changes made."
    exit 0
  fi
  echo "ERROR: main.jsx mentions ActionHistoryView but the component/CSS is missing." >&2
  echo "Inspect the current project before applying another patch." >&2
  exit 1
fi

if [ -e "$COMPONENT" ] || [ -e "$STYLE" ]; then
  echo "ERROR: Action History component/CSS already exists and was not created by this script." >&2
  echo "No changes made to protect your files." >&2
  exit 1
fi

for bin in python3 node npm tar; do
  command -v "$bin" >/dev/null || { echo "ERROR: missing command $bin" >&2; exit 1; }
done

# Fail early when the uploaded source structure does not match local main.jsx.
python3 - "$MAIN" <<'PY_CHECK'
from pathlib import Path
import sys

text = Path(sys.argv[1]).read_text(encoding='utf-8')
markers = [
    "import './ops-tools.css'",
    "  { key: 'incidents', label: 'Incidents' },",
    "            {view === 'admin' && (",
    "  if (view === 'admin') return 'Admin'",
]
for marker in markers:
    count = text.count(marker)
    if count != 1:
        raise SystemExit(f"ERROR: expected exactly one match ({count}) for: {marker}")
print('PASS: frontend integration points verified')
PY_CHECK

backup_dir="artifacts/backups/web-console-react-action-history-$(date +%Y%m%d%H%M%S)"
mkdir -p "$backup_dir"
cp "$MAIN" "$backup_dir/main.jsx"

stage=""
applied=0
deployment_started=0
cleanup() {
  rc=$?
  trap - EXIT
  if [ "$applied" -ne 1 ] && [ "$deployment_started" -eq 0 ]; then
    cp "$backup_dir/main.jsx" "$MAIN"
    rm -f "$COMPONENT" "$STYLE"
    echo "[ROLLBACK] Build/validation failed before deployment; original frontend sources restored." >&2
    echo "Backup: $backup_dir" >&2
  elif [ "$applied" -ne 1 ]; then
    echo "[WARNING] Deployment was started; keeping modified sources to avoid a source/deployment mismatch." >&2
    echo "Review the error and backup: $backup_dir" >&2
  fi
  if [ -n "$stage" ] && [ -d "$stage" ]; then
    rm -rf "$stage"
  fi
  exit "$rc"
}
trap cleanup EXIT

echo "==> Create Action History React component"
cat > "$COMPONENT" <<'REACT_ACTION_HISTORY_COMPONENT'
import React, { useEffect, useMemo, useState } from 'react'
import './action-history.css'

const ACTION_OPTIONS = ['ALL', 'start', 'stop', 'restart']
const RESULT_OPTIONS = ['ALL', 'SUCCESS', 'FAILED', 'BLOCKED']
const POLL_INTERVAL_MS = 30000

function actionResult(entry) {
  if (entry.blocked) return 'BLOCKED'
  if (entry.ok === true) return 'SUCCESS'
  if (entry.ok === false) return 'FAILED'
  return 'UNKNOWN'
}

function display(value) {
  if (value === null || value === undefined || value === '') return '—'
  return String(value)
}

export default function ActionHistoryView() {
  const [items, setItems] = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')
  const [lastLoaded, setLastLoaded] = useState('')
  const [reloadKey, setReloadKey] = useState(0)
  const [keyword, setKeyword] = useState('')
  const [action, setAction] = useState('ALL')
  const [result, setResult] = useState('ALL')

  useEffect(() => {
    let active = true
    let controller = null

    async function load() {
      if (controller) controller.abort()
      controller = new AbortController()
      const request = controller
      setLoading(true)

      try {
        const response = await fetch('/api/actions/history?limit=100', {
          cache: 'no-store',
          signal: request.signal
        })
        if (!response.ok) throw new Error(`HTTP ${response.status}`)

        const payload = await response.json()
        if (!Array.isArray(payload.items)) throw new Error('Action History 응답에 items 배열이 없습니다.')
        if (!active || request.signal.aborted) return

        setItems(payload.items)
        setError(payload.error ? String(payload.error) : '')
        setLastLoaded(new Date().toLocaleString('ko-KR'))
      } catch (err) {
        if (!active || request.signal.aborted) return
        setError(err instanceof Error ? err.message : String(err))
      } finally {
        if (active && !request.signal.aborted) setLoading(false)
      }
    }

    load()
    const interval = window.setInterval(load, POLL_INTERVAL_MS)
    return () => {
      active = false
      window.clearInterval(interval)
      if (controller) controller.abort()
    }
  }, [reloadKey])

  const filteredItems = useMemo(() => {
    const q = keyword.trim().toLowerCase()

    return items.filter((entry) => {
      const currentAction = String(entry.action || '').toLowerCase()
      const currentResult = actionResult(entry)
      const text = [
        entry.recorded_at,
        entry.timestamp,
        entry.group,
        entry.component,
        entry.host,
        entry.service,
        entry.component_id,
        entry.action,
        entry.reason,
        entry.stderr_summary,
        entry.stdout_summary
      ].map((value) => String(value ?? '')).join(' ').toLowerCase()

      return (action === 'ALL' || currentAction === action) &&
        (result === 'ALL' || currentResult === result) &&
        (!q || text.includes(q))
    })
  }, [items, keyword, action, result])

  return (
    <section className="card action-history-view">
      <div className="action-history-heading">
        <div>
          <h2>Action History</h2>
          <p>Web Console에서 실행한 서비스 작업 이력을 조회합니다. 최근 최대 100건, 30초마다 갱신합니다.</p>
        </div>
        <button className="btn" type="button" onClick={() => setReloadKey((n) => n + 1)} disabled={loading}>
          {loading ? 'Loading...' : 'Refresh History'}
        </button>
      </div>

      <div className="action-history-summary" aria-live="polite">
        <div><strong>{items.length}</strong><span>Loaded actions</span></div>
        <div><strong>{items.filter((item) => actionResult(item) === 'SUCCESS').length}</strong><span>Success</span></div>
        <div><strong>{items.filter((item) => actionResult(item) === 'FAILED').length}</strong><span>Failed</span></div>
        <div><strong>{items.filter((item) => actionResult(item) === 'BLOCKED').length}</strong><span>Blocked</span></div>
      </div>

      <div className="action-history-filters">
        <label>
          <span>Search</span>
          <input type="search" value={keyword} onChange={(event) => setKeyword(event.target.value)} placeholder="Host / Service / Output" />
        </label>
        <label>
          <span>Action</span>
          <select value={action} onChange={(event) => setAction(event.target.value)}>
            {ACTION_OPTIONS.map((option) => <option key={option} value={option}>{option === 'ALL' ? 'All actions' : option.toUpperCase()}</option>)}
          </select>
        </label>
        <label>
          <span>Result</span>
          <select value={result} onChange={(event) => setResult(event.target.value)}>
            {RESULT_OPTIONS.map((option) => <option key={option} value={option}>{option === 'ALL' ? 'All results' : option}</option>)}
          </select>
        </label>
      </div>

      {error && <div className="action-history-error" role="alert">History API error: {error}</div>}

      <div className="action-history-meta">
        <span>{filteredItems.length} / {items.length} shown</span>
        <span>Last refresh: {lastLoaded || '—'}</span>
      </div>

      <div className="action-history-table-scroll">
        <table className="action-history-table">
          <thead>
            <tr>
              <th>Time</th>
              <th>Host</th>
              <th>Component / Service</th>
              <th>Action</th>
              <th>Result</th>
              <th>RC</th>
              <th>Output</th>
            </tr>
          </thead>
          <tbody>
            {filteredItems.map((item, index) => {
              const state = actionResult(item)
              const stdout = item.stdout_summary || ''
              const stderr = item.stderr_summary || ''
              const details = [stderr && `[stderr]\n${stderr}`, stdout && `[stdout]\n${stdout}`].filter(Boolean).join('\n\n')
              return (
                <tr key={`${item.recorded_at || item.timestamp || 'record'}-${item.component_id || item.service || 'service'}-${index}`}>
                  <td className="action-history-nowrap">{display(item.recorded_at || item.timestamp)}</td>
                  <td>{display(item.host)}</td>
                  <td>
                    <strong>{display(item.component || item.service)}</strong>
                    <small>{display(item.group)} · {display(item.service)}</small>
                  </td>
                  <td><span className="action-history-operation">{display(item.action).toUpperCase()}</span></td>
                  <td><span className={`action-history-result ${state.toLowerCase()}`}>{state}</span></td>
                  <td>{display(item.returncode)}</td>
                  <td className="action-history-output">
                    {details || item.reason ? (
                      <details>
                        <summary>View details</summary>
                        <pre>{details || display(item.reason)}</pre>
                      </details>
                    ) : '—'}
                  </td>
                </tr>
              )
            })}
          </tbody>
        </table>
        {!loading && filteredItems.length === 0 && (
          <div className="action-history-empty">
            {items.length === 0 ? '기록된 작업이 없습니다. 서비스 작업을 실행하면 여기에 표시됩니다.' : '필터 조건에 맞는 작업 이력이 없습니다.'}
          </div>
        )}
        {loading && items.length === 0 && !error && <div className="action-history-empty">작업 이력을 불러오는 중...</div>}
      </div>
    </section>
  )
}
REACT_ACTION_HISTORY_COMPONENT
cat > "$STYLE" <<'REACT_ACTION_HISTORY_CSS'
.action-history-view {
  min-width: 0;
}

.action-history-heading {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  flex-wrap: wrap;
  gap: 12px;
  margin-bottom: 18px;
}

.action-history-heading h2 {
  margin: 0;
  font-size: 19px;
  font-weight: 800;
}

.action-history-heading p {
  margin: 6px 0 0;
  color: var(--muted);
  font-size: 13px;
  line-height: 1.5;
}

.action-history-summary {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 10px;
  margin-bottom: 18px;
}

.action-history-summary > div {
  min-width: 0;
  padding: 14px;
  border: 1px solid var(--border);
  border-radius: 10px;
  background: var(--surface-muted);
}

.action-history-summary strong {
  display: block;
  color: var(--text);
  font-size: 24px;
  font-weight: 800;
}

.action-history-summary span {
  display: block;
  margin-top: 2px;
  color: var(--muted);
  font-size: 12px;
}

.action-history-filters {
  display: grid;
  grid-template-columns: minmax(180px, 1fr) 170px 170px;
  gap: 10px;
  margin-bottom: 14px;
}

.action-history-filters label {
  display: flex;
  flex-direction: column;
  gap: 5px;
  min-width: 0;
}

.action-history-filters label > span {
  color: var(--muted);
  font-size: 12px;
  font-weight: 700;
}

.action-history-filters input,
.action-history-filters select {
  width: 100%;
  min-width: 0;
  height: 38px;
  border: 1px solid var(--border-strong);
  border-radius: 8px;
  background: white;
  color: var(--text);
  padding: 0 10px;
  outline-offset: 2px;
  font-size: 13px;
}

.action-history-meta {
  display: flex;
  align-items: center;
  justify-content: space-between;
  flex-wrap: wrap;
  gap: 8px;
  margin-bottom: 9px;
  color: var(--muted);
  font-size: 12px;
}

.action-history-table-scroll {
  width: 100%;
  overflow-x: auto;
  border: 1px solid var(--border);
  border-radius: 10px;
}

.action-history-table {
  width: 100%;
  min-width: 790px;
  border-collapse: collapse;
  text-align: left;
  font-size: 13px;
}

.action-history-table th {
  background: var(--surface-muted);
  padding: 11px 13px;
  color: var(--muted);
  font-weight: 750;
  white-space: nowrap;
  border-bottom: 1px solid var(--border);
}

.action-history-table td {
  padding: 12px 13px;
  vertical-align: top;
  border-bottom: 1px solid var(--border);
}

.action-history-table tr:last-child td {
  border-bottom: 0;
}

.action-history-table tbody tr:hover {
  background: var(--surface-muted);
}

.action-history-table td strong,
.action-history-table td small {
  display: block;
}

.action-history-table td small {
  margin-top: 4px;
  color: var(--muted);
  font-size: 11px;
}

.action-history-nowrap {
  white-space: nowrap;
}

.action-history-result {
  display: inline-block;
  padding: 4px 8px;
  border-radius: 6px;
  font-size: 11px;
  font-weight: 800;
  letter-spacing: 0.01em;
}

.action-history-result.success { background: var(--success-soft); color: #166534; }
.action-history-result.failed { background: var(--danger-soft); color: #991b1b; }
.action-history-result.blocked { background: var(--warning-soft); color: #92400e; }
.action-history-result.unknown { background: var(--surface-muted); color: var(--muted); }

.action-history-operation {
  font-weight: 750;
}

.action-history-output {
  min-width: 140px;
  max-width: 300px;
}

.action-history-output summary {
  color: var(--primary);
  cursor: pointer;
  font-weight: 700;
  white-space: nowrap;
}

.action-history-output pre {
  margin: 9px 0 0;
  max-height: 180px;
  overflow: auto;
  white-space: pre-wrap;
  overflow-wrap: anywhere;
  color: var(--text);
  font-size: 11px;
  line-height: 1.5;
}

.action-history-error {
  padding: 10px 12px;
  margin-bottom: 12px;
  border: 1px solid #fecaca;
  border-radius: 8px;
  background: var(--danger-soft);
  color: #991b1b;
  font-size: 13px;
}

.action-history-empty {
  padding: 34px 14px;
  text-align: center;
  color: var(--muted);
  font-size: 13px;
}

@media (max-width: 800px) {
  .action-history-summary { grid-template-columns: repeat(2, minmax(0, 1fr)); }
  .action-history-filters { grid-template-columns: 1fr; }
}
REACT_ACTION_HISTORY_CSS

echo "==> Integrate menu, view, and title into main.jsx"
python3 - "$MAIN" <<'PY_PATCH'
from pathlib import Path
import sys

path = Path(sys.argv[1])
text = path.read_text(encoding='utf-8')
patches = [
    ("import './ops-tools.css'", "import './ops-tools.css'\nimport ActionHistoryView from './ActionHistoryView.jsx'"),
    ("  { key: 'incidents', label: 'Incidents' },", "  { key: 'incidents', label: 'Incidents' },\n  { key: 'action-history', label: 'Action History' },"),
    ("            {view === 'admin' && (", "            {view === 'action-history' && (\n              <ActionHistoryView />\n            )}\n\n            {view === 'admin' && ("),
    ("  if (view === 'admin') return 'Admin'", "  if (view === 'action-history') return 'Action History'\n  if (view === 'admin') return 'Admin'"),
]
for old, new in patches:
    if text.count(old) != 1:
        raise SystemExit(f"ERROR: expected one match for {old}")
    text = text.replace(old, new, 1)
path.write_text(text, encoding='utf-8')
print('PASS: sidebar and view routing updated')
PY_PATCH

echo "==> Frontend source checks"
python3 - "$MAIN" "$COMPONENT" <<'PY_SOURCE'
from pathlib import Path
import sys

main, component = [Path(item).read_text(encoding='utf-8') for item in sys.argv[1:]]
assert "{ key: 'action-history', label: 'Action History' }" in main
assert "<ActionHistoryView />" in main
assert "'/api/actions/history?limit=100'" in component
assert 'POLL_INTERVAL_MS = 30000' in component
print('PASS: integration and API markers')
PY_SOURCE

# IMPORTANT: test in a separate temporary copy BEFORE deploying.
echo "==> Verify Vite frontend build in isolated staging directory"
stage="$(mktemp -d "${TMPDIR:-/tmp}/bigtop-ui-action-history.XXXXXX")"
tar -C "$FRONTEND" --exclude='./node_modules' --exclude='./dist' -cf - . | tar -C "$stage" -xf -

if [ -x "$FRONTEND/node_modules/.bin/vite" ]; then
  ln -s "$PROJECT_ROOT/$FRONTEND/node_modules" "$stage/node_modules"
elif [ -x "/opt/bigtop-web-console/frontend-src/node_modules/.bin/vite" ]; then
  ln -s "/opt/bigtop-web-console/frontend-src/node_modules" "$stage/node_modules"
else
  echo "No existing Vite dependencies found. Installing in isolated staging directory..."
  if [ -f "$stage/package-lock.json" ]; then
    npm --prefix "$stage" ci --no-audit --no-fund
  else
    npm --prefix "$stage" install --no-audit --no-fund
  fi
fi

npm --prefix "$stage" run build
test -f "$stage/dist/index.html" || { echo "ERROR: Vite did not generate dist/index.html" >&2; exit 1; }
echo 'PASS: Vite build'

echo "==> Run Ansible lint"
make fix-newline
make lint

echo "==> Deploy Web Console (only after successful frontend build)"
deployment_started=1
make web-console
make web-console-check

echo "==> Verify internal action history API"
.venv/bin/ansible ops -m shell -a "curl -fsS http://127.0.0.1:18090/api/actions/history?limit=5 | python3 -m json.tool"

applied=1
echo
echo "DONE: Open Web Console (via your VirtualBox forwarded URL) and choose Action History in the left sidebar."
echo "Backup directory: $backup_dir"
