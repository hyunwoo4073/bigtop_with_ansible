#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="${PROJECT_ROOT:-$(pwd)}"
cd "$PROJECT_ROOT"
PROJECT_ROOT="$(pwd -P)"

FRONTEND="roles/web_console/files/frontend"
MAIN="$FRONTEND/src/main.jsx"
RUNBOOK_COMPONENT="$FRONTEND/src/RunbooksView.jsx"
RUNBOOK_CSS="$FRONTEND/src/runbooks.css"
SELF_COMPONENT="$FRONTEND/src/SelfCheckPanel.jsx"
SELF_CSS="$FRONTEND/src/self-check.css"

for file in "$MAIN" "$FRONTEND/src/ActionHistoryView.jsx" "$FRONTEND/package.json" "$FRONTEND/index.html"; do
  if [ ! -f "$file" ]; then
    echo "ERROR: required file is missing: $file" >&2
    echo "Apply the previous Action History integration first." >&2
    exit 1
  fi
done
for bin in python3 node npm tar; do
  command -v "$bin" >/dev/null || { echo "ERROR: missing command: $bin" >&2; exit 1; }
done

if grep -q 'RunbooksView' "$MAIN" || grep -q 'SelfCheckPanel' "$MAIN"; then
  if grep -q 'RunbooksView' "$MAIN" && grep -q 'SelfCheckPanel' "$MAIN" && \
     [ -f "$RUNBOOK_COMPONENT" ] && [ -f "$RUNBOOK_CSS" ] && [ -f "$SELF_COMPONENT" ] && [ -f "$SELF_CSS" ]; then
    echo "Runbooks and Self Check integration already exists. No changes made."
    exit 0
  fi
  echo "ERROR: partial existing integration detected; inspect before applying." >&2
  exit 1
fi
for file in "$RUNBOOK_COMPONENT" "$RUNBOOK_CSS" "$SELF_COMPONENT" "$SELF_CSS"; do
  if [ -e "$file" ]; then
    echo "ERROR: target already exists: $file. No files overwritten." >&2
    exit 1
  fi
done

python3 - "$MAIN" <<'PY_CHECK'
from pathlib import Path
import sys
s = Path(sys.argv[1]).read_text(encoding='utf-8')
markers = [
    "import ActionHistoryView from './ActionHistoryView.jsx'",
    "  { key: 'action-history', label: 'Action History' },",
    "            {view === 'admin' && (",
    "  if (view === 'admin') return 'Admin'",
    '      <div className="settings-card">',
]
for marker in markers:
    count = s.count(marker)
    if count != 1:
        raise SystemExit(f"ERROR: expected exactly one match ({count}): {marker}")
print('PASS: actual React integration points verified')
PY_CHECK

backup_dir="artifacts/backups/web-console-react-runbooks-selfcheck-$(date +%Y%m%d%H%M%S)"
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
    rm -f "$RUNBOOK_COMPONENT" "$RUNBOOK_CSS" "$SELF_COMPONENT" "$SELF_CSS"
    echo "[ROLLBACK] Validation failed before deployment. Frontend source restored." >&2
    echo "Backup: $backup_dir" >&2
  elif [ "$applied" -ne 1 ]; then
    echo "[WARNING] Deployment started; keeping modified source files for diagnosis." >&2
    echo "Backup: $backup_dir" >&2
  fi
  if [ -n "$stage" ] && [ -d "$stage" ]; then rm -rf "$stage"; fi
  exit "$rc"
}
trap cleanup EXIT

echo "==> Create Runbooks React page and Admin Self Check panel"
cat > "$RUNBOOK_COMPONENT" <<'REACT_RUNBOOKS_COMPONENT'
import React, { useEffect, useMemo, useState } from 'react'
import './runbooks.css'

const HEADINGS = /^\s{0,3}(#{1,6})\s+(.+)$/
const BULLET = /^\s*[-*+]\s+(.+)$/
const NUMBERED = /^\s*\d+[.)]\s+(.+)$/

function safeLink(url) {
  if (typeof url !== 'string') return null
  const trimmed = url.trim()
  if (trimmed.startsWith('/') && !trimmed.startsWith('//')) return trimmed
  if (trimmed.startsWith('#')) return trimmed
  if (/^https?:\/\//i.test(trimmed)) return trimmed
  return null
}

function inlineMarkup(text) {
  const content = String(text ?? '')
  const pattern = /(\[([^\]]+)\]\(([^)]+)\)|\*\*([^*]+)\*\*|`([^`]+)`)/g
  const pieces = []
  let cursor = 0
  let match

  while ((match = pattern.exec(content)) !== null) {
    if (match.index > cursor) pieces.push(content.slice(cursor, match.index))
    const id = `inline-${match.index}`
    if (match[2]) {
      const href = safeLink(match[3])
      pieces.push(href
        ? <a key={id} href={href} target="_blank" rel="noopener noreferrer">{match[2]}</a>
        : <span key={id}>{match[2]}</span>)
    } else if (match[4]) {
      pieces.push(<strong key={id}>{match[4]}</strong>)
    } else {
      pieces.push(<code key={id}>{match[5]}</code>)
    }
    cursor = pattern.lastIndex
  }
  if (cursor < content.length) pieces.push(content.slice(cursor))
  return pieces
}

function parseMarkdown(markdown) {
  const lines = String(markdown || '').replace(/\r\n/g, '\n').split('\n')
  const blocks = []
  let index = 0

  while (index < lines.length) {
    const line = lines[index]
    const trimmed = line.trim()
    if (!trimmed) { index += 1; continue }

    if (/^\s*(```|~~~)/.test(line)) {
      const fence = trimmed.slice(0, 3)
      const language = trimmed.slice(3).trim()
      const code = []
      index += 1
      while (index < lines.length && !lines[index].trim().startsWith(fence)) {
        code.push(lines[index]); index += 1
      }
      if (index < lines.length) index += 1
      blocks.push({ type: 'code', language, text: code.join('\n') })
      continue
    }

    const heading = line.match(HEADINGS)
    if (heading) {
      blocks.push({ type: 'heading', level: heading[1].length, text: heading[2] })
      index += 1
      continue
    }

    if (/^\s*(---+|___+|\*\*\*+)\s*$/.test(line)) {
      blocks.push({ type: 'rule' })
      index += 1
      continue
    }

    const bullet = line.match(BULLET)
    const numbered = line.match(NUMBERED)
    if (bullet || numbered) {
      const ordered = Boolean(numbered)
      const entries = []
      while (index < lines.length) {
        const current = lines[index].match(ordered ? NUMBERED : BULLET)
        if (!current) break
        entries.push(current[1]); index += 1
      }
      blocks.push({ type: ordered ? 'ordered' : 'unordered', entries })
      continue
    }

    if (/^\s*>\s?/.test(line)) {
      const quote = []
      while (index < lines.length && /^\s*>\s?/.test(lines[index])) {
        quote.push(lines[index].replace(/^\s*>\s?/, ''))
        index += 1
      }
      blocks.push({ type: 'quote', text: quote.join(' ') })
      continue
    }

    const paragraph = [trimmed]
    index += 1
    while (index < lines.length) {
      const next = lines[index]
      if (!next.trim() || HEADINGS.test(next) || BULLET.test(next) || NUMBERED.test(next) || /^\s*(```|~~~|>)/.test(next)) break
      paragraph.push(next.trim()); index += 1
    }
    blocks.push({ type: 'paragraph', text: paragraph.join(' ') })
  }
  return blocks
}

function MarkdownDocument({ markdown }) {
  const blocks = useMemo(() => parseMarkdown(markdown), [markdown])
  return (
    <article className="runbook-markdown">
      {blocks.map((block, index) => {
        const key = `block-${index}`
        if (block.type === 'heading') {
          if (block.level === 1) return <h1 key={key}>{inlineMarkup(block.text)}</h1>
          if (block.level === 2) return <h2 key={key}>{inlineMarkup(block.text)}</h2>
          if (block.level === 3) return <h3 key={key}>{inlineMarkup(block.text)}</h3>
          return <h4 key={key}>{inlineMarkup(block.text)}</h4>
        }
        if (block.type === 'code') return <div className="runbook-code-wrap" key={key}>{block.language && <span>{block.language}</span>}<pre><code>{block.text}</code></pre></div>
        if (block.type === 'unordered') return <ul key={key}>{block.entries.map((entry, i) => <li key={i}>{inlineMarkup(entry)}</li>)}</ul>
        if (block.type === 'ordered') return <ol key={key}>{block.entries.map((entry, i) => <li key={i}>{inlineMarkup(entry)}</li>)}</ol>
        if (block.type === 'quote') return <blockquote key={key}>{inlineMarkup(block.text)}</blockquote>
        if (block.type === 'rule') return <hr key={key} />
        return <p key={key}>{inlineMarkup(block.text)}</p>
      })}
    </article>
  )
}

function assertRunbookUrl(url) {
  if (typeof url !== 'string' || !url.startsWith('/runbooks/') || url.startsWith('//')) {
    throw new Error('유효하지 않은 Runbook 문서 주소입니다.')
  }
  return url
}

export default function RunbooksView() {
  const [items, setItems] = useState([])
  const [selectedUrl, setSelectedUrl] = useState('')
  const [keyword, setKeyword] = useState('')
  const [markdown, setMarkdown] = useState('')
  const [listError, setListError] = useState('')
  const [documentError, setDocumentError] = useState('')
  const [listLoading, setListLoading] = useState(true)
  const [documentLoading, setDocumentLoading] = useState(false)
  const [reloadList, setReloadList] = useState(0)
  const [reloadDocument, setReloadDocument] = useState(0)

  useEffect(() => {
    const controller = new AbortController()
    async function fetchList() {
      setListLoading(true); setListError('')
      try {
        const response = await fetch('/api/runbooks', { cache: 'no-store', signal: controller.signal })
        if (!response.ok) throw new Error(`HTTP ${response.status}`)
        const data = await response.json()
        if (!Array.isArray(data.items)) throw new Error('Runbooks 응답에 items 배열이 없습니다.')
        if (controller.signal.aborted) return
        setItems(data.items)
        setSelectedUrl((previous) => data.items.find((entry) => entry.url === previous)?.url || data.items[0]?.url || '')
      } catch (err) {
        if (!controller.signal.aborted) setListError(err instanceof Error ? err.message : String(err))
      } finally {
        if (!controller.signal.aborted) setListLoading(false)
      }
    }
    fetchList()
    return () => controller.abort()
  }, [reloadList])

  useEffect(() => {
    if (!selectedUrl) { setMarkdown(''); setDocumentError(''); return }
    const controller = new AbortController()
    async function fetchDocument() {
      setDocumentLoading(true); setDocumentError(''); setMarkdown('')
      try {
        const response = await fetch(assertRunbookUrl(selectedUrl), { cache: 'no-store', signal: controller.signal })
        if (!response.ok) throw new Error(`문서를 불러올 수 없습니다 (HTTP ${response.status}). worker3의 docs/runbooks 파일과 PROJECT_DIR을 확인하세요.`)
        const value = await response.text()
        if (!controller.signal.aborted) setMarkdown(value)
      } catch (err) {
        if (!controller.signal.aborted) setDocumentError(err instanceof Error ? err.message : String(err))
      } finally {
        if (!controller.signal.aborted) setDocumentLoading(false)
      }
    }
    fetchDocument()
    return () => controller.abort()
  }, [selectedUrl, reloadDocument])

  const filtered = useMemo(() => items.filter((entry) => (
    `${entry.title || ''} ${entry.path || ''} ${entry.reason || ''}`.toLowerCase().includes(keyword.trim().toLowerCase())
  )), [items, keyword])
  const selected = items.find((entry) => entry.url === selectedUrl)

  return (
    <section className="card runbooks-view">
      <header className="runbooks-header">
        <div><h2>Runbooks</h2><p>서비스 장애 대응 및 운영 절차를 Web Console 안에서 확인합니다.</p></div>
        <button type="button" className="btn" onClick={() => { setReloadList((n) => n + 1); setReloadDocument((n) => n + 1) }}>Refresh</button>
      </header>
      {listError && <div className="runbook-error" role="alert">Runbook 목록 조회 실패: {listError}</div>}
      <div className="runbooks-layout">
        <aside className="runbooks-list" aria-label="Runbooks 목록">
          <label className="runbooks-search"><span>Search runbooks</span><input value={keyword} onChange={(event) => setKeyword(event.target.value)} placeholder="제목 또는 서비스 검색" /></label>
          <div className="runbooks-list-count">{listLoading ? '목록 로딩 중...' : `${filtered.length} / ${items.length} documents`}</div>
          {filtered.map((entry) => (
            <button key={entry.url} type="button" className={selectedUrl === entry.url ? 'runbook-item selected' : 'runbook-item'} onClick={() => setSelectedUrl(entry.url)}>
              <strong>{entry.title || entry.path || 'Untitled'}</strong>
              <small>{entry.reason || entry.path}</small>
            </button>
          ))}
          {!listLoading && filtered.length === 0 && <div className="runbooks-empty">표시할 Runbook이 없습니다.</div>}
        </aside>
        <div className="runbook-content">
          <div className="runbook-content-toolbar">
            <div><strong>{selected?.title || 'Select a runbook'}</strong><span>{selected?.path || '왼쪽에서 문서를 선택하세요.'}</span></div>
            {selected && <a href={safeLink(selected.url) || '#'} target="_blank" rel="noopener noreferrer">Raw source ↗</a>}
          </div>
          {documentLoading && <div className="runbooks-empty">문서 불러오는 중...</div>}
          {documentError && <div className="runbook-error" role="alert">{documentError}</div>}
          {!documentLoading && !documentError && markdown && <MarkdownDocument markdown={markdown} />}
          {!documentLoading && !documentError && !markdown && <div className="runbooks-empty">조회할 문서를 선택하세요.</div>}
        </div>
      </div>
    </section>
  )
}
REACT_RUNBOOKS_COMPONENT
cat > "$RUNBOOK_CSS" <<'REACT_RUNBOOKS_CSS'
.runbooks-view { min-width: 0; }
.runbooks-header { display: flex; align-items: flex-start; justify-content: space-between; gap: 16px; margin-bottom: 18px; flex-wrap: wrap; }
.runbooks-header h2 { font-size: 20px; margin: 0; }
.runbooks-header p { color: var(--muted); font-size: 13px; margin: 6px 0 0; }
.runbooks-layout { display: grid; grid-template-columns: minmax(190px, 270px) minmax(0, 1fr); min-height: 440px; border: 1px solid var(--border); border-radius: 12px; overflow: hidden; }
.runbooks-list { background: var(--surface-muted); border-right: 1px solid var(--border); min-width: 0; padding: 10px; }
.runbooks-search { display: grid; gap: 6px; margin-bottom: 10px; }
.runbooks-search span { font-size: 12px; color: var(--muted); font-weight: 750; }
.runbooks-search input { padding: 10px 12px; width: 100%; min-width: 0; border: 1px solid var(--border-strong); border-radius: 8px; font: inherit; font-size: 13px; background: var(--surface); }
.runbooks-list-count { padding: 4px 7px 10px; font-size: 11px; color: var(--muted); }
.runbook-item { display: flex; flex-direction: column; align-items: flex-start; text-align: left; width: 100%; gap: 5px; border: 1px solid transparent; border-radius: 9px; background: transparent; padding: 12px; cursor: pointer; margin-bottom: 5px; color: var(--text); }
.runbook-item:hover { background: #eef5ff; }
.runbook-item.selected { border-color: #bfdbfe; background: #eff6ff; }
.runbook-item strong { font-size: 13px; font-weight: 780; line-height: 1.35; }
.runbook-item small { color: var(--muted); font-size: 11px; line-height: 1.4; }
.runbook-content { min-width: 0; overflow: hidden; }
.runbook-content-toolbar { display: flex; justify-content: space-between; gap: 12px; align-items: flex-start; padding: 16px 18px; background: var(--surface-muted); border-bottom: 1px solid var(--border); }
.runbook-content-toolbar > div { display: grid; gap: 5px; min-width: 0; }
.runbook-content-toolbar strong { font-size: 15px; }
.runbook-content-toolbar span { font-size: 11px; color: var(--muted); overflow-wrap: anywhere; }
.runbook-content-toolbar a { white-space: nowrap; color: var(--primary); font-size: 12px; font-weight: 700; text-decoration: none; }
.runbooks-empty { padding: 32px 18px; text-align: center; color: var(--muted); font-size: 13px; }
.runbook-error { padding: 12px 14px; border: 1px solid #fecaca; border-radius: 8px; margin: 12px; background: var(--danger-soft); color: #991b1b; font-size: 13px; overflow-wrap: anywhere; }
.runbook-markdown { padding: 14px 20px 28px; max-height: 680px; overflow: auto; color: var(--text); font-size: 13px; line-height: 1.75; overflow-wrap: anywhere; }
.runbook-markdown h1 { font-size: 21px; padding-bottom: 8px; border-bottom: 1px solid var(--border); margin: 12px 0 16px; }
.runbook-markdown h2 { font-size: 17px; margin: 24px 0 10px; }
.runbook-markdown h3, .runbook-markdown h4 { font-size: 14px; margin: 20px 0 8px; }
.runbook-markdown p { margin: 11px 0; }
.runbook-markdown ul, .runbook-markdown ol { padding-left: 24px; margin: 10px 0; }
.runbook-markdown li { margin: 4px 0; }
.runbook-markdown code { font-size: 12px; background: #f1f5f9; padding: 2px 5px; border-radius: 4px; }
.runbook-code-wrap { max-width: 100%; overflow: auto; border: 1px solid #334155; border-radius: 8px; background: #0f172a; margin: 12px 0; }
.runbook-code-wrap > span { display: block; font-size: 11px; color: #94a3b8; padding: 8px 12px 0; }
.runbook-code-wrap pre { padding: 12px 14px; margin: 0; color: #e2e8f0; overflow-x: auto; font-size: 12px; line-height: 1.6; }
.runbook-code-wrap code { background: transparent; color: inherit; padding: 0; white-space: pre; }
.runbook-markdown blockquote { border-left: 3px solid #93c5fd; margin: 14px 0; background: #eff6ff; padding: 6px 14px; }
.runbook-markdown hr { border: 0; border-top: 1px solid var(--border); margin: 22px 0; }
.runbook-markdown a { color: var(--primary); }
@media (max-width: 760px) { .runbooks-layout { grid-template-columns: minmax(0, 1fr); } .runbooks-list { border-right: 0; border-bottom: 1px solid var(--border); } .runbook-markdown { max-height: none; } }
REACT_RUNBOOKS_CSS
cat > "$SELF_COMPONENT" <<'REACT_SELF_CHECK_COMPONENT'
import React, { useEffect, useMemo, useState } from 'react'
import './self-check.css'

const LABELS = {
  project_dir_exists: '프로젝트 디렉터리',
  project_dir_is_dir: '프로젝트 디렉터리 형식',
  frontend_dist_exists: 'React 배포 파일',
  frontend_dist_is_dir: 'React 배포 디렉터리',
  opsctl_exists: 'Ops CLI',
  ansible_available: 'Ansible 명령',
  audit_log_dir_exists: 'Audit Log 디렉터리',
  audit_log_dir_writable: 'Audit Log 디렉터리 쓰기 권한',
  audit_log_file_exists: 'Audit Log 파일',
  audit_log_file_writable: 'Audit Log 파일 쓰기 권한',
  logrotate_config_exists: 'Logrotate 설정'
}

export default function SelfCheckPanel() {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')
  const [reloadKey, setReloadKey] = useState(0)

  useEffect(() => {
    let controller = null
    let active = true
    async function load() {
      if (controller) controller.abort()
      controller = new AbortController()
      const current = controller
      setLoading(true)
      try {
        const response = await fetch('/api/self-check', { cache: 'no-store', signal: current.signal })
        if (!response.ok) throw new Error(`HTTP ${response.status}`)
        const result = await response.json()
        if (!result || typeof result.checks !== 'object' || Array.isArray(result.checks)) throw new Error('Self Check 응답 형식이 올바르지 않습니다.')
        if (!active || current.signal.aborted) return
        setData(result); setError('')
      } catch (err) {
        if (active && !current.signal.aborted) setError(err instanceof Error ? err.message : String(err))
      } finally {
        if (active && !current.signal.aborted) setLoading(false)
      }
    }
    load()
    const interval = window.setInterval(load, 30000)
    return () => { active = false; window.clearInterval(interval); if (controller) controller.abort() }
  }, [reloadKey])

  const checks = useMemo(() => Object.entries(data?.checks || {}), [data])
  const ok = checks.filter(([, value]) => value === true).length
  const warnings = Array.isArray(data?.warnings) ? data.warnings : []
  const overall = error ? 'ERROR' : data?.status === 'ok' ? 'OK' : 'WARNING'

  return (
    <section className="self-check-panel" aria-label="Web Console Self Check">
      <header className="self-check-heading">
        <div><h3>Console Self Check</h3><p>Web Console의 필수 경로, 실행 환경, 감사 로그 권한을 점검합니다.</p></div>
        <div className="self-check-actions">
          <span className={`self-check-state ${overall.toLowerCase()}`}>{overall}</span>
          <button type="button" className="btn tiny" onClick={() => setReloadKey((n) => n + 1)} disabled={loading}>Check Now</button>
        </div>
      </header>
      {error && <div className="self-check-error" role="alert">Self Check API 오류: {error}</div>}
      <div className="self-check-meta">
        <span>{loading ? 'Checking...' : `${ok} / ${checks.length} checks passed`}</span>
        <span>Checked: {data?.checked_at || '—'}</span>
      </div>
      <div className="self-check-grid">
        {checks.map(([key, passed]) => (
          <div className="self-check-item" key={key}>
            <span className={passed ? 'self-check-indicator pass' : 'self-check-indicator fail'} aria-hidden="true">{passed ? '✓' : '!'}</span>
            <span>{LABELS[key] || key}</span>
            <strong>{passed ? 'PASS' : 'CHECK'}</strong>
          </div>
        ))}
      </div>
      {warnings.length > 0 && (
        <div className="self-check-warnings" role="status">
          <strong>Warnings</strong>
          <ul>{warnings.map((warning, index) => <li key={index}>{String(warning)}</li>)}</ul>
        </div>
      )}
      {!loading && !error && warnings.length === 0 && checks.length > 0 && <p className="self-check-success">Self Check 경고가 없습니다.</p>}
    </section>
  )
}
REACT_SELF_CHECK_COMPONENT
cat > "$SELF_CSS" <<'REACT_SELF_CHECK_CSS'
.self-check-panel { margin: 20px 0; padding: 18px; border: 1px solid var(--border); border-radius: 12px; background: var(--surface-muted); min-width: 0; }
.self-check-heading { display: flex; align-items: flex-start; justify-content: space-between; gap: 10px; flex-wrap: wrap; margin-bottom: 12px; }
.self-check-heading h3 { margin: 0; font-size: 16px; }
.self-check-heading p { margin: 5px 0 0; font-size: 12px; color: var(--muted); }
.self-check-actions { display: flex; gap: 8px; align-items: center; }
.self-check-state { display: inline-block; padding: 5px 9px; border-radius: 999px; font-size: 11px; font-weight: 800; color: #475569; background: #e2e8f0; }
.self-check-state.ok { color: #166534; background: #dcfce7; }
.self-check-state.warning { color: #92400e; background: #fef3c7; }
.self-check-state.error { color: #991b1b; background: #fee2e2; }
.self-check-meta { display: flex; flex-wrap: wrap; justify-content: space-between; gap: 8px; color: var(--muted); font-size: 12px; margin: 10px 0 12px; }
.self-check-grid { display: grid; grid-template-columns: repeat(2, minmax(0,1fr)); gap: 8px; }
.self-check-item { min-width: 0; display: flex; align-items: center; gap: 9px; padding: 11px 12px; border: 1px solid var(--border); border-radius: 9px; background: var(--surface); font-size: 12px; }
.self-check-item > span:nth-child(2) { flex: 1; min-width: 0; }
.self-check-item strong { color: var(--muted); font-size: 11px; }
.self-check-indicator { display: inline-flex; justify-content: center; align-items: center; flex: 0 0 19px; width: 19px; height: 19px; border-radius: 50%; font-weight: 800; }
.self-check-indicator.pass { background: #dcfce7; color: #166534; }
.self-check-indicator.fail { background: #fee2e2; color: #991b1b; }
.self-check-warnings { border-radius: 9px; border: 1px solid #fde68a; background: #fffbeb; color: #92400e; padding: 12px; font-size: 12px; margin-top: 12px; }
.self-check-warnings ul { padding-left: 20px; margin: 7px 0 0; }
.self-check-warnings li { margin: 5px 0; overflow-wrap: anywhere; }
.self-check-success { color: #166534; font-size: 12px; margin: 12px 0 0; }
.self-check-error { color: #991b1b; border: 1px solid #fecaca; background: #fef2f2; padding: 10px; font-size: 12px; border-radius: 8px; }
@media (max-width: 760px) { .self-check-grid { grid-template-columns: 1fr; } }
REACT_SELF_CHECK_CSS


echo "==> Update sidebar, view selection, and Admin Self Check (main.jsx)"
python3 - "$MAIN" <<'PY_PATCH'
from pathlib import Path
import sys
p = Path(sys.argv[1])
s = p.read_text(encoding='utf-8')
patches = [
    (
        "import ActionHistoryView from './ActionHistoryView.jsx'",
        "import ActionHistoryView from './ActionHistoryView.jsx'\nimport RunbooksView from './RunbooksView.jsx'\nimport SelfCheckPanel from './SelfCheckPanel.jsx'",
    ),
    (
        "  { key: 'action-history', label: 'Action History' },",
        "  { key: 'action-history', label: 'Action History' },\n  { key: 'runbooks', label: 'Runbooks' },",
    ),
    (
        "            {view === 'admin' && (",
        "            {view === 'runbooks' && (\n              <RunbooksView />\n            )}\n\n            {view === 'admin' && (",
    ),
    (
        "  if (view === 'admin') return 'Admin'",
        "  if (view === 'runbooks') return 'Runbooks'\n  if (view === 'admin') return 'Admin'",
    ),
    (
        '      <div className="settings-card">',
        '      <SelfCheckPanel />\n\n      <div className="settings-card">',
    ),
]
for old, new in patches:
    if s.count(old) != 1:
        raise SystemExit(f"ERROR: expected a unique patch marker: {old}")
    s = s.replace(old, new, 1)
p.write_text(s, encoding='utf-8')
print('PASS: React sidebar, Runbooks view, and Admin integration patched')
PY_PATCH

python3 - "$MAIN" "$RUNBOOK_COMPONENT" "$SELF_COMPONENT" <<'PY_VERIFY'
from pathlib import Path
import sys
main, runbooks, selfcheck = (Path(p).read_text(encoding='utf-8') for p in sys.argv[1:])
checks = {
    'Runbooks sidebar': "{ key: 'runbooks', label: 'Runbooks' }" in main,
    'Runbooks rendered': '<RunbooksView />' in main,
    'Admin Self Check rendered': '<SelfCheckPanel />' in main,
    'Runbooks list API': "'/api/runbooks'" in runbooks,
    'Runbooks text endpoint': "'/runbooks/'" in runbooks,
    'Self Check API': "'/api/self-check'" in selfcheck,
    'Action History preserved': '<ActionHistoryView />' in main,
}
for label, ok in checks.items():
    if not ok: raise SystemExit(f'ERROR: failed marker validation: {label}')
print('PASS: all UI integration and API markers checked')
PY_VERIFY

# Build in an isolated temporary directory before touching the deployed files.
echo "==> Verify Vite build in an isolated staging directory"
stage="$(mktemp -d "${TMPDIR:-/tmp}/bigtop-ui-runbooks-selfcheck.XXXXXX")"
tar -C "$FRONTEND" --exclude='./node_modules' --exclude='./dist' -cf - . | tar -C "$stage" -xf -

if [ -x "$FRONTEND/node_modules/.bin/vite" ]; then
  ln -s "$PROJECT_ROOT/$FRONTEND/node_modules" "$stage/node_modules"
elif [ -x '/opt/bigtop-web-console/frontend-src/node_modules/.bin/vite' ]; then
  ln -s '/opt/bigtop-web-console/frontend-src/node_modules' "$stage/node_modules"
else
  echo "No existing Vite dependencies found. Installing in isolated staging directory."
  if [ -f "$stage/package-lock.json" ]; then
    npm --prefix "$stage" ci --no-audit --no-fund
  else
    npm --prefix "$stage" install --no-audit --no-fund
  fi
fi

npm --prefix "$stage" run build
test -f "$stage/dist/index.html" || { echo 'ERROR: dist/index.html not generated' >&2; exit 1; }
echo 'PASS: Vite build'

echo "==> Run Ansible validation"
make fix-newline
make lint

echo "==> Deploy Web Console (only after a successful frontend build)"
deployment_started=1
make web-console
make web-console-check

echo "==> Check backend API responses (without relying on JSON content in terminal)"
.venv/bin/ansible ops -m shell -a "curl -fsS http://127.0.0.1:18090/api/runbooks | python3 -c 'import sys,json; d=json.load(sys.stdin); assert isinstance(d.get(\"items\"),list); print(\"runbooks:\",len(d[\"items\"]))'"
.venv/bin/ansible ops -m shell -a "curl -fsS http://127.0.0.1:18090/api/self-check | python3 -c 'import sys,json; d=json.load(sys.stdin); assert isinstance(d.get(\"checks\"),dict); print(\"self-check:\",d.get(\"status\"))'"

applied=1
echo
echo 'DONE: In the Web Console, choose Runbooks in the left sidebar, or Admin > Console Self Check.'
echo "Backup directory: $backup_dir"
