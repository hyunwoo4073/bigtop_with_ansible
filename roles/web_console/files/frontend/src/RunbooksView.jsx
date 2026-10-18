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

export default function RunbooksView({ requestedRunbook = null, onBackToIncidents }) {
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
    if (!requestedRunbook?.url) return
    setKeyword('')
    setSelectedUrl(requestedRunbook.url)
  }, [requestedRunbook])

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
        setSelectedUrl((previous) => requestedRunbook?.url || data.items.find((entry) => entry.url === previous)?.url || data.items[0]?.url || '')
      } catch (err) {
        if (!controller.signal.aborted) setListError(err instanceof Error ? err.message : String(err))
      } finally {
        if (!controller.signal.aborted) setListLoading(false)
      }
    }
    fetchList()
    return () => controller.abort()
  }, [reloadList, requestedRunbook?.url])

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
  const selected = items.find((entry) => entry.url === selectedUrl) || (requestedRunbook?.url === selectedUrl ? requestedRunbook : null)
  const activeIncident = requestedRunbook?.url === selectedUrl ? requestedRunbook.issue : null

  return (
    <section className="card runbooks-view">
      <header className="runbooks-header">
        <div><h2>Runbooks</h2><p>서비스 장애 대응 및 운영 절차를 Web Console 안에서 확인합니다.</p></div>
        <button type="button" className="btn" onClick={() => { setReloadList((n) => n + 1); setReloadDocument((n) => n + 1) }}>Refresh</button>
      </header>
      {activeIncident && (
        <div className={activeIncident.simulated ? "runbook-incident-context incident-demo-context" : "runbook-incident-context"}>
          <div>
            <span className="runbook-context-label">{activeIncident.simulated ? "SIMULATION — NOT A REAL INCIDENT" : "FROM INCIDENTS"}</span>
            <h3>{activeIncident.message}</h3>
            <p>{activeIncident.host} / {activeIncident.service} · 상태 {activeIncident.status || 'UNKNOWN'} · 심각도 {activeIncident.severity || '-'}</p>
            <small>연결된 권장 문서: {selected?.title || requestedRunbook?.title || 'Runbook'} (서비스별 추천이며 원인 확정이 아닙니다.)</small>
            {activeIncident.simulated && <p className="incident-demo-notice">브라우저 UI에서만 생성한 가상 장애입니다. 실제 NameNode 상태·클러스터 지표·작업 기록은 변경되지 않았습니다.</p>}
          </div>
          <button type="button" className="btn" onClick={onBackToIncidents}>Back to Incident</button>
        </div>
      )}
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
