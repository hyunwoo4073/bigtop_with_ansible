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
