import React, { useCallback, useEffect, useState } from 'react'
import './backup-health.css'

const API_PATH = '/api/ops/postgresql-backup'

function timestamp(seconds) {
  if (!seconds || !Number.isFinite(Number(seconds))) return '—'
  return new Date(Number(seconds) * 1000).toLocaleString()
}

function elapsed(seconds) {
  if (seconds == null || !Number.isFinite(Number(seconds))) return '—'
  const count = Math.max(0, Math.floor(Number(seconds)))
  const hours = Math.floor(count / 3600)
  const mins = Math.floor((count % 3600) / 60)
  return hours > 0 ? `${hours}h ${mins}m` : `${mins}m`
}

function fileSize(value) {
  if (value == null || !Number.isFinite(Number(value))) return '—'
  const bytes = Number(value)
  if (bytes < 1024) return `${bytes} B`
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KiB`
  return `${(bytes / (1024 * 1024)).toFixed(1)} MiB`
}

function StatusPill({ value }) {
  const state = String(value || 'UNKNOWN').toUpperCase()
  const cls = ['HEALTHY', 'ACTIVE', 'VALID', 'INACTIVE'].includes(state)
    ? 'pgb-good' : state === 'DEGRADED' || state === 'PENDING' ? 'pgb-warn' : 'pgb-bad'
  return <span className={`pgb-pill ${cls}`}>{state}</span>
}

export default function BackupHealthPanel() {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')

  const refresh = useCallback(async (signal) => {
    try {
      const res = await fetch(API_PATH, { cache: 'no-store', signal })
      if (!res.ok) throw new Error(`API HTTP ${res.status}`)
      const payload = await res.json()
      if (!signal?.aborted) {
        setData(payload)
        setError('')
      }
    } catch (err) {
      if (!signal?.aborted) { setError(String(err)); setData(null) }
    } finally {
      if (!signal?.aborted) setLoading(false)
    }
  }, [])

  useEffect(() => {
    const controller = new AbortController()
    refresh(controller.signal)
    const timer = setInterval(() => refresh(controller.signal), 60000)
    return () => { clearInterval(timer); controller.abort() }
  }, [refresh])

  const m = data?.metrics || {}
  const ruleCount = data?.alert_summary?.total ?? 0
  const activeAlerts = data?.alerts?.filter(item => item.state === 'firing' || item.state === 'pending') || []

  return (
    <section className="pgb-panel" aria-label="PostgreSQL backup monitoring">
      <div className="pgb-heading">
        <div>
          <h3>PostgreSQL Backup</h3>
          <p>Prometheus 관측값 · 읽기 전용 · 60초 자동 갱신</p>
        </div>
        <div className="pgb-heading-actions">
          <StatusPill value={data?.status || 'UNKNOWN'} />
          <button type="button" className="btn tiny" onClick={() => { setLoading(true); refresh() }} disabled={loading}>
            {loading ? 'Loading...' : 'Refresh'}
          </button>
        </div>
      </div>

      {error && <div role="alert" className="pgb-notice pgb-error">조회 실패: {error}</div>}
      {data?.errors?.length > 0 && (
        <div role="alert" className="pgb-notice pgb-error">{data.errors.join(' · ')}</div>
      )}
      {data?.issues?.length > 0 && (
        <div role="status" className="pgb-notice pgb-warning">{data.issues.join(' · ')}</div>
      )}

      <div className="pgb-stats">
        <div><span>최신 백업</span><strong>{timestamp(m.latest_archive_timestamp_seconds)}</strong></div>
        <div><span>백업 경과 시간</span><strong>{elapsed(data?.backup_age_seconds)}</strong></div>
        <div><span>최신 파일 크기</span><strong>{fileSize(m.latest_archive_size_bytes)}</strong></div>
        <div><span>보관 중인 파일</span><strong>{m.archive_count ?? '—'}</strong></div>
        <div><span>파일 유효성</span><strong>{m.latest_archive_valid == null ? 'UNKNOWN' : m.latest_archive_valid === 1 ? 'VALID' : 'INVALID'}</strong></div>
        <div><span>백업 Timer</span><strong>{m.timer_active == null ? 'UNKNOWN' : m.timer_active === 1 ? 'ACTIVE' : 'INACTIVE'}</strong></div>
        <div><span>Observer 갱신</span><strong>{elapsed(data?.observer_age_seconds)} 전</strong></div>
        <div><span>Alertmanager 연결</span><strong>{data?.alertmanager_connected == null ? 'UNKNOWN' : data.alertmanager_connected ? 'CONNECTED' : 'DISCONNECTED'}</strong></div>
      </div>

      <div className="pgb-alerts-title">
        <strong>Backup Alert Rules</strong>
        <span>{ruleCount} rules · {data?.alert_summary?.firing ?? 0} firing · {data?.alert_summary?.pending ?? 0} pending</span>
      </div>
      {activeAlerts.length > 0 && (
        <div className="pgb-active-alerts" role="status">
          {activeAlerts.map(item => <div key={item.name}><strong>{item.name}</strong> · {item.state} · {item.summary}</div>)}
        </div>
      )}
      <div className="pgb-rule-list">
        {(data?.alerts || []).map(item => (
          <div className="pgb-rule" key={item.name}>
            <div><strong>{item.name}</strong><small>{item.summary}</small></div>
            <StatusPill value={item.state} />
          </div>
        ))}
        {!loading && !error && ruleCount === 0 && <p className="pgb-empty">현재 조회된 PostgreSQL 백업 Alert Rule이 없습니다.</p>}
      </div>
      <p className="pgb-foot">최종 조회: {timestamp(data?.checked_at)} · 이 화면에서 백업 생성·복원·알림 해제는 실행할 수 없습니다.</p>
    </section>
  )
}
