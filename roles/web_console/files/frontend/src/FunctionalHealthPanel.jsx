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
