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
