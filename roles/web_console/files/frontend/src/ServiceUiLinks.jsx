import React, { useCallback, useEffect, useState } from 'react'
import './service-ui-links.css'

function safeUrl(url) {
  try {
    const parsed = new URL(url)
    if (!['http:', 'https:'].includes(parsed.protocol)) return null
    if (parsed.username || parsed.password) return null
    return parsed.href
  } catch {
    return null
  }
}

export function getServiceUiLink(links, component) {
  if (!component) return null
  return links.find((link) => link.host === component.host && link.service === component.service) || null
}

export function useServiceUiLinks() {
  const [links, setLinks] = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')

  const reload = useCallback(async () => {
    setLoading(true)
    try {
      const response = await fetch('/api/service-ui-links', { cache: 'no-store' })
      if (!response.ok) throw new Error(`HTTP ${response.status}`)
      const data = await response.json()
      if (!data.ok || !Array.isArray(data.links)) {
        throw new Error(data.error || 'Invalid shortcut API response')
      }
      const filtered = data.links.filter((link) =>
        link && typeof link.title === 'string' &&
        typeof link.host === 'string' && typeof link.service === 'string' &&
        safeUrl(link.url)
      )
      setLinks(filtered)
      setError('')
    } catch (cause) {
      setLinks([])
      setError(String(cause))
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => { reload() }, [reload])
  return { links, loading, error, reload }
}

export function openServiceUi(link) {
  const url = link ? safeUrl(link.url) : null
  if (!url) return
  window.open(url, '_blank', 'noopener,noreferrer')
}

export default function ServiceUiLinksPanel({ links, loading, error, onReload, components }) {
  const state = new Map((components || []).map((item) => [
    `${item.host}::${item.service}`, item.status,
  ]))

  return (
    <section className="card">
      <div className="service-ui-shortcuts-heading">
        <div>
          <h3>Service Web UIs</h3>
          <p>서비스의 관리 화면을 새 탭에서 엽니다. URL은 Ansible 설정으로 관리합니다.</p>
        </div>
        <button className="btn tiny" type="button" onClick={onReload} disabled={loading}>
          {loading ? 'Loading...' : 'Reload Links'}
        </button>
      </div>
      {error && (
        <div className="service-ui-shortcuts-error">
          바로가기 설정을 읽지 못했습니다: {error}
        </div>
      )}
      {!error && links.length === 0 && (
        <p className="service-ui-shortcuts-empty">{loading ? 'Loading service links...' : '설정된 바로가기가 없습니다.'}</p>
      )}
      <div className="service-ui-shortcuts-grid">
        {links.map((link) => {
          const current = state.get(`${link.host}::${link.service}`)
          return (
            <a
              key={`${link.host}::${link.service}`}
              className="service-ui-shortcut"
              href={safeUrl(link.url) || undefined}
              target="_blank"
              rel="noopener noreferrer"
              title={link.url}
            >
              <div className="service-ui-shortcut-top">
                <span>{link.group}</span>
                <span className={current === 'UP' ? 'service-ui-shortcut-state up' : 'service-ui-shortcut-state'}>
                  {current || 'UNKNOWN'}
                </span>
              </div>
              <strong>{link.title}</strong>
              <small>{link.host} · Open UI ↗</small>
            </a>
          )
        })}
      </div>
    </section>
  )
}
