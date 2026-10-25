import React, { useEffect, useMemo, useState } from 'react'
import './inventory.css'

function formatTime(value) {
  if (!value) return 'Not synchronized'
  const date = new Date(value)
  return Number.isNaN(date.getTime()) ? String(value) : date.toLocaleString()
}

export default function InventoryView() {
  const [snapshot, setSnapshot] = useState(null)
  const [error, setError] = useState('')
  const [busy, setBusy] = useState(true)
  const [includeAbsent, setIncludeAbsent] = useState(false)
  const [query, setQuery] = useState('')
  const [group, setGroup] = useState('ALL')
  const [reloadToken, setReloadToken] = useState(0)

  useEffect(() => {
    let active = true
    const controller = new AbortController()
    async function load() {
      try {
        setBusy(true)
        const response = await fetch(`/api/metadata?include_absent=${includeAbsent}`, {
          cache: 'no-store', signal: controller.signal,
        })
        if (!response.ok) {
          let detail = `HTTP ${response.status}`
          try { const body = await response.json(); detail = body.detail || detail } catch { /* ignore */ }
          throw new Error(detail)
        }
        const result = await response.json()
        if (!result.ok || !Array.isArray(result.hosts) || !Array.isArray(result.services)) {
          throw new Error('Invalid metadata API response')
        }
        if (active) { setSnapshot(result); setError('') }
      } catch (err) {
        if (active && err.name !== 'AbortError') { setError(String(err.message || err)); setSnapshot(null) }
      } finally {
        if (active) setBusy(false)
      }
    }
    load()
    const interval = setInterval(load, 60000)
    return () => { active = false; controller.abort(); clearInterval(interval) }
  }, [includeAbsent, reloadToken])

  const services = snapshot?.services || []
  const groups = useMemo(() => ['ALL', ...Array.from(new Set(services.map(s => s.group))).sort()], [services])
  const visibleHosts = useMemo(() => {
    const keyword = query.trim().toLowerCase()
    return (snapshot?.hosts || []).map(host => {
      const hostServices = services.filter(service => service.cluster_slug === host.cluster_slug && service.host === host.name)
      const matchingServices = hostServices.filter(service => group === 'ALL' || group === service.group)
      const hostMatches = `${host.cluster_slug} ${host.name} ${host.address} ${host.groups.join(' ')}`.toLowerCase().includes(keyword)
      const filtered = matchingServices.filter(service => !keyword || hostMatches || `${service.component} ${service.service} ${service.group}`.toLowerCase().includes(keyword))
      return { ...host, services: filtered, allServiceCount: hostServices.length, hostMatches }
    }).filter(host => {
      if (group !== 'ALL' && host.services.length === 0) return false
      if (keyword && !host.hostMatches && host.services.length === 0) return false
      return true
    })
  }, [snapshot, services, query, group])

  return (
    <section className="inventory-page">
      <div className="inventory-head">
        <div>
          <div className="section-kicker">PostgreSQL / Ansible metadata</div>
          <h2>Cluster Inventory</h2>
          <p>인벤토리와 서비스 구성의 마지막 동기화 스냅샷입니다. 현재 서비스의 실행 상태는 Services에서 확인하세요.</p>
        </div>
        <button className="btn" onClick={() => setReloadToken(value => value + 1)} disabled={busy}>Refresh</button>
      </div>

      {error && <div className="inventory-error" role="alert"><strong>Metadata 조회 실패</strong><div>{error}</div><small>DB 연동 상태 및 Ansible 메타데이터 동기화 여부를 확인하세요.</small></div>}
      {busy && !snapshot && !error && <div className="inventory-notice">Loading inventory...</div>}

      {snapshot && <>
        <div className="inventory-stats">
          <div><small>Clusters</small><strong>{snapshot.summary.clusters}</strong></div>
          <div><small>Hosts</small><strong>{snapshot.summary.hosts}</strong></div>
          <div><small>Service instances</small><strong>{snapshot.summary.services}</strong></div>
          <div><small>Last sync</small><strong className="inventory-date">{formatTime(snapshot.latest_sync?.at)}</strong></div>
        </div>
        <div className="inventory-tools">
          <input className="search" type="search" placeholder="Search hostname, service, IP..." value={query} onChange={event => setQuery(event.target.value)} aria-label="Inventory search" />
          <select value={group} onChange={event => setGroup(event.target.value)} aria-label="Service group">
            {groups.map(option => <option value={option} key={option}>{option}</option>)}
          </select>
          <label className="inventory-toggle"><input type="checkbox" checked={includeAbsent} onChange={event => setIncludeAbsent(event.target.checked)} /> Include removed</label>
        </div>
        {snapshot.clusters.length === 0 && <div className="inventory-notice">등록된 클러스터가 없습니다. Ansible 메타데이터 동기화를 먼저 실행하세요.</div>}
        {snapshot.clusters.map(cluster => {
          const hosts = visibleHosts.filter(host => host.cluster_slug === cluster.slug)
          return (
            <div className="inventory-cluster" key={cluster.slug}>
              <div className="inventory-cluster-title"><strong>{cluster.slug}</strong><span>{hosts.length} hosts · Synced {formatTime(cluster.last_synced_at)}</span></div>
              {hosts.length === 0 ? <div className="inventory-notice">현재 필터에 일치하는 호스트가 없습니다.</div> :
                <div className="inventory-hosts">{hosts.map(host => <article className="inventory-host" key={`${cluster.slug}-${host.name}`}>
                  <div className="inventory-host-header"><div><h3>{host.name}</h3><code>{host.address}</code></div><span className={`inventory-presence ${host.is_present ? 'present' : 'removed'}`}>{host.is_present ? 'IN INVENTORY' : 'REMOVED'}</span></div>
                  <div className="inventory-groups">{host.groups.length ? host.groups.map(item => <span key={item}>{item}</span>) : <span>no groups</span>}</div>
                  <div className="inventory-service-list">{host.services.length ? host.services.map(service => <div className="inventory-service" key={service.service}>
                    <div><strong>{service.component}</strong><small>{service.service}</small></div><span>{service.group}</span>{!service.is_present && <span className="inventory-presence removed">REMOVED</span>}
                  </div>) : <div className="inventory-empty">No services in this view</div>}</div>
                </article>)}</div>}
            </div>
          )
        })}
        <p className="inventory-footnote">Source: {snapshot.source} · Read only · 변경 사항은 Ansible 동기화 이후 반영됩니다.</p>
      </>}
    </section>
  )
}
