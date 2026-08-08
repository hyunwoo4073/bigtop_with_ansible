import React, { useEffect, useMemo, useState } from 'react'
import { createRoot } from 'react-dom/client'
import './styles.css'

const API = {
  async serviceAction(componentId, action) {
    const res = await fetch(`/api/component/${encodeURIComponent(componentId)}/${action}`, {
      method: 'POST',
      cache: 'no-store'
    })
    if (!res.ok) throw new Error(await res.text())
    return res.json()
  },

  async status() {
    const res = await fetch('/api/status', { cache: 'no-store' })
    if (!res.ok) throw new Error(await res.text())
    return res.json()
  },

  async logs(componentId) {
    const res = await fetch(`/api/component/${encodeURIComponent(componentId)}/logs`, { cache: 'no-store' })
    if (!res.ok) throw new Error(await res.text())
    return res.text()
  },

  async restart(componentId) {
    const res = await fetch(`/api/component/${encodeURIComponent(componentId)}/restart`, {
      method: 'POST',
      cache: 'no-store'
    })
    if (!res.ok) throw new Error(await res.text())
    return res.json()
  },

  async incidentSummary() {
    const res = await fetch('/api/action/incident-summary', {
      method: 'POST',
      cache: 'no-store'
    })
    if (!res.ok) throw new Error(await res.text())
    return res.json()
  }
}

const VIEWS = [
  { key: 'home', label: 'Home' },
  { key: 'services', label: 'Services' },
  { key: 'grid', label: 'Grid' },
  { key: 'incidents', label: 'Incidents' },
  { key: 'admin', label: 'Admin' }
]

const DETAIL_TABS = ['Overview', 'Logs', 'Actions', 'Details']

function tone(status) {
  if (status === 'UP' || status === 'HEALTHY') return 'success'
  if (status === 'DEGRADED' || status === 'UNKNOWN' || status === 'LOADING') return 'warning'
  return 'danger'
}

function shortService(service) {
  return service
    .replace('hadoop-hdfs-', '')
    .replace('hadoop-yarn-', '')
    .replace('prometheus-', '')
    .replace('grafana-server', 'grafana')
    .replace('spark-history-server', 'spark-history')
    .replace('bigtop-web-console', 'web-console')
}

function App() {
  const [view, setView] = useState('home')
  const [status, setStatus] = useState(null)
  const [selected, setSelected] = useState(null)
  const [detailTab, setDetailTab] = useState('Overview')
  const [detailVisible, setDetailVisible] = useState(false)
  const [output, setOutput] = useState('Select a service, then open Logs or Actions.')
  const [loading, setLoading] = useState(false)
  const [refreshIntervalSeconds, setRefreshIntervalSeconds] = useState(() => {
    try {
      const saved = Number(localStorage.getItem('bigtopRefreshIntervalSeconds'))
      return [5, 10, 30, 60].includes(saved) ? saved : 10
    } catch {
      return 10
    }
  })
  const [filter, setFilter] = useState('ALL')
  const [groupFilter, setGroupFilter] = useState('ALL')
  const [query, setQuery] = useState('')
  const [logQuery, setLogQuery] = useState('')
  const [error, setError] = useState(null)

  async function refresh() {
    setLoading(true)
    setError(null)

    try {
      const data = await API.status()
      setStatus(data)

      if (!selected && data.components?.length > 0) {
        const firstIssue = data.issues?.[0]
        const target = firstIssue
          ? data.components.find((item) => item.id === firstIssue.component_id)
          : data.components[0]

        setSelected(target || data.components[0])
      }
    } catch (err) {
      setError(String(err))
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    refresh()
    const timer = setInterval(refresh, refreshIntervalSeconds * 1000)
    return () => clearInterval(timer)
  }, [refreshIntervalSeconds])

  useEffect(() => {
    try {
      localStorage.setItem('bigtopRefreshIntervalSeconds', String(refreshIntervalSeconds))
    } catch {
      // ignore localStorage errors
    }
  }, [refreshIntervalSeconds])

  const components = status?.components || []
  const issues = status?.issues || []
  const nodes = status?.nodes || []
  const summary = status?.summary || []
  const metrics = status?.metrics || {
    total_components: 0,
    up_components: 0,
    down_components: 0,
    issue_count: 0
  }

  const groups = useMemo(() => {
    return ['ALL', ...Array.from(new Set(components.map((item) => item.group)))]
  }, [components])

  const filteredComponents = useMemo(() => {
    return components.filter((item) => {
      const byState =
        filter === 'ALL' ||
        (filter === 'UP' && item.status === 'UP') ||
        (filter === 'DOWN' && item.status !== 'UP')

      const byGroup = groupFilter === 'ALL' || item.group === groupFilter

      const keyword = query.trim().toLowerCase()
      const byQuery =
        keyword.length === 0 ||
        `${item.group} ${item.component} ${item.host} ${item.service} ${item.status}`
          .toLowerCase()
          .includes(keyword)

      return byState && byGroup && byQuery
    })
  }, [components, filter, groupFilter, query])

  const filteredOutput = useMemo(() => {
    const keyword = logQuery.trim().toLowerCase()
    if (!keyword) return output

    return output
      .split('\n')
      .filter((line) => line.toLowerCase().includes(keyword))
      .join('\n') || '(no matching log lines)'
  }, [output, logQuery])

  const detailCapableViews = ['services', 'grid', 'incidents']
  const showDetailPanel = detailCapableViews.includes(view) && detailVisible && selected

  function navigateView(nextView) {
    setView(nextView)

    // Home/Admin/Incidents should not show Service Details by default.
    // Services/Grid open it when the user selects a row or grid cell.
    if (!['services', 'grid'].includes(nextView)) {
      setDetailVisible(false)
    }
  }

  async function viewLogs(component) {
    setSelected(component)
    setDetailVisible(true)
    setDetailTab('Logs')
    setOutput(`Loading logs for ${component.host} / ${component.service} ...`)

    try {
      const text = await API.logs(component.id)
      setOutput(text || '(no logs)')
    } catch (err) {
      setOutput(`[logs error]\n${String(err)}`)
    }
  }

  async function runLifecycleAction(component, action) {
    setSelected(component)
    setDetailVisible(true)
    setDetailTab('Actions')

    const actionLabel = action === 'start' ? '시작' : '중지'
    const actionName = action === 'start' ? 'Start' : 'Stop'

    const confirmed = window.confirm(`${component.host} / ${component.service} 서비스를 ${actionLabel}합니다. 계속할까요?`)
    if (!confirmed) return

    setOutput(`${actionName} ${component.host} / ${component.service} ...`)

    try {
      const result = await API.serviceAction(component.id, action)
      setOutput(
        `Action: ${result.action}\n` +
        `OK: ${result.ok}\n` +
        `Return code: ${result.returncode}\n\n` +
        `[stdout]\n${result.stdout || ''}\n\n` +
        `[stderr]\n${result.stderr || ''}`
      )
      await refresh()
    } catch (err) {
      setOutput(`[${action} error]\n${String(err)}`)
    }
  }

  async function start(component) {
    return runLifecycleAction(component, 'start')
  }

  async function stop(component) {
    return runLifecycleAction(component, 'stop')
  }

  async function restart(component) {
    setSelected(component)
    setDetailVisible(true)
    setDetailTab('Actions')

    const confirmed = window.confirm(`${component.host} / ${component.service} 서비스를 재시작합니다. 계속할까요?`)
    if (!confirmed) return

    setOutput(`Restarting ${component.host} / ${component.service} ...`)

    try {
      const result = await API.restart(component.id)
      setOutput(
        `OK: ${result.ok}\n` +
        `Return code: ${result.returncode}\n\n` +
        `[stdout]\n${result.stdout || ''}\n\n` +
        `[stderr]\n${result.stderr || ''}`
      )
      await refresh()
    } catch (err) {
      setOutput(`[restart error]\n${String(err)}`)
    }
  }

  async function incidentSummary() {
    navigateView('incidents')
    setDetailTab('Logs')
    setOutput('Running incident summary ...')

    try {
      const result = await API.incidentSummary()
      setOutput(
        `OK: ${result.ok}\n` +
        `Return code: ${result.returncode}\n\n` +
        `[stdout]\n${result.stdout || ''}\n\n` +
        `[stderr]\n${result.stderr || ''}`
      )
    } catch (err) {
      setOutput(`[incident summary error]\n${String(err)}`)
    }
  }

  function selectComponent(component, tab = 'Overview') {
    if (selected?.id === component.id && detailVisible) {
      setDetailVisible(false)
      return
    }

    setSelected(component)
    setDetailVisible(true)
    setDetailTab(tab)
  }

  function openIssue(issue) {
    const component = components.find((item) => item.id === issue.component_id)
    if (component) {
      setSelected(component)
      setDetailVisible(true)
      setDetailTab('Overview')
      setView('incidents')
    }
  }

  return (
    <div className="airflow-shell">
      <aside className="sidebar">
        <div className="brand">
          <div className="brand-logo">B</div>
          <div>
            <div className="brand-title">Bigtop</div>
            <div className="brand-sub">Cluster Console</div>
          </div>
        </div>

        <nav className="nav">
          {VIEWS.map((item) => (
            <button
              key={item.key}
              className={view === item.key ? 'nav-item active' : 'nav-item'}
              onClick={() => navigateView(item.key)}
            >
              {item.label}
            </button>
          ))}
        </nav>

        <div className="sidebar-health">
          <div className="sidebar-label">Environment</div>
          <StatusBadge status={status?.cluster_status || 'LOADING'} />
          <div className="sidebar-meta">{status?.checked_at || 'waiting for first check'}</div>
        </div>
      </aside>

      <main className="workspace">
        <header className="topbar">
          <div>
            <div className="breadcrumb">Bigtop / {view}</div>
            <h1>{viewTitle(view)}</h1>
          </div>

          <div className="topbar-actions">
            <div className="refresh-pill">
              <span className={loading ? 'refresh-dot loading' : 'refresh-dot'}></span>
              {loading ? 'Refreshing' : `Auto refresh ${refreshIntervalSeconds}s`}
            </div>
            <button className="btn" onClick={refresh}>Refresh</button>
            <button className="btn primary" onClick={incidentSummary}>Incident Summary</button>
          </div>
        </header>

        {error && (
          <div className="alert danger-alert">
            <strong>Status API Error</strong>
            <pre>{error}</pre>
          </div>
        )}

        <div className={showDetailPanel ? 'layout with-detail' : 'layout no-detail'}>
          <section className="page">
            {view === 'home' && (
              <HomeView
                status={status}
                metrics={metrics}
                issues={issues}
                summary={summary}
                components={components}
                onViewServices={() => navigateView('services')}
                onViewIncidents={() => navigateView('incidents')}
                onIssueClick={openIssue}
              />
            )}

            {view === 'services' && (
              <ServicesView
                components={filteredComponents}
                query={query}
                setQuery={setQuery}
                filter={filter}
                setFilter={setFilter}
                groupFilter={groupFilter}
                setGroupFilter={setGroupFilter}
                groups={groups}
                selected={selected}
                onSelect={selectComponent}
                onLogs={viewLogs}
                onStart={start}
                onStop={stop}
                onRestart={restart}
              />
            )}

            {view === 'grid' && (
              <GridView
                nodes={nodes}
                selected={selected}
                onSelect={selectComponent}
                onLogs={viewLogs}
              />
            )}

            {view === 'incidents' && (
              <IncidentsView
                issues={issues}
                components={components}
                onSelect={selectComponent}
                onLogs={viewLogs}
                onRestart={restart}
              />
            )}

            {view === 'admin' && (
              <AdminView
                status={status}
                onRefresh={refresh}
                onIncidentSummary={incidentSummary}
                refreshIntervalSeconds={refreshIntervalSeconds}
                setRefreshIntervalSeconds={setRefreshIntervalSeconds}
              />
            )}
          </section>

          {showDetailPanel && (
            <DetailPanel
              selected={selected}
              detailTab={detailTab}
              setDetailTab={setDetailTab}
              output={filteredOutput}
              rawOutput={output}
              logQuery={logQuery}
              setLogQuery={setLogQuery}
              onLogs={viewLogs}
              onStart={start}
              onStop={stop}
              onRestart={restart}
              onClose={() => setDetailVisible(false)}
            />
          )}
        </div>
      </main>
    </div>
  )
}

function viewTitle(view) {
  if (view === 'home') return 'Home'
  if (view === 'services') return 'Services'
  if (view === 'grid') return 'Grid'
  if (view === 'incidents') return 'Incidents'
  if (view === 'admin') return 'Admin'
  return 'Home'
}

function HomeView({ status, metrics, issues, summary, components, onViewServices, onViewIncidents, onIssueClick }) {
  return (
    <>
      <section className="home-hero">
        <div className="home-hero-main">
          <div className="section-kicker">Environment Health</div>
          <div className="hero-line">
            <h2>{status?.cluster_status || 'LOADING'}</h2>
            <StatusBadge status={status?.cluster_status || 'LOADING'} />
          </div>
          <p>
            {issues.length === 0
              ? 'All registered cluster services are healthy.'
              : `${issues.length} issue(s) detected. Review incidents or open affected services.`}
          </p>
          <div className="quick-actions">
            <button className="btn primary" onClick={onViewServices}>Open Services</button>
            <button className="btn" onClick={onViewIncidents}>Open Incidents</button>
          </div>
        </div>

        <div className="home-stat-grid">
          <Stat label="Services" value={metrics.total_components} />
          <Stat label="Healthy" value={metrics.up_components} tone="success" />
          <Stat label="Down" value={metrics.down_components} tone={metrics.down_components > 0 ? 'danger' : 'success'} />
          <Stat label="Issues" value={metrics.issue_count} tone={metrics.issue_count > 0 ? 'danger' : 'success'} />
        </div>
      </section>

      <section className="card">
        <CardHeader title="Quick Links" description="운영자가 자주 들어가는 상태 필터와 조치 진입점입니다." />
        <div className="quick-link-grid">
          <QuickLink title="Failed Services" value={metrics.down_components} tone="danger" onClick={onViewIncidents} />
          <QuickLink title="All Services" value={metrics.total_components} onClick={onViewServices} />
          <QuickLink title="Healthy Services" value={metrics.up_components} tone="success" onClick={onViewServices} />
          <QuickLink title="Recent Check" value={status?.checked_at || '-'} />
        </div>
      </section>

      <section className="card">
        <CardHeader title="System Components" description="클러스터 핵심 컴포넌트 상태 요약입니다." />
        <div className="group-grid">
          {summary.map((item) => (
            <GroupCard key={item.group} item={item} />
          ))}
        </div>
      </section>

      <section className="card">
        <CardHeader title="Recent Incidents" description="현재 비정상 상태로 감지된 서비스입니다." />
        {issues.length === 0 ? (
          <EmptyState title="No active incidents" description="현재 등록된 서비스는 모두 정상 상태입니다." />
        ) : (
          <div className="incident-table">
            {issues.slice(0, 6).map((issue) => (
              <button key={issue.component_id} className="incident-line" onClick={() => onIssueClick(issue)}>
                <div>
                  <strong>{issue.message}</strong>
                  <span>{issue.host} · {issue.service}</span>
                </div>
                <StatusBadge status="DOWN" />
              </button>
            ))}
          </div>
        )}
      </section>

      <section className="card">
        <CardHeader title="Service Snapshot" description="최근 서비스 상태 요약입니다." />
        <div className="snapshot-list">
          {components.slice(0, 8).map((component) => (
            <div key={component.id} className="snapshot-row">
              <div>
                <strong>{component.group} / {component.component}</strong>
                <span>{component.host} · {component.service}</span>
              </div>
              <StatusBadge status={component.status} />
            </div>
          ))}
        </div>
      </section>
    </>
  )
}

function ServicesView({ components, query, setQuery, filter, setFilter, groupFilter, setGroupFilter, groups, selected, onSelect, onLogs, onStart, onStop, onRestart }) {
  return (
    <section className="card no-padding">
      <div className="list-header">
        <CardHeader title="Services" description="Airflow DAG List처럼 검색, 필터, 상태, row action을 한 화면에서 제공합니다." />

        <div className="list-toolbar">
          <div className="tabs">
            {['ALL', 'DOWN', 'UP'].map((item) => (
              <button key={item} className={filter === item ? 'tab active' : 'tab'} onClick={() => setFilter(item)}>
                {item}
              </button>
            ))}
          </div>

          <select className="select" value={groupFilter} onChange={(event) => setGroupFilter(event.target.value)}>
            {groups.map((group) => <option key={group} value={group}>{group}</option>)}
          </select>

          <input
            className="search"
            value={query}
            onChange={(event) => setQuery(event.target.value)}
            placeholder="Search services, hosts, groups..."
          />
        </div>
      </div>

      <div className="service-list">
        <div className="service-head">
          <div>Service</div>
          <div>Host</div>
          <div>State</div>
          <div>PID</div>
          <div>Last Check</div>
          <div>Actions</div>
        </div>

        {components.map((component) => (
          <button
            key={component.id}
            className={selected?.id === component.id ? 'service-row selected' : 'service-row'}
            onClick={() => onSelect(component, 'Overview')}
          >
            <div>
              <strong>{component.group} / {component.component}</strong>
              <span>{component.service}</span>
            </div>
            <div>{component.host}</div>
            <div><StatusBadge status={component.status} /></div>
            <div>{component.pid}</div>
            <div>{component.checked_at}</div>
            <div className="row-actions" onClick={(event) => event.stopPropagation()}>
              <button className="btn tiny" onClick={() => onLogs(component)}>Logs</button>
              {component.status === 'UP' ? (
                <button className="btn tiny warning" onClick={() => onStop(component)}>Stop</button>
              ) : (
                <button className="btn tiny success" onClick={() => onStart(component)}>Start</button>
              )}
              <button className="btn tiny danger" onClick={() => onRestart(component)}>Restart</button>
            </div>
          </button>
        ))}

        {components.length === 0 && <EmptyState title="No services" description="조건에 맞는 서비스가 없습니다." />}
      </div>
    </section>
  )
}

function GridView({ nodes, selected, onSelect, onLogs }) {
  const totals = nodes.reduce(
    (acc, node) => {
      acc.total += node.total || 0
      acc.up += node.up || 0
      acc.down += (node.total || 0) - (node.up || 0)
      return acc
    },
    { total: 0, up: 0, down: 0 }
  )

  return (
    <section className="card">
      <CardHeader
        title="Grid"
        description="노드별 서비스 상태를 읽기 쉬운 카드형 Grid로 표시합니다. 같은 타일을 한 번 더 누르면 Service Details가 닫힙니다."
      />

      <div className="grid-summary-bar">
        <div className="grid-summary-item">
          <span>Total</span>
          <strong>{totals.total}</strong>
        </div>
        <div className="grid-summary-item success">
          <span>Healthy</span>
          <strong>{totals.up}</strong>
        </div>
        <div className="grid-summary-item danger">
          <span>Down</span>
          <strong>{totals.down}</strong>
        </div>
        <div className="grid-legend">
          <span className="legend-title">Status</span>
          <span><i className="legend-dot success"></i>Healthy</span>
          <span><i className="legend-dot warning"></i>Unknown</span>
          <span><i className="legend-dot danger"></i>Down</span>
        </div>
      </div>

      <div className="readable-grid">
        {nodes.map((node) => (
          <div key={node.host} className={`grid-node-card ${tone(node.status)}`}>
            <div className="grid-node-header">
              <div>
                <div className="grid-node-name">{node.host}</div>
                <div className="grid-node-meta">{node.up}/{node.total} services healthy</div>
              </div>
              <StatusBadge status={node.status} />
            </div>

            <div className="grid-service-tiles">
              {node.components.map((component) => (
                <button
                  key={component.id}
                  className={selected?.id === component.id ? `grid-service-tile selected ${tone(component.status)}` : `grid-service-tile ${tone(component.status)}`}
                  onClick={() => onSelect(component, component.status === 'UP' ? 'Overview' : 'Logs')}
                  onDoubleClick={() => onLogs(component)}
                  title={`${component.host} / ${component.service} / ${component.status}`}
                >
                  <div className="tile-top">
                    <span className={`state-dot ${tone(component.status)}`}></span>
                    <strong>{component.group}</strong>
                    <StatusBadge status={component.status} />
                  </div>

                  <div className="tile-component">{component.component}</div>
                  <div className="tile-service">{shortService(component.service)}</div>

                  <div className="tile-footer">
                    <span>PID {component.pid || '-'}</span>
                    <span>{component.sub || '-'}</span>
                  </div>
                </button>
              ))}
            </div>
          </div>
        ))}
      </div>
    </section>
  )
}

function IncidentsView({ issues, components, onSelect, onLogs, onRestart }) {
  if (issues.length === 0) {
    return (
      <section className="card">
        <CardHeader title="Incidents" description="현재 감지된 장애 또는 비정상 서비스입니다." />
        <EmptyState title="No active incidents" description="현재 등록된 서비스는 모두 정상입니다." />
      </section>
    )
  }

  return (
    <section className="card">
      <CardHeader title="Incidents" description="이상 상태가 감지된 서비스만 표시합니다." />

      <div className="incident-table">
        {issues.map((issue) => {
          const component = components.find((item) => item.id === issue.component_id)
          return (
            <div key={issue.component_id} className="incident-card">
              <div>
                <strong>{issue.message}</strong>
                <span>{issue.host} · {issue.service}</span>
              </div>

              <div className="row-actions">
                <button className="btn tiny" onClick={() => component && onSelect(component, 'Overview')}>Details</button>
                <button className="btn tiny" onClick={() => component && onLogs(component)}>Logs</button>
                <button className="btn tiny danger" onClick={() => component && onRestart(component)}>Restart</button>
              </div>
            </div>
          )
        })}
      </div>
    </section>
  )
}

function AdminView({ status, onRefresh, onIncidentSummary, refreshIntervalSeconds, setRefreshIntervalSeconds }) {
  const intervalOptions = [5, 10, 30, 60]

  return (
    <section className="card">
      <CardHeader title="Admin" description="운영 콘솔 자체 상태와 UI 동작 설정입니다." />

      <div className="admin-grid">
        <Info label="Console" value="bigtop-web-console" />
        <Info label="Last Check" value={status?.checked_at || '-'} />
        <Info label="Cluster Status" value={status?.cluster_status || '-'} />
        <Info label="Check Interval" value={`${refreshIntervalSeconds} seconds`} />
      </div>

      <div className="settings-card">
        <div>
          <strong>Auto Refresh Interval</strong>
          <span>상태 API를 다시 조회하는 주기를 UI에서 변경합니다. 선택값은 브라우저 localStorage에 저장됩니다.</span>
        </div>

        <div className="interval-options">
          {intervalOptions.map((seconds) => (
            <button
              key={seconds}
              className={refreshIntervalSeconds === seconds ? 'interval-button active' : 'interval-button'}
              onClick={() => setRefreshIntervalSeconds(seconds)}
            >
              {seconds}s
            </button>
          ))}
        </div>
      </div>

      <div className="admin-actions">
        <button className="btn primary" onClick={onRefresh}>Refresh Status</button>
        <button className="btn" onClick={onIncidentSummary}>Run Incident Summary</button>
      </div>
    </section>
  )
}

function DetailPanel({ selected, detailTab, setDetailTab, output, rawOutput, logQuery, setLogQuery, onLogs, onStart, onStop, onRestart, onClose }) {
  return (
    <aside className="detail-panel">
      <div className="detail-header">
        <div>
          <div className="section-kicker">Service Details</div>
          <h2>{selected ? `${selected.group} / ${selected.component}` : 'No Service Selected'}</h2>
        </div>
        <div className="detail-header-actions">
          {selected && <StatusBadge status={selected.status} />}
          <button className="btn tiny" onClick={onClose}>Hide</button>
        </div>
      </div>

      {!selected ? (
        <EmptyState title="Select a service" description="서비스를 선택하면 상세 정보와 로그, 액션이 표시됩니다." />
      ) : (
        <>
          <div className="detail-tabs">
            {DETAIL_TABS.map((tab) => (
              <button key={tab} className={detailTab === tab ? 'detail-tab active' : 'detail-tab'} onClick={() => setDetailTab(tab)}>
                {tab}
              </button>
            ))}
          </div>

          {detailTab === 'Overview' && (
            <div className="detail-body">
              <div className="detail-title-row">
                <div>
                  <strong>{selected.service}</strong>
                  <span>{selected.host}</span>
                </div>
              </div>

              <div className="info-grid">
                <Info label="Host" value={selected.host} />
                <Info label="Service" value={selected.service} />
                <Info label="Active" value={selected.active} />
                <Info label="Sub State" value={selected.sub} />
                <Info label="PID" value={selected.pid} />
                <Info label="Load" value={selected.load} />
              </div>
            </div>
          )}

          {detailTab === 'Logs' && (
            <div className="detail-body">
              <div className="log-toolbar">
                <input
                  className="search full"
                  value={logQuery}
                  onChange={(event) => setLogQuery(event.target.value)}
                  placeholder="Filter logs..."
                />
                <button className="btn tiny" onClick={() => onLogs(selected)}>Reload Logs</button>
              </div>
              <pre className="log-viewer">{output}</pre>
            </div>
          )}

          {detailTab === 'Actions' && (
            <div className="detail-body">
              <div className="action-list">
                <div className="action-card">
                  <div>
                    <strong>View Journal</strong>
                    <span>최근 systemd journal 로그를 조회합니다.</span>
                  </div>
                  <button className="btn" onClick={() => onLogs(selected)}>Run</button>
                </div>

                <div className="action-card success-action">
                  <div>
                    <strong>Start Service</strong>
                    <span>선택한 systemd service를 시작합니다.</span>
                  </div>
                  <button className="btn success" onClick={() => onStart(selected)}>Start</button>
                </div>

                <div className="action-card warning-action">
                  <div>
                    <strong>Stop Service</strong>
                    <span>선택한 systemd service를 중지합니다. web-console/nginx stop은 backend에서 차단합니다.</span>
                  </div>
                  <button className="btn warning" onClick={() => onStop(selected)}>Stop</button>
                </div>

                <div className="action-card danger-action">
                  <div>
                    <strong>Restart Service</strong>
                    <span>선택한 systemd service를 재시작합니다.</span>
                  </div>
                  <button className="btn danger" onClick={() => onRestart(selected)}>Restart</button>
                </div>
              </div>

              <pre className="log-viewer compact">{rawOutput}</pre>
            </div>
          )}

          {detailTab === 'Details' && (
            <div className="detail-body">
              <div className="metadata-list">
                {Object.entries(selected).map(([key, value]) => (
                  <div key={key} className="metadata-row">
                    <span>{key}</span>
                    <code>{String(value)}</code>
                  </div>
                ))}
              </div>
            </div>
          )}
        </>
      )}
    </aside>
  )
}

function GroupCard({ item }) {
  const percent = item.total > 0 ? Math.round((item.up / item.total) * 100) : 0

  return (
    <div className="group-card">
      <div className="group-card-top">
        <div>
          <strong>{item.group}</strong>
          <span>{item.up}/{item.total} healthy</span>
        </div>
        <StatusBadge status={item.status} />
      </div>

      <div className="progress">
        <div className={`progress-fill ${tone(item.status)}`} style={{ width: `${percent}%` }}></div>
      </div>
    </div>
  )
}

function QuickLink({ title, value, tone, onClick }) {
  return (
    <button className={`quick-link ${tone || ''}`} onClick={onClick}>
      <span>{title}</span>
      <strong>{value}</strong>
    </button>
  )
}

function Stat({ label, value, tone }) {
  return (
    <div className={`stat ${tone || ''}`}>
      <span>{label}</span>
      <strong>{value}</strong>
    </div>
  )
}

function StatusBadge({ status }) {
  return <span className={`status-badge ${tone(status)}`}>{status}</span>
}

function CardHeader({ title, description }) {
  return (
    <div className="card-header">
      <h2>{title}</h2>
      {description && <p>{description}</p>}
    </div>
  )
}

function Info({ label, value }) {
  return (
    <div className="info-box">
      <span>{label}</span>
      <strong>{value || '-'}</strong>
    </div>
  )
}

function EmptyState({ title, description }) {
  return (
    <div className="empty-state">
      <strong>{title}</strong>
      <span>{description}</span>
    </div>
  )
}

createRoot(document.getElementById('root')).render(<App />)
