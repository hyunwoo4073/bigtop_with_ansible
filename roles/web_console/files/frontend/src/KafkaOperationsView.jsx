import React, { useEffect, useState } from 'react'
import './kafka-operations.css'

async function getJson(path) {
  const res = await fetch(path, { cache: 'no-store' })
  const data = await res.json().catch(() => ({}))
  if (!res.ok) throw new Error(data.detail || `HTTP ${res.status}`)
  return data
}

function showOffset(value) {
  return value == null ? '—' : Number(value).toLocaleString()
}

export default function KafkaOperationsView({ uiLinks = [] }) {
  const [overview, setOverview] = useState(null)
  const [overviewError, setOverviewError] = useState('')
  const [tab, setTab] = useState('topics')
  const [list, setList] = useState(null)
  const [listError, setListError] = useState('')
  const [group, setGroup] = useState('')
  const [groupDetail, setGroupDetail] = useState(null)
  const [groupError, setGroupError] = useState('')
  const [loading, setLoading] = useState(false)

  async function refreshOverview() {
    try { setOverview(await getJson('/api/ops/kafka/overview')); setOverviewError('') }
    catch (e) { setOverviewError(String(e)) }
  }
  useEffect(() => {
    refreshOverview()
    const interval = setInterval(refreshOverview, 30000)
    return () => clearInterval(interval)
  }, [])

  async function loadList(target = tab) {
    setLoading(true)
    try {
      const data = await getJson(`/api/ops/kafka/${target}`)
      setList(data)
      setListError('')
    } catch (e) {
      setList(null)
      setListError(String(e))
    } finally { setLoading(false) }
  }
  useEffect(() => { loadList(tab); setGroup(''); setGroupDetail(null) }, [tab])

  async function openGroup(name) {
    setGroup(name)
    setGroupDetail(null)
    setGroupError('')
    try { setGroupDetail(await getJson(`/api/ops/kafka/groups/${encodeURIComponent(name)}/lag`)) }
    catch (e) { setGroupError(String(e)) }
  }

  const streaming = overview?.streaming || {}
  const kafbat = uiLinks.find((link) => link.service === 'kafbat-ui')
  const grafana = uiLinks.find((link) => link.service === 'grafana-server')
  const liveBrokers = overview?.brokers?.filter((b) => b.tcp_reachable).length

  return (
    <div className="k70-page">
      <section className="card k70-head">
        <div>
          <h2>Kafka Operations</h2>
          <p>읽기 전용 운영 화면 · Broker TCP, Topic/Group 조회, Step65 CDC 처리 상태</p>
        </div>
        <div className="k70-actions">
          <button className="btn" onClick={refreshOverview} type="button">Refresh metrics</button>
          {kafbat && <a className="btn" href={kafbat.url} target="_blank" rel="noopener noreferrer">Kafbat UI</a>}
          {grafana && <a className="btn" href={grafana.url} target="_blank" rel="noopener noreferrer">Grafana</a>}
        </div>
      </section>
      {overviewError && <div className="alert danger-alert">Kafka overview API: {overviewError}</div>}
      {overview?.prometheus_error && <div className="alert danger-alert">{overview.prometheus_error}</div>}
      <div className="k70-stats">
        <div className="card"><small>Broker TCP reachable</small><strong>{overview ? `${liveBrokers}/${overview.brokers.length}` : '—'}</strong><span>연결 확인일 뿐 Broker health 검사는 아님</span></div>
        <div className="card"><small>Step65 committed lag</small><strong>{showOffset(streaming.lag_total)}</strong><span>Step68 recording rule 기준 · 미수집 시 —</span></div>
        <div className="card"><small>Collector (Kafka / Spark)</small><strong>{['kafka', 'spark'].map(k => streaming.collectors?.[k] == null ? '?' : streaming.collectors[k] ? 'OK' : 'FAIL').join(' / ')}</strong><span>최종 관측: {['kafka', 'spark'].map(k => `${k} ${streaming.collector_age_seconds?.[k] ?? '?'}s`).join(' · ')}</span></div>
      </div>
      <section className="card">
        <h3>Kafka Brokers · TCP connectivity</h3>
        <p>Kafka 메타데이터/리더/ISR 건강 상태를 의미하지 않습니다.</p>
        <div className="k70-brokers">
          {(overview?.brokers || []).map((item) => <div key={item.address} className="k70-broker"><code>{item.address}</code><b className={item.tcp_reachable ? 'k70-ok' : 'k70-bad'}>{item.tcp_reachable ? 'TCP OK' : 'UNREACHABLE'}</b></div>)}
        </div>
      </section>
      <section className="card">
        <h3>Step65 Streaming · {streaming.topic || 'CDC topic'}</h3>
        <div className="k70-table-wrap"><table className="k70-table"><thead><tr><th>Partition</th><th>Kafka end</th><th>Spark committed</th><th>Lag</th></tr></thead><tbody>
          {(streaming.partitions || []).map(p => <tr key={p.partition}><td>{p.partition}</td><td>{showOffset(p.kafka_end_offset)}</td><td>{showOffset(p.spark_committed_offset)}</td><td>{showOffset(p.offset_lag)}</td></tr>)}
        </tbody></table></div>
        {!(streaming.partitions || []).length && <p>수집된 파티션 메트릭이 없습니다. Step67/68 및 Prometheus 상태를 확인하세요.</p>}
      </section>
      <section className="card">
        <div className="k70-head"><div><h3>Kafka metadata</h3><p>조회 전용 Kafka Admin CLI · 선택 시에만 조회</p></div><button className="btn" disabled={loading} onClick={() => loadList(tab)} type="button">{loading ? 'Loading...' : 'Reload list'}</button></div>
        <div className="k70-tabs"><button type="button" className={tab === 'topics' ? 'active' : ''} onClick={() => setTab('topics')}>Topics</button><button type="button" className={tab === 'groups' ? 'active' : ''} onClick={() => setTab('groups')}>Consumer groups</button></div>
        {listError && <div className="alert danger-alert">{listError}</div>}
        {list && <p>{list.total} {tab === 'topics' ? 'topics' : 'groups'} {list.truncated ? '(first 200 shown)' : ''}</p>}
        <div className="k70-items">{(list?.items || []).map(name => tab === 'groups' ? <button className="k70-item" type="button" key={name} onClick={() => openGroup(name)}>{name}</button> : <div className="k70-item" key={name}>{name}</div>)}</div>
        {tab === 'groups' && group && <section className="k70-detail"><h4>Consumer group: {group}</h4>{groupError && <div className="alert danger-alert">{groupError}</div>}{groupDetail && <><p>Reported lag: {showOffset(groupDetail.total_lag)} {groupDetail.truncated ? '(limited to 200 partitions)' : ''}</p><div className="k70-table-wrap"><table className="k70-table"><thead><tr><th>Topic</th><th>Partition</th><th>Current</th><th>End</th><th>Lag</th></tr></thead><tbody>{groupDetail.partitions.map((p, i) => <tr key={`${p.topic}-${p.partition}-${i}`}><td>{p.topic}</td><td>{p.partition}</td><td>{showOffset(p.current_offset)}</td><td>{showOffset(p.log_end_offset)}</td><td>{showOffset(p.lag)}</td></tr>)}</tbody></table></div></>}</section>}
      </section>
    </div>
  )
}
