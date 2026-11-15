import React, { useState } from 'react'
import './kafka-health75.css'

const fmt = (v) => v == null ? '—' : Number(v).toLocaleString()

export default function KafkaHealthPanel75() {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')

  async function refresh() {
    setLoading(true)
    setError('')
    try {
      const res = await fetch('/api/ops/kafka/cluster-health', { cache: 'no-store' })
      const result = await res.json().catch(() => ({}))
      if (!res.ok) throw new Error(result.detail || `HTTP ${res.status}`)
      setData(result)
    } catch (err) {
      setData(null)
      setError(String(err))
    } finally {
      setLoading(false)
    }
  }

  const partition = data?.partition_health
  const quorum = data?.quorum
  const brokers = data?.broker_metadata
  return (
    <section className="card k75-panel">
      <div className="k70-head">
        <div>
          <h3>Kafka metadata health · Step75</h3>
          <p>Kafka CLI 읽기 전용 스냅샷 · 사용자가 요청할 때만 조회합니다.</p>
        </div>
        <button className="btn" disabled={loading} type="button" onClick={refresh}>
          {loading ? 'Reading Kafka metadata…' : 'Inspect Kafka cluster'}
        </button>
      </div>
      {error && <div className="alert danger-alert" role="alert">API 오류: {error}</div>}
      {(data?.errors || []).length > 0 && <div className="alert danger-alert" role="alert">일부 정보를 확인하지 못했습니다: {data.errors.join('; ')}</div>}
      {!data && !loading && !error && <p>버튼을 눌러 Broker, KRaft Quorum, Topic ISR 메타데이터를 확인하세요.</p>}
      {data && <>
        <div className="k75-cards">
          <div className="k75-metric"><small>Metadata brokers</small><strong>{fmt(brokers?.broker_count)}</strong><span>Kafka Admin CLI에 응답한 Broker</span></div>
          <div className="k75-metric"><small>Controller leader</small><strong>{quorum?.leader_id ?? '—'}</strong><span>Voters: {fmt(quorum?.voter_count)} · Epoch: {quorum?.leader_epoch ?? '—'}</span></div>
          <div className="k75-metric"><small>Under-replicated partitions</small><strong>{fmt(partition?.under_replicated_count)}</strong><span>전체 파티션 {fmt(partition?.partition_count)}</span></div>
          <div className="k75-metric"><small>No leader partitions</small><strong>{fmt(partition?.leader_missing_count)}</strong><span>Leader가 -1 또는 없는 경우</span></div>
        </div>
        <p className="k75-note">KRaft Voter는 Controller ID이며, Broker와 반드시 동일한 노드가 아닙니다. TCP 연결 성공이나 Quorum Leader 존재만으로 전체 클러스터 정상 상태를 단정하지 않습니다.</p>
        <div className="k75-columns">
          <div>
            <h4>Broker metadata</h4>
            {(brokers?.brokers || []).length ? (
              <div className="k75-mini-list">{brokers.brokers.map(b => <div key={b.id}><b>Broker {b.id}</b><code>{b.endpoint}</code></div>)}</div>
            ) : <p>조회된 Broker 메타데이터 없음</p>}
          </div>
          <div>
            <h4>KRaft quorum</h4>
            <div className="k75-mini-list">
              <div><span>Controller voters</span><code>{quorum?.current_voters?.join(', ') || '—'}</code></div>
              <div><span>High watermark</span><code>{quorum?.high_watermark ?? '—'}</code></div>
              <div><span>Max follower lag</span><code>{quorum?.max_follower_lag ?? '—'}</code></div>
            </div>
          </div>
        </div>
        <h4>ISR / Leader anomalies</h4>
        <p>Replica 수보다 ISR 수가 적거나 Leader가 없는 파티션을 표시합니다.</p>
        {(partition?.anomalies || []).length ? (
          <div className="k70-table-wrap"><table className="k70-table"><thead><tr><th>Topic</th><th>Partition</th><th>Leader</th><th>Replicas</th><th>ISR</th><th>Status</th></tr></thead><tbody>
            {partition.anomalies.map(a => <tr key={`${a.topic}-${a.partition}`}><td>{a.topic}</td><td>{a.partition}</td><td>{a.leader ?? '—'}</td><td>{a.replicas.join(', ')}</td><td>{a.isr.join(', ')}</td><td>{[a.under_replicated && 'Under ISR', a.leader_missing && 'No leader'].filter(Boolean).join(' / ')}</td></tr>)}
          </tbody></table></div>
        ) : <p>{partition?.parse_complete ? '현재 조회된 파티션에서 ISR/Leader 이상을 발견하지 못했습니다.' : '파티션 메타데이터 확인 필요'}</p>}
        {partition?.anomalies_truncated && <p>표시 제한: 첫 200개 이상 파티션만 표시합니다.</p>}
      </>}
    </section>
  )
}
