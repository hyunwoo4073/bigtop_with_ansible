import React from 'react'

// Client-only fixture: never sent to /api/status and never associated with lifecycle API actions.
const EXAMPLE_NAMENODE_INCIDENT = Object.freeze({
  simulated: true,
  component_id: 'demo::hadoop-hdfs-namenode',
  group: 'HDFS',
  component: 'NameNode',
  host: 'master',
  service: 'hadoop-hdfs-namenode',
  severity: 'CRITICAL',
  message: '[SIMULATION] HDFS NameNode on master is DOWN',
  runbook: {
    title: 'Cluster reboot and HDFS recovery',
    path: 'docs/runbooks/cluster-reboot-recovery.md',
    url: '/runbooks/cluster-reboot-recovery.md',
    reason: 'NameNode or HDFS core service issue'
  }
})

export default function IncidentDemoPanel({ enabled, onToggle, onOpenRunbook }) {
  return (
    <section className="card incident-demo-panel" aria-label="Incident UI simulation">
      <div className="incident-demo-heading">
        <div>
          <span className="incident-demo-tag">SIMULATION · CLIENT SIDE ONLY</span>
          <h2>Incident → Runbook 테스트</h2>
          <p>클러스터를 중지하지 않고 화면 이동만 시험합니다. 실제 장애·서비스 상태·이력에는 영향을 주지 않습니다.</p>
        </div>
        <button type="button" className="btn" onClick={onToggle}>
          {enabled ? 'End simulation' : 'Show test incident'}
        </button>
      </div>

      {enabled && (
        <div className="incident-demo-example">
          <div className="incident-demo-example-body">
            <span className="incident-demo-tag">TEST DATA — NOT A REAL FAILURE</span>
            <strong>{EXAMPLE_NAMENODE_INCIDENT.message}</strong>
            <span>Host: master · Service: hadoop-hdfs-namenode · Simulated state: DOWN</span>
            <small>Recommended Runbook: {EXAMPLE_NAMENODE_INCIDENT.runbook.title}</small>
          </div>
          <button type="button" className="btn primary" onClick={() => onOpenRunbook(EXAMPLE_NAMENODE_INCIDENT)}>
            Open Runbook
          </button>
        </div>
      )}
    </section>
  )
}
