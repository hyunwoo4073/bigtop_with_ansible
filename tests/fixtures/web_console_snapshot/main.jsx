import React from 'react'
import SelfCheckPanel from './SelfCheckPanel.jsx'

function App() {
  const [view, setView] = useState('home')
  function selectComponent(component, tab = 'Overview') {
  }
  return (
    <>
      <HomeView
                onIssueClick={openIssue}
      />
      <ServicesView
                onRestart={restart}
              />
            )}

            {view === 'grid'
    </>
  )
}
function HomeView({ status, metrics, issues, summary, components, onViewServices, onViewIncidents, onIssueClick }) {
  return (
    <>
      <section className="card">
        <CardHeader title="Quick Links" description="shortcuts" />
      </section>
    </>
  )
}
function ServicesView({ components, query, setQuery, filter, setFilter, groupFilter, setGroupFilter, groups, selected, onSelect, onLogs, onStart, onStop, onRestart }) {
  return (
    <div>
              <button className="btn tiny" onClick={() => onLogs(component)}>Logs</button>
    </div>
  )
}
