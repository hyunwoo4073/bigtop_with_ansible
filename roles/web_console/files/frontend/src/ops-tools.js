function createOpsTools() {
  if (document.getElementById('bigtop-ops-tools')) {
    return
  }

  const root = document.createElement('div')
  root.id = 'bigtop-ops-tools'
  root.innerHTML = `
    <button class="ops-tools-toggle" type="button" aria-expanded="false">
      <span class="ops-tools-dot"></span>
      Ops
    </button>
    <div class="ops-tools-panel" hidden>
      <div class="ops-tools-header">
        <div>
          <strong>Ops Tools</strong>
          <small>Operational shortcuts</small>
        </div>
        <span id="ops-tools-status" class="ops-tools-status">checking</span>
      </div>

      <div class="ops-tools-section">
        <div class="ops-tools-section-title">Pages</div>
        <div class="ops-tools-links">
          <a href="/">Web Console Home</a>
          <a href="/actions/history">Action History</a>
          <a href="/runbooks">Runbooks</a>
        </div>
      </div>

      <div class="ops-tools-section">
        <div class="ops-tools-section-title">Health</div>
        <div id="ops-tools-summary" class="ops-tools-summary">
          Checking Web Console self status...
        </div>
      </div>

      <div class="ops-tools-hint">
        CLI: make wcf
      </div>
    </div>
  `

  document.body.appendChild(root)

  const toggle = root.querySelector('.ops-tools-toggle')
  const panel = root.querySelector('.ops-tools-panel')
  const status = root.querySelector('#ops-tools-status')
  const summary = root.querySelector('#ops-tools-summary')

  function setOpen(open) {
    panel.hidden = !open
    toggle.setAttribute('aria-expanded', String(open))
    root.classList.toggle('is-open', open)
  }

  toggle.addEventListener('click', (event) => {
    event.stopPropagation()
    setOpen(panel.hidden)
  })

  panel.addEventListener('click', (event) => {
    event.stopPropagation()
  })

  document.addEventListener('click', () => setOpen(false))

  function renderWarnings(warnings) {
    if (!warnings || warnings.length === 0) {
      return 'Self check OK. Audit log, frontend, and backend checks are healthy.'
    }

    const first = warnings.slice(0, 2).map((item) => `• ${item}`).join('<br>')
    const suffix = warnings.length > 2 ? `<br>+ ${warnings.length - 2} more warning(s)` : ''
    return first + suffix
  }

  async function refreshStatus() {
    try {
      const response = await fetch('/api/self-check', { cache: 'no-store' })
      if (!response.ok) {
        throw new Error(`HTTP ${response.status}`)
      }

      const payload = await response.json()
      const currentStatus = payload.status || 'unknown'

      status.textContent = currentStatus
      status.className = `ops-tools-status ${currentStatus === 'ok' ? 'ok' : 'warning'}`
      root.classList.toggle('has-warning', currentStatus !== 'ok')
      summary.innerHTML = renderWarnings(payload.warnings)
      summary.className = `ops-tools-summary ${currentStatus === 'ok' ? 'ok' : 'warning'}`
    } catch (error) {
      status.textContent = 'down'
      status.className = 'ops-tools-status danger'
      root.classList.add('has-warning')
      summary.textContent = `Self check request failed: ${error.message}`
      summary.className = 'ops-tools-summary danger'
    }
  }

  refreshStatus()
  window.setInterval(refreshStatus, 30000)
}

if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', createOpsTools)
} else {
  createOpsTools()
}
