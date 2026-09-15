const MAX_ROWS = 100;
const MAX_CHART_POINTS = 40;

let eventCountThisSecond = 0;
let lastRateSample = Date.now();

// ---------------------------------------------------------------- tabs
document.querySelectorAll('.tab-btn').forEach((btn) => {
  btn.addEventListener('click', () => {
    document.querySelectorAll('.tab-btn').forEach((b) => b.classList.remove('active'));
    document.querySelectorAll('.tab-panel').forEach((p) => p.classList.remove('active'));
    btn.classList.add('active');
    document.getElementById(`tab-${btn.dataset.tab}`).classList.add('active');
    if (btn.dataset.tab === 'history') loadHistory();
  });
});

// ------------------------------------------------------------- websocket
function connectWebSocket() {
  const proto = window.location.protocol === 'https:' ? 'wss' : 'ws';
  const ws = new WebSocket(`${proto}://${window.location.host}/ws`);

  ws.onopen = () => setWsStatus(true);
  ws.onclose = () => {
    setWsStatus(false);
    setTimeout(connectWebSocket, 2000);
  };
  ws.onerror = () => ws.close();
  ws.onmessage = (msg) => {
    try {
      const payload = JSON.parse(msg.data);
      handleUpdate(payload);
    } catch (e) {
      console.error('bad ws payload', e);
    }
  };
}

function setWsStatus(connected) {
  const dot = document.getElementById('wsStatusDot');
  const text = document.getElementById('wsStatusText');
  dot.className = connected ? 'dot dot-green' : 'dot dot-red';
  text.textContent = connected ? 'Live' : 'Reconnecting…';
}

function handleUpdate(payload) {
  if (payload.type === 'event') {
    addLiveRow(payload.event);
    updateCharts(payload.event);
    eventCountThisSecond += 1;
    (payload.anomalies || []).forEach(addAnomalyRow);
  } else if (payload.type === 'silence') {
    addAnomalyRow({
      timestamp: Date.now() / 1000,
      severity: payload.severity,
      source: payload.source,
      instance_id: payload.instance_id,
      metric: 'heartbeat',
      observed_value: payload.elapsed,
      reason: `Source went silent for ${payload.elapsed.toFixed(1)}s`,
    });
  }
}

// ---------------------------------------------------------------- tables
function addLiveRow(ev) {
  const tbody = document.querySelector('#liveTable tbody');
  const tr = document.createElement('tr');
  const t = new Date(ev.timestamp * 1000).toLocaleTimeString();
  tr.innerHTML = `<td>${t}</td><td>${ev.source}</td><td>${ev.instance_id}</td>
    <td class="status-${ev.status}">${ev.status}</td>
    <td>${ev.cpu_percent.toFixed(1)}</td><td>${ev.memory_percent.toFixed(1)}</td>
    <td>${ev.latency_ms.toFixed(1)}</td><td>${(ev.error_rate * 100).toFixed(2)}</td>
    <td>${ev.requests_per_sec.toFixed(1)}</td>`;
  tbody.prepend(tr);
  while (tbody.children.length > MAX_ROWS) tbody.removeChild(tbody.lastChild);
}

function addAnomalyRow(a) {
  const tbody = document.querySelector('#anomalyTable tbody');
  const tr = document.createElement('tr');
  const t = new Date(a.timestamp * 1000).toLocaleTimeString();
  tr.innerHTML = `<td>${t}</td><td class="sev-${a.severity}">${a.severity}</td>
    <td>${a.source}</td><td>${a.instance_id}</td><td>${a.metric}</td>
    <td>${Number(a.observed_value).toFixed(2)}</td><td>${a.reason}</td>`;
  tbody.prepend(tr);
  while (tbody.children.length > MAX_ROWS) tbody.removeChild(tbody.lastChild);
  document.getElementById('statAnomalies').textContent =
    (parseInt(document.getElementById('statAnomalies').textContent) || 0) + 1;
}

// ---------------------------------------------------------------- charts
function makeLineChart(ctx, labelsCfg) {
  return new Chart(ctx, {
    type: 'line',
    data: { labels: [], datasets: labelsCfg.map((c) => ({ ...c, data: [], tension: 0.3, borderWidth: 2, pointRadius: 0 })) },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      animation: false,
      scales: {
        x: { display: false },
        y: { grid: { color: '#232c38' }, ticks: { color: '#8593a3' } },
      },
      plugins: { legend: { labels: { color: '#e6edf3' } } },
    },
  });
}

const chartRate = makeLineChart(document.getElementById('chartRate'), [
  { label: 'Events / sec', borderColor: '#4fd1c5', backgroundColor: 'rgba(79,209,197,0.1)', fill: true },
]);
const chartLatency = makeLineChart(document.getElementById('chartLatency'), [
  { label: 'Latency (ms)', borderColor: '#eab308' },
]);
const chartCpuMem = makeLineChart(document.getElementById('chartCpuMem'), [
  { label: 'CPU %', borderColor: '#ef4444' },
  { label: 'Memory %', borderColor: '#38bdf8' },
]);
const chartErrors = makeLineChart(document.getElementById('chartErrors'), [
  { label: 'Error Rate %', borderColor: '#f97316' },
]);

function pushPoint(chart, label, values) {
  chart.data.labels.push(label);
  chart.data.datasets.forEach((ds, i) => ds.data.push(values[i]));
  if (chart.data.labels.length > MAX_CHART_POINTS) {
    chart.data.labels.shift();
    chart.data.datasets.forEach((ds) => ds.data.shift());
  }
  chart.update('none');
}

function updateCharts(ev) {
  const label = new Date(ev.timestamp * 1000).toLocaleTimeString();
  pushPoint(chartLatency, label, [ev.latency_ms]);
  pushPoint(chartCpuMem, label, [ev.cpu_percent, ev.memory_percent]);
  pushPoint(chartErrors, label, [ev.error_rate * 100]);
}

setInterval(() => {
  const now = Date.now();
  const seconds = (now - lastRateSample) / 1000;
  const rate = eventCountThisSecond / Math.max(seconds, 0.5);
  pushPoint(chartRate, new Date().toLocaleTimeString(), [rate]);
  document.getElementById('statEventsPerSec').textContent = rate.toFixed(1);
  eventCountThisSecond = 0;
  lastRateSample = now;
}, 1000);

// ---------------------------------------------------------------- polling
async function refreshOverview() {
  try {
    const [stats, health] = await Promise.all([
      fetch('/api/statistics').then((r) => r.json()),
      fetch('/health').then((r) => r.json()),
    ]);
    document.getElementById('statTotalEvents').textContent = stats.total_events ?? 0;
    document.getElementById('statActiveSources').textContent = stats.active_sources ?? 0;
    document.getElementById('statErrors').textContent = stats.recent_error_events_60s ?? 0;
    document.getElementById('statAnomalies').textContent = stats.total_anomalies ?? 0;
    const statusEl = document.getElementById('statSystemStatus');
    statusEl.textContent = health.status === 'ok' ? 'Healthy' : 'Degraded';
    statusEl.className = 'card-value ' + (health.status === 'ok' ? 'status-ok' : 'status-degraded');
  } catch (e) {
    console.error('overview refresh failed', e);
  }
}
setInterval(refreshOverview, 3000);
refreshOverview();

async function loadHistory() {
  try {
    const rows = await fetch('/api/aggregates?limit=100').then((r) => r.json());
    const tbody = document.querySelector('#historyTable tbody');
    tbody.innerHTML = '';
    rows.forEach((row) => {
      const tr = document.createElement('tr');
      const t = new Date(row.window_start * 1000).toLocaleString();
      tr.innerHTML = `<td>${t}</td><td>${row.source}</td><td>${row.event_count}</td>
        <td>${row.avg_cpu?.toFixed(1) ?? '-'}</td><td>${row.avg_memory?.toFixed(1) ?? '-'}</td>
        <td>${row.avg_latency?.toFixed(1) ?? '-'}</td><td>${(row.avg_error_rate * 100)?.toFixed(2) ?? '-'}</td>
        <td>${row.anomaly_count}</td>`;
      tbody.appendChild(tr);
    });
  } catch (e) {
    console.error('history load failed', e);
  }
}
document.getElementById('refreshHistory').addEventListener('click', loadHistory);

// -------------------------------------------------------------------- init
connectWebSocket();
