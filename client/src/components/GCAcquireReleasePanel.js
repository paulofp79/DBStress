import React, { useEffect, useMemo, useState } from 'react';
import { Line } from 'react-chartjs-2';
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Tooltip,
  Legend
} from 'chart.js';

ChartJS.register(
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Tooltip,
  Legend
);

const getServerUrl = () => {
  if (window.location.hostname !== 'localhost') {
    return `http://${window.location.host}`;
  }
  return 'http://localhost:3001';
};

const API_BASE = `${getServerUrl()}/api`;

const emptyMonitor = {
  activeSessions: [],
  waitRows: [],
  ashRows: []
};

const TRACKED_WAIT_EVENTS = [
  'gc buffer busy acquire',
  'gc buffer busy release',
  'gc current block busy',
  'gc cr block busy',
  'buffer busy waits',
  'enq: TX - allocate ITL entry',
  'enq: TX - row lock contention'
];

const WAIT_COLORS = [
  '#f59e0b',
  '#38bdf8',
  '#22c55e',
  '#ef4444',
  '#a78bfa',
  '#14b8a6'
];

function GCAcquireReleasePanel({ dbStatus, socket }) {
  const [validation, setValidation] = useState(null);
  const [setupRows, setSetupRows] = useState([]);
  const [busy, setBusy] = useState('');
  const [message, setMessage] = useState('');
  const [status, setStatus] = useState({
    isRunning: false,
    workerCount: 0,
    stats: {},
    logs: [],
    monitor: emptyMonitor
  });
  const [monitor, setMonitor] = useState(emptyMonitor);
  const [waitEventFilter, setWaitEventFilter] = useState('all');
  const [waitChartHistory, setWaitChartHistory] = useState({
    labels: [],
    series: {},
    lastTimestamp: null
  });
  const [confirmLaunch, setConfirmLaunch] = useState(false);
  const [config, setConfig] = useState({
    mode: 'one-instance',
    instance1ConnectionString: '',
    instance2ConnectionString: dbStatus.config?.connectionString || '',
    workers: 50,
    workersInstance1: 25,
    workersInstance2: 25,
    loopsPerWorker: 20000,
    commitEvery: 1,
    rowCount: 128,
    hotRowMin: 1,
    hotRowMax: 128,
    rowTargetMode: 'spread',
    workloadShape: 'insert-hot-index',
    monitorRefreshMs: 2000,
    killExistingSessions: true
  });

  useEffect(() => {
    if (dbStatus.config?.connectionString) {
      setConfig(prev => ({
        ...prev,
        instance2ConnectionString: prev.instance2ConnectionString || dbStatus.config.connectionString,
        instance1ConnectionString: prev.instance1ConnectionString || dbStatus.config.connectionString
      }));
    }
  }, [dbStatus.config?.connectionString]);

  const fetchStatus = async () => {
    try {
      const response = await fetch(`${API_BASE}/gc-acquire-release/status`);
      const data = await response.json();
      if (response.ok) {
        setStatus(data);
        if (data.monitor) {
          setMonitor(data.monitor);
        }
      }
    } catch (err) {
      // Ignore startup status failures.
    }
  };

  useEffect(() => {
    if (!dbStatus.connected) return;
    fetchStatus();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [dbStatus.connected]);

  useEffect(() => {
    if (!socket) return;
    const onStatus = (payload) => {
      setStatus(payload);
      if (payload.monitor) setMonitor(payload.monitor);
    };
    const onMonitor = (payload) => setMonitor(payload || emptyMonitor);
    const onLog = (entry) => {
      setStatus(prev => ({
        ...prev,
        logs: [entry, ...(prev.logs || [])].slice(0, 200)
      }));
    };

    socket.on('gc-ar-status', onStatus);
    socket.on('gc-ar-monitor', onMonitor);
    socket.on('gc-ar-log', onLog);
    return () => {
      socket.off('gc-ar-status', onStatus);
      socket.off('gc-ar-monitor', onMonitor);
      socket.off('gc-ar-log', onLog);
    };
  }, [socket]);

  useEffect(() => {
    if (!dbStatus.connected || status.isRunning) return undefined;
    const interval = setInterval(() => {
      fetch(`${API_BASE}/gc-acquire-release/monitor`)
        .then(res => res.json())
        .then(data => {
          if (data.success && data.monitor) setMonitor(data.monitor);
        })
        .catch(() => {});
    }, config.monitorRefreshMs);
    return () => clearInterval(interval);
  }, [dbStatus.connected, status.isRunning, config.monitorRefreshMs]);

  useEffect(() => {
    if (!monitor?.timestamp) return;

    const rows = monitor.waitRows || [];
    const label = new Date(monitor.timestamp).toLocaleTimeString([], {
      minute: '2-digit',
      second: '2-digit'
    });
    const countByEvent = rows.reduce((acc, row) => {
      const eventName = row.event || 'Unknown';
      acc[eventName] = (acc[eventName] || 0) + 1;
      return acc;
    }, {});
    const trackedEvents = Array.from(new Set([
      ...TRACKED_WAIT_EVENTS,
      ...Object.keys(countByEvent)
    ]));

    setWaitChartHistory(prev => {
      if (prev.lastTimestamp === monitor.timestamp) {
        return prev;
      }

      const nextLabels = [...prev.labels, label].slice(-60);
      const nextSeries = {};
      trackedEvents.forEach(eventName => {
        nextSeries[eventName] = [
          ...(prev.series[eventName] || []),
          countByEvent[eventName] || 0
        ].slice(-60);
      });

      return {
        labels: nextLabels,
        series: nextSeries,
        lastTimestamp: monitor.timestamp
      };
    });
  }, [monitor?.timestamp, monitor?.waitRows]);

  const updateConfig = (field, value) => {
    setConfig(prev => ({ ...prev, [field]: value }));
  };

  const updateNumber = (field, value, min, max) => {
    const parsed = Number(value);
    if (!Number.isFinite(parsed)) return;
    setConfig(prev => ({
      ...prev,
      [field]: Math.min(max, Math.max(min, Math.floor(parsed)))
    }));
  };

  const runAction = async (label, path, body = {}) => {
    setBusy(label);
    setMessage('');
    try {
      const response = await fetch(`${API_BASE}${path}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body)
      });
      const data = await response.json();
      if (!response.ok) {
        throw new Error(data.error || data.message || `${label} failed`);
      }
      setMessage(data.message || `${label} completed`);
      await fetchStatus();
      return data;
    } catch (err) {
      setMessage(`Error: ${err.message}`);
      return null;
    } finally {
      setBusy('');
    }
  };

  const handleValidate = async () => {
    const result = await runAction('Validate', '/gc-acquire-release/validate');
    if (result) {
      setValidation(result);
      setMessage(result.ready ? 'Connection validated for RAC monitor views' : 'Connection reached, but one or more required views failed validation');
    }
  };

  const handleSetup = async () => {
    const result = await runAction('Setup Lab', '/gc-acquire-release/setup', { rowCount: config.rowCount });
    if (result) {
      const distribution = result.distribution || [];
      setSetupRows(distribution);
      if (distribution.length > 0) {
        setConfig(prev => ({
          ...prev,
          hotRowMin: distribution[0].minId || prev.hotRowMin,
          hotRowMax: distribution[0].maxId || prev.hotRowMax
        }));
      }
    }
  };

  const handleStart = async () => {
    if (!confirmLaunch) {
      setMessage('Confirm this is an internal lab workload before launching.');
      return;
    }
    const payload = { ...config };
    if (payload.mode === 'one-instance') {
      delete payload.instance1ConnectionString;
      delete payload.workersInstance1;
      delete payload.workersInstance2;
    } else {
      delete payload.workers;
    }
    await runAction('Start Workload', '/gc-acquire-release/start', payload);
  };

  const handleStop = async () => {
    await runAction('Stop Workload', '/gc-acquire-release/stop', { kill: true, drainSeconds: 10 });
  };

  const handleCleanup = async () => {
    const result = await runAction('Cleanup Lab', '/gc-acquire-release/cleanup');
    if (result) setSetupRows([]);
  };

  const handleMonitor = async () => {
    setBusy('Monitor');
    try {
      const response = await fetch(`${API_BASE}/gc-acquire-release/monitor`);
      const data = await response.json();
      if (!response.ok) {
        throw new Error(data.error || 'Monitor refresh failed');
      }
      setMonitor(data.monitor || emptyMonitor);
      setMessage('Monitor refreshed');
    } catch (err) {
      setMessage(`Error: ${err.message}`);
    } finally {
      setBusy('');
    }
  };

  const totalWorkers = useMemo(() => {
    if (config.mode === 'two-instance') {
      return Number(config.workersInstance1 || 0) + Number(config.workersInstance2 || 0);
    }
    return Number(config.workers || 0);
  }, [config]);

  const monitorRows = monitor || emptyMonitor;
  const running = !!status.isRunning;
  const hasVisibleLabSessions = (monitorRows.activeSessions || []).length > 0;
  const canRun = dbStatus.connected && !busy;
  const waitRows = useMemo(() => monitorRows.waitRows || [], [monitorRows.waitRows]);
  const waitEventOptions = useMemo(() => (
    Array.from(new Set([
      ...TRACKED_WAIT_EVENTS,
      ...waitRows.map(row => row.event).filter(Boolean),
      ...Object.keys(waitChartHistory.series)
    ]))
  ), [waitRows, waitChartHistory.series]);
  const filteredWaitRows = waitEventFilter === 'all'
    ? waitRows
    : waitRows.filter(row => row.event === waitEventFilter);
  const chartEventNames = waitEventFilter === 'all'
    ? waitEventOptions
    : [waitEventFilter];
  const waitChartData = {
    labels: waitChartHistory.labels,
    datasets: chartEventNames.map((eventName, index) => ({
      label: eventName,
      data: waitChartHistory.series[eventName] || [],
      borderColor: WAIT_COLORS[index % WAIT_COLORS.length],
      backgroundColor: `${WAIT_COLORS[index % WAIT_COLORS.length]}33`,
      borderWidth: 2,
      tension: 0.3,
      pointRadius: 0,
      pointHoverRadius: 4,
      spanGaps: true
    }))
  };
  const waitChartOptions = {
    responsive: true,
    maintainAspectRatio: false,
    animation: { duration: 0 },
    plugins: {
      legend: {
        position: 'top',
        labels: {
          color: '#a0a0b0',
          boxWidth: 10,
          usePointStyle: true
        }
      },
      tooltip: {
        backgroundColor: 'rgba(26, 26, 46, 0.95)',
        titleColor: '#ffffff',
        bodyColor: '#a0a0b0',
        borderColor: '#2a2a45',
        borderWidth: 1
      }
    },
    scales: {
      x: {
        grid: { color: 'rgba(42, 42, 69, 0.45)' },
        ticks: { color: '#6b6b80', maxTicksLimit: 10 }
      },
      y: {
        beginAtZero: true,
        precision: 0,
        grid: { color: 'rgba(42, 42, 69, 0.45)' },
        ticks: { color: '#6b6b80', stepSize: 1 },
        title: {
          display: true,
          text: 'Current sessions waiting',
          color: '#6b6b80'
        }
      }
    }
  };

  const renderSessionTable = (rows, emptyText) => (
    <div className="gc-ar-table-wrap">
      <table className="gc-ar-table">
        <thead>
          <tr>
            <th>INST_ID</th>
            <th>SID</th>
            <th>EVENT</th>
            <th>USERNAME</th>
            <th>STATE</th>
            <th>WIS</th>
            <th>P1_P2_P3_TEXT</th>
            <th>MODULE</th>
            <th>ACTION</th>
            <th>SQL_ID</th>
          </tr>
        </thead>
        <tbody>
          {rows.length === 0 && (
            <tr>
              <td colSpan="10" className="gc-ar-empty">{emptyText}</td>
            </tr>
          )}
          {rows.map((row, index) => (
            <tr key={`${row.instId}-${row.sid}-${row.action}-${index}`}>
              <td>{row.instId}</td>
              <td>{row.sid}</td>
              <td>{row.event || '-'}</td>
              <td>{row.username || '-'}</td>
              <td>{row.state || '-'}</td>
              <td>{Number(row.waitSeconds || 0).toFixed(3)}</td>
              <td>{row.pText || '-'}</td>
              <td>{row.module || '-'}</td>
              <td>{row.action || '-'}</td>
              <td>{row.sqlId || '-'}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );

  if (!dbStatus.connected) {
    return (
      <div className="panel">
        <div className="panel-header">
          <h2>GC Acquire/Release Lab</h2>
        </div>
        <div className="panel-content">
          <p style={{ color: 'var(--text-muted)' }}>Connect to an Oracle RAC database first.</p>
        </div>
      </div>
    );
  }

  return (
    <div className="gc-ar-layout">
      <section className="gc-ar-sidebar">
        <h2>GC Acquire/Release Lab</h2>

        <div className="gc-ar-warning">
          This is a workload stress tool. Run it only in an internal lab where extra RAC traffic, row locks, and session kill operations are acceptable.
        </div>

        <div className="gc-ar-help">
          <strong>Right-growing inserts</strong> model the RAC hot-index pattern from sequence keys and compact index leaf blocks. <strong>Hot-block updates</strong> model frequent updates to rows in the same block, but can be masked by row locks.
        </div>

        <div className="gc-ar-section">
          <div className="gc-ar-section-title">Connection Validation</div>
          <button className="btn btn-secondary" disabled={!canRun} onClick={handleValidate}>
            Validate Views
          </button>
          {validation?.info && (
            <div className="gc-ar-kv">
              <div><span>Database</span><strong>{validation.info.DATABASE_NAME || '-'}</strong></div>
              <div><span>PDB</span><strong>{validation.info.PDB_NAME || '-'}</strong></div>
              <div><span>Instance</span><strong>{validation.info.INSTANCE_NAME || '-'}</strong></div>
              <div><span>Host</span><strong>{validation.info.HOST_NAME || '-'}</strong></div>
              <div><span>User</span><strong>{validation.info.USERNAME || '-'}</strong></div>
            </div>
          )}
          {validation?.permissions && (
            <div className="gc-ar-permissions">
              {validation.permissions.map(item => (
                <div key={item.object} className={item.ok ? 'ok' : 'bad'}>
                  {item.object}: {item.ok ? 'OK' : 'FAILED'}
                </div>
              ))}
            </div>
          )}
        </div>

        <div className="gc-ar-section">
          <div className="gc-ar-section-title">Lab Setup</div>
          <div className="form-row">
            <div className="form-group">
              <label>Row count / hot range max</label>
              <input type="number" min="1" max="1000" value={config.rowCount} onChange={(e) => updateNumber('rowCount', e.target.value, 1, 1000)} />
            </div>
          </div>
          <button className="btn btn-primary" disabled={!canRun || running} onClick={handleSetup}>
            Setup Lab
          </button>
        </div>

        <div className="gc-ar-section">
          <div className="gc-ar-section-title">Workload Mode</div>
          <div className="gc-ar-segment">
            <button className={config.mode === 'one-instance' ? 'active' : ''} onClick={() => updateConfig('mode', 'one-instance')} disabled={running}>One Instance Only</button>
            <button className={config.mode === 'two-instance' ? 'active' : ''} onClick={() => updateConfig('mode', 'two-instance')} disabled={running}>Acquire vs Release</button>
          </div>

          {config.mode === 'one-instance' ? (
            <>
              <div className="form-group">
                <label>Instance 2 service connect string</label>
                <input value={config.instance2ConnectionString} onChange={(e) => updateConfig('instance2ConnectionString', e.target.value)} />
              </div>
              <div className="form-row">
                <div className="form-group">
                  <label>Worker sessions</label>
                  <input type="number" min="1" max="1000" value={config.workers} onChange={(e) => updateNumber('workers', e.target.value, 1, 1000)} />
                </div>
                <div className="form-group gc-ar-inline-note">
                  <label>Target shape</label>
                  <div>Use spread mode and commit every 1 to reduce row-lock masking.</div>
                </div>
              </div>
            </>
          ) : (
            <>
              <div className="form-group">
                <label>Instance 1 service connect string</label>
                <input value={config.instance1ConnectionString} onChange={(e) => updateConfig('instance1ConnectionString', e.target.value)} />
              </div>
              <div className="form-group">
                <label>Instance 2 service connect string</label>
                <input value={config.instance2ConnectionString} onChange={(e) => updateConfig('instance2ConnectionString', e.target.value)} />
              </div>
              <div className="form-row">
                <div className="form-group">
                  <label>Workers inst 1</label>
                  <input type="number" min="0" max="1000" value={config.workersInstance1} onChange={(e) => updateNumber('workersInstance1', e.target.value, 0, 1000)} />
                </div>
                <div className="form-group">
                  <label>Workers inst 2</label>
                  <input type="number" min="0" max="1000" value={config.workersInstance2} onChange={(e) => updateNumber('workersInstance2', e.target.value, 0, 1000)} />
                </div>
              </div>
            </>
          )}

          <div className="form-row">
            <div className="form-group">
              <label>Loops per worker</label>
              <input type="number" min="1" max="1000000" value={config.loopsPerWorker} onChange={(e) => updateNumber('loopsPerWorker', e.target.value, 1, 1000000)} />
            </div>
            <div className="form-group">
              <label>Refresh ms</label>
              <input type="number" min="1000" max="30000" value={config.monitorRefreshMs} onChange={(e) => updateNumber('monitorRefreshMs', e.target.value, 1000, 30000)} />
            </div>
          </div>
          <div className="form-row">
            <div className="form-group">
              <label>Workload shape</label>
              <select value={config.workloadShape} onChange={(e) => updateConfig('workloadShape', e.target.value)}>
                <option value="insert-hot-index">Right-growing inserts</option>
                <option value="update-hot-block">Hot-block updates</option>
              </select>
            </div>
            <div className="form-group">
              <label>Row target mode</label>
              <select value={config.rowTargetMode} onChange={(e) => updateConfig('rowTargetMode', e.target.value)}>
                <option value="spread">Spread across hot rows</option>
                <option value="random">Random hot rows</option>
              </select>
            </div>
          </div>
          <div className="form-row">
            <div className="form-group">
              <label>Commit every N loops</label>
              <input type="number" min="1" max="10000" value={config.commitEvery} onChange={(e) => updateNumber('commitEvery', e.target.value, 1, 10000)} />
            </div>
            <div className="form-group gc-ar-inline-note">
              <label>GC wait tip</label>
              <div>Use two-instance mode plus right-growing inserts for the strongest GC busy signal.</div>
            </div>
          </div>
          <div className="form-row">
            <div className="form-group">
              <label>Hot row min</label>
              <input type="number" min="1" max={config.rowCount} value={config.hotRowMin} onChange={(e) => updateNumber('hotRowMin', e.target.value, 1, config.rowCount)} />
            </div>
            <div className="form-group">
              <label>Hot row max</label>
              <input type="number" min={config.hotRowMin} max={config.rowCount} value={config.hotRowMax} onChange={(e) => updateNumber('hotRowMax', e.target.value, config.hotRowMin, config.rowCount)} />
            </div>
          </div>

          <label className="gc-ar-confirm">
            <input type="checkbox" checked={confirmLaunch} onChange={(e) => setConfirmLaunch(e.target.checked)} />
            I confirm this will run only in an internal lab.
          </label>
          <label className="gc-ar-confirm">
            <input
              type="checkbox"
              checked={config.killExistingSessions}
              onChange={(e) => updateConfig('killExistingSessions', e.target.checked)}
            />
            Stop existing DBSTRESS_GC_AR sessions before starting.
          </label>
          <div className="gc-ar-actions">
            <button className="btn btn-success" disabled={!canRun || running || totalWorkers < 1} onClick={handleStart}>
              {config.mode === 'one-instance' ? 'Start One-Instance Workload' : 'Start Two-Instance Workload'}
            </button>
            <button className="btn btn-danger" disabled={(!running && !hasVisibleLabSessions) || !!busy} onClick={handleStop}>
              {running ? 'Stop Workload' : 'Stop Existing Sessions'}
            </button>
            <button className="btn btn-secondary" disabled={!canRun || running} onClick={handleCleanup}>
              Cleanup Lab
            </button>
          </div>
        </div>

        <div className="gc-ar-section">
          <div className="gc-ar-section-title">Worker Status</div>
          <div className="gc-ar-kv">
            <div><span>Running</span><strong>{running ? 'Yes' : 'No'}</strong></div>
            <div><span>Workers</span><strong>{status.workerCount || 0}</strong></div>
            <div><span>Loops</span><strong>{status.stats?.completedLoops || 0}</strong></div>
            <div><span>Commits</span><strong>{status.stats?.commits || 0}</strong></div>
            <div><span>Errors</span><strong>{status.stats?.errors || 0}</strong></div>
          </div>
          {message && (
            <div className={message.startsWith('Error') ? 'gc-ar-message error' : 'gc-ar-message'}>
              {busy ? `${busy}: ` : ''}{message}
            </div>
          )}
        </div>
      </section>

      <section className="gc-ar-main">
        <div className="gc-ar-topline">
          <div>
            <h3>Real-Time Monitor</h3>
            <span>Refresh target: {config.monitorRefreshMs / 1000}s | last sample: {monitorRows.timestamp ? new Date(monitorRows.timestamp).toLocaleTimeString() : '-'}</span>
          </div>
          <button className="btn btn-secondary" disabled={!canRun} onClick={handleMonitor}>
            Monitor
          </button>
        </div>

        {setupRows.length > 0 && (
          <div className="panel gc-ar-panel">
            <div className="panel-header"><h2>Row Distribution by File/Block</h2></div>
            <div className="panel-content">
              <div className="gc-ar-table-wrap">
                <table className="gc-ar-table compact">
                  <thead>
                    <tr>
                      <th>FILE#</th>
                      <th>BLOCK#</th>
                      <th>ROWS</th>
                      <th>MIN_ID</th>
                      <th>MAX_ID</th>
                    </tr>
                  </thead>
                  <tbody>
                    {setupRows.map(row => (
                      <tr key={`${row.fileNo}-${row.blockNo}`}>
                        <td>{row.fileNo}</td>
                        <td>{row.blockNo}</td>
                        <td>{row.rowsInBlock}</td>
                        <td>{row.minId}</td>
                        <td>{row.maxId}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          </div>
        )}

        <div className="panel gc-ar-panel">
          <div className="panel-header"><h2>Active Lab Sessions by Instance</h2></div>
          <div className="panel-content">
            {renderSessionTable(monitorRows.activeSessions || [], 'No DBSTRESS_GC_AR sessions are active.')}
          </div>
        </div>

        <div className="panel gc-ar-panel">
          <div className="panel-header gc-ar-chart-header">
            <h2>Current GC / Buffer Wait Rows</h2>
            <div className="gc-ar-filter">
              <label>Wait event</label>
              <select value={waitEventFilter} onChange={(e) => setWaitEventFilter(e.target.value)}>
                <option value="all">All wait events</option>
                {waitEventOptions.map(eventName => (
                  <option key={eventName} value={eventName}>{eventName}</option>
                ))}
              </select>
            </div>
          </div>
          <div className="panel-content">
            <div className="gc-ar-chart">
              <Line data={waitChartData} options={waitChartOptions} />
            </div>
            {renderSessionTable(filteredWaitRows, 'No current waits match the selected wait event.')}
          </div>
        </div>

        <div className="panel gc-ar-panel">
          <div className="panel-header"><h2>Recent ASH Samples, Last 30 Seconds</h2></div>
          <div className="panel-content">
            <div className="gc-ar-table-wrap">
              <table className="gc-ar-table">
                <thead>
                  <tr>
                    <th>INST_ID</th>
                    <th>EVENT</th>
                    <th>P1_P2_P3_TEXT</th>
                    <th>MODULE</th>
                    <th>ACTION</th>
                    <th>SQL_ID</th>
                    <th>SAMPLE_COUNT</th>
                    <th>SESSION_COUNT</th>
                  </tr>
                </thead>
                <tbody>
                  {(monitorRows.ashRows || []).length === 0 && (
                    <tr><td colSpan="8" className="gc-ar-empty">No recent ASH samples for the lab wait events.</td></tr>
                  )}
                  {(monitorRows.ashRows || []).map((row, index) => (
                    <tr key={`${row.instId}-${row.event}-${row.action}-${index}`}>
                      <td>{row.instId}</td>
                      <td>{row.event || '-'}</td>
                      <td>{row.pText || '-'}</td>
                      <td>{row.module || '-'}</td>
                      <td>{row.action || '-'}</td>
                      <td>{row.sqlId || '-'}</td>
                      <td>{row.sampleCount}</td>
                      <td>{row.sessionCount}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        </div>

        <div className="panel gc-ar-panel">
          <div className="panel-header"><h2>Worker Logs</h2></div>
          <div className="panel-content">
            <div className="gc-ar-log">
              {(status.logs || []).length === 0 && <div className="gc-ar-empty">No worker logs yet.</div>}
              {(status.logs || []).map((entry, index) => (
                <div key={`${entry.ts}-${index}`} className={entry.level}>
                  <span>{new Date(entry.ts).toLocaleTimeString()}</span>
                  <strong>{entry.level}</strong>
                  <em>{entry.message}</em>
                </div>
              ))}
            </div>
          </div>
        </div>
      </section>
    </div>
  );
}

export default GCAcquireReleasePanel;
