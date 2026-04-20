import React, { useEffect, useRef, useState } from 'react';
import { Bar, Line } from 'react-chartjs-2';
import {
  BarElement,
  CategoryScale,
  Chart as ChartJS,
  Filler,
  Legend,
  LinearScale,
  LineElement,
  PointElement,
  Title,
  Tooltip
} from 'chart.js';

ChartJS.register(
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  BarElement,
  Title,
  Tooltip,
  Legend,
  Filler
);

const getServerUrl = () => {
  if (window.location.hostname !== 'localhost') {
    return `http://${window.location.host}`;
  }
  return 'http://localhost:3001';
};

const API_BASE = `${getServerUrl()}/api`;
const LOCAL_STORAGE_KEY = 'dbstress-library-cache-lock-runs';

const DEFAULT_PROCEDURE_SQL = `CREATE OR REPLACE NONEDITIONABLE PROCEDURE GRAV_SESSION_MFES_ONLINE (
  pModuleName VARCHAR2
)
IS
  pActionName     VARCHAR2(14);
  pModuleName_mod VARCHAR2(48);
BEGIN
  pActionName := 'MFES_ONLINE';

  pModuleName_mod := SUBSTR(pModuleName, 1, 22) || '0000000' || SUBSTR(pModuleName, 30);

  DBMS_APPLICATION_INFO.SET_MODULE(pModuleName_mod, pActionName);
  DBMS_SESSION.SET_IDENTIFIER(pModuleName);

  EXECUTE IMMEDIATE 'ALTER SESSION SET OPTIMIZER_MODE = first_rows_1';
  EXECUTE IMMEDIATE 'ALTER SESSION SET "_optimizer_use_feedback" = false';
  EXECUTE IMMEDIATE 'ALTER SESSION SET "_optimizer_adaptive_cursor_sharing" = false';
  EXECUTE IMMEDIATE 'ALTER SESSION SET "_optimizer_extended_cursor_sharing_rel" = none';
END;
/`;

const KEY_WAIT_ORDER = [
  'library cache: mutex X',
  'latch: ges resource hash list',
  'gc current block congested',
  'gc cr failure',
  'gc buffer busy acquire',
  'cursor: mutex X',
  'cursor: pin S wait on X',
  'library cache lock'
];

const cardStyle = {
  background: 'var(--surface)',
  border: '1px solid var(--border)',
  borderRadius: '12px',
  padding: '1rem'
};

const chartOptions = {
  responsive: true,
  maintainAspectRatio: false,
  animation: { duration: 0 },
  plugins: {
    legend: { display: false }
  },
  scales: {
    x: {
      grid: { color: 'rgba(255,255,255,0.08)' },
      ticks: { color: '#9ca3af', maxTicksLimit: 8 }
    },
    y: {
      beginAtZero: true,
      grid: { color: 'rgba(255,255,255,0.08)' },
      ticks: { color: '#9ca3af' }
    }
  }
};

const waitChartOptions = {
  indexAxis: 'y',
  responsive: true,
  maintainAspectRatio: false,
  animation: { duration: 0 },
  plugins: {
    legend: { display: false }
  },
  scales: {
    x: {
      beginAtZero: true,
      grid: { color: 'rgba(255,255,255,0.08)' },
      ticks: { color: '#9ca3af' }
    },
    y: {
      grid: { display: false },
      ticks: { color: '#cbd5e1' }
    }
  }
};

const emptyMetrics = {
  runId: null,
  totalCalls: 0,
  errors: 0,
  callsPerSecond: 0,
  avgLatencyMs: 0,
  durationSeconds: 0,
  latestSample: null,
  lastError: null
};

const formatNumber = (value, digits = 0) => {
  const num = Number(value || 0);
  return num.toLocaleString(undefined, {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits
  });
};

const formatDateTime = (value) => {
  if (!value) return '-';
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? '-' : date.toLocaleString();
};

const buildWaitMap = (summary) => {
  const map = new Map();
  (summary?.keyWaits || []).forEach((wait) => {
    map.set(wait.event, wait);
  });
  return map;
};

function MetricCard({ title, value, hint, accent }) {
  return (
    <div style={{
      ...cardStyle,
      minHeight: '110px',
      background: `linear-gradient(180deg, rgba(15,23,42,0.96), rgba(30,41,59,0.96)), ${accent || 'var(--surface)'}`
    }}>
      <div style={{ color: 'var(--text-muted)', fontSize: '0.75rem', textTransform: 'uppercase', letterSpacing: '0.08em' }}>
        {title}
      </div>
      <div style={{ fontSize: '1.7rem', fontWeight: 700, marginTop: '0.35rem' }}>{value}</div>
      <div style={{ fontSize: '0.82rem', color: 'var(--text-secondary)', marginTop: '0.35rem' }}>{hint}</div>
    </div>
  );
}

function LibraryCacheLockPanel({ dbStatus, socket }) {
  const [config, setConfig] = useState({
    threads: 96,
    loopDelay: 0,
    moduleLength: 42,
    procedureOwner: '',
    procedureName: 'GRAV_SESSION_MFES_ONLINE',
    modulePrefix: 'MFES',
    runLabel: 'Baseline'
  });
  const [procedureSql, setProcedureSql] = useState(DEFAULT_PROCEDURE_SQL);
  const [isInstalling, setIsInstalling] = useState(false);
  const [isRunning, setIsRunning] = useState(false);
  const [statusMessage, setStatusMessage] = useState('');
  const [metrics, setMetrics] = useState(emptyMetrics);
  const [sample, setSample] = useState(null);
  const [latestSummary, setLatestSummary] = useState(null);
  const [savedRuns, setSavedRuns] = useState([]);
  const [compareRunA, setCompareRunA] = useState('');
  const [compareRunB, setCompareRunB] = useState('');

  const [labels, setLabels] = useState([]);
  const [callsHistory, setCallsHistory] = useState([]);
  const [latencyHistory, setLatencyHistory] = useState([]);
  const [aasHistory, setAasHistory] = useState([]);
  const [cpuShareHistory, setCpuShareHistory] = useState([]);

  const maxDataPoints = 60;
  const lastSampleAtRef = useRef(null);

  const addOrUpdateSavedRun = (runSummary) => {
    if (!runSummary?.runId) return;

    setSavedRuns((prev) => {
      const next = [runSummary, ...prev.filter((item) => item.runId !== runSummary.runId)].slice(0, 12);
      localStorage.setItem(LOCAL_STORAGE_KEY, JSON.stringify(next));
      return next;
    });
  };

  useEffect(() => {
    try {
      const stored = JSON.parse(localStorage.getItem(LOCAL_STORAGE_KEY) || '[]');
      if (Array.isArray(stored)) {
        setSavedRuns(stored);
      }
    } catch (err) {
      // Ignore corrupted local storage.
    }
  }, []);

  useEffect(() => {
    if (!socket) return undefined;

    const onMetrics = (data) => {
      setMetrics({
        runId: data.runId || null,
        totalCalls: data.totalCalls || 0,
        errors: data.errors || 0,
        callsPerSecond: data.callsPerSecond || 0,
        avgLatencyMs: data.avgLatencyMs || 0,
        durationSeconds: data.durationSeconds || 0,
        latestSample: data.latestSample || null,
        lastError: data.lastError || null
      });

      if (data.latestSample?.capturedAt && data.latestSample.capturedAt !== lastSampleAtRef.current) {
        lastSampleAtRef.current = data.latestSample.capturedAt;
        const stamp = new Date(data.latestSample.capturedAt);
        const label = `${stamp.getHours().toString().padStart(2, '0')}:${stamp.getMinutes().toString().padStart(2, '0')}:${stamp.getSeconds().toString().padStart(2, '0')}`;

        setLabels((prev) => [...prev, label].slice(-maxDataPoints));
        setCallsHistory((prev) => [...prev, data.callsPerSecond || 0].slice(-maxDataPoints));
        setLatencyHistory((prev) => [...prev, data.avgLatencyMs || 0].slice(-maxDataPoints));
        setAasHistory((prev) => [...prev, data.latestSample.averageActiveSessions || 0].slice(-maxDataPoints));
        setCpuShareHistory((prev) => [...prev, data.latestSample.dbCpuSharePct || 0].slice(-maxDataPoints));
      }
    };

    const onStatus = (data) => {
      setStatusMessage(data.message || '');
      if (typeof data.running === 'boolean') {
        setIsRunning(data.running);
      }
    };

    const onWaits = (data) => {
      setSample(data.sample || null);
    };

    const onStopped = (data) => {
      setIsRunning(false);
      setStatusMessage('Stopped');
      if (data.summary) {
        setLatestSummary(data.summary);
        setSample(data.summary);
        addOrUpdateSavedRun(data.summary);
      }
    };

    socket.on('library-cache-lock-metrics', onMetrics);
    socket.on('library-cache-lock-status', onStatus);
    socket.on('library-cache-lock-wait-events', onWaits);
    socket.on('library-cache-lock-stopped', onStopped);

    return () => {
      socket.off('library-cache-lock-metrics', onMetrics);
      socket.off('library-cache-lock-status', onStatus);
      socket.off('library-cache-lock-wait-events', onWaits);
      socket.off('library-cache-lock-stopped', onStopped);
    };
  }, [socket]);

  useEffect(() => {
    const loadStatus = async () => {
      try {
        const response = await fetch(`${API_BASE}/library-cache-lock/status`);
        const data = await response.json();
        setIsRunning(!!data.isRunning);
        if (data.lastRunSummary) {
          setLatestSummary(data.lastRunSummary);
          setSample(data.lastRunSummary);
        }
      } catch (err) {
        // Ignore status bootstrap failures.
      }
    };

    loadStatus();
  }, []);

  useEffect(() => {
    if (!compareRunA && savedRuns[0]) {
      setCompareRunA(savedRuns[0].runId);
    }
    if (!compareRunB && savedRuns[1]) {
      setCompareRunB(savedRuns[1].runId);
    }
  }, [savedRuns, compareRunA, compareRunB]);

  const handleInstallProcedure = async () => {
    try {
      setIsInstalling(true);
      setStatusMessage('Compiling procedure...');

      const response = await fetch(`${API_BASE}/library-cache-lock/install-procedure`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ sqlText: procedureSql })
      });

      const data = await response.json();
      if (!response.ok) {
        const compileErrors = (data.compileErrors || [])
          .map((err) => `line ${err.line}:${err.position} ${err.text}`)
          .join(' | ');
        throw new Error(compileErrors || data.error || 'Compilation failed');
      }

      setStatusMessage(data.message || 'Procedure compiled successfully');
    } catch (err) {
      setStatusMessage(`Compile error: ${err.message}`);
    } finally {
      setIsInstalling(false);
    }
  };

  const handleStart = async () => {
    try {
      setIsRunning(true);
      setStatusMessage('Starting workload...');
      setMetrics(emptyMetrics);
      setSample(null);
      setLatestSummary(null);
      lastSampleAtRef.current = null;
      setLabels([]);
      setCallsHistory([]);
      setLatencyHistory([]);
      setAasHistory([]);
      setCpuShareHistory([]);

      const response = await fetch(`${API_BASE}/library-cache-lock/start`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(config)
      });

      const data = await response.json();
      if (!response.ok) {
        throw new Error(data.error || 'Failed to start workload');
      }

      setStatusMessage(data.message || 'Running');
    } catch (err) {
      setIsRunning(false);
      setStatusMessage(`Start error: ${err.message}`);
    }
  };

  const handleStop = async () => {
    try {
      setStatusMessage('Stopping workload...');
      const response = await fetch(`${API_BASE}/library-cache-lock/stop`, {
        method: 'POST'
      });
      const data = await response.json();
      if (!response.ok) {
        throw new Error(data.error || 'Failed to stop workload');
      }

      setIsRunning(false);
      if (data.summary) {
        setLatestSummary(data.summary);
        setSample(data.summary);
        addOrUpdateSavedRun(data.summary);
      }
      setStatusMessage('Stopped');
    } catch (err) {
      setStatusMessage(`Stop error: ${err.message}`);
    }
  };

  const handleReset = () => {
    setMetrics(emptyMetrics);
    setSample(null);
    setLatestSummary(null);
    setLabels([]);
    setCallsHistory([]);
    setLatencyHistory([]);
    setAasHistory([]);
    setCpuShareHistory([]);
    lastSampleAtRef.current = null;
    setStatusMessage('Charts and current run summary cleared');
  };

  const currentWaits = sample?.keyWaits || latestSummary?.keyWaits || [];
  const currentTopWaits = sample?.topWaitEvents || latestSummary?.topWaitEvents || [];
  const currentMatchedSql = latestSummary?.matchedSql || sample?.matchedSql || [];

  const lineData = (label, data, color, fillColor) => ({
    labels,
    datasets: [{
      label,
      data,
      borderColor: color,
      backgroundColor: fillColor,
      fill: true,
      tension: 0.3,
      pointRadius: 0
    }]
  });

  const compareSummaryA = savedRuns.find((run) => run.runId === compareRunA) || null;
  const compareSummaryB = savedRuns.find((run) => run.runId === compareRunB) || null;
  const compareWaitMapA = buildWaitMap(compareSummaryA);
  const compareWaitMapB = buildWaitMap(compareSummaryB);

  const compareMetrics = compareSummaryA && compareSummaryB
    ? [
        ['Calls/sec', compareSummaryA.callsPerSecond, compareSummaryB.callsPerSecond, 2],
        ['Avg latency ms', compareSummaryA.avgLatencyMs, compareSummaryB.avgLatencyMs, 2],
        ['AAS', compareSummaryA.averageActiveSessions, compareSummaryB.averageActiveSessions, 2],
        ['DB CPU share %', compareSummaryA.dbCpuSharePct, compareSummaryB.dbCpuSharePct, 2],
        ['Hard parses/sec', compareSummaryA.parseHardPerSecond, compareSummaryB.parseHardPerSecond, 2],
        ['Commits/sec', compareSummaryA.commitRatePerSecond, compareSummaryB.commitRatePerSecond, 2]
      ]
    : [];

  const waitComparisonRows = compareSummaryA && compareSummaryB
    ? KEY_WAIT_ORDER.map((eventName) => {
        const a = compareWaitMapA.get(eventName) || { timeWaitedSeconds: 0, totalWaits: 0 };
        const b = compareWaitMapB.get(eventName) || { timeWaitedSeconds: 0, totalWaits: 0 };
        return {
          event: eventName,
          aTime: a.timeWaitedSeconds || 0,
          bTime: b.timeWaitedSeconds || 0,
          delta: (b.timeWaitedSeconds || 0) - (a.timeWaitedSeconds || 0)
        };
      })
    : [];

  if (!dbStatus.connected) {
    return (
      <div className="panel" style={{ height: '100%' }}>
        <div className="panel-header">
          <h2>Library Cache Lock</h2>
        </div>
        <div className="panel-content">
          <p style={{ color: 'var(--text-muted)' }}>Connect to the database first.</p>
        </div>
      </div>
    );
  }

  return (
    <div style={{ display: 'flex', gap: '1rem', height: '100%', padding: '1rem', alignItems: 'flex-start' }}>
      <div style={{
        width: '360px',
        flexShrink: 0,
        display: 'flex',
        flexDirection: 'column',
        gap: '1rem',
        maxHeight: 'calc(100vh - 160px)',
        overflowY: 'auto'
      }}>
        <div style={cardStyle}>
          <h2 style={{ margin: 0, fontSize: '1.15rem' }}>Library Cache Lock</h2>
          <p style={{ marginTop: '0.55rem', color: 'var(--text-secondary)', fontSize: '0.9rem' }}>
            Replays high-concurrency calls to <code>{config.procedureName}</code> and captures run-scoped deltas for mutex, latch, and GC waits.
          </p>
          <div style={{
            marginTop: '0.85rem',
            padding: '0.85rem',
            borderRadius: '10px',
            background: 'linear-gradient(135deg, rgba(239,68,68,0.14), rgba(59,130,246,0.10))',
            border: '1px solid rgba(239,68,68,0.22)',
            fontSize: '0.82rem',
            color: 'var(--text-secondary)'
          }}>
            Tracks the same family of symptoms you described: <code>library cache: mutex X</code>, <code>latch: ges resource hash list</code>, <code>gc current block congested</code>, <code>gc cr failure</code>, and <code>gc buffer busy acquire</code>.
          </div>
        </div>

        <div style={cardStyle}>
          <div className="form-group">
            <label>Procedure SQL</label>
            <textarea
              value={procedureSql}
              onChange={(e) => setProcedureSql(e.target.value)}
              disabled={isRunning || isInstalling}
              style={{ minHeight: '280px' }}
            />
          </div>

          <button
            className="btn btn-secondary"
            type="button"
            onClick={handleInstallProcedure}
            disabled={isRunning || isInstalling}
            style={{ width: '100%' }}
          >
            {isInstalling ? 'Compiling...' : 'Compile Procedure'}
          </button>
        </div>

        <div style={cardStyle}>
          <div className="form-group">
            <label>Run Label</label>
            <input
              value={config.runLabel}
              onChange={(e) => setConfig((prev) => ({ ...prev, runLabel: e.target.value }))}
              disabled={isRunning}
            />
          </div>

          <div className="form-group">
            <label>Procedure Owner (optional)</label>
            <input
              value={config.procedureOwner}
              onChange={(e) => setConfig((prev) => ({ ...prev, procedureOwner: e.target.value.toUpperCase() }))}
              disabled={isRunning}
              placeholder="Current schema if blank"
            />
          </div>

          <div className="form-group">
            <label>Procedure Name</label>
            <input
              value={config.procedureName}
              onChange={(e) => setConfig((prev) => ({ ...prev, procedureName: e.target.value.toUpperCase() }))}
              disabled={isRunning}
            />
          </div>

          <div className="form-row">
            <div className="form-group">
              <label>Concurrent Sessions</label>
              <input
                type="number"
                min="1"
                max="500"
                value={config.threads}
                onChange={(e) => setConfig((prev) => ({ ...prev, threads: Math.max(1, Number.parseInt(e.target.value || '1', 10)) }))}
                disabled={isRunning}
              />
            </div>

            <div className="form-group">
              <label>Loop Delay (ms)</label>
              <input
                type="number"
                min="0"
                max="5000"
                value={config.loopDelay}
                onChange={(e) => setConfig((prev) => ({ ...prev, loopDelay: Math.max(0, Number.parseInt(e.target.value || '0', 10)) }))}
                disabled={isRunning}
              />
            </div>
          </div>

          <div className="form-row">
            <div className="form-group">
              <label>Module Prefix</label>
              <input
                value={config.modulePrefix}
                onChange={(e) => setConfig((prev) => ({ ...prev, modulePrefix: e.target.value.toUpperCase() }))}
                disabled={isRunning}
              />
            </div>

            <div className="form-group">
              <label>Module Length</label>
              <input
                type="number"
                min="30"
                max="96"
                value={config.moduleLength}
                onChange={(e) => setConfig((prev) => ({ ...prev, moduleLength: Math.max(30, Number.parseInt(e.target.value || '30', 10)) }))}
                disabled={isRunning}
              />
            </div>
          </div>

          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '0.75rem' }}>
            <button className="btn btn-danger" type="button" onClick={handleStart} disabled={isRunning || isInstalling}>
              Start Workload
            </button>
            <button className="btn btn-secondary" type="button" onClick={handleStop} disabled={!isRunning}>
              Stop
            </button>
          </div>

          <button
            className="btn btn-secondary"
            type="button"
            onClick={handleReset}
            disabled={isRunning}
            style={{ width: '100%', marginTop: '0.75rem' }}
          >
            Clear Current Charts
          </button>

          <div style={{
            marginTop: '0.9rem',
            padding: '0.8rem',
            borderRadius: '10px',
            background: 'rgba(15,23,42,0.55)',
            border: '1px solid var(--border)'
          }}>
            <div style={{ fontSize: '0.75rem', textTransform: 'uppercase', color: 'var(--text-muted)', letterSpacing: '0.08em' }}>
              Status
            </div>
            <div style={{ marginTop: '0.35rem', color: isRunning ? '#fca5a5' : 'var(--text-secondary)' }}>
              {statusMessage || 'Idle'}
            </div>
            <div style={{ marginTop: '0.45rem', fontSize: '0.82rem', color: 'var(--text-secondary)' }}>
              Duration: {formatNumber(metrics.durationSeconds, 0)} sec
            </div>
            {metrics.lastError && (
              <div style={{ marginTop: '0.45rem', fontSize: '0.8rem', color: '#fca5a5' }}>
                Last error: {metrics.lastError}
              </div>
            )}
          </div>
        </div>
      </div>

      <div style={{ flex: 1, display: 'grid', gap: '1rem' }}>
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))', gap: '1rem' }}>
          <MetricCard title="Calls / Sec" value={formatNumber(metrics.callsPerSecond, 2)} hint="Application-side execution rate" />
          <MetricCard title="Avg Latency" value={`${formatNumber(metrics.avgLatencyMs, 2)} ms`} hint="Average call time for recent executions" />
          <MetricCard title="AAS" value={formatNumber(sample?.averageActiveSessions || latestSummary?.averageActiveSessions, 2)} hint="DB time / elapsed time for the sampled interval" />
          <MetricCard title="DB CPU Share" value={`${formatNumber(sample?.dbCpuSharePct || latestSummary?.dbCpuSharePct, 2)}%`} hint="How much of DB time remained on CPU" />
          <MetricCard title="Hard Parses / Sec" value={formatNumber(sample?.parseHardPerSecond || latestSummary?.parseHardPerSecond, 2)} hint="Useful to spot parse storms around the procedure" />
          <MetricCard title="Commits / Sec" value={formatNumber(sample?.commitRatePerSecond || latestSummary?.commitRatePerSecond, 2)} hint="Environment transaction rate during the run" />
        </div>

        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(320px, 1fr))', gap: '1rem' }}>
          <div style={cardStyle}>
            <div style={{ marginBottom: '0.75rem', fontWeight: 600 }}>Calls per Second</div>
            <div style={{ height: '240px' }}>
              <Line data={lineData('Calls/sec', callsHistory, '#f97316', 'rgba(249,115,22,0.18)')} options={chartOptions} />
            </div>
          </div>

          <div style={cardStyle}>
            <div style={{ marginBottom: '0.75rem', fontWeight: 600 }}>Latency</div>
            <div style={{ height: '240px' }}>
              <Line data={lineData('Latency', latencyHistory, '#ef4444', 'rgba(239,68,68,0.18)')} options={chartOptions} />
            </div>
          </div>

          <div style={cardStyle}>
            <div style={{ marginBottom: '0.75rem', fontWeight: 600 }}>Average Active Sessions</div>
            <div style={{ height: '240px' }}>
              <Line data={lineData('AAS', aasHistory, '#38bdf8', 'rgba(56,189,248,0.16)')} options={chartOptions} />
            </div>
          </div>

          <div style={cardStyle}>
            <div style={{ marginBottom: '0.75rem', fontWeight: 600 }}>DB CPU Share</div>
            <div style={{ height: '240px' }}>
              <Line data={lineData('DB CPU %', cpuShareHistory, '#22c55e', 'rgba(34,197,94,0.16)')} options={chartOptions} />
            </div>
          </div>
        </div>

        <div style={{ display: 'grid', gridTemplateColumns: 'minmax(360px, 1.2fr) minmax(280px, 0.8fr)', gap: '1rem' }}>
          <div style={cardStyle}>
            <div style={{ display: 'flex', justifyContent: 'space-between', gap: '1rem', marginBottom: '0.75rem' }}>
              <div style={{ fontWeight: 600 }}>Top Waits for This Run</div>
              <div style={{ color: 'var(--text-muted)', fontSize: '0.82rem' }}>
                Delta time waited, not cumulative instance totals
              </div>
            </div>
            <div style={{ height: '320px' }}>
              <Bar
                data={{
                  labels: currentTopWaits.map((wait) => wait.event.length > 38 ? `${wait.event.slice(0, 38)}...` : wait.event),
                  datasets: [{
                    label: 'Time waited (s)',
                    data: currentTopWaits.map((wait) => wait.timeWaitedSeconds || 0),
                    backgroundColor: 'rgba(239,68,68,0.72)',
                    borderColor: '#ef4444',
                    borderWidth: 1
                  }]
                }}
                options={waitChartOptions}
              />
            </div>
          </div>

          <div style={cardStyle}>
            <div style={{ fontWeight: 600, marginBottom: '0.75rem' }}>Tracked Wait Events</div>
            <div style={{ display: 'grid', gap: '0.6rem' }}>
              {KEY_WAIT_ORDER.map((eventName) => {
                const wait = currentWaits.find((item) => item.event === eventName) || {
                  timeWaitedSeconds: 0,
                  totalWaits: 0
                };
                return (
                  <div
                    key={eventName}
                    style={{
                      padding: '0.75rem',
                      borderRadius: '10px',
                      border: '1px solid var(--border)',
                      background: 'rgba(15,23,42,0.45)'
                    }}
                  >
                    <div style={{ fontSize: '0.85rem', color: 'var(--text-secondary)' }}>{eventName}</div>
                    <div style={{ marginTop: '0.35rem', fontSize: '1.1rem', fontWeight: 700 }}>
                      {formatNumber(wait.timeWaitedSeconds, 3)} s
                    </div>
                    <div style={{ fontSize: '0.78rem', color: 'var(--text-muted)' }}>
                      waits: {formatNumber(wait.totalWaits, 0)}
                    </div>
                  </div>
                );
              })}
            </div>
          </div>
        </div>

        <div style={{ display: 'grid', gridTemplateColumns: 'minmax(360px, 1fr) minmax(320px, 1fr)', gap: '1rem' }}>
          <div style={cardStyle}>
            <div style={{ fontWeight: 600, marginBottom: '0.75rem' }}>Latest Run Summary</div>
            {latestSummary ? (
              <div style={{ display: 'grid', gap: '0.55rem', color: 'var(--text-secondary)', fontSize: '0.9rem' }}>
                <div><strong style={{ color: 'white' }}>{latestSummary.runLabel}</strong> on <code>{latestSummary.qualifiedProcedure}</code></div>
                <div>Started: {formatDateTime(latestSummary.startedAt)}</div>
                <div>Finished: {formatDateTime(latestSummary.completedAt)}</div>
                <div>Total calls: {formatNumber(latestSummary.totalCalls, 0)}</div>
                <div>Errors: {formatNumber(latestSummary.errors, 0)}</div>
                <div>User calls/sec: {formatNumber(latestSummary.userCallsPerSecond, 2)}</div>
                <div>Execute count/sec: {formatNumber(latestSummary.executeCountPerSecond, 2)}</div>
                <div>DB time: {formatNumber(latestSummary.dbTimeSeconds, 3)} s</div>
                <div>DB CPU: {formatNumber(latestSummary.dbCpuSeconds, 3)} s</div>
              </div>
            ) : (
              <div style={{ color: 'var(--text-muted)' }}>Stop a run to capture a compareable summary snapshot.</div>
            )}
          </div>

          <div style={cardStyle}>
            <div style={{ fontWeight: 600, marginBottom: '0.75rem' }}>Matched SQL</div>
            {currentMatchedSql.length > 0 ? (
              <div style={{ display: 'grid', gap: '0.75rem' }}>
                {currentMatchedSql.map((sql) => (
                  <div key={sql.sqlId} style={{ padding: '0.75rem', borderRadius: '10px', background: 'rgba(15,23,42,0.45)', border: '1px solid var(--border)' }}>
                    <div style={{ fontWeight: 600 }}>{sql.sqlId}</div>
                    <div style={{ marginTop: '0.35rem', color: 'var(--text-secondary)', fontSize: '0.82rem' }}>{sql.sqlText}</div>
                    <div style={{ marginTop: '0.45rem', fontSize: '0.8rem', color: 'var(--text-muted)' }}>
                      execs {formatNumber(sql.executions, 0)} | elapsed {formatNumber(sql.elapsedSeconds, 3)} s | CPU {formatNumber(sql.cpuSeconds, 3)} s
                    </div>
                  </div>
                ))}
              </div>
            ) : (
              <div style={{ color: 'var(--text-muted)' }}>Run the workload to capture SQL rows that include the procedure call text.</div>
            )}
          </div>
        </div>

        <div style={cardStyle}>
          <div style={{ display: 'flex', justifyContent: 'space-between', gap: '1rem', alignItems: 'center', marginBottom: '1rem' }}>
            <div>
              <div style={{ fontWeight: 600 }}>Baseline vs Candidate Comparison</div>
              <div style={{ color: 'var(--text-muted)', fontSize: '0.82rem' }}>
                Save one run as baseline, change the procedure, run again, and compare wait deltas directly here.
              </div>
            </div>
            <div style={{ color: 'var(--text-muted)', fontSize: '0.82rem' }}>
              Saved runs: {savedRuns.length}
            </div>
          </div>

          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))', gap: '1rem', marginBottom: '1rem' }}>
            <div className="form-group" style={{ marginBottom: 0 }}>
              <label>Baseline Run</label>
              <select value={compareRunA} onChange={(e) => setCompareRunA(e.target.value)}>
                <option value="">Select run</option>
                {savedRuns.map((run) => (
                  <option key={run.runId} value={run.runId}>
                    {run.runLabel} - {new Date(run.completedAt).toLocaleTimeString()}
                  </option>
                ))}
              </select>
            </div>
            <div className="form-group" style={{ marginBottom: 0 }}>
              <label>Candidate Run</label>
              <select value={compareRunB} onChange={(e) => setCompareRunB(e.target.value)}>
                <option value="">Select run</option>
                {savedRuns.map((run) => (
                  <option key={run.runId} value={run.runId}>
                    {run.runLabel} - {new Date(run.completedAt).toLocaleTimeString()}
                  </option>
                ))}
              </select>
            </div>
          </div>

          {compareSummaryA && compareSummaryB ? (
            <div style={{ display: 'grid', gap: '1rem' }}>
              <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))', gap: '0.75rem' }}>
                {compareMetrics.map(([title, valueA, valueB, digits]) => {
                  const delta = Number(valueB || 0) - Number(valueA || 0);
                  const improving = title === 'Calls/sec'
                    ? delta >= 0
                    : delta <= 0;
                  return (
                    <div key={title} style={{ padding: '0.85rem', borderRadius: '10px', border: '1px solid var(--border)', background: 'rgba(15,23,42,0.45)' }}>
                      <div style={{ fontSize: '0.8rem', color: 'var(--text-muted)' }}>{title}</div>
                      <div style={{ marginTop: '0.35rem', color: 'var(--text-secondary)', fontSize: '0.85rem' }}>
                        {compareSummaryA.runLabel}: {formatNumber(valueA, digits)}
                      </div>
                      <div style={{ color: 'var(--text-secondary)', fontSize: '0.85rem' }}>
                        {compareSummaryB.runLabel}: {formatNumber(valueB, digits)}
                      </div>
                      <div style={{ marginTop: '0.4rem', color: improving ? '#22c55e' : '#fca5a5', fontWeight: 600 }}>
                        Δ {formatNumber(delta, digits)}
                      </div>
                    </div>
                  );
                })}
              </div>

              <div style={{ overflowX: 'auto' }}>
                <table style={{ width: '100%', borderCollapse: 'collapse' }}>
                  <thead>
                    <tr style={{ color: 'var(--text-muted)', textAlign: 'left', fontSize: '0.78rem', textTransform: 'uppercase' }}>
                      <th style={{ padding: '0.7rem 0.6rem', borderBottom: '1px solid var(--border)' }}>Wait Event</th>
                      <th style={{ padding: '0.7rem 0.6rem', borderBottom: '1px solid var(--border)' }}>{compareSummaryA.runLabel}</th>
                      <th style={{ padding: '0.7rem 0.6rem', borderBottom: '1px solid var(--border)' }}>{compareSummaryB.runLabel}</th>
                      <th style={{ padding: '0.7rem 0.6rem', borderBottom: '1px solid var(--border)' }}>Delta (sec)</th>
                    </tr>
                  </thead>
                  <tbody>
                    {waitComparisonRows.map((row) => (
                      <tr key={row.event}>
                        <td style={{ padding: '0.7rem 0.6rem', borderBottom: '1px solid rgba(255,255,255,0.06)' }}>{row.event}</td>
                        <td style={{ padding: '0.7rem 0.6rem', borderBottom: '1px solid rgba(255,255,255,0.06)', color: 'var(--text-secondary)' }}>
                          {formatNumber(row.aTime, 3)}
                        </td>
                        <td style={{ padding: '0.7rem 0.6rem', borderBottom: '1px solid rgba(255,255,255,0.06)', color: 'var(--text-secondary)' }}>
                          {formatNumber(row.bTime, 3)}
                        </td>
                        <td style={{ padding: '0.7rem 0.6rem', borderBottom: '1px solid rgba(255,255,255,0.06)', color: row.delta <= 0 ? '#22c55e' : '#fca5a5', fontWeight: 600 }}>
                          {formatNumber(row.delta, 3)}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          ) : (
            <div style={{ color: 'var(--text-muted)' }}>
              Save at least two finished runs to compare them here.
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

export default LibraryCacheLockPanel;
