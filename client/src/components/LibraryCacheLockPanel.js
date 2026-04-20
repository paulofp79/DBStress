import React, { useEffect, useMemo, useRef, useState } from 'react';
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

const WAIT_EVENT_ORDER = [
  'library cache: mutex X',
  'latch: ges resource hash list',
  'gc current block congested',
  'gc cr failure',
  'gc buffer busy acquire',
  'cursor: mutex X',
  'cursor: pin S wait on X',
  'library cache lock'
];

const emptyMetrics = {
  runId: null,
  totalTransactions: 0,
  totalErrors: 0,
  totalLogons: 0,
  totalLogoffs: 0,
  transactionsPerSecond: 0,
  avgTransactionMs: 0,
  durationSeconds: 0,
  routeMetrics: [],
  latestSample: null,
  lastError: null,
  scenario: 'single-service',
  loginMode: 'dedicated'
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

const cardStyle = {
  background: 'var(--surface)',
  border: '1px solid var(--border)',
  borderRadius: '12px',
  padding: '1rem'
};

const makeService = (index) => ({
  name: `Service ${index + 1}`,
  connectionString: '',
  procedureOwner: '',
  procedureName: `GRAV_SESSION_MFES_ONLINE_${index + 1}`
});

const formatNumber = (value, digits = 0) => {
  const numeric = Number(value || 0);
  return numeric.toLocaleString(undefined, {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits
  });
};

const formatDateTime = (value) => {
  if (!value) return '-';
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? '-' : date.toLocaleString();
};

const averageRouteMetric = (routes, metric) => {
  if (!Array.isArray(routes) || routes.length === 0) return 0;
  return routes.reduce((sum, route) => sum + Number(route?.[metric] || 0), 0);
};

const buildWaitMap = (summary) => {
  const map = new Map();
  (summary?.keyWaits || []).forEach((wait) => map.set(wait.event, wait));
  return map;
};

function MetricCard({ title, value, hint }) {
  return (
    <div style={{
      ...cardStyle,
      minHeight: '108px',
      background: 'linear-gradient(180deg, rgba(15,23,42,0.96), rgba(30,41,59,0.96))'
    }}>
      <div style={{ color: 'var(--text-muted)', fontSize: '0.75rem', textTransform: 'uppercase', letterSpacing: '0.08em' }}>
        {title}
      </div>
      <div style={{ fontSize: '1.7rem', fontWeight: 700, marginTop: '0.35rem' }}>{value}</div>
      <div style={{ marginTop: '0.35rem', fontSize: '0.82rem', color: 'var(--text-secondary)' }}>{hint}</div>
    </div>
  );
}

function LibraryCacheLockPanel({ dbStatus, socket, schemas = [] }) {
  const [config, setConfig] = useState({
    scenario: 'single-service',
    loginMode: 'dedicated',
    runLabel: 'Scenario 1 Baseline',
    threads: 1000,
    loopDelay: 0,
    schemaPrefix: '',
    modulePrefix: 'MFES',
    moduleLength: 42,
    selectsPerTxn: 2,
    insertsPerTxn: 1,
    updatesPerTxn: 1,
    deletesPerTxn: 1,
    procedureOwner: '',
    procedureName: 'GRAV_SESSION_MFES_ONLINE',
    singleServiceName: 'Primary Service',
    singleServiceConnectionString: '',
    services: [makeService(0), makeService(1), makeService(2), makeService(3)]
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
  const [tpsHistory, setTpsHistory] = useState([]);
  const [latencyHistory, setLatencyHistory] = useState([]);
  const [aasHistory, setAasHistory] = useState([]);
  const [cpuShareHistory, setCpuShareHistory] = useState([]);

  const lastSampleAtRef = useRef(null);
  const maxDataPoints = 60;

  useEffect(() => {
    const defaultConnectionString = dbStatus?.config?.connectionString || '';
    if (!defaultConnectionString) {
      return;
    }

    setConfig((prev) => ({
      ...prev,
      singleServiceConnectionString: prev.singleServiceConnectionString || defaultConnectionString,
      services: prev.services.map((service) => ({
        ...service,
        connectionString: service.connectionString || defaultConnectionString
      }))
    }));
  }, [dbStatus]);

  useEffect(() => {
    try {
      const stored = JSON.parse(localStorage.getItem(LOCAL_STORAGE_KEY) || '[]');
      if (Array.isArray(stored)) {
        setSavedRuns(stored);
      }
    } catch (err) {
      // Ignore local storage corruption.
    }
  }, []);

  const persistRun = (summary) => {
    if (!summary?.runId) return;
    setSavedRuns((prev) => {
      const next = [summary, ...prev.filter((item) => item.runId !== summary.runId)].slice(0, 16);
      localStorage.setItem(LOCAL_STORAGE_KEY, JSON.stringify(next));
      return next;
    });
  };

  useEffect(() => {
    if (!socket) return undefined;

    const onMetrics = (data) => {
      setMetrics({
        runId: data.runId || null,
        totalTransactions: data.totalTransactions || 0,
        totalErrors: data.totalErrors || 0,
        totalLogons: data.totalLogons || 0,
        totalLogoffs: data.totalLogoffs || 0,
        transactionsPerSecond: data.transactionsPerSecond || 0,
        avgTransactionMs: data.avgTransactionMs || 0,
        durationSeconds: data.durationSeconds || 0,
        routeMetrics: data.routeMetrics || [],
        latestSample: data.latestSample || null,
        lastError: data.lastError || null,
        scenario: data.scenario || 'single-service',
        loginMode: data.loginMode || 'dedicated'
      });

      if (data.latestSample?.capturedAt && data.latestSample.capturedAt !== lastSampleAtRef.current) {
        lastSampleAtRef.current = data.latestSample.capturedAt;
        const timestamp = new Date(data.latestSample.capturedAt);
        const label = `${timestamp.getHours().toString().padStart(2, '0')}:${timestamp.getMinutes().toString().padStart(2, '0')}:${timestamp.getSeconds().toString().padStart(2, '0')}`;

        setLabels((prev) => [...prev, label].slice(-maxDataPoints));
        setTpsHistory((prev) => [...prev, data.transactionsPerSecond || 0].slice(-maxDataPoints));
        setLatencyHistory((prev) => [...prev, data.avgTransactionMs || 0].slice(-maxDataPoints));
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
        persistRun(data.summary);
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

  const currentWaits = sample?.keyWaits || latestSummary?.keyWaits || [];
  const currentTopWaits = sample?.topWaitEvents || latestSummary?.topWaitEvents || [];
  const currentRoutes = metrics.routeMetrics.length > 0 ? metrics.routeMetrics : (latestSummary?.routes || []);

  const compareSummaryA = useMemo(
    () => savedRuns.find((run) => run.runId === compareRunA) || null,
    [savedRuns, compareRunA]
  );
  const compareSummaryB = useMemo(
    () => savedRuns.find((run) => run.runId === compareRunB) || null,
    [savedRuns, compareRunB]
  );

  const compareMetrics = compareSummaryA && compareSummaryB
    ? [
        ['Transactions/sec', compareSummaryA.transactionsPerSecond, compareSummaryB.transactionsPerSecond, 2, true],
        ['Avg ms/txn', compareSummaryA.avgTransactionMs, compareSummaryB.avgTransactionMs, 2, false],
        ['AAS', compareSummaryA.averageActiveSessions, compareSummaryB.averageActiveSessions, 2, false],
        ['DB CPU share %', compareSummaryA.dbCpuSharePct, compareSummaryB.dbCpuSharePct, 2, true],
        ['Hard parses/sec', compareSummaryA.parseHardPerSecond, compareSummaryB.parseHardPerSecond, 2, false],
        ['Commits/sec', compareSummaryA.commitRatePerSecond, compareSummaryB.commitRatePerSecond, 2, true]
      ]
    : [];

  const compareWaitRows = (() => {
    if (!compareSummaryA || !compareSummaryB) return [];
    const waitMapA = buildWaitMap(compareSummaryA);
    const waitMapB = buildWaitMap(compareSummaryB);
    return WAIT_EVENT_ORDER.map((eventName) => {
      const a = waitMapA.get(eventName) || { timeWaitedSeconds: 0 };
      const b = waitMapB.get(eventName) || { timeWaitedSeconds: 0 };
      return {
        event: eventName,
        aTime: Number(a.timeWaitedSeconds || 0),
        bTime: Number(b.timeWaitedSeconds || 0),
        delta: Number(b.timeWaitedSeconds || 0) - Number(a.timeWaitedSeconds || 0)
      };
    });
  })();

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

  const handleCompile = async () => {
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

      setStatusMessage(data.message || 'Procedure compiled');
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
      setTpsHistory([]);
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
      const response = await fetch(`${API_BASE}/library-cache-lock/stop`, { method: 'POST' });
      const data = await response.json();
      if (!response.ok) {
        throw new Error(data.error || 'Failed to stop workload');
      }
      setIsRunning(false);
      if (data.summary) {
        setLatestSummary(data.summary);
        setSample(data.summary);
        persistRun(data.summary);
      }
      setStatusMessage('Stopped');
    } catch (err) {
      setStatusMessage(`Stop error: ${err.message}`);
    }
  };

  const updateService = (index, field, value) => {
    setConfig((prev) => ({
      ...prev,
      services: prev.services.map((service, serviceIndex) => (
        serviceIndex === index ? { ...service, [field]: value } : service
      ))
    }));
  };

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
        width: '390px',
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
            Session lifecycle workload: logon, call the procedure, run mixed SELECT/INSERT/UPDATE/DELETE, commit, logout, then compare single-service and split-service runs.
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
            Scenario 1 runs one service with <code>GRAV_SESSION_MFES_ONLINE</code>. Scenario 2 splits the same workload across 4 services and 4 procedures, one per route.
          </div>
        </div>

        <div style={cardStyle}>
          <div className="form-group">
            <label>Scenario</label>
            <select
              value={config.scenario}
              onChange={(e) => setConfig((prev) => ({
                ...prev,
                scenario: e.target.value,
                runLabel: e.target.value === 'split-services' ? 'Scenario 2 Split Services' : 'Scenario 1 Baseline'
              }))}
              disabled={isRunning}
            >
              <option value="single-service">Scenario 1: Single service / one procedure</option>
              <option value="split-services">Scenario 2: Four services / four procedures</option>
            </select>
          </div>

          <div className="form-group">
            <label>Login Model</label>
            <select
              value={config.loginMode}
              onChange={(e) => setConfig((prev) => ({ ...prev, loginMode: e.target.value }))}
              disabled={isRunning}
            >
              <option value="dedicated">Dedicated logon/logoff per transaction</option>
              <option value="pool">Pooled sessions with checkout/close</option>
            </select>
          </div>

          <div className="form-group">
            <label>Run Label</label>
            <input
              value={config.runLabel}
              onChange={(e) => setConfig((prev) => ({ ...prev, runLabel: e.target.value }))}
              disabled={isRunning}
            />
          </div>

          <div className="form-row">
            <div className="form-group">
              <label>Concurrent Sessions</label>
              <input
                type="number"
                min="1"
                max="5000"
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

          <div className="form-group">
            <label>Schema Prefix</label>
            <select
              value={config.schemaPrefix}
              onChange={(e) => setConfig((prev) => ({ ...prev, schemaPrefix: e.target.value }))}
              disabled={isRunning}
            >
              <option value="">default</option>
              {schemas.map((schema) => (
                <option key={schema.prefix || 'default'} value={schema.prefix || ''}>
                  {schema.prefix || 'default'}
                </option>
              ))}
            </select>
          </div>

          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', gap: '0.5rem' }}>
            <div className="form-group" style={{ marginBottom: 0 }}>
              <label>Selects</label>
              <input
                type="number"
                min="1"
                max="10"
                value={config.selectsPerTxn}
                onChange={(e) => setConfig((prev) => ({ ...prev, selectsPerTxn: Math.max(1, Number.parseInt(e.target.value || '1', 10)) }))}
                disabled={isRunning}
              />
            </div>
            <div className="form-group" style={{ marginBottom: 0 }}>
              <label>Inserts</label>
              <input
                type="number"
                min="0"
                max="10"
                value={config.insertsPerTxn}
                onChange={(e) => setConfig((prev) => ({ ...prev, insertsPerTxn: Math.max(0, Number.parseInt(e.target.value || '0', 10)) }))}
                disabled={isRunning}
              />
            </div>
            <div className="form-group" style={{ marginBottom: 0 }}>
              <label>Updates</label>
              <input
                type="number"
                min="0"
                max="10"
                value={config.updatesPerTxn}
                onChange={(e) => setConfig((prev) => ({ ...prev, updatesPerTxn: Math.max(0, Number.parseInt(e.target.value || '0', 10)) }))}
                disabled={isRunning}
              />
            </div>
            <div className="form-group" style={{ marginBottom: 0 }}>
              <label>Deletes</label>
              <input
                type="number"
                min="0"
                max="10"
                value={config.deletesPerTxn}
                onChange={(e) => setConfig((prev) => ({ ...prev, deletesPerTxn: Math.max(0, Number.parseInt(e.target.value || '0', 10)) }))}
                disabled={isRunning}
              />
            </div>
          </div>
        </div>

        <div style={cardStyle}>
          {config.scenario === 'single-service' ? (
            <>
              <div className="form-group">
                <label>Service Name</label>
                <input
                  value={config.singleServiceName}
                  onChange={(e) => setConfig((prev) => ({ ...prev, singleServiceName: e.target.value }))}
                  disabled={isRunning}
                />
              </div>
              <div className="form-group">
                <label>Service Connection String</label>
                <input
                  value={config.singleServiceConnectionString}
                  onChange={(e) => setConfig((prev) => ({ ...prev, singleServiceConnectionString: e.target.value }))}
                  disabled={isRunning}
                  placeholder={dbStatus.config?.connectionString || 'host:port/service'}
                />
              </div>
              <div className="form-row">
                <div className="form-group">
                  <label>Procedure Owner</label>
                  <input
                    value={config.procedureOwner}
                    onChange={(e) => setConfig((prev) => ({ ...prev, procedureOwner: e.target.value.toUpperCase() }))}
                    disabled={isRunning}
                    placeholder="current schema"
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
              </div>
            </>
          ) : (
            <div style={{ display: 'grid', gap: '0.85rem' }}>
              {config.services.map((service, index) => (
                <div key={`service-${index}`} style={{ padding: '0.85rem', borderRadius: '10px', border: '1px solid var(--border)', background: 'rgba(15,23,42,0.45)' }}>
                  <div style={{ fontWeight: 600, marginBottom: '0.6rem' }}>{service.name}</div>
                  <div className="form-group">
                    <label>Service Name</label>
                    <input
                      value={service.name}
                      onChange={(e) => updateService(index, 'name', e.target.value)}
                      disabled={isRunning}
                    />
                  </div>
                  <div className="form-group">
                    <label>Connection String</label>
                    <input
                      value={service.connectionString}
                      onChange={(e) => updateService(index, 'connectionString', e.target.value)}
                      disabled={isRunning}
                      placeholder={dbStatus.config?.connectionString || 'host:port/service'}
                    />
                  </div>
                  <div className="form-row">
                    <div className="form-group">
                      <label>Owner</label>
                      <input
                        value={service.procedureOwner}
                        onChange={(e) => updateService(index, 'procedureOwner', e.target.value.toUpperCase())}
                        disabled={isRunning}
                        placeholder="current schema"
                      />
                    </div>
                    <div className="form-group">
                      <label>Procedure</label>
                      <input
                        value={service.procedureName}
                        onChange={(e) => updateService(index, 'procedureName', e.target.value.toUpperCase())}
                        disabled={isRunning}
                      />
                    </div>
                  </div>
                </div>
              ))}
            </div>
          )}

          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '0.75rem', marginTop: '1rem' }}>
            <button className="btn btn-danger" type="button" onClick={handleStart} disabled={isRunning || isInstalling}>
              Start Workload
            </button>
            <button className="btn btn-secondary" type="button" onClick={handleStop} disabled={!isRunning}>
              Stop
            </button>
          </div>

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
            {metrics.lastError && (
              <div style={{ marginTop: '0.45rem', fontSize: '0.8rem', color: '#fca5a5' }}>
                Last error: {metrics.lastError}
              </div>
            )}
          </div>
        </div>

        <div style={cardStyle}>
          <div className="form-group">
            <label>Procedure SQL (optional, compile one procedure at a time)</label>
            <textarea
              value={procedureSql}
              onChange={(e) => setProcedureSql(e.target.value)}
              disabled={isRunning || isInstalling}
              style={{ minHeight: '240px' }}
            />
          </div>
          <button
            className="btn btn-secondary"
            type="button"
            onClick={handleCompile}
            disabled={isRunning || isInstalling}
            style={{ width: '100%' }}
          >
            {isInstalling ? 'Compiling...' : 'Compile Current Procedure'}
          </button>
        </div>
      </div>

      <div style={{ flex: 1, display: 'grid', gap: '1rem' }}>
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))', gap: '1rem' }}>
          <MetricCard title="Txn / Sec" value={formatNumber(metrics.transactionsPerSecond, 2)} hint="Committed transactions per second for this run" />
          <MetricCard title="Avg ms / Txn" value={formatNumber(metrics.avgTransactionMs, 2)} hint="Average end-to-end transaction time" />
          <MetricCard title="Total Txns" value={formatNumber(metrics.totalTransactions, 0)} hint="Committed business transactions" />
          <MetricCard title="Errors" value={formatNumber(metrics.totalErrors, 0)} hint="Rolled back or failed transaction attempts" />
          <MetricCard title="Logons" value={formatNumber(metrics.totalLogons, 0)} hint="Actual logons in dedicated mode, logical opens in pool mode" />
          <MetricCard title="AAS" value={formatNumber(sample?.averageActiveSessions || latestSummary?.averageActiveSessions, 2)} hint="DB time divided by elapsed run time" />
          <MetricCard title="DB CPU Share" value={`${formatNumber(sample?.dbCpuSharePct || latestSummary?.dbCpuSharePct, 2)}%`} hint="How much DB time stayed on CPU" />
          <MetricCard title="Commits / Sec" value={formatNumber(sample?.commitRatePerSecond || latestSummary?.commitRatePerSecond, 2)} hint="Environment commit rate during the run" />
        </div>

        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(320px, 1fr))', gap: '1rem' }}>
          <div style={cardStyle}>
            <div style={{ marginBottom: '0.75rem', fontWeight: 600 }}>Transactions per Second</div>
            <div style={{ height: '240px' }}>
              <Line data={lineData('TPS', tpsHistory, '#f97316', 'rgba(249,115,22,0.18)')} options={chartOptions} />
            </div>
          </div>

          <div style={cardStyle}>
            <div style={{ marginBottom: '0.75rem', fontWeight: 600 }}>Average ms per Transaction</div>
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

        <div style={{ display: 'grid', gridTemplateColumns: 'minmax(360px, 1.2fr) minmax(300px, 0.8fr)', gap: '1rem' }}>
          <div style={cardStyle}>
            <div style={{ display: 'flex', justifyContent: 'space-between', gap: '1rem', marginBottom: '0.75rem' }}>
              <div style={{ fontWeight: 600 }}>Top Waits for This Run</div>
              <div style={{ color: 'var(--text-muted)', fontSize: '0.82rem' }}>
                Delta waits captured only during this run
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
            <div style={{ display: 'grid', gap: '0.55rem' }}>
              {WAIT_EVENT_ORDER.map((eventName) => {
                const wait = currentWaits.find((item) => item.event === eventName) || {
                  timeWaitedSeconds: 0,
                  totalWaits: 0
                };
                return (
                  <div key={eventName} style={{ padding: '0.72rem', borderRadius: '10px', border: '1px solid var(--border)', background: 'rgba(15,23,42,0.45)' }}>
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

        <div style={cardStyle}>
          <div style={{ fontWeight: 600, marginBottom: '0.8rem' }}>Live Route Metrics</div>
          <div style={{ overflowX: 'auto' }}>
            <table style={{ width: '100%', borderCollapse: 'collapse' }}>
              <thead>
                <tr style={{ color: 'var(--text-muted)', textAlign: 'left', fontSize: '0.78rem', textTransform: 'uppercase' }}>
                  <th style={{ padding: '0.7rem 0.6rem', borderBottom: '1px solid var(--border)' }}>Route</th>
                  <th style={{ padding: '0.7rem 0.6rem', borderBottom: '1px solid var(--border)' }}>Instance</th>
                  <th style={{ padding: '0.7rem 0.6rem', borderBottom: '1px solid var(--border)' }}>Procedure</th>
                  <th style={{ padding: '0.7rem 0.6rem', borderBottom: '1px solid var(--border)' }}>Workers</th>
                  <th style={{ padding: '0.7rem 0.6rem', borderBottom: '1px solid var(--border)' }}>Txn/s</th>
                  <th style={{ padding: '0.7rem 0.6rem', borderBottom: '1px solid var(--border)' }}>Avg ms</th>
                  <th style={{ padding: '0.7rem 0.6rem', borderBottom: '1px solid var(--border)' }}>Txns</th>
                  <th style={{ padding: '0.7rem 0.6rem', borderBottom: '1px solid var(--border)' }}>Errs</th>
                </tr>
              </thead>
              <tbody>
                {currentRoutes.map((route) => (
                  <tr key={`${route.routeId || route.name}-${route.procedure}`}>
                    <td style={{ padding: '0.7rem 0.6rem', borderBottom: '1px solid rgba(255,255,255,0.06)' }}>
                      <div style={{ fontWeight: 600 }}>{route.name}</div>
                      <div style={{ color: 'var(--text-muted)', fontSize: '0.78rem' }}>{route.connectionString}</div>
                    </td>
                    <td style={{ padding: '0.7rem 0.6rem', borderBottom: '1px solid rgba(255,255,255,0.06)' }}>{route.instanceName || '-'}</td>
                    <td style={{ padding: '0.7rem 0.6rem', borderBottom: '1px solid rgba(255,255,255,0.06)' }}>{route.procedure}</td>
                    <td style={{ padding: '0.7rem 0.6rem', borderBottom: '1px solid rgba(255,255,255,0.06)' }}>{formatNumber(route.assignedWorkers, 0)}</td>
                    <td style={{ padding: '0.7rem 0.6rem', borderBottom: '1px solid rgba(255,255,255,0.06)' }}>{formatNumber(route.transactionsPerSecond, 2)}</td>
                    <td style={{ padding: '0.7rem 0.6rem', borderBottom: '1px solid rgba(255,255,255,0.06)' }}>{formatNumber(route.avgTransactionMs, 2)}</td>
                    <td style={{ padding: '0.7rem 0.6rem', borderBottom: '1px solid rgba(255,255,255,0.06)' }}>{formatNumber(route.totalTransactions, 0)}</td>
                    <td style={{ padding: '0.7rem 0.6rem', borderBottom: '1px solid rgba(255,255,255,0.06)' }}>{formatNumber(route.totalErrors, 0)}</td>
                  </tr>
                ))}
                {currentRoutes.length === 0 && (
                  <tr>
                    <td colSpan="8" style={{ padding: '1rem 0.6rem', color: 'var(--text-muted)' }}>
                      Start the workload to populate route metrics.
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </div>

        <div style={{ display: 'grid', gridTemplateColumns: 'minmax(340px, 1fr) minmax(340px, 1fr)', gap: '1rem' }}>
          <div style={cardStyle}>
            <div style={{ fontWeight: 600, marginBottom: '0.75rem' }}>Latest Run Summary</div>
            {latestSummary ? (
              <div style={{ display: 'grid', gap: '0.45rem', color: 'var(--text-secondary)', fontSize: '0.9rem' }}>
                <div><strong style={{ color: 'white' }}>{latestSummary.runLabel}</strong></div>
                <div>Scenario: {latestSummary.scenario}</div>
                <div>Login mode: {latestSummary.loginMode}</div>
                <div>Started: {formatDateTime(latestSummary.startedAt)}</div>
                <div>Finished: {formatDateTime(latestSummary.completedAt)}</div>
                <div>Total transactions: {formatNumber(latestSummary.totalTransactions, 0)}</div>
                <div>Average ms/txn: {formatNumber(latestSummary.avgTransactionMs, 2)}</div>
                <div>Logons: {formatNumber(latestSummary.totalLogons, 0)}</div>
                <div>User calls/sec: {formatNumber(latestSummary.userCallsPerSecond, 2)}</div>
                <div>Execute count/sec: {formatNumber(latestSummary.executeCountPerSecond, 2)}</div>
              </div>
            ) : (
              <div style={{ color: 'var(--text-muted)' }}>Stop a run to capture a saved summary.</div>
            )}
          </div>

          <div style={cardStyle}>
            <div style={{ fontWeight: 600, marginBottom: '0.75rem' }}>Matched SQL</div>
            {(latestSummary?.matchedSql || []).length > 0 ? (
              <div style={{ display: 'grid', gap: '0.7rem' }}>
                {latestSummary.matchedSql.map((sql) => (
                  <div key={sql.sqlId} style={{ padding: '0.75rem', borderRadius: '10px', border: '1px solid var(--border)', background: 'rgba(15,23,42,0.45)' }}>
                    <div style={{ fontWeight: 600 }}>{sql.sqlId}</div>
                    <div style={{ marginTop: '0.35rem', fontSize: '0.82rem', color: 'var(--text-secondary)' }}>{sql.sqlText}</div>
                    <div style={{ marginTop: '0.45rem', fontSize: '0.78rem', color: 'var(--text-muted)' }}>
                      execs {formatNumber(sql.executions, 0)} | elapsed {formatNumber(sql.elapsedSeconds, 3)} s | CPU {formatNumber(sql.cpuSeconds, 3)} s
                    </div>
                  </div>
                ))}
              </div>
            ) : (
              <div style={{ color: 'var(--text-muted)' }}>Procedure-call SQL will appear here after a run.</div>
            )}
          </div>
        </div>

        <div style={cardStyle}>
          <div style={{ display: 'flex', justifyContent: 'space-between', gap: '1rem', alignItems: 'center', marginBottom: '1rem' }}>
            <div>
              <div style={{ fontWeight: 600 }}>Saved Runs Comparison</div>
              <div style={{ color: 'var(--text-muted)', fontSize: '0.82rem' }}>
                Compare scenario 1 versus scenario 2 using throughput, latency, CPU share, and wait deltas.
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
                {compareMetrics.map(([title, leftValue, rightValue, digits, higherIsBetter]) => {
                  const delta = Number(rightValue || 0) - Number(leftValue || 0);
                  const improving = higherIsBetter ? delta >= 0 : delta <= 0;
                  return (
                    <div key={title} style={{ padding: '0.85rem', borderRadius: '10px', border: '1px solid var(--border)', background: 'rgba(15,23,42,0.45)' }}>
                      <div style={{ fontSize: '0.8rem', color: 'var(--text-muted)' }}>{title}</div>
                      <div style={{ marginTop: '0.35rem', color: 'var(--text-secondary)', fontSize: '0.85rem' }}>
                        {compareSummaryA.runLabel}: {formatNumber(leftValue, digits)}
                      </div>
                      <div style={{ color: 'var(--text-secondary)', fontSize: '0.85rem' }}>
                        {compareSummaryB.runLabel}: {formatNumber(rightValue, digits)}
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
                    {compareWaitRows.map((row) => (
                      <tr key={row.event}>
                        <td style={{ padding: '0.7rem 0.6rem', borderBottom: '1px solid rgba(255,255,255,0.06)' }}>{row.event}</td>
                        <td style={{ padding: '0.7rem 0.6rem', borderBottom: '1px solid rgba(255,255,255,0.06)' }}>{formatNumber(row.aTime, 3)}</td>
                        <td style={{ padding: '0.7rem 0.6rem', borderBottom: '1px solid rgba(255,255,255,0.06)' }}>{formatNumber(row.bTime, 3)}</td>
                        <td style={{ padding: '0.7rem 0.6rem', borderBottom: '1px solid rgba(255,255,255,0.06)', color: row.delta <= 0 ? '#22c55e' : '#fca5a5', fontWeight: 600 }}>
                          {formatNumber(row.delta, 3)}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>

              <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '1rem' }}>
                {[compareSummaryA, compareSummaryB].map((summary) => (
                  <div key={summary.runId} style={{ padding: '0.85rem', borderRadius: '10px', border: '1px solid var(--border)', background: 'rgba(15,23,42,0.45)' }}>
                    <div style={{ fontWeight: 600 }}>{summary.runLabel}</div>
                    <div style={{ marginTop: '0.35rem', color: 'var(--text-secondary)', fontSize: '0.85rem' }}>
                      Scenario: {summary.scenario} | Login mode: {summary.loginMode}
                    </div>
                    <div style={{ marginTop: '0.65rem', color: 'var(--text-muted)', fontSize: '0.8rem' }}>
                      Route totals
                    </div>
                    <div style={{ marginTop: '0.35rem', display: 'grid', gap: '0.35rem', fontSize: '0.84rem', color: 'var(--text-secondary)' }}>
                      <div>Routes: {summary.routes?.length || 0}</div>
                      <div>Total route TPS: {formatNumber(averageRouteMetric(summary.routes, 'transactionsPerSecond'), 2)}</div>
                      <div>Average route ms/txn: {summary.routes?.length ? formatNumber(averageRouteMetric(summary.routes, 'avgTransactionMs') / summary.routes.length, 2) : '0.00'}</div>
                    </div>
                  </div>
                ))}
              </div>
            </div>
          ) : (
            <div style={{ color: 'var(--text-muted)' }}>
              Save two completed runs to compare scenario 1 and scenario 2.
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

export default LibraryCacheLockPanel;
