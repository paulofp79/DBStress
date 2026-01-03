import React, { useState, useEffect, useRef } from 'react';
import { Line, Bar } from 'react-chartjs-2';
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  BarElement,
  Title,
  Tooltip,
  Legend,
  Filler
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

// Auto-detect server URL
const getServerUrl = () => {
  if (window.location.hostname !== 'localhost') {
    return `http://${window.location.host}`;
  }
  return 'http://localhost:3001';
};

const API_BASE = `${getServerUrl()}/api`;

// Index type options based on Oracle Real-World Performance demo
const INDEX_TYPES = [
  { value: 'none_no_seq', label: 'No Index, No Sequence', description: 'Heap table with random ID - fastest baseline, no contention' },
  { value: 'none_cached_seq', label: 'No Index, Cached Sequence', description: 'Heap table with CACHE 1000 sequence - shows cached seq has no contention' },
  { value: 'standard', label: 'Standard B-Tree Index', description: 'Normal ascending sequence PK - maximum right-hand contention' },
  { value: 'reverse', label: 'Reverse Key Index', description: 'Distributes inserts across leaf blocks, but increases I/O' },
  { value: 'hash_partition', label: 'Hash Partitioned Index', description: 'Partitions index by hash - helps single instance, not RAC' },
  { value: 'scalable_sequence', label: 'Scalable Sequence (18c+)', description: 'Uses instance-prefixed sequences to avoid contention' }
];

function IndexContentionPanel({ dbStatus, socket, schemas }) {
  // Configuration state
  const [config, setConfig] = useState({
    threads: 50,
    thinkTime: 0,
    indexType: 'standard',
    tableCount: 1,
    rowsPerCommit: 10
  });

  // Runtime state
  const [isRunning, setIsRunning] = useState(false);
  const [selectedSchema, setSelectedSchema] = useState('');

  // Metrics state
  const [metrics, setMetrics] = useState({
    tpsApp: 0,
    tpsOracle: 0,
    avgResponseTime: 0,
    totalTransactions: 0,
    errors: 0
  });

  // Wait events state
  const [waitEvents, setWaitEvents] = useState({
    'buffer busy waits': 0,
    'enq: TX - index contention': 0,
    'gc buffer busy acquire': 0,
    'gc buffer busy release': 0,
    'cell single block physical read': 0
  });

  // TOP 10 wait events state
  const [top10WaitEvents, setTop10WaitEvents] = useState([]);

  // Chart data - keep last 60 seconds
  const maxDataPoints = 60;
  const [tpsAppHistory, setTpsAppHistory] = useState([]);
  const [tpsOracleHistory, setTpsOracleHistory] = useState([]);
  const [responseTimeHistory, setResponseTimeHistory] = useState([]);
  const [labels, setLabels] = useState([]);

  // Status message
  const [statusMessage, setStatusMessage] = useState('');
  const [isChangingIndex, setIsChangingIndex] = useState(false);

  // A/B test state
  const [abTestConfig, setAbTestConfig] = useState({ cacheA: 0, cacheB: 100, warmup: 5, duration: 10 });
  const [abTestRunning, setAbTestRunning] = useState(false);
  const [abTestResult, setAbTestResult] = useState(null);

  // Timer for uptime
  const [uptime, setUptime] = useState(0);
  const uptimeRef = useRef(null);
  const startTimeRef = useRef(null);

  // Select first schema by default
  useEffect(() => {
    if (schemas && schemas.length > 0 && !selectedSchema) {
      setSelectedSchema(schemas[0].prefix || '');
    }
  }, [schemas, selectedSchema]);

  // Listen for index contention metrics
  useEffect(() => {
    if (socket) {
      socket.on('index-contention-metrics', (data) => {
        setMetrics({
          tpsApp: data.tpsApp || 0,
          tpsOracle: data.tpsOracle || 0,
          avgResponseTime: data.avgResponseTime || 0,
          totalTransactions: data.totalTransactions || 0,
          errors: data.errors || 0
        });

        // Only update chart data if we have actual TPS (skip zeros to keep charts clean)
        if (data.tpsApp > 0 || data.tpsOracle > 0) {
          const now = new Date();
          const timeLabel = `${now.getMinutes()}:${now.getSeconds().toString().padStart(2, '0')}`;

          setLabels(prev => {
            const newLabels = [...prev, timeLabel];
            return newLabels.slice(-maxDataPoints);
          });

          setTpsAppHistory(prev => {
            const newData = [...prev, data.tpsApp || 0];
            return newData.slice(-maxDataPoints);
          });

          setTpsOracleHistory(prev => {
            const newData = [...prev, data.tpsOracle || 0];
            return newData.slice(-maxDataPoints);
          });

          setResponseTimeHistory(prev => {
            const newData = [...prev, data.avgResponseTime || 0];
            return newData.slice(-maxDataPoints);
          });
        }

        // Update wait events if provided
        if (data.waitEvents) {
          setWaitEvents(data.waitEvents);
        }
      });

      socket.on('index-contention-status', (data) => {
        setStatusMessage(data.message || '');
        if (data.running !== undefined) {
          setIsRunning(data.running);
        }
        if (data.indexChanged) {
          setIsChangingIndex(false);
        }
      });

      socket.on('index-contention-abtest-result', (data) => {
        setAbTestResult(data);
        setAbTestRunning(false);
        setStatusMessage('A/B test complete');
      });

      socket.on('index-contention-stopped', (data) => {
        setIsRunning(false);
        setStatusMessage('Stopped');
      });

      socket.on('index-contention-wait-events', (data) => {
        if (data.waitEvents) {
          setWaitEvents(data.waitEvents);
        }
        if (data.top10WaitEvents) {
          setTop10WaitEvents(data.top10WaitEvents);
        }
      });
    }

    return () => {
      if (socket) {
        socket.off('index-contention-metrics');
        socket.off('index-contention-status');
        socket.off('index-contention-stopped');
        socket.off('index-contention-abtest-result');
        socket.off('index-contention-wait-events');
      }
    };
  }, [socket]);

  // Uptime timer
  useEffect(() => {
    if (isRunning) {
      if (!startTimeRef.current) {
        startTimeRef.current = Date.now();
      }
      uptimeRef.current = setInterval(() => {
        setUptime(Math.floor((Date.now() - startTimeRef.current) / 1000));
      }, 1000);
    } else {
      if (uptimeRef.current) {
        clearInterval(uptimeRef.current);
        uptimeRef.current = null;
      }
      startTimeRef.current = null;
      setUptime(0);
    }

    return () => {
      if (uptimeRef.current) {
        clearInterval(uptimeRef.current);
      }
    };
  }, [isRunning]);

  const handleStart = async () => {
    try {
      setStatusMessage('Starting Index Contention Demo...');
      // Clear chart data
      setTpsAppHistory([]);
      setTpsOracleHistory([]);
      setResponseTimeHistory([]);
      setLabels([]);

      // Set running immediately for UI responsiveness
      setIsRunning(true);

      const response = await fetch(`${API_BASE}/index-contention/start`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          ...config,
          schemaPrefix: selectedSchema
        })
      });

      const data = await response.json();

      if (!response.ok) {
        setIsRunning(false);
        throw new Error(data.error || 'Failed to start');
      }

      setStatusMessage('Running...');
    } catch (err) {
      setIsRunning(false);
      setStatusMessage(`Error: ${err.message}`);
    }
  };

  const handleStop = async () => {
    try {
      setStatusMessage('Stopping...');

      await fetch(`${API_BASE}/index-contention/stop`, { method: 'POST' });

      setIsRunning(false);
      setStatusMessage('Stopped');

      // Stop uptime timer
      if (uptimeRef.current) {
        clearInterval(uptimeRef.current);
        uptimeRef.current = null;
      }
    } catch (err) {
      setIsRunning(false);
      setStatusMessage(`Error: ${err.message}`);
    }
  };

  const handleReset = () => {
    setTpsAppHistory([]);
    setTpsOracleHistory([]);
    setResponseTimeHistory([]);
    setLabels([]);
    setMetrics({ tpsApp: 0, tpsOracle: 0, avgResponseTime: 0, totalTransactions: 0, errors: 0 });
    setWaitEvents({
      'buffer busy waits': 0,
      'enq: TX - index contention': 0,
      'gc buffer busy acquire': 0,
      'gc buffer busy release': 0,
      'cell single block physical read': 0
    });
    setTop10WaitEvents([]);
  };

  const handleIndexTypeChange = async (newType) => {
    setConfig(prev => ({ ...prev, indexType: newType }));

    if (isRunning) {
      setIsChangingIndex(true);
      setStatusMessage(`Changing index type to ${INDEX_TYPES.find(t => t.value === newType)?.label}...`);

      try {
        const response = await fetch(`${API_BASE}/index-contention/change-index`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            indexType: newType,
            schemaPrefix: selectedSchema
          })
        });

        if (!response.ok) {
          const error = await response.json();
          throw new Error(error.error || 'Failed to change index');
        }
      } catch (err) {
        setStatusMessage(`Error changing index: ${err.message}`);
        setIsChangingIndex(false);
      }
    }
  };

  const handleRunABTest = async () => {
    if (!isRunning || abTestRunning) return;
    setAbTestRunning(true);
    setAbTestResult(null);
    setStatusMessage('Running A/B test...');

    try {
      const response = await fetch(`${API_BASE}/index-contention/ab-test-sequence-cache`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          cacheA: parseInt(abTestConfig.cacheA, 10),
          cacheB: parseInt(abTestConfig.cacheB, 10),
          duration: parseInt(abTestConfig.duration, 10),
          warmup: parseInt(abTestConfig.warmup, 10)
        })
      });

      const data = await response.json();
      if (!response.ok) {
        throw new Error(data.error || 'A/B test failed');
      }

      setAbTestResult(data.result || data);
      setStatusMessage('A/B test complete');
    } catch (err) {
      setStatusMessage(`A/B test error: ${err.message}`);
    } finally {
      setAbTestRunning(false);
    }
  };

  const formatUptime = (seconds) => {
    const mins = Math.floor(seconds / 60);
    const secs = seconds % 60;
    return `${mins}:${secs.toString().padStart(2, '0')}`;
  };

  // TPS from App Chart configuration
  const tpsAppChartData = {
    labels,
    datasets: [{
      label: 'TPS from App',
      data: tpsAppHistory,
      borderColor: '#f59e0b',
      backgroundColor: 'rgba(245, 158, 11, 0.2)',
      fill: true,
      tension: 0.3,
      pointRadius: 0
    }]
  };

  // TPS from Oracle Chart configuration
  const tpsOracleChartData = {
    labels,
    datasets: [{
      label: 'TPS from Oracle',
      data: tpsOracleHistory,
      borderColor: '#10b981',
      backgroundColor: 'rgba(16, 185, 129, 0.2)',
      fill: true,
      tension: 0.3,
      pointRadius: 0
    }]
  };

  // Response Time Chart configuration
  const responseTimeChartData = {
    labels,
    datasets: [{
      label: 'Response Time (ms)',
      data: responseTimeHistory,
      borderColor: '#ef4444',
      backgroundColor: 'rgba(239, 68, 68, 0.3)',
      fill: true,
      tension: 0.3,
      pointRadius: 0
    }]
  };

  // TOP 10 Wait Events Chart configuration (horizontal bar)
  const top10WaitEventsChartData = {
    labels: top10WaitEvents.map(e => e.event?.substring(0, 30) || 'Unknown'),
    datasets: [{
      label: 'Time Waited (seconds)',
      data: top10WaitEvents.map(e => e.timeSeconds || 0),
      backgroundColor: [
        'rgba(239, 68, 68, 0.7)',   // red
        'rgba(249, 115, 22, 0.7)',  // orange
        'rgba(245, 158, 11, 0.7)', // amber
        'rgba(234, 179, 8, 0.7)',  // yellow
        'rgba(132, 204, 22, 0.7)', // lime
        'rgba(34, 197, 94, 0.7)',  // green
        'rgba(20, 184, 166, 0.7)', // teal
        'rgba(6, 182, 212, 0.7)',  // cyan
        'rgba(59, 130, 246, 0.7)', // blue
        'rgba(139, 92, 246, 0.7)'  // violet
      ],
      borderColor: [
        'rgb(239, 68, 68)',
        'rgb(249, 115, 22)',
        'rgb(245, 158, 11)',
        'rgb(234, 179, 8)',
        'rgb(132, 204, 22)',
        'rgb(34, 197, 94)',
        'rgb(20, 184, 166)',
        'rgb(6, 182, 212)',
        'rgb(59, 130, 246)',
        'rgb(139, 92, 246)'
      ],
      borderWidth: 1
    }]
  };

  const top10WaitEventsChartOptions = {
    indexAxis: 'y',
    responsive: true,
    maintainAspectRatio: false,
    animation: { duration: 0 },
    scales: {
      x: {
        display: true,
        beginAtZero: true,
        grid: { color: 'rgba(255,255,255,0.1)' },
        ticks: { color: '#9ca3af' },
        title: { display: true, text: 'Time (seconds)', color: '#9ca3af' }
      },
      y: {
        display: true,
        grid: { display: false },
        ticks: { color: '#9ca3af', font: { size: 11 } }
      }
    },
    plugins: {
      legend: { display: false },
      tooltip: {
        callbacks: {
          label: (context) => {
            const event = top10WaitEvents[context.dataIndex];
            return [
              `Time: ${event?.timeSeconds?.toFixed(2) || 0} seconds`,
              `Waits: ${event?.totalWaits?.toLocaleString() || 0}`
            ];
          }
        }
      }
    }
  };

  const chartOptions = {
    responsive: true,
    maintainAspectRatio: false,
    animation: { duration: 0 },
    scales: {
      x: {
        display: true,
        grid: { color: 'rgba(255,255,255,0.1)' },
        ticks: { color: '#9ca3af', maxTicksLimit: 10 }
      },
      y: {
        display: true,
        beginAtZero: true,
        grid: { color: 'rgba(255,255,255,0.1)' },
        ticks: { color: '#9ca3af' }
      }
    },
    plugins: {
      legend: { display: false }
    }
  };

  // A/B test chart data (derived from abTestResult)
  const getResultFor = (cache) => {
    if (!abTestResult) return null;
    const results = abTestResult.results || abTestResult.result?.results || abTestResult;
    return results && (results[cache] || results[String(cache)]) ? results[cache] || results[String(cache)] : null;
  };

  const resA = getResultFor(abTestConfig.cacheA);
  const resB = getResultFor(abTestConfig.cacheB);
  const abLabels = resA?.samples?.map((_, i) => `${i + 1}s`) || resB?.samples?.map((_, i) => `${i + 1}s`) || [];

  const abChartData = {
    labels: abLabels,
    datasets: [
      {
        label: `Cache ${abTestConfig.cacheA} TPS`,
        data: resA?.samples?.map(s => s.tps) || [],
        borderColor: '#3b82f6',
        backgroundColor: 'rgba(59,130,246,0.1)',
        fill: true,
        tension: 0.3,
        pointRadius: 0
      },
      {
        label: `Cache ${abTestConfig.cacheB} TPS`,
        data: resB?.samples?.map(s => s.tps) || [],
        borderColor: '#10b981',
        backgroundColor: 'rgba(16,185,129,0.1)',
        fill: true,
        tension: 0.3,
        pointRadius: 0
      }
    ]
  };

  const existingSchemas = schemas || [];
  const hasSchemas = existingSchemas.length > 0;

  if (!dbStatus.connected) {
    return (
      <div className="panel" style={{ height: '100%' }}>
        <div className="panel-header">
          <h2>Index Contention Demo</h2>
        </div>
        <div className="panel-content">
          <p style={{ color: 'var(--text-muted)' }}>Connect to database first</p>
        </div>
      </div>
    );
  }

  return (
    <div style={{ display: 'flex', gap: '1rem', height: '100%', padding: '1rem' }}>
      {/* Left Panel - Controls */}
      <div style={{
        width: '280px',
        flexShrink: 0,
        background: 'var(--surface)',
        borderRadius: '8px',
        padding: '1rem',
        display: 'flex',
        flexDirection: 'column',
        gap: '1rem'
      }}>
        <h2 style={{ margin: 0, fontSize: '1.1rem', borderBottom: '1px solid var(--border)', paddingBottom: '0.5rem' }}>
          Index Contention Demo
        </h2>

        {/* Schema Selection */}
        <div className="form-group">
          <label style={{ fontSize: '0.85rem', fontWeight: '500' }}>Schema:</label>
          <select
            value={selectedSchema}
            onChange={(e) => setSelectedSchema(e.target.value)}
            disabled={isRunning}
            style={{
              width: '100%',
              padding: '0.5rem',
              background: 'var(--bg-primary)',
              border: '1px solid var(--border)',
              borderRadius: '4px',
              color: 'var(--text-primary)',
              marginTop: '0.25rem'
            }}
          >
            {existingSchemas.map(s => (
              <option key={s.prefix || 'default'} value={s.prefix || ''}>
                {s.prefix || 'default'}
              </option>
            ))}
          </select>
        </div>

        {/* Number of Threads */}
        <div className="form-group">
          <label style={{ fontSize: '0.85rem', fontWeight: '500' }}>Number of Threads:</label>
          <input
            type="number"
            min="1"
            max="500"
            value={config.threads}
            onChange={(e) => setConfig(prev => ({ ...prev, threads: Math.max(1, parseInt(e.target.value) || 1) }))}
            disabled={isRunning}
            style={{
              width: '100%',
              padding: '0.5rem',
              background: 'var(--bg-primary)',
              border: '1px solid var(--border)',
              borderRadius: '4px',
              color: 'var(--text-primary)',
              marginTop: '0.25rem'
            }}
          />
        </div>

        {/* Think Time */}
        <div className="form-group">
          <label style={{ fontSize: '0.85rem', fontWeight: '500' }}>Think Time (ms):</label>
          <input
            type="number"
            min="0"
            max="1000"
            value={config.thinkTime}
            onChange={(e) => setConfig(prev => ({ ...prev, thinkTime: Math.max(0, parseInt(e.target.value) || 0) }))}
            disabled={isRunning}
            style={{
              width: '100%',
              padding: '0.5rem',
              background: 'var(--bg-primary)',
              border: '1px solid var(--border)',
              borderRadius: '4px',
              color: 'var(--text-primary)',
              marginTop: '0.25rem'
            }}
          />
        </div>

        {/* Rows per Commit */}
        <div className="form-group">
          <label style={{ fontSize: '0.85rem', fontWeight: '500' }}>Rows per Commit:</label>
          <input
            type="number"
            min="1"
            max="1000"
            value={config.rowsPerCommit}
            onChange={(e) => setConfig(prev => ({ ...prev, rowsPerCommit: Math.max(1, Math.min(1000, parseInt(e.target.value) || 1)) }))}
            disabled={isRunning}
            style={{
              width: '100%',
              padding: '0.5rem',
              background: 'var(--bg-primary)',
              border: '1px solid var(--border)',
              borderRadius: '4px',
              color: 'var(--text-primary)',
              marginTop: '0.25rem'
            }}
          />
          <div style={{ fontSize: '0.7rem', color: 'var(--text-muted)', marginTop: '0.25rem' }}>
            Higher = less commit overhead, more visible index contention
          </div>
        </div>

        {/* Index Type Selector - Key feature */}
        <div className="form-group">
          <label style={{ fontSize: '0.85rem', fontWeight: '500', color: 'var(--accent-warning)' }}>
            B-Tree Index Type:
          </label>
          <select
            value={config.indexType}
            onChange={(e) => handleIndexTypeChange(e.target.value)}
            disabled={isChangingIndex}
            style={{
              width: '100%',
              padding: '0.5rem',
              background: isChangingIndex ? 'var(--surface)' : 'var(--bg-primary)',
              border: '2px solid var(--accent-warning)',
              borderRadius: '4px',
              color: 'var(--text-primary)',
              marginTop: '0.25rem',
              fontWeight: '500'
            }}
          >
            {INDEX_TYPES.map(type => (
              <option key={type.value} value={type.value}>{type.label}</option>
            ))}
          </select>
          <p style={{ fontSize: '0.7rem', color: 'var(--text-muted)', margin: '0.25rem 0 0 0' }}>
            {INDEX_TYPES.find(t => t.value === config.indexType)?.description}
          </p>
          {isChangingIndex && (
            <p style={{ fontSize: '0.75rem', color: 'var(--accent-warning)', margin: '0.25rem 0 0 0' }}>
              Rebuilding index...
            </p>
          )}
        </div>

        {/* Table Count */}
        <div className="form-group">
          <label style={{ fontSize: '0.85rem', fontWeight: '500' }}>Tables:</label>
          <input
            type="number"
            min="1"
            max="10"
            value={config.tableCount}
            onChange={(e) => setConfig(prev => ({ ...prev, tableCount: Math.max(1, Math.min(10, parseInt(e.target.value) || 1)) }))}
            disabled={isRunning}
            style={{
              width: '80px',
              padding: '0.5rem',
              background: 'var(--bg-primary)',
              border: '1px solid var(--border)',
              borderRadius: '4px',
              color: 'var(--text-primary)',
              marginTop: '0.25rem'
            }}
          />
        </div>

        {/* Status */}
        {statusMessage && (
          <div style={{
            padding: '0.5rem',
            background: 'var(--bg-primary)',
            borderRadius: '4px',
            fontSize: '0.8rem',
            color: statusMessage.startsWith('Error') ? 'var(--accent-danger)' : 'var(--text-muted)'
          }}>
            {statusMessage}
          </div>
        )}

        {/* A/B Test - Sequence Cache */}
        <div style={{ display: 'flex', flexDirection: 'column', gap: '0.5rem' }}>
          <label style={{ fontSize: '0.85rem', fontWeight: '500' }}>A/B Test - Sequence Cache</label>
          <div style={{ display: 'flex', gap: '0.5rem' }}>
            <input
              type="number"
              min="0"
              value={abTestConfig.cacheA}
              onChange={(e) => setAbTestConfig(prev => ({ ...prev, cacheA: Math.max(0, parseInt(e.target.value || 0, 10)) }))}
              disabled={!isRunning || abTestRunning}
              style={{ flex: 1, padding: '0.4rem' }}
            />
            <input
              type="number"
              min="0"
              value={abTestConfig.cacheB}
              onChange={(e) => setAbTestConfig(prev => ({ ...prev, cacheB: Math.max(0, parseInt(e.target.value || 0, 10)) }))}
              disabled={!isRunning || abTestRunning}
              style={{ flex: 1, padding: '0.4rem' }}
            />
          </div>
          <div style={{ display: 'flex', gap: '0.5rem' }}>
            <input
              type="number"
              min="1"
              value={abTestConfig.warmup}
              onChange={(e) => setAbTestConfig(prev => ({ ...prev, warmup: Math.max(1, parseInt(e.target.value || 1, 10)) }))}
              disabled={!isRunning || abTestRunning}
              style={{ width: '60px', padding: '0.4rem' }}
            />
            <input
              type="number"
              min="1"
              value={abTestConfig.duration}
              onChange={(e) => setAbTestConfig(prev => ({ ...prev, duration: Math.max(1, parseInt(e.target.value || 1, 10)) }))}
              disabled={!isRunning || abTestRunning}
              style={{ width: '80px', padding: '0.4rem' }}
            />
            <button
              onClick={handleRunABTest}
              disabled={!isRunning || abTestRunning}
              className="btn btn-primary"
              style={{ flex: 1 }}
            >
              {abTestRunning ? 'Running A/B...' : 'Run A/B Test'}
            </button>
          </div>
          <div style={{ fontSize: '0.75rem', color: 'var(--text-muted)' }}>
            <div>Cache A &nbsp;&nbsp;&nbsp; Cache B &nbsp;&nbsp;&nbsp; Warmup(s) &nbsp;&nbsp;&nbsp; Duration(s)</div>
          </div>
        </div>

        {/* Buttons */}
        <div style={{ display: 'flex', gap: '0.5rem', marginTop: 'auto' }}>
          <button
            onClick={handleReset}
            disabled={isRunning}
            style={{
              flex: 1,
              padding: '0.5rem',
              background: 'var(--surface)',
              border: '1px solid var(--border)',
              borderRadius: '4px',
              color: 'var(--text-primary)',
              cursor: isRunning ? 'not-allowed' : 'pointer',
              opacity: isRunning ? 0.5 : 1
            }}
          >
            Reset
          </button>
          {!isRunning ? (
            <button
              onClick={handleStart}
              disabled={!hasSchemas}
              className="btn btn-success"
              style={{ flex: 2 }}
            >
              Start
            </button>
          ) : (
            <button
              onClick={handleStop}
              className="btn btn-danger"
              style={{ flex: 2 }}
            >
              Stop
            </button>
          )}
        </div>

        {isRunning && (
          <div style={{ textAlign: 'center', fontSize: '0.9rem', color: 'var(--text-muted)' }}>
            Running: {formatUptime(uptime)}
          </div>
        )}
      </div>

      {/* Right Panel - Charts and Metrics */}
      <div style={{ flex: 1, display: 'flex', flexDirection: 'column', gap: '1rem' }}>
        {/* Top Row - Charts */}
        <div style={{ display: 'flex', gap: '1rem', flex: 1 }}>
          {/* Response Time Chart */}
          <div style={{
            flex: 1,
            background: 'var(--surface)',
            borderRadius: '8px',
            padding: '1rem',
            display: 'flex',
            flexDirection: 'column'
          }}>
            <h3 style={{ margin: '0 0 0.5rem 0', fontSize: '0.9rem', color: 'var(--text-muted)' }}>
              Response Time (ms)
            </h3>
            <div style={{ flex: 1, minHeight: '150px' }}>
              <Line data={responseTimeChartData} options={chartOptions} />
            </div>
          </div>

          {/* TPS from App Chart */}
          <div style={{
            flex: 1,
            background: 'var(--surface)',
            borderRadius: '8px',
            padding: '1rem',
            display: 'flex',
            flexDirection: 'column'
          }}>
            <h3 style={{ margin: '0 0 0.5rem 0', fontSize: '0.9rem', color: '#f59e0b' }}>
              TPS from App
            </h3>
            <div style={{ flex: 1, minHeight: '150px' }}>
              <Line data={tpsAppChartData} options={chartOptions} />
            </div>
          </div>

          {/* TPS from Oracle Chart */}
          <div style={{
            flex: 1,
            background: 'var(--surface)',
            borderRadius: '8px',
            padding: '1rem',
            display: 'flex',
            flexDirection: 'column'
          }}>
            <h3 style={{ margin: '0 0 0.5rem 0', fontSize: '0.9rem', color: '#10b981' }}>
              TPS from Oracle
            </h3>
            <div style={{ flex: 1, minHeight: '150px' }}>
              <Line data={tpsOracleChartData} options={chartOptions} />
            </div>
          </div>

          {/* Wait Events Panel */}
          <div style={{
            width: '250px',
            background: 'var(--surface)',
            borderRadius: '8px',
            padding: '1rem'
          }}>
            <h3 style={{ margin: '0 0 0.75rem 0', fontSize: '0.9rem', color: 'var(--text-muted)' }}>
              Index Contention (ms)
            </h3>
            <div style={{ display: 'flex', flexDirection: 'column', gap: '0.5rem' }}>
              {Object.entries(waitEvents).map(([event, value]) => (
                <div key={event} style={{
                  display: 'flex',
                  justifyContent: 'space-between',
                  fontSize: '0.8rem',
                  padding: '0.25rem 0',
                  borderBottom: '1px solid var(--border)'
                }}>
                  <span style={{ color: 'var(--text-muted)' }}>{event}:</span>
                  <span style={{
                    fontWeight: '600',
                    color: value > 1 ? 'var(--accent-danger)' : value > 0.1 ? 'var(--accent-warning)' : 'var(--accent-success)'
                  }}>
                    {value.toFixed(2)}
                  </span>
                </div>
              ))}
            </div>
          </div>
        </div>

        {/* Bottom Row - Summary Stats */}
        <div style={{
          display: 'flex',
          gap: '1rem',
          background: 'var(--surface)',
          borderRadius: '8px',
          padding: '1rem'
        }}>
          <div style={{ flex: 1, textAlign: 'center' }}>
            <div style={{ fontSize: '2rem', fontWeight: '700', color: '#f59e0b' }}>
              {metrics.tpsApp.toLocaleString()}
            </div>
            <div style={{ fontSize: '0.8rem', color: 'var(--text-muted)' }}>TPS from App</div>
          </div>
          <div style={{ flex: 1, textAlign: 'center' }}>
            <div style={{ fontSize: '2rem', fontWeight: '700', color: '#10b981' }}>
              {metrics.tpsOracle.toLocaleString()}
            </div>
            <div style={{ fontSize: '0.8rem', color: 'var(--text-muted)' }}>TPS from Oracle</div>
          </div>
          <div style={{ flex: 1, textAlign: 'center' }}>
            <div style={{ fontSize: '2rem', fontWeight: '700', color: 'var(--accent-warning)' }}>
              {metrics.avgResponseTime.toFixed(2)}
            </div>
            <div style={{ fontSize: '0.8rem', color: 'var(--text-muted)' }}>Avg Response (ms)</div>
          </div>
          <div style={{ flex: 1, textAlign: 'center' }}>
            <div style={{ fontSize: '2rem', fontWeight: '700', color: 'var(--accent-primary)' }}>
              {metrics.totalTransactions.toLocaleString()}
            </div>
            <div style={{ fontSize: '0.8rem', color: 'var(--text-muted)' }}>Total Txns</div>
          </div>
          <div style={{ flex: 1, textAlign: 'center' }}>
            <div style={{ fontSize: '2rem', fontWeight: '700', color: metrics.errors > 0 ? 'var(--accent-danger)' : 'var(--text-muted)' }}>
              {metrics.errors}
            </div>
            <div style={{ fontSize: '0.8rem', color: 'var(--text-muted)' }}>Errors</div>
          </div>
        </div>

        {/* TOP 10 Wait Events Chart */}
        {top10WaitEvents.length > 0 && (
          <div style={{
            background: 'var(--surface)',
            borderRadius: '8px',
            padding: '1rem'
          }}>
            <h3 style={{ margin: '0 0 0.75rem 0', fontSize: '0.9rem', color: 'var(--text-muted)' }}>
              TOP 10 Wait Events (RAC-wide)
            </h3>
            <div style={{ height: '280px' }}>
              <Bar data={top10WaitEventsChartData} options={top10WaitEventsChartOptions} />
            </div>
          </div>
        )}

        {/* A/B Test Results */}
        {abTestResult && (
          <div style={{ marginTop: '1rem', background: 'var(--surface)', borderRadius: '8px', padding: '1rem' }}>
            <h3 style={{ margin: '0 0 0.5rem 0', fontSize: '0.9rem', color: 'var(--text-muted)' }}>A/B Test Results</h3>
            <div style={{ display:'flex', gap:'1rem' }}>
              <div style={{ flex:1 }}>
                <div style={{ fontSize:'0.85rem', color:'var(--text-muted)' }}>Cache {abTestResult.cacheA}</div>
                <div style={{ fontSize:'1.2rem', fontWeight:700 }}>{(abTestResult.results && abTestResult.results[abTestResult.cacheA] && abTestResult.results[abTestResult.cacheA].meanTps) ? Math.round(abTestResult.results[abTestResult.cacheA].meanTps) : 'N/A' } TPS</div>
                <div style={{ fontSize:'0.8rem', color:'var(--text-muted)' }}>Avg Response: {(abTestResult.results && abTestResult.results[abTestResult.cacheA] && abTestResult.results[abTestResult.cacheA].meanAvgResponseTime) ? abTestResult.results[abTestResult.cacheA].meanAvgResponseTime.toFixed(2) : 'N/A'} ms</div>
              </div>
              <div style={{ flex:1 }}>
                <div style={{ fontSize:'0.85rem', color:'var(--text-muted)' }}>Cache {abTestResult.cacheB}</div>
                <div style={{ fontSize:'1.2rem', fontWeight:700 }}>{(abTestResult.results && abTestResult.results[abTestResult.cacheB] && abTestResult.results[abTestResult.cacheB].meanTps) ? Math.round(abTestResult.results[abTestResult.cacheB].meanTps) : 'N/A' } TPS</div>
                <div style={{ fontSize:'0.8rem', color:'var(--text-muted)' }}>Avg Response: {(abTestResult.results && abTestResult.results[abTestResult.cacheB] && abTestResult.results[abTestResult.cacheB].meanAvgResponseTime) ? abTestResult.results[abTestResult.cacheB].meanAvgResponseTime.toFixed(2) : 'N/A'} ms</div>
              </div>
            </div>
            <div style={{ marginTop:'0.75rem', height:'180px' }}>
              <Line data={abChartData} options={{ ...chartOptions, plugins: { legend: { display: true } } }} />
            </div>
          </div>
        )}

      </div>
    </div>
  );
}

export default IndexContentionPanel;
