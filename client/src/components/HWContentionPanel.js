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

function HWContentionPanel({ dbStatus, socket, schemas }) {
  // Configuration state
  const [config, setConfig] = useState({
    threads: 50,                    // Number of inserting sessions
    testMode: 'no_prealloc',        // 'no_prealloc', 'prealloc', 'partitioned'
    preAllocExtents: 100,           // Number of extents to pre-allocate
    partitionCount: 8,              // Number of partitions
    loopDelay: 0                    // Delay between inserts (ms)
  });

  // Runtime state
  const [isRunning, setIsRunning] = useState(false);
  const [selectedSchema, setSelectedSchema] = useState('');

  // Metrics state
  const [metrics, setMetrics] = useState({
    tps: 0,
    avgResponseTime: 0,
    totalInserts: 0,
    errors: 0
  });

  // Wait events state
  const [hwEvents, setHwEvents] = useState({});
  const [top10WaitEvents, setTop10WaitEvents] = useState([]);
  const [segmentStats, setSegmentStats] = useState({});

  // Histogram/Stats state
  const [statsMethodOpt, setStatsMethodOpt] = useState('AUTO'); // 'AUTO' or 'SIZE_254'
  const [histogramInfo, setHistogramInfo] = useState([]);
  const [tableStats, setTableStats] = useState({});
  const [histgramRows, setHistgramRows] = useState(0);
  const [isGatheringStats, setIsGatheringStats] = useState(false);

  // Chart data - keep last 60 seconds
  const maxDataPoints = 60;
  const [tpsHistory, setTpsHistory] = useState([]);
  const [responseTimeHistory, setResponseTimeHistory] = useState([]);
  const [labels, setLabels] = useState([]);

  // Status message
  const [statusMessage, setStatusMessage] = useState('');

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

  // Listen for HW contention metrics
  useEffect(() => {
    if (socket) {
      socket.on('hw-contention-metrics', (data) => {
        setMetrics({
          tps: data.tps || 0,
          avgResponseTime: data.avgResponseTime || 0,
          totalInserts: data.totalInserts || 0,
          errors: data.errors || 0
        });

        // Update chart data
        if (data.tps > 0 || data.totalInserts > 0) {
          const now = new Date();
          const timeLabel = `${now.getMinutes()}:${now.getSeconds().toString().padStart(2, '0')}`;

          setLabels(prev => {
            const newLabels = [...prev, timeLabel];
            return newLabels.slice(-maxDataPoints);
          });

          setTpsHistory(prev => {
            const newData = [...prev, data.tps || 0];
            return newData.slice(-maxDataPoints);
          });

          setResponseTimeHistory(prev => {
            const newData = [...prev, data.avgResponseTime || 0];
            return newData.slice(-maxDataPoints);
          });
        }
      });

      socket.on('hw-contention-status', (data) => {
        setStatusMessage(data.message || '');
        if (data.running !== undefined) {
          setIsRunning(data.running);
        }
      });

      socket.on('hw-contention-wait-events', (data) => {
        if (data.hwEvents) {
          setHwEvents(data.hwEvents);
        }
        if (data.top10WaitEvents) {
          setTop10WaitEvents(data.top10WaitEvents);
        }
        if (data.segmentStats) {
          setSegmentStats(data.segmentStats);
        }
      });

      socket.on('hw-contention-stopped', (data) => {
        setIsRunning(false);
        setStatusMessage('Stopped');
      });

      socket.on('hw-contention-histogram-info', (data) => {
        if (data.histogramInfo) {
          setHistogramInfo(data.histogramInfo);
        }
        if (data.tableStats) {
          setTableStats(data.tableStats);
        }
        if (data.histgramRows !== undefined) {
          setHistgramRows(data.histgramRows);
        }
      });
    }

    return () => {
      if (socket) {
        socket.off('hw-contention-metrics');
        socket.off('hw-contention-status');
        socket.off('hw-contention-wait-events');
        socket.off('hw-contention-stopped');
        socket.off('hw-contention-histogram-info');
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
      setStatusMessage('Starting HW Contention Demo...');
      // Clear chart data
      setTpsHistory([]);
      setResponseTimeHistory([]);
      setLabels([]);
      setHwEvents({});
      setTop10WaitEvents([]);
      setSegmentStats({});

      // Set running immediately for UI responsiveness
      setIsRunning(true);

      const response = await fetch(`${API_BASE}/hw-contention/start`, {
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

      await fetch(`${API_BASE}/hw-contention/stop`, { method: 'POST' });

      setIsRunning(false);
      setStatusMessage('Stopped');

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
    setTpsHistory([]);
    setResponseTimeHistory([]);
    setLabels([]);
    setMetrics({ tps: 0, avgResponseTime: 0, totalInserts: 0, errors: 0 });
    setHwEvents({});
    setTop10WaitEvents([]);
    setSegmentStats({});
    setHistogramInfo([]);
    setTableStats({});
    setHistgramRows(0);
  };

  const handleGatherStats = async () => {
    try {
      setIsGatheringStats(true);
      setStatusMessage(`Gathering stats (${statsMethodOpt})...`);

      const response = await fetch(`${API_BASE}/hw-contention/gather-stats`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ methodOpt: statsMethodOpt })
      });

      const data = await response.json();

      if (!response.ok) {
        throw new Error(data.error || 'Failed to gather stats');
      }

      setStatusMessage(`Stats gathered in ${data.elapsed}s`);
    } catch (err) {
      setStatusMessage(`Stats error: ${err.message}`);
    } finally {
      setIsGatheringStats(false);
    }
  };

  const formatUptime = (seconds) => {
    const mins = Math.floor(seconds / 60);
    const secs = seconds % 60;
    return `${mins}:${secs.toString().padStart(2, '0')}`;
  };

  // TPS Chart configuration
  const tpsChartData = {
    labels,
    datasets: [{
      label: 'Inserts/sec',
      data: tpsHistory,
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
      borderColor: '#f59e0b',
      backgroundColor: 'rgba(245, 158, 11, 0.3)',
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
        'rgba(239, 68, 68, 0.7)',
        'rgba(249, 115, 22, 0.7)',
        'rgba(245, 158, 11, 0.7)',
        'rgba(234, 179, 8, 0.7)',
        'rgba(132, 204, 22, 0.7)',
        'rgba(34, 197, 94, 0.7)',
        'rgba(20, 184, 166, 0.7)',
        'rgba(6, 182, 212, 0.7)',
        'rgba(59, 130, 246, 0.7)',
        'rgba(139, 92, 246, 0.7)'
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

  const existingSchemas = schemas || [];
  const hasSchemas = existingSchemas.length > 0;

  // Key HW contention events to highlight
  const keyEvents = [
    'enq: HW - contention',
    'enq: TM - contention',
    'enq: TX - row lock contention',
    'enq: TX - index contention',
    'buffer busy waits',
    'free buffer waits',
    'log file sync',
    'log buffer space'
  ];

  const testModeLabels = {
    'no_prealloc': 'No Pre-allocation (Max HW contention)',
    'prealloc': 'Pre-allocate Extents (Reduced HW)',
    'partitioned': 'Partitioned Table (Distributed HW)'
  };

  if (!dbStatus.connected) {
    return (
      <div className="panel" style={{ height: '100%' }}>
        <div className="panel-header">
          <h2>HW Contention Demo</h2>
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
        width: '320px',
        flexShrink: 0,
        background: 'var(--surface)',
        borderRadius: '8px',
        padding: '1rem',
        display: 'flex',
        flexDirection: 'column',
        gap: '1rem',
        overflowY: 'auto'
      }}>
        <h2 style={{ margin: 0, fontSize: '1.1rem', borderBottom: '1px solid var(--border)', paddingBottom: '0.5rem' }}>
          HW Contention Demo
        </h2>

        {/* Info Box */}
        <div style={{
          padding: '0.75rem',
          background: 'rgba(245, 158, 11, 0.1)',
          border: '1px solid rgba(245, 158, 11, 0.3)',
          borderRadius: '4px',
          fontSize: '0.75rem',
          color: 'var(--text-muted)'
        }}>
          <strong style={{ color: '#f59e0b' }}>enq: HW - contention</strong>
          <div style={{ marginTop: '0.25rem' }}>
            High Water Mark contention occurs when multiple sessions try to allocate space in the same segment.
          </div>
          <ul style={{ margin: '0.5rem 0 0 0', paddingLeft: '1.2rem' }}>
            <li><strong>Test 1:</strong> No pre-allocation (max contention)</li>
            <li><strong>Test 2:</strong> Pre-allocate extents (reduced)</li>
            <li><strong>Test 3:</strong> Partitioned table (distributed)</li>
          </ul>
        </div>

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

        {/* Test Mode Selection */}
        <div className="form-group">
          <label style={{ fontSize: '0.85rem', fontWeight: '500' }}>Test Mode:</label>
          <select
            value={config.testMode}
            onChange={(e) => setConfig(prev => ({ ...prev, testMode: e.target.value }))}
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
            <option value="no_prealloc">1. No Pre-allocation (Max HW)</option>
            <option value="prealloc">2. Pre-allocate Extents</option>
            <option value="partitioned">3. Partitioned Table</option>
          </select>
          <div style={{ fontSize: '0.7rem', color: 'var(--text-muted)', marginTop: '0.25rem' }}>
            {testModeLabels[config.testMode]}
          </div>
        </div>

        {/* Number of Sessions */}
        <div className="form-group">
          <label style={{ fontSize: '0.85rem', fontWeight: '500' }}>Concurrent Sessions:</label>
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

        {/* Pre-alloc Extents (only for prealloc mode) */}
        {config.testMode === 'prealloc' && (
          <div className="form-group">
            <label style={{ fontSize: '0.85rem', fontWeight: '500' }}>Pre-allocate Extents:</label>
            <input
              type="number"
              min="10"
              max="1000"
              value={config.preAllocExtents}
              onChange={(e) => setConfig(prev => ({ ...prev, preAllocExtents: Math.max(10, parseInt(e.target.value) || 100) }))}
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
              ALTER TABLE ... ALLOCATE EXTENT
            </div>
          </div>
        )}

        {/* Partition Count (only for partitioned mode) */}
        {config.testMode === 'partitioned' && (
          <div className="form-group">
            <label style={{ fontSize: '0.85rem', fontWeight: '500' }}>Number of Partitions:</label>
            <input
              type="number"
              min="2"
              max="64"
              value={config.partitionCount}
              onChange={(e) => setConfig(prev => ({ ...prev, partitionCount: Math.max(2, Math.min(64, parseInt(e.target.value) || 8)) }))}
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
              Hash partitioned by ID
            </div>
          </div>
        )}

        {/* Loop Delay */}
        <div className="form-group">
          <label style={{ fontSize: '0.85rem', fontWeight: '500' }}>Loop Delay (ms):</label>
          <input
            type="number"
            min="0"
            max="1000"
            step="10"
            value={config.loopDelay}
            onChange={(e) => setConfig(prev => ({ ...prev, loopDelay: Math.max(0, parseInt(e.target.value) || 0) }))}
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
            0 = tight loop (max contention)
          </div>
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

        {/* Segment Stats */}
        <div style={{
          padding: '0.75rem',
          background: 'var(--bg-primary)',
          borderRadius: '4px'
        }}>
          <h4 style={{ margin: '0 0 0.5rem 0', fontSize: '0.85rem', color: 'var(--text-muted)' }}>Segment Statistics</h4>
          <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: '0.8rem' }}>
            <span>Size:</span>
            <span style={{ fontWeight: '600' }}>{(segmentStats.sizeMB || 0).toFixed(2)} MB</span>
          </div>
          <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: '0.8rem', marginTop: '0.25rem' }}>
            <span>Extents:</span>
            <span style={{ fontWeight: '600', color: '#f59e0b' }}>{(segmentStats.extents || 0).toLocaleString()}</span>
          </div>
          <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: '0.8rem', marginTop: '0.25rem' }}>
            <span>Blocks:</span>
            <span style={{ fontWeight: '600' }}>{(segmentStats.blocks || 0).toLocaleString()}</span>
          </div>
        </div>

        {/* Gather Statistics Section */}
        <div style={{
          padding: '0.75rem',
          background: 'rgba(59, 130, 246, 0.1)',
          border: '1px solid rgba(59, 130, 246, 0.3)',
          borderRadius: '4px'
        }}>
          <h4 style={{ margin: '0 0 0.5rem 0', fontSize: '0.85rem', color: '#3b82f6' }}>Gather Statistics</h4>

          {/* METHOD_OPT Selection */}
          <div style={{ marginBottom: '0.5rem' }}>
            <label style={{ fontSize: '0.75rem', color: 'var(--text-muted)' }}>METHOD_OPT:</label>
            <select
              value={statsMethodOpt}
              onChange={(e) => setStatsMethodOpt(e.target.value)}
              disabled={isGatheringStats}
              style={{
                width: '100%',
                padding: '0.4rem',
                background: 'var(--bg-primary)',
                border: '1px solid var(--border)',
                borderRadius: '4px',
                color: 'var(--text-primary)',
                fontSize: '0.8rem',
                marginTop: '0.25rem'
              }}
            >
              <option value="AUTO">FOR ALL COLUMNS SIZE AUTO</option>
              <option value="SIZE_254">FOR ALL COLUMNS SIZE 254</option>
            </select>
          </div>

          <button
            onClick={handleGatherStats}
            disabled={isGatheringStats || !metrics.totalInserts}
            style={{
              width: '100%',
              padding: '0.5rem',
              background: isGatheringStats ? 'var(--surface)' : '#3b82f6',
              border: 'none',
              borderRadius: '4px',
              color: '#fff',
              cursor: (isGatheringStats || !metrics.totalInserts) ? 'not-allowed' : 'pointer',
              opacity: (isGatheringStats || !metrics.totalInserts) ? 0.5 : 1,
              fontSize: '0.85rem',
              fontWeight: '500'
            }}
          >
            {isGatheringStats ? 'Gathering...' : 'DBMS_STATS.GATHER_TABLE_STATS'}
          </button>

          {/* Table Stats Summary */}
          {tableStats.numRows !== undefined && (
            <div style={{ marginTop: '0.5rem', fontSize: '0.75rem' }}>
              <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                <span>Rows (stats):</span>
                <span style={{ fontWeight: '600' }}>{(tableStats.numRows || 0).toLocaleString()}</span>
              </div>
              <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                <span>Avg Row Len:</span>
                <span style={{ fontWeight: '600' }}>{tableStats.avgRowLen || 0}</span>
              </div>
              <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                <span>SYS.HISTGRM$ rows:</span>
                <span style={{ fontWeight: '600', color: '#3b82f6' }}>{histgramRows.toLocaleString()}</span>
              </div>
            </div>
          )}
        </div>
      </div>

      {/* Right Panel - Charts and Metrics */}
      <div style={{ flex: 1, display: 'flex', flexDirection: 'column', gap: '1rem', overflowY: 'auto' }}>
        {/* Top Row - Charts */}
        <div style={{ display: 'flex', gap: '1rem' }}>
          {/* TPS Chart */}
          <div style={{
            flex: 1,
            background: 'var(--surface)',
            borderRadius: '8px',
            padding: '1rem',
            display: 'flex',
            flexDirection: 'column'
          }}>
            <h3 style={{ margin: '0 0 0.5rem 0', fontSize: '0.9rem', color: '#10b981' }}>
              Inserts/sec
            </h3>
            <div style={{ flex: 1, minHeight: '150px' }}>
              <Line data={tpsChartData} options={chartOptions} />
            </div>
          </div>

          {/* Response Time Chart */}
          <div style={{
            flex: 1,
            background: 'var(--surface)',
            borderRadius: '8px',
            padding: '1rem',
            display: 'flex',
            flexDirection: 'column'
          }}>
            <h3 style={{ margin: '0 0 0.5rem 0', fontSize: '0.9rem', color: '#f59e0b' }}>
              Response Time (ms)
            </h3>
            <div style={{ flex: 1, minHeight: '150px' }}>
              <Line data={responseTimeChartData} options={chartOptions} />
            </div>
          </div>

          {/* HW Contention Events Panel */}
          <div style={{
            width: '280px',
            background: 'var(--surface)',
            borderRadius: '8px',
            padding: '1rem'
          }}>
            <h3 style={{ margin: '0 0 0.75rem 0', fontSize: '0.9rem', color: '#f59e0b' }}>
              HW & Space Contention
            </h3>
            <div style={{ display: 'flex', flexDirection: 'column', gap: '0.4rem' }}>
              {keyEvents.map(event => {
                const eventData = hwEvents[event];
                const timeSeconds = eventData?.timeSeconds || 0;
                const isHW = event === 'enq: HW - contention';
                return (
                  <div key={event} style={{
                    display: 'flex',
                    justifyContent: 'space-between',
                    fontSize: '0.75rem',
                    padding: '0.2rem 0',
                    borderBottom: '1px solid var(--border)',
                    background: isHW && timeSeconds > 0 ? 'rgba(245, 158, 11, 0.1)' : 'transparent'
                  }}>
                    <span style={{
                      color: isHW ? '#f59e0b' : 'var(--text-muted)',
                      fontWeight: isHW ? '600' : '400',
                      maxWidth: '180px',
                      overflow: 'hidden',
                      textOverflow: 'ellipsis'
                    }}>
                      {event}:
                    </span>
                    <span style={{
                      fontWeight: '600',
                      color: timeSeconds > 1 ? '#ef4444' : timeSeconds > 0.1 ? '#f59e0b' : '#10b981'
                    }}>
                      {timeSeconds.toFixed(2)}s
                    </span>
                  </div>
                );
              })}
            </div>
          </div>
        </div>

        {/* Summary Stats */}
        <div style={{
          display: 'flex',
          gap: '1rem',
          background: 'var(--surface)',
          borderRadius: '8px',
          padding: '1rem'
        }}>
          <div style={{ flex: 1, textAlign: 'center' }}>
            <div style={{ fontSize: '2rem', fontWeight: '700', color: '#10b981' }}>
              {metrics.tps.toLocaleString()}
            </div>
            <div style={{ fontSize: '0.8rem', color: 'var(--text-muted)' }}>Inserts/sec</div>
          </div>
          <div style={{ flex: 1, textAlign: 'center' }}>
            <div style={{ fontSize: '2rem', fontWeight: '700', color: '#f59e0b' }}>
              {metrics.avgResponseTime.toFixed(2)}
            </div>
            <div style={{ fontSize: '0.8rem', color: 'var(--text-muted)' }}>Avg Response (ms)</div>
          </div>
          <div style={{ flex: 1, textAlign: 'center' }}>
            <div style={{ fontSize: '2rem', fontWeight: '700', color: 'var(--accent-primary)' }}>
              {metrics.totalInserts.toLocaleString()}
            </div>
            <div style={{ fontSize: '0.8rem', color: 'var(--text-muted)' }}>Total Inserts</div>
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

        {/* Histogram Info Table */}
        {histogramInfo.length > 0 && (
          <div style={{
            background: 'var(--surface)',
            borderRadius: '8px',
            padding: '1rem'
          }}>
            <h3 style={{ margin: '0 0 0.75rem 0', fontSize: '0.9rem', color: '#3b82f6' }}>
              Column Statistics & Histograms
            </h3>
            <div style={{ overflowX: 'auto' }}>
              <table style={{
                width: '100%',
                borderCollapse: 'collapse',
                fontSize: '0.8rem'
              }}>
                <thead>
                  <tr style={{ borderBottom: '2px solid var(--border)' }}>
                    <th style={{ padding: '0.5rem', textAlign: 'left', color: 'var(--text-muted)' }}>Column</th>
                    <th style={{ padding: '0.5rem', textAlign: 'right', color: 'var(--text-muted)' }}>Distinct</th>
                    <th style={{ padding: '0.5rem', textAlign: 'right', color: 'var(--text-muted)' }}>Nulls</th>
                    <th style={{ padding: '0.5rem', textAlign: 'right', color: 'var(--text-muted)' }}>Buckets</th>
                    <th style={{ padding: '0.5rem', textAlign: 'left', color: 'var(--text-muted)' }}>Histogram Type</th>
                  </tr>
                </thead>
                <tbody>
                  {histogramInfo.map((col, idx) => (
                    <tr key={col.columnName} style={{
                      borderBottom: '1px solid var(--border)',
                      background: idx % 2 === 0 ? 'transparent' : 'rgba(255,255,255,0.02)'
                    }}>
                      <td style={{ padding: '0.4rem 0.5rem', fontWeight: '500' }}>{col.columnName}</td>
                      <td style={{ padding: '0.4rem 0.5rem', textAlign: 'right' }}>{(col.numDistinct || 0).toLocaleString()}</td>
                      <td style={{ padding: '0.4rem 0.5rem', textAlign: 'right' }}>{(col.numNulls || 0).toLocaleString()}</td>
                      <td style={{ padding: '0.4rem 0.5rem', textAlign: 'right', color: col.numBuckets > 1 ? '#3b82f6' : 'var(--text-muted)' }}>
                        {col.numBuckets || 0}
                      </td>
                      <td style={{
                        padding: '0.4rem 0.5rem',
                        color: col.histogramType === 'NONE' ? 'var(--text-muted)' :
                               col.histogramType === 'FREQUENCY' ? '#10b981' :
                               col.histogramType === 'HEIGHT BALANCED' ? '#f59e0b' :
                               col.histogramType === 'HYBRID' ? '#a855f7' : '#3b82f6'
                      }}>
                        {col.histogramType || 'NONE'}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

export default HWContentionPanel;
