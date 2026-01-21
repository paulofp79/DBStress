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

function LibraryCacheLockPanel({ dbStatus, socket, schemas }) {
  // Configuration state - using non-existent sequence pattern
  const [config, setConfig] = useState({
    threads: 50,                    // Number of workers
    sequenceCount: 3,               // Number of non-existent sequences to query per iteration
    loopDelay: 0                    // Delay between loops (ms), 0 = tight loop
  });

  // Runtime state
  const [isRunning, setIsRunning] = useState(false);
  const [selectedSchema, setSelectedSchema] = useState('');

  // Metrics state
  const [metrics, setMetrics] = useState({
    tps: 0,
    avgResponseTime: 0,
    totalCalls: 0,
    totalSelects: 0,
    errors: 0
  });

  // Wait events state
  const [libraryCacheEvents, setLibraryCacheEvents] = useState({});
  const [top10WaitEvents, setTop10WaitEvents] = useState([]);
  const [hardParses, setHardParses] = useState(0);
  const [parseCount, setParseCount] = useState(0);

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

  // Listen for library cache lock metrics
  useEffect(() => {
    if (socket) {
      socket.on('library-cache-lock-metrics', (data) => {
        setMetrics({
          tps: data.tps || 0,
          avgResponseTime: data.avgResponseTime || 0,
          totalCalls: data.totalCalls || 0,
          totalSelects: data.totalSelects || 0,
          errors: data.errors || 0
        });

        // Only update chart data if we have actual TPS
        if (data.tps > 0) {
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

      socket.on('library-cache-lock-status', (data) => {
        setStatusMessage(data.message || '');
        if (data.running !== undefined) {
          setIsRunning(data.running);
        }
      });

      socket.on('library-cache-lock-wait-events', (data) => {
        if (data.libraryCacheEvents) {
          setLibraryCacheEvents(data.libraryCacheEvents);
        }
        if (data.top10WaitEvents) {
          setTop10WaitEvents(data.top10WaitEvents);
        }
        if (data.hardParses !== undefined) {
          setHardParses(data.hardParses);
        }
        if (data.parseCount !== undefined) {
          setParseCount(data.parseCount);
        }
      });

      socket.on('library-cache-lock-stopped', (data) => {
        setIsRunning(false);
        setStatusMessage('Stopped');
      });
    }

    return () => {
      if (socket) {
        socket.off('library-cache-lock-metrics');
        socket.off('library-cache-lock-status');
        socket.off('library-cache-lock-wait-events');
        socket.off('library-cache-lock-stopped');
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
      setStatusMessage('Starting Library Cache Lock Demo...');
      // Clear chart data
      setTpsHistory([]);
      setResponseTimeHistory([]);
      setLabels([]);
      setLibraryCacheEvents({});
      setTop10WaitEvents([]);

      // Set running immediately for UI responsiveness
      setIsRunning(true);

      const response = await fetch(`${API_BASE}/library-cache-lock/start`, {
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

      await fetch(`${API_BASE}/library-cache-lock/stop`, { method: 'POST' });

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
    setMetrics({ tps: 0, avgResponseTime: 0, totalCalls: 0, totalSelects: 0, errors: 0 });
    setLibraryCacheEvents({});
    setTop10WaitEvents([]);
    setHardParses(0);
    setParseCount(0);
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
      label: 'Iterations/sec',
      data: tpsHistory,
      borderColor: '#f59e0b',
      backgroundColor: 'rgba(245, 158, 11, 0.2)',
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

  // Key library cache events to highlight
  const keyEvents = [
    'library cache lock',
    'library cache pin',
    'cursor: pin S wait on X',
    'cursor: pin S',
    'cursor: mutex S',
    'cursor: mutex X',
    'latch: shared pool',
    'latch: library cache'
  ];

  if (!dbStatus.connected) {
    return (
      <div className="panel" style={{ height: '100%' }}>
        <div className="panel-header">
          <h2>Library Cache Lock Demo</h2>
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
        width: '300px',
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
          Library Cache Lock Demo
        </h2>

        {/* Info Box */}
        <div style={{
          padding: '0.75rem',
          background: 'rgba(239, 68, 68, 0.1)',
          border: '1px solid rgba(239, 68, 68, 0.3)',
          borderRadius: '4px',
          fontSize: '0.75rem',
          color: 'var(--text-muted)'
        }}>
          <strong style={{ color: '#ef4444' }}>Non-Existent Sequence Pattern:</strong>
          <ul style={{ margin: '0.25rem 0 0 0', paddingLeft: '1.2rem' }}>
            <li>Multiple sessions SELECT from non-existent sequences</li>
            <li>ORA-02289 errors cause library cache lock contention</li>
          </ul>
          <div style={{ marginTop: '0.5rem', fontStyle: 'italic' }}>
            Reproduces: library cache lock, hard parse storms
          </div>
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

        {/* Number of Workers */}
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
          <div style={{ fontSize: '0.7rem', color: 'var(--text-muted)', marginTop: '0.25rem' }}>
            Number of parallel sessions
          </div>
        </div>

        {/* Sequence Count */}
        <div className="form-group">
          <label style={{ fontSize: '0.85rem', fontWeight: '500' }}>SELECTs per Iteration:</label>
          <input
            type="number"
            min="1"
            max="10"
            value={config.sequenceCount}
            onChange={(e) => setConfig(prev => ({ ...prev, sequenceCount: Math.max(1, Math.min(10, parseInt(e.target.value) || 3)) }))}
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
            Number of non-existent sequences to query (customer had 3)
          </div>
        </div>

        {/* Loop Delay */}
        <div className="form-group">
          <label style={{ fontSize: '0.85rem', fontWeight: '500' }}>Loop Delay (ms):</label>
          <input
            type="number"
            min="0"
            max="5000"
            step="100"
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
            Delay between iterations (0 = tight loop, max contention)
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

        {/* Parse Stats */}
        <div style={{
          padding: '0.75rem',
          background: 'var(--bg-primary)',
          borderRadius: '4px'
        }}>
          <h4 style={{ margin: '0 0 0.5rem 0', fontSize: '0.85rem', color: 'var(--text-muted)' }}>Parse Statistics</h4>
          <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: '0.8rem' }}>
            <span>Hard Parses:</span>
            <span style={{ fontWeight: '600', color: '#ef4444' }}>{hardParses.toLocaleString()}</span>
          </div>
          <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: '0.8rem', marginTop: '0.25rem' }}>
            <span>Total Parses:</span>
            <span style={{ fontWeight: '600' }}>{parseCount.toLocaleString()}</span>
          </div>
          {parseCount > 0 && (
            <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: '0.8rem', marginTop: '0.25rem' }}>
              <span>Hard Parse Ratio:</span>
              <span style={{ fontWeight: '600', color: (hardParses / parseCount) > 0.1 ? '#ef4444' : '#10b981' }}>
                {((hardParses / parseCount) * 100).toFixed(1)}%
              </span>
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
            <h3 style={{ margin: '0 0 0.5rem 0', fontSize: '0.9rem', color: '#f59e0b' }}>
              Iterations/sec
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
            <h3 style={{ margin: '0 0 0.5rem 0', fontSize: '0.9rem', color: '#ef4444' }}>
              Response Time (ms)
            </h3>
            <div style={{ flex: 1, minHeight: '150px' }}>
              <Line data={responseTimeChartData} options={chartOptions} />
            </div>
          </div>

          {/* Library Cache Events Panel */}
          <div style={{
            width: '280px',
            background: 'var(--surface)',
            borderRadius: '8px',
            padding: '1rem'
          }}>
            <h3 style={{ margin: '0 0 0.75rem 0', fontSize: '0.9rem', color: '#ef4444' }}>
              Library Cache Contention
            </h3>
            <div style={{ display: 'flex', flexDirection: 'column', gap: '0.4rem' }}>
              {keyEvents.map(event => {
                const eventData = libraryCacheEvents[event];
                const timeSeconds = eventData?.timeSeconds || 0;
                return (
                  <div key={event} style={{
                    display: 'flex',
                    justifyContent: 'space-between',
                    fontSize: '0.75rem',
                    padding: '0.2rem 0',
                    borderBottom: '1px solid var(--border)'
                  }}>
                    <span style={{ color: 'var(--text-muted)', maxWidth: '180px', overflow: 'hidden', textOverflow: 'ellipsis' }}>
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
            <div style={{ fontSize: '2rem', fontWeight: '700', color: '#f59e0b' }}>
              {metrics.tps.toLocaleString()}
            </div>
            <div style={{ fontSize: '0.8rem', color: 'var(--text-muted)' }}>Iterations/sec</div>
          </div>
          <div style={{ flex: 1, textAlign: 'center' }}>
            <div style={{ fontSize: '2rem', fontWeight: '700', color: '#ef4444' }}>
              {metrics.avgResponseTime.toFixed(2)}
            </div>
            <div style={{ fontSize: '0.8rem', color: 'var(--text-muted)' }}>Avg Response (ms)</div>
          </div>
          <div style={{ flex: 1, textAlign: 'center' }}>
            <div style={{ fontSize: '2rem', fontWeight: '700', color: 'var(--accent-primary)' }}>
              {metrics.totalCalls.toLocaleString()}
            </div>
            <div style={{ fontSize: '0.8rem', color: 'var(--text-muted)' }}>Total Iterations</div>
          </div>
          <div style={{ flex: 1, textAlign: 'center' }}>
            <div style={{ fontSize: '2rem', fontWeight: '700', color: '#a855f7' }}>
              {metrics.totalSelects.toLocaleString()}
            </div>
            <div style={{ fontSize: '0.8rem', color: 'var(--text-muted)' }}>SELECTs (ORA-02289)</div>
          </div>
          <div style={{ flex: 1, textAlign: 'center' }}>
            <div style={{ fontSize: '2rem', fontWeight: '700', color: metrics.errors > 0 ? 'var(--accent-danger)' : 'var(--text-muted)' }}>
              {metrics.errors}
            </div>
            <div style={{ fontSize: '0.8rem', color: 'var(--text-muted)' }}>Other Errors</div>
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
      </div>
    </div>
  );
}

export default LibraryCacheLockPanel;
