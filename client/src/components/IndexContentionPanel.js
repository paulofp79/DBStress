import React, { useState, useEffect, useRef } from 'react';
import { Line } from 'react-chartjs-2';
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
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
  { value: 'none', label: 'NONE (No Index - Heap Table)', description: 'No primary key index, heap organized table' },
  { value: 'standard', label: 'Standard B-Tree Index', description: 'Normal ascending sequence PK - maximum right-hand contention' },
  { value: 'reverse', label: 'Reverse Key Index', description: 'Distributes inserts across leaf blocks, but increases I/O' },
  { value: 'hash_partition', label: 'Hash Partitioned Index', description: 'Partitions index by hash - helps single instance, not RAC' },
  { value: 'scalable_sequence', label: 'Scalable Sequence (18c+)', description: 'Uses instance-prefixed sequences to avoid contention' }
];

function IndexContentionPanel({ dbStatus, socket, schemas }) {
  // Configuration state
  const [config, setConfig] = useState({
    threads: 50,
    thinkTime: 10,
    indexType: 'standard',
    tableCount: 1
  });

  // Runtime state
  const [isRunning, setIsRunning] = useState(false);
  const [selectedSchema, setSelectedSchema] = useState('');

  // Metrics state
  const [metrics, setMetrics] = useState({
    tps: 0,
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

  // Chart data - keep last 60 seconds
  const maxDataPoints = 60;
  const [tpsHistory, setTpsHistory] = useState([]);
  const [responseTimeHistory, setResponseTimeHistory] = useState([]);
  const [labels, setLabels] = useState([]);

  // Status message
  const [statusMessage, setStatusMessage] = useState('');
  const [isChangingIndex, setIsChangingIndex] = useState(false);

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
          tps: data.tps || 0,
          avgResponseTime: data.avgResponseTime || 0,
          totalTransactions: data.totalTransactions || 0,
          errors: data.errors || 0
        });

        // Update chart data
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

      socket.on('index-contention-stopped', () => {
        setIsRunning(false);
        if (uptimeRef.current) {
          clearInterval(uptimeRef.current);
          uptimeRef.current = null;
        }
      });
    }

    return () => {
      if (socket) {
        socket.off('index-contention-metrics');
        socket.off('index-contention-status');
        socket.off('index-contention-stopped');
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
      setTpsHistory([]);
      setResponseTimeHistory([]);
      setLabels([]);

      const response = await fetch(`${API_BASE}/index-contention/start`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          ...config,
          schemaPrefix: selectedSchema
        })
      });

      if (!response.ok) {
        const error = await response.json();
        throw new Error(error.error || 'Failed to start');
      }

      setIsRunning(true);
      setStatusMessage('Running...');
    } catch (err) {
      setStatusMessage(`Error: ${err.message}`);
    }
  };

  const handleStop = async () => {
    try {
      setStatusMessage('Stopping...');
      await fetch(`${API_BASE}/index-contention/stop`, { method: 'POST' });
      setIsRunning(false);
      setStatusMessage('Stopped');
    } catch (err) {
      setStatusMessage(`Error: ${err.message}`);
    }
  };

  const handleReset = () => {
    setTpsHistory([]);
    setResponseTimeHistory([]);
    setLabels([]);
    setMetrics({ tps: 0, avgResponseTime: 0, totalTransactions: 0, errors: 0 });
    setWaitEvents({
      'buffer busy waits': 0,
      'enq: TX - index contention': 0,
      'gc buffer busy acquire': 0,
      'gc buffer busy release': 0,
      'cell single block physical read': 0
    });
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

  const formatUptime = (seconds) => {
    const mins = Math.floor(seconds / 60);
    const secs = seconds % 60;
    return `${mins}:${secs.toString().padStart(2, '0')}`;
  };

  // TPS Chart configuration
  const tpsChartData = {
    labels,
    datasets: [{
      label: 'Throughput (TPS)',
      data: tpsHistory,
      borderColor: '#f59e0b',
      backgroundColor: 'rgba(245, 158, 11, 0.1)',
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

  const chartOptions = {
    responsive: true,
    maintainAspectRatio: false,
    animation: { duration: 0 },
    scales: {
      x: {
        display: true,
        grid: { color: 'rgba(255,255,255,0.1)' },
        ticks: { color: 'var(--text-muted)', maxTicksLimit: 10 }
      },
      y: {
        display: true,
        beginAtZero: true,
        grid: { color: 'rgba(255,255,255,0.1)' },
        ticks: { color: 'var(--text-muted)' }
      }
    },
    plugins: {
      legend: { display: false }
    }
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

          {/* TPS Chart */}
          <div style={{
            flex: 1,
            background: 'var(--surface)',
            borderRadius: '8px',
            padding: '1rem',
            display: 'flex',
            flexDirection: 'column'
          }}>
            <h3 style={{ margin: '0 0 0.5rem 0', fontSize: '0.9rem', color: 'var(--text-muted)' }}>
              TPS x1000
            </h3>
            <div style={{ flex: 1, minHeight: '150px' }}>
              <Line data={tpsChartData} options={chartOptions} />
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
            <div style={{ fontSize: '2rem', fontWeight: '700', color: 'var(--accent-success)' }}>
              {metrics.tps.toLocaleString()}
            </div>
            <div style={{ fontSize: '0.8rem', color: 'var(--text-muted)' }}>Current TPS</div>
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
            <div style={{ fontSize: '0.8rem', color: 'var(--text-muted)' }}>Total Transactions</div>
          </div>
          <div style={{ flex: 1, textAlign: 'center' }}>
            <div style={{ fontSize: '2rem', fontWeight: '700', color: metrics.errors > 0 ? 'var(--accent-danger)' : 'var(--text-muted)' }}>
              {metrics.errors}
            </div>
            <div style={{ fontSize: '0.8rem', color: 'var(--text-muted)' }}>Errors</div>
          </div>
        </div>
      </div>
    </div>
  );
}

export default IndexContentionPanel;
