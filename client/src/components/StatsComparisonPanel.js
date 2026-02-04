import React, { useState, useEffect } from 'react';
import { Bar } from 'react-chartjs-2';
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  BarElement,
  Title,
  Tooltip,
  Legend
} from 'chart.js';

ChartJS.register(
  CategoryScale,
  LinearScale,
  BarElement,
  Title,
  Tooltip,
  Legend
);

// Auto-detect server URL
const getServerUrl = () => {
  if (window.location.hostname !== 'localhost') {
    return `http://${window.location.host}`;
  }
  return 'http://localhost:3001';
};

const API_BASE = `${getServerUrl()}/api`;

function StatsComparisonPanel({ dbStatus, socket }) {
  // Configuration state
  const [config, setConfig] = useState({
    tableCount: 10,
    rowsPerTable: 100000,
    columnsPerTable: 20,
    schemaPrefix: 'STATS_TEST',
    parallelDegree: 4,
    cleanup: true
  });

  // Runtime state
  const [isRunning, setIsRunning] = useState(false);
  const [statusMessage, setStatusMessage] = useState('');
  const [progress, setProgress] = useState(0);

  // Results state
  const [results, setResults] = useState(null);

  // Listen for socket events
  useEffect(() => {
    if (socket) {
      socket.on('stats-comparison-status', (data) => {
        setIsRunning(data.running);
        setStatusMessage(data.message || '');
        setProgress(data.progress || 0);
      });

      socket.on('stats-comparison-results', (data) => {
        setResults(data);
        setIsRunning(false);
      });
    }

    return () => {
      if (socket) {
        socket.off('stats-comparison-status');
        socket.off('stats-comparison-results');
      }
    };
  }, [socket]);

  const handleStart = async () => {
    try {
      setStatusMessage('Starting Stats Comparison Test...');
      setProgress(0);
      setResults(null);
      setIsRunning(true);

      const response = await fetch(`${API_BASE}/stats-comparison/start`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(config)
      });

      const data = await response.json();

      if (!response.ok) {
        setIsRunning(false);
        throw new Error(data.error || 'Failed to start');
      }
    } catch (err) {
      setIsRunning(false);
      setStatusMessage(`Error: ${err.message}`);
    }
  };

  const handleStop = async () => {
    try {
      setStatusMessage('Stopping...');

      await fetch(`${API_BASE}/stats-comparison/stop`, { method: 'POST' });

      setIsRunning(false);
      setStatusMessage('Stopped');
    } catch (err) {
      setStatusMessage(`Error: ${err.message}`);
    }
  };

  // Chart data for time comparison
  const timeChartData = results ? {
    labels: ['SIZE 254', 'SIZE AUTO'],
    datasets: [{
      label: 'Time (seconds)',
      data: [
        results.size254?.totalTimeMs / 1000 || 0,
        results.sizeAuto?.totalTimeMs / 1000 || 0
      ],
      backgroundColor: ['rgba(59, 130, 246, 0.7)', 'rgba(34, 197, 94, 0.7)'],
      borderColor: ['rgb(59, 130, 246)', 'rgb(34, 197, 94)'],
      borderWidth: 1
    }]
  } : null;

  // Chart data for histogram rows comparison
  const histogramChartData = results ? {
    labels: ['SIZE 254', 'SIZE AUTO'],
    datasets: [{
      label: 'Histogram Rows Added',
      data: [
        results.size254?.histogramRowsAdded || 0,
        results.sizeAuto?.histogramRowsAdded || 0
      ],
      backgroundColor: ['rgba(249, 115, 22, 0.7)', 'rgba(139, 92, 246, 0.7)'],
      borderColor: ['rgb(249, 115, 22)', 'rgb(139, 92, 246)'],
      borderWidth: 1
    }]
  } : null;

  const chartOptions = {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      legend: { display: false }
    },
    scales: {
      x: {
        grid: { color: 'rgba(255,255,255,0.1)' },
        ticks: { color: '#9ca3af' }
      },
      y: {
        beginAtZero: true,
        grid: { color: 'rgba(255,255,255,0.1)' },
        ticks: { color: '#9ca3af' }
      }
    }
  };

  if (!dbStatus.connected) {
    return (
      <div className="panel" style={{ height: '100%' }}>
        <div className="panel-header">
          <h2>Stats Comparison Test</h2>
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
          Stats Method Comparison
        </h2>

        {/* Info Box */}
        <div style={{
          padding: '0.75rem',
          background: 'rgba(59, 130, 246, 0.1)',
          border: '1px solid rgba(59, 130, 246, 0.3)',
          borderRadius: '4px',
          fontSize: '0.75rem',
          color: 'var(--text-muted)'
        }}>
          <strong style={{ color: '#3b82f6' }}>Compares:</strong>
          <ul style={{ margin: '0.25rem 0 0 0', paddingLeft: '1.2rem' }}>
            <li><code>FOR ALL COLUMNS SIZE 254</code></li>
            <li><code>FOR ALL COLUMNS SIZE AUTO</code></li>
          </ul>
          <p style={{ margin: '0.5rem 0 0 0' }}>
            Measures execution time and SYS.HISTGRM$ table growth.
          </p>
        </div>

        {/* Configuration */}
        <div className="form-group">
          <label style={{ fontSize: '0.85rem', fontWeight: '500' }}>Number of Test Tables:</label>
          <input
            type="number"
            min="1"
            max="100"
            value={config.tableCount}
            onChange={(e) => setConfig(prev => ({ ...prev, tableCount: Math.max(1, parseInt(e.target.value) || 1) }))}
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

        <div className="form-group">
          <label style={{ fontSize: '0.85rem', fontWeight: '500' }}>Rows Per Table:</label>
          <input
            type="number"
            min="1000"
            max="10000000"
            step="10000"
            value={config.rowsPerTable}
            onChange={(e) => setConfig(prev => ({ ...prev, rowsPerTable: Math.max(1000, parseInt(e.target.value) || 10000) }))}
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
            Total rows: {(config.tableCount * config.rowsPerTable).toLocaleString()}
          </div>
        </div>

        <div className="form-group">
          <label style={{ fontSize: '0.85rem', fontWeight: '500' }}>Columns Per Table:</label>
          <input
            type="number"
            min="5"
            max="100"
            value={config.columnsPerTable}
            onChange={(e) => setConfig(prev => ({ ...prev, columnsPerTable: Math.max(5, parseInt(e.target.value) || 20) }))}
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

        <div className="form-group">
          <label style={{ fontSize: '0.85rem', fontWeight: '500' }}>Parallel Degree:</label>
          <input
            type="number"
            min="1"
            max="32"
            value={config.parallelDegree}
            onChange={(e) => setConfig(prev => ({ ...prev, parallelDegree: Math.max(1, parseInt(e.target.value) || 4) }))}
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

        <div className="form-group">
          <label style={{ fontSize: '0.85rem', fontWeight: '500' }}>Schema Prefix:</label>
          <input
            type="text"
            value={config.schemaPrefix}
            onChange={(e) => setConfig(prev => ({ ...prev, schemaPrefix: e.target.value.toUpperCase() }))}
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

        <div className="form-group">
          <label style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', fontSize: '0.85rem', cursor: 'pointer' }}>
            <input
              type="checkbox"
              checked={config.cleanup}
              onChange={(e) => setConfig(prev => ({ ...prev, cleanup: e.target.checked }))}
              disabled={isRunning}
            />
            <span style={{ fontWeight: '500' }}>
              Cleanup test tables after completion
            </span>
          </label>
        </div>

        {/* Progress */}
        {isRunning && (
          <div style={{ marginTop: '0.5rem' }}>
            <div style={{
              display: 'flex',
              justifyContent: 'space-between',
              fontSize: '0.8rem',
              marginBottom: '0.25rem'
            }}>
              <span>{statusMessage}</span>
              <span>{progress}%</span>
            </div>
            <div style={{
              width: '100%',
              height: '8px',
              background: 'var(--bg-primary)',
              borderRadius: '4px',
              overflow: 'hidden'
            }}>
              <div style={{
                width: `${progress}%`,
                height: '100%',
                background: progress < 0 ? '#ef4444' : '#3b82f6',
                transition: 'width 0.3s ease'
              }} />
            </div>
          </div>
        )}

        {/* Status */}
        {statusMessage && !isRunning && (
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
          {!isRunning ? (
            <button
              onClick={handleStart}
              className="btn btn-success"
              style={{ flex: 1 }}
            >
              Start Comparison
            </button>
          ) : (
            <button
              onClick={handleStop}
              className="btn btn-danger"
              style={{ flex: 1 }}
            >
              Stop
            </button>
          )}
        </div>
      </div>

      {/* Right Panel - Results */}
      <div style={{ flex: 1, display: 'flex', flexDirection: 'column', gap: '1rem', overflowY: 'auto' }}>
        {!results && !isRunning && (
          <div style={{
            flex: 1,
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            background: 'var(--surface)',
            borderRadius: '8px',
            color: 'var(--text-muted)'
          }}>
            <div style={{ textAlign: 'center' }}>
              <p style={{ fontSize: '1.1rem', marginBottom: '0.5rem' }}>No results yet</p>
              <p style={{ fontSize: '0.85rem' }}>Configure and start the comparison test</p>
            </div>
          </div>
        )}

        {isRunning && !results && (
          <div style={{
            flex: 1,
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            background: 'var(--surface)',
            borderRadius: '8px',
            color: 'var(--text-muted)'
          }}>
            <div style={{ textAlign: 'center' }}>
              <div style={{
                width: '50px',
                height: '50px',
                border: '3px solid var(--border)',
                borderTopColor: '#3b82f6',
                borderRadius: '50%',
                animation: 'spin 1s linear infinite',
                margin: '0 auto 1rem'
              }} />
              <p style={{ fontSize: '1.1rem', marginBottom: '0.5rem' }}>{statusMessage}</p>
              <p style={{ fontSize: '0.85rem' }}>Progress: {progress}%</p>
            </div>
          </div>
        )}

        {results && (
          <>
            {/* Summary Cards */}
            <div style={{ display: 'flex', gap: '1rem' }}>
              {/* SIZE 254 Summary */}
              <div style={{
                flex: 1,
                background: 'var(--surface)',
                borderRadius: '8px',
                padding: '1rem',
                borderLeft: '4px solid #3b82f6'
              }}>
                <h3 style={{ margin: '0 0 1rem 0', fontSize: '1rem', color: '#3b82f6' }}>
                  SIZE 254 (Fixed Buckets)
                </h3>
                <div style={{ display: 'flex', flexDirection: 'column', gap: '0.5rem' }}>
                  <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                    <span style={{ color: 'var(--text-muted)' }}>Time:</span>
                    <span style={{ fontWeight: '600' }}>{(results.size254?.totalTimeMs / 1000).toFixed(2)}s</span>
                  </div>
                  <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                    <span style={{ color: 'var(--text-muted)' }}>Avg per table:</span>
                    <span style={{ fontWeight: '600' }}>{(results.size254?.avgTimePerTableMs / 1000).toFixed(2)}s</span>
                  </div>
                  <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                    <span style={{ color: 'var(--text-muted)' }}>Histogram rows:</span>
                    <span style={{ fontWeight: '600', color: '#f97316' }}>{results.size254?.histogramRowsAdded?.toLocaleString()}</span>
                  </div>
                  <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                    <span style={{ color: 'var(--text-muted)' }}>Cols w/ histograms:</span>
                    <span style={{ fontWeight: '600' }}>{results.size254?.histogramDetails?.columnsWithHistograms}</span>
                  </div>
                </div>
              </div>

              {/* SIZE AUTO Summary */}
              <div style={{
                flex: 1,
                background: 'var(--surface)',
                borderRadius: '8px',
                padding: '1rem',
                borderLeft: '4px solid #22c55e'
              }}>
                <h3 style={{ margin: '0 0 1rem 0', fontSize: '1rem', color: '#22c55e' }}>
                  SIZE AUTO (Oracle Recommended)
                </h3>
                <div style={{ display: 'flex', flexDirection: 'column', gap: '0.5rem' }}>
                  <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                    <span style={{ color: 'var(--text-muted)' }}>Time:</span>
                    <span style={{ fontWeight: '600' }}>{(results.sizeAuto?.totalTimeMs / 1000).toFixed(2)}s</span>
                  </div>
                  <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                    <span style={{ color: 'var(--text-muted)' }}>Avg per table:</span>
                    <span style={{ fontWeight: '600' }}>{(results.sizeAuto?.avgTimePerTableMs / 1000).toFixed(2)}s</span>
                  </div>
                  <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                    <span style={{ color: 'var(--text-muted)' }}>Histogram rows:</span>
                    <span style={{ fontWeight: '600', color: '#8b5cf6' }}>{results.sizeAuto?.histogramRowsAdded?.toLocaleString()}</span>
                  </div>
                  <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                    <span style={{ color: 'var(--text-muted)' }}>Cols w/ histograms:</span>
                    <span style={{ fontWeight: '600' }}>{results.sizeAuto?.histogramDetails?.columnsWithHistograms}</span>
                  </div>
                </div>
              </div>
            </div>

            {/* Comparison Summary */}
            <div style={{
              background: 'var(--surface)',
              borderRadius: '8px',
              padding: '1rem'
            }}>
              <h3 style={{ margin: '0 0 1rem 0', fontSize: '1rem' }}>Comparison Results</h3>
              <div style={{ display: 'flex', gap: '2rem', flexWrap: 'wrap' }}>
                <div>
                  <span style={{ color: 'var(--text-muted)', fontSize: '0.85rem' }}>Time Difference:</span>
                  <div style={{ fontSize: '1.5rem', fontWeight: '700', color: results.comparison?.timeDifferenceMs > 0 ? '#22c55e' : '#ef4444' }}>
                    {results.comparison?.timeDifferenceMs > 0 ? '+' : ''}{(results.comparison?.timeDifferenceMs / 1000).toFixed(2)}s
                  </div>
                  <div style={{ fontSize: '0.75rem', color: 'var(--text-muted)' }}>
                    SIZE 254 is {results.comparison?.timeDifferenceMs > 0 ? 'slower' : 'faster'}
                  </div>
                </div>
                <div>
                  <span style={{ color: 'var(--text-muted)', fontSize: '0.85rem' }}>Storage Difference:</span>
                  <div style={{ fontSize: '1.5rem', fontWeight: '700', color: results.comparison?.histogramRowsDifference > 0 ? '#ef4444' : '#22c55e' }}>
                    {results.comparison?.histogramRowsDifference > 0 ? '+' : ''}{results.comparison?.histogramRowsDifference?.toLocaleString()} rows
                  </div>
                  <div style={{ fontSize: '0.75rem', color: 'var(--text-muted)' }}>
                    SIZE 254 creates {results.comparison?.histogramRatio?.toFixed(1)}x more histogram data
                  </div>
                </div>
                <div>
                  <span style={{ color: 'var(--text-muted)', fontSize: '0.85rem' }}>Winner (Speed):</span>
                  <div style={{ fontSize: '1.5rem', fontWeight: '700', color: results.comparison?.summary?.winner?.speed === 'SIZE AUTO' ? '#22c55e' : '#3b82f6' }}>
                    {results.comparison?.summary?.winner?.speed}
                  </div>
                </div>
                <div>
                  <span style={{ color: 'var(--text-muted)', fontSize: '0.85rem' }}>Winner (Storage):</span>
                  <div style={{ fontSize: '1.5rem', fontWeight: '700', color: results.comparison?.summary?.winner?.storage === 'SIZE AUTO' ? '#22c55e' : '#3b82f6' }}>
                    {results.comparison?.summary?.winner?.storage}
                  </div>
                </div>
              </div>
            </div>

            {/* Charts */}
            <div style={{ display: 'flex', gap: '1rem' }}>
              {/* Time Chart */}
              <div style={{
                flex: 1,
                background: 'var(--surface)',
                borderRadius: '8px',
                padding: '1rem'
              }}>
                <h3 style={{ margin: '0 0 0.5rem 0', fontSize: '0.9rem', color: 'var(--text-muted)' }}>
                  Execution Time Comparison
                </h3>
                <div style={{ height: '200px' }}>
                  {timeChartData && <Bar data={timeChartData} options={chartOptions} />}
                </div>
              </div>

              {/* Histogram Rows Chart */}
              <div style={{
                flex: 1,
                background: 'var(--surface)',
                borderRadius: '8px',
                padding: '1rem'
              }}>
                <h3 style={{ margin: '0 0 0.5rem 0', fontSize: '0.9rem', color: 'var(--text-muted)' }}>
                  HISTGRM$ Rows Added
                </h3>
                <div style={{ height: '200px' }}>
                  {histogramChartData && <Bar data={histogramChartData} options={chartOptions} />}
                </div>
              </div>
            </div>

            {/* Recommendation */}
            <div style={{
              background: 'rgba(34, 197, 94, 0.1)',
              border: '1px solid rgba(34, 197, 94, 0.3)',
              borderRadius: '8px',
              padding: '1rem'
            }}>
              <h3 style={{ margin: '0 0 0.5rem 0', fontSize: '0.9rem', color: '#22c55e' }}>
                Recommendation
              </h3>
              <p style={{ margin: 0, fontSize: '0.9rem', color: 'var(--text-primary)' }}>
                {results.comparison?.summary?.recommendation}
              </p>
            </div>

            {/* Histogram Type Distribution */}
            <div style={{ display: 'flex', gap: '1rem' }}>
              {/* SIZE 254 Histogram Types */}
              <div style={{
                flex: 1,
                background: 'var(--surface)',
                borderRadius: '8px',
                padding: '1rem'
              }}>
                <h3 style={{ margin: '0 0 0.75rem 0', fontSize: '0.9rem', color: '#3b82f6' }}>
                  SIZE 254 - Histogram Distribution
                </h3>
                <div style={{ display: 'flex', flexDirection: 'column', gap: '0.4rem' }}>
                  {results.size254?.histogramDetails?.histogramsByType &&
                    Object.entries(results.size254.histogramDetails.histogramsByType).map(([type, count]) => (
                      <div key={type} style={{
                        display: 'flex',
                        justifyContent: 'space-between',
                        fontSize: '0.8rem',
                        padding: '0.25rem 0',
                        borderBottom: '1px solid var(--border)'
                      }}>
                        <span style={{ color: 'var(--text-muted)' }}>{type}:</span>
                        <span style={{ fontWeight: '600' }}>{count}</span>
                      </div>
                    ))
                  }
                </div>
              </div>

              {/* SIZE AUTO Histogram Types */}
              <div style={{
                flex: 1,
                background: 'var(--surface)',
                borderRadius: '8px',
                padding: '1rem'
              }}>
                <h3 style={{ margin: '0 0 0.75rem 0', fontSize: '0.9rem', color: '#22c55e' }}>
                  SIZE AUTO - Histogram Distribution
                </h3>
                <div style={{ display: 'flex', flexDirection: 'column', gap: '0.4rem' }}>
                  {results.sizeAuto?.histogramDetails?.histogramsByType &&
                    Object.entries(results.sizeAuto.histogramDetails.histogramsByType).map(([type, count]) => (
                      <div key={type} style={{
                        display: 'flex',
                        justifyContent: 'space-between',
                        fontSize: '0.8rem',
                        padding: '0.25rem 0',
                        borderBottom: '1px solid var(--border)'
                      }}>
                        <span style={{ color: 'var(--text-muted)' }}>{type}:</span>
                        <span style={{ fontWeight: '600' }}>{count}</span>
                      </div>
                    ))
                  }
                </div>
              </div>
            </div>
          </>
        )}
      </div>

      {/* CSS for spinner animation */}
      <style>{`
        @keyframes spin {
          0% { transform: rotate(0deg); }
          100% { transform: rotate(360deg); }
        }
      `}</style>
    </div>
  );
}

export default StatsComparisonPanel;
