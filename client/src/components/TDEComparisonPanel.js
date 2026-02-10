import React, { useEffect, useState } from 'react';

const getServerUrl = () => {
  if (window.location.hostname !== 'localhost') {
    return `http://${window.location.host}`;
  }
  return 'http://localhost:3001';
};

const API_BASE = `${getServerUrl()}/api`;

const defaultConfig = {
  rowCount: 200000,
  batchSize: 5000,
  runSelect: true,
  runInsert: true,
  runUpdate: true,
  gatherStats: true,
  schemaPrefix: 'TDE_DEMO'
};

function TDEComparisonPanel({ dbStatus, socket }) {
  const [config, setConfig] = useState(defaultConfig);
  const [isRunning, setIsRunning] = useState(false);
  const [statusMessage, setStatusMessage] = useState('');
  const [progress, setProgress] = useState(0);
  const [results, setResults] = useState(null);

  useEffect(() => {
    fetchStatus();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [dbStatus.connected]);

  useEffect(() => {
    if (!socket) return;

    const handleStatus = (payload) => {
      setStatusMessage(payload.message);
      setProgress(payload.progress || 0);
      if (payload.results) {
        setResults(payload.results);
      }
    };

    socket.on('tde-comparison-status', handleStatus);

    return () => {
      socket.off('tde-comparison-status', handleStatus);
    };
  }, [socket]);

  const fetchStatus = async () => {
    if (!dbStatus.connected) return;

    try {
      const response = await fetch(`${API_BASE}/tde-comparison/status`);
      const data = await response.json();
      setIsRunning(data.isRunning);
      setResults(data.results || null);
      setStatusMessage(data.currentPhase || '');
      setProgress(data.progress || 0);
      if (data.config) {
        setConfig(prev => ({ ...prev, ...data.config }));
      }
    } catch (err) {
      console.error('Error fetching TDE comparison status:', err);
    }
  };

  const handleInputChange = (field, value) => {
    setConfig(prev => ({
      ...prev,
      [field]: value
    }));
  };

  const handleNumericChange = (field, value) => {
    const numeric = parseInt(value, 10);
    if (Number.isNaN(numeric)) {
      setConfig(prev => ({ ...prev, [field]: '' }));
    } else {
      setConfig(prev => ({ ...prev, [field]: Math.max(1, numeric) }));
    }
  };

  const handleStart = async () => {
    try {
      setIsRunning(true);
      setStatusMessage('Starting TDE comparison test...');
      setProgress(0);
      setResults(null);

      const response = await fetch(`${API_BASE}/tde-comparison/start`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(config)
      });

      const data = await response.json();

      if (!response.ok) {
        throw new Error(data.error || 'Failed to start TDE comparison');
      }

      setResults(data.results || null);
      setStatusMessage('TDE comparison test complete.');
      setProgress(100);
    } catch (err) {
      setStatusMessage(`Error: ${err.message}`);
    } finally {
      setIsRunning(false);
    }
  };

  const handleStop = async () => {
    try {
      const response = await fetch(`${API_BASE}/tde-comparison/stop`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' }
      });

      const data = await response.json();

      if (!response.ok) {
        throw new Error(data.error || 'Failed to stop TDE comparison');
      }

      setStatusMessage('TDE comparison stopped.');
      setIsRunning(false);
    } catch (err) {
      setStatusMessage(`Error: ${err.message}`);
    }
  };

  const renderMetricCard = (title, metricKey) => {
    if (!results || !results.comparison || !results.comparison[metricKey]) {
      return null;
    }

    const metric = results.comparison[metricKey];
    const formatValue = (value) => {
      if (value === null || value === undefined) return '-';
      if (Math.abs(value) >= 1000) {
        return value.toLocaleString(undefined, { maximumFractionDigits: 0 });
      }
      return value.toLocaleString(undefined, { maximumFractionDigits: 2 });
    };

    return (
      <div style={{
        flex: 1,
        minWidth: '200px',
        background: 'var(--surface)',
        borderRadius: '8px',
        padding: '1rem',
        display: 'flex',
        flexDirection: 'column',
        gap: '0.5rem'
      }}>
        <div style={{ fontSize: '0.9rem', color: 'var(--text-muted)' }}>{title}</div>
        <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: '0.85rem' }}>
          <span>Encrypted</span>
          <span style={{ fontWeight: '600' }}>{formatValue(metric.encrypted)}</span>
        </div>
        <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: '0.85rem' }}>
          <span>Non-encrypted</span>
          <span style={{ fontWeight: '600' }}>{formatValue(metric.unencrypted)}</span>
        </div>
        <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: '0.85rem', color: metric.delta > 0 ? '#ef4444' : '#22c55e' }}>
          <span>Delta</span>
          <span style={{ fontWeight: '600' }}>{formatValue(metric.delta)}</span>
        </div>
        <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: '0.85rem', color: metric.deltaPercent > 0 ? '#ef4444' : '#22c55e' }}>
          <span>Delta %</span>
          <span style={{ fontWeight: '600' }}>{metric.deltaPercent !== null ? `${metric.deltaPercent.toFixed(2)}%` : '-'}</span>
        </div>
      </div>
    );
  };

  const renderOperationTable = (title, data) => {
    if (!data || !data.operations) {
      return null;
    }

    return (
      <div style={{
        background: 'var(--surface)',
        borderRadius: '8px',
        padding: '1rem',
        flex: 1,
        minWidth: '300px'
      }}>
        <h3 style={{ margin: '0 0 0.75rem 0', fontSize: '0.95rem', color: '#ef4444' }}>{title}</h3>
        <div style={{ overflowX: 'auto' }}>
          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.8rem' }}>
            <thead>
              <tr style={{ borderBottom: '1px solid var(--border)' }}>
                <th style={{ padding: '0.5rem', textAlign: 'left', color: 'var(--text-muted)' }}>Operation</th>
                <th style={{ padding: '0.5rem', textAlign: 'left', color: 'var(--text-muted)' }}>Type</th>
                <th style={{ padding: '0.5rem', textAlign: 'right', color: 'var(--text-muted)' }}>Elapsed (s)</th>
              </tr>
            </thead>
            <tbody>
              {data.operations.map(op => (
                <tr key={op.label} style={{ borderBottom: '1px solid var(--border)' }}>
                  <td style={{ padding: '0.5rem', fontWeight: '500' }}>{op.label}</td>
                  <td style={{ padding: '0.5rem', color: 'var(--text-muted)' }}>{op.type}</td>
                  <td style={{ padding: '0.5rem', textAlign: 'right' }}>{op.elapsedSeconds.toFixed(3)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    );
  };

  const renderSessionStats = (title, data) => {
    if (!data || !data.sessionStats || data.sessionStats.length === 0) {
      return null;
    }

    return (
      <div style={{
        background: 'var(--surface)',
        borderRadius: '8px',
        padding: '1rem',
        flex: 1,
        minWidth: '320px'
      }}>
        <h3 style={{ margin: '0 0 0.75rem 0', fontSize: '0.95rem', color: '#ef4444' }}>{title}</h3>
        <div style={{ overflowX: 'auto' }}>
          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.75rem' }}>
            <thead>
              <tr style={{ borderBottom: '1px solid var(--border)' }}>
                <th style={{ padding: '0.5rem', textAlign: 'left', color: 'var(--text-muted)' }}>SQL ID</th>
                <th style={{ padding: '0.5rem', textAlign: 'right', color: 'var(--text-muted)' }}>Buffer Gets</th>
                <th style={{ padding: '0.5rem', textAlign: 'right', color: 'var(--text-muted)' }}>CPU Time</th>
                <th style={{ padding: '0.5rem', textAlign: 'right', color: 'var(--text-muted)' }}>Elapsed</th>
                <th style={{ padding: '0.5rem', textAlign: 'right', color: 'var(--text-muted)' }}>Executions</th>
              </tr>
            </thead>
            <tbody>
              {data.sessionStats.map((row, idx) => (
                <tr key={`${row.SQL_ID}-${idx}`} style={{ borderBottom: '1px solid var(--border)' }}>
                  <td style={{ padding: '0.5rem', fontWeight: '500' }}>{row.SQL_ID}</td>
                  <td style={{ padding: '0.5rem', textAlign: 'right' }}>{(row.BUFFER_GETS || 0).toLocaleString()}</td>
                  <td style={{ padding: '0.5rem', textAlign: 'right' }}>{(row.CPU_TIME || 0).toLocaleString()}</td>
                  <td style={{ padding: '0.5rem', textAlign: 'right' }}>{(row.ELAPSED_TIME || 0).toLocaleString()}</td>
                  <td style={{ padding: '0.5rem', textAlign: 'right' }}>{(row.EXECUTIONS || 0).toLocaleString()}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    );
  };

  if (!dbStatus.connected) {
    return (
      <div className="panel" style={{ height: '100%' }}>
        <div className="panel-header">
          <h2>TDE Comparison</h2>
        </div>
        <div className="panel-content">
          <p style={{ color: 'var(--text-muted)' }}>Connect to the database to start the TDE comparison demo.</p>
        </div>
      </div>
    );
  }

  return (
    <div style={{ display: 'flex', gap: '1rem', height: '100%', padding: '1rem' }}>
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
          TDE Comparison
        </h2>

        <div style={{
          padding: '0.75rem',
          background: 'rgba(239, 68, 68, 0.1)',
          border: '1px solid rgba(239, 68, 68, 0.3)',
          borderRadius: '4px',
          fontSize: '0.75rem',
          color: 'var(--text-muted)'
        }}>
          <strong style={{ color: '#ef4444' }}>Transparent Data Encryption</strong>
          <div style={{ marginTop: '0.25rem' }}>
            Measures the impact of AES-256 TDE on DML and query performance by comparing encrypted vs. plain tables.
          </div>
          <ul style={{ margin: '0.5rem 0 0 0', paddingLeft: '1.2rem' }}>
            <li>Creates AES-256 encrypted and plain tables</li>
            <li>Runs configurable insert, update, and select workloads</li>
            <li>Compares CPU time, buffer gets, and elapsed time</li>
          </ul>
        </div>

        <div>
          <label style={{ display: 'block', fontSize: '0.8rem', color: 'var(--text-muted)' }}>Rows per table</label>
          <input
            type="number"
            min="1000"
            value={config.rowCount}
            onChange={(e) => handleNumericChange('rowCount', e.target.value)}
            style={{
              width: '100%',
              padding: '0.4rem',
              background: 'var(--bg-primary)',
              border: '1px solid var(--border)',
              borderRadius: '4px',
              color: 'var(--text-primary)',
              fontSize: '0.8rem'
            }}
          />
        </div>

        <div>
          <label style={{ display: 'block', fontSize: '0.8rem', color: 'var(--text-muted)' }}>Batch size</label>
          <input
            type="number"
            min="100"
            value={config.batchSize}
            onChange={(e) => handleNumericChange('batchSize', e.target.value)}
            style={{
              width: '100%',
              padding: '0.4rem',
              background: 'var(--bg-primary)',
              border: '1px solid var(--border)',
              borderRadius: '4px',
              color: 'var(--text-primary)',
              fontSize: '0.8rem'
            }}
          />
        </div>

        <div>
          <label style={{ display: 'block', fontSize: '0.8rem', color: 'var(--text-muted)' }}>Schema prefix</label>
          <input
            type="text"
            value={config.schemaPrefix}
            onChange={(e) => handleInputChange('schemaPrefix', e.target.value.toUpperCase().replace(/[^A-Z0-9_]/g, ''))}
            style={{
              width: '100%',
              padding: '0.4rem',
              background: 'var(--bg-primary)',
              border: '1px solid var(--border)',
              borderRadius: '4px',
              color: 'var(--text-primary)',
              fontSize: '0.8rem'
            }}
          />
        </div>

        <div style={{ display: 'flex', flexDirection: 'column', gap: '0.5rem' }}>
          <label style={{ fontSize: '0.8rem', color: 'var(--text-muted)' }}>Workload phases</label>
          {[{
            label: 'Run SELECT workload',
            field: 'runSelect'
          }, {
            label: 'Run INSERT workload',
            field: 'runInsert'
          }, {
            label: 'Run UPDATE workload',
            field: 'runUpdate'
          }, {
            label: 'Gather statistics after load',
            field: 'gatherStats'
          }].map(toggle => (
            <label key={toggle.field} style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', fontSize: '0.8rem' }}>
              <input
                type="checkbox"
                checked={config[toggle.field]}
                onChange={(e) => handleInputChange(toggle.field, e.target.checked)}
              />
              {toggle.label}
            </label>
          ))}
        </div>

        <div style={{ display: 'flex', gap: '0.5rem' }}>
          <button
            onClick={handleStart}
            disabled={isRunning}
            className="btn btn-primary"
            style={{ flex: 1 }}
          >
            {isRunning ? 'Running...' : 'Start Comparison'}
          </button>
          <button
            onClick={handleStop}
            disabled={!isRunning}
            className="btn btn-danger"
            style={{ flex: 1 }}
          >
            Stop
          </button>
        </div>

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

        {progress > 0 && progress <= 100 && (
          <div>
            <div style={{ height: '8px', background: 'var(--bg-primary)', borderRadius: '4px', overflow: 'hidden' }}>
              <div style={{ height: '100%', width: `${progress}%`, background: '#ef4444', transition: 'width 0.3s ease' }} />
            </div>
            <div style={{ fontSize: '0.75rem', color: 'var(--text-muted)', marginTop: '0.25rem', textAlign: 'center' }}>
              {progress}%
            </div>
          </div>
        )}
      </div>

      <div style={{ flex: 1, display: 'flex', flexDirection: 'column', gap: '1rem', overflowY: 'auto' }}>
        {results && results.comparison && (
          <div style={{ display: 'flex', flexWrap: 'wrap', gap: '1rem' }}>
            {renderMetricCard('Buffer Gets', 'buffer_gets')}
            {renderMetricCard('CPU Time', 'cpu_time')}
            {renderMetricCard('Elapsed Time', 'elapsed_time')}
            {renderMetricCard('Disk Reads', 'disk_reads')}
          </div>
        )}

        <div style={{ display: 'flex', gap: '1rem', flexWrap: 'wrap' }}>
          {renderOperationTable('Encrypted Operations', results?.encrypted)}
          {renderSessionStats('Encrypted SQL Monitor', results?.encrypted)}
        </div>

        <div style={{ display: 'flex', gap: '1rem', flexWrap: 'wrap' }}>
          {renderOperationTable('Non-encrypted Operations', results?.unencrypted)}
          {renderSessionStats('Non-encrypted SQL Monitor', results?.unencrypted)}
        </div>

        {!results && (
          <div style={{
            background: 'var(--surface)',
            borderRadius: '8px',
            padding: '2rem',
            textAlign: 'center',
            color: 'var(--text-muted)'
          }}>
            <h3 style={{ margin: '0 0 1rem 0', fontSize: '1.1rem', color: '#ef4444' }}>
              TDE Comparison Demo
            </h3>
            <p>Start the comparison to gather metrics for encrypted vs. plain tables using Transparent Data Encryption.</p>
            <ul style={{ textAlign: 'left', maxWidth: '500px', margin: '1rem auto', paddingLeft: '2rem' }}>
              <li>Creates tables using AES-256 TDE encryption</li>
              <li>Runs configurable workloads (select, insert, update)</li>
              <li>Summarizes performance metrics (buffer gets, CPU, elapsed time)</li>
            </ul>
          </div>
        )}
      </div>
    </div>
  );
}

export default TDEComparisonPanel;
