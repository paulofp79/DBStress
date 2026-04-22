import React, { useEffect, useMemo, useState } from 'react';
import axios from 'axios';

const getServerUrl = () => {
  if (window.location.hostname !== 'localhost') {
    return `http://${window.location.host}`;
  }
  return 'http://localhost:3001';
};

const API_BASE = `${getServerUrl()}/api`;

function InsertBlastPanel({ dbStatus, socket, onSuccess, onError }) {
  const [config, setConfig] = useState({
    tablePrefix: 'IBLAST',
    tableCount: 8,
    columnsPerTable: 24,
    sessions: 8,
    durationSeconds: 60,
    commitEvery: 50
  });
  const [schemaStatus, setSchemaStatus] = useState(null);
  const [workloadStatus, setWorkloadStatus] = useState({ isRunning: false });
  const [metrics, setMetrics] = useState(null);
  const [progress, setProgress] = useState(null);
  const [busy, setBusy] = useState(false);

  const loadStatus = async (nextConfig = config) => {
    if (!dbStatus.connected) {
      return;
    }

    try {
      const response = await axios.get(`${API_BASE}/insert-blast/status`, {
        params: {
          tablePrefix: nextConfig.tablePrefix,
          tableCount: nextConfig.tableCount,
          columnsPerTable: nextConfig.columnsPerTable
        }
      });
      if (response.data.success) {
        setSchemaStatus(response.data.schema);
        setWorkloadStatus(response.data.workload || { isRunning: false });
      }
    } catch (err) {
      onError?.(err.response?.data?.message || 'Failed to load insert blast status');
    }
  };

  useEffect(() => {
    if (dbStatus.connected) {
      loadStatus();
    } else {
      setSchemaStatus(null);
      setWorkloadStatus({ isRunning: false });
      setMetrics(null);
      setProgress(null);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [dbStatus.connected]);

  useEffect(() => {
    if (!socket) {
      return undefined;
    }

    const handleProgress = (data) => {
      setProgress(data);
      if (data.progress === 100 || data.progress === -1) {
        setBusy(false);
        setTimeout(() => loadStatus(), 500);
      }
    };
    const handleStatus = (data) => setWorkloadStatus(data);
    const handleMetrics = (data) => setMetrics(data);

    socket.on('insert-blast-progress', handleProgress);
    socket.on('insert-blast-status', handleStatus);
    socket.on('insert-blast-metrics', handleMetrics);

    return () => {
      socket.off('insert-blast-progress', handleProgress);
      socket.off('insert-blast-status', handleStatus);
      socket.off('insert-blast-metrics', handleMetrics);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [socket, config.tablePrefix, config.tableCount, config.columnsPerTable]);

  const handleChange = (field, value) => {
    setConfig((prev) => ({
      ...prev,
      [field]: value
    }));
  };

  const handleCreate = async () => {
    setBusy(true);
    setProgress({ step: 'Creating insert-blast tables...', progress: 0 });
    try {
      const response = await axios.post(`${API_BASE}/insert-blast/create`, config, {
        timeout: 600000
      });
      if (response.data.success) {
        onSuccess?.(response.data.message);
        await loadStatus(config);
      }
    } catch (err) {
      setBusy(false);
      onError?.(err.response?.data?.message || 'Failed to create insert-blast tables');
    }
  };

  const handleDrop = async () => {
    if (!window.confirm(`Drop insert-blast tables for prefix '${config.tablePrefix}'?`)) {
      return;
    }

    setBusy(true);
    setProgress({ step: 'Dropping insert-blast tables...', progress: 0 });
    try {
      const response = await axios.post(`${API_BASE}/insert-blast/drop`, config, {
        timeout: 600000
      });
      if (response.data.success) {
        onSuccess?.(response.data.message);
        await loadStatus(config);
      }
    } catch (err) {
      setBusy(false);
      onError?.(err.response?.data?.message || 'Failed to drop insert-blast tables');
    }
  };

  const handleStart = async () => {
    setBusy(true);
    try {
      const response = await axios.post(`${API_BASE}/insert-blast/start`, config, {
        timeout: 120000
      });
      if (response.data.success) {
        setWorkloadStatus(response.data);
        onSuccess?.('Insert-only workload started');
      }
    } catch (err) {
      onError?.(err.response?.data?.message || 'Failed to start insert-only workload');
    } finally {
      setBusy(false);
    }
  };

  const handleStop = async () => {
    setBusy(true);
    try {
      const response = await axios.post(`${API_BASE}/insert-blast/stop`, {}, {
        timeout: 60000
      });
      if (response.data.success) {
        setWorkloadStatus(response.data);
        onSuccess?.('Insert-only workload stopped');
      }
    } catch (err) {
      onError?.(err.response?.data?.message || 'Failed to stop insert-only workload');
    } finally {
      setBusy(false);
    }
  };

  const existingCount = schemaStatus?.existingTables?.length || 0;
  const tableSummary = useMemo(() => {
    const byTable = metrics?.byTable || {};
    return Object.entries(byTable)
      .sort((a, b) => b[1] - a[1])
      .slice(0, 8);
  }, [metrics]);

  if (!dbStatus.connected) {
    return (
      <div className="panel">
        <div className="panel-header">
          <h2>Insert Blast</h2>
        </div>
        <div className="panel-content">
          <p style={{ color: 'var(--text-muted)' }}>Connect to Oracle first.</p>
        </div>
      </div>
    );
  }

  return (
    <div className="panel">
      <div className="panel-header">
        <h2>Insert Blast</h2>
        <button className="btn btn-secondary btn-sm" type="button" onClick={() => loadStatus()} disabled={busy}>
          Refresh
        </button>
      </div>
      <div className="panel-content">
        <p style={{ marginTop: 0, color: 'var(--text-muted)' }}>
          Create many wide tables in the current schema, then run an insert-only workload for a chosen duration and session count.
        </p>

        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, minmax(0, 1fr))', gap: '0.85rem' }}>
          <div className="form-group">
            <label htmlFor="ib-prefix">Table Prefix</label>
            <input
              id="ib-prefix"
              value={config.tablePrefix}
              onChange={(e) => handleChange('tablePrefix', e.target.value.toUpperCase().replace(/[^A-Z0-9_$#]/g, ''))}
              disabled={busy || workloadStatus.isRunning}
            />
          </div>
          <div className="form-group">
            <label htmlFor="ib-table-count">Tables</label>
            <input
              id="ib-table-count"
              type="number"
              min="1"
              max="200"
              value={config.tableCount}
              onChange={(e) => handleChange('tableCount', e.target.value)}
              disabled={busy || workloadStatus.isRunning}
            />
          </div>
          <div className="form-group">
            <label htmlFor="ib-columns-per-table">Columns Per Table</label>
            <input
              id="ib-columns-per-table"
              type="number"
              min="4"
              max="200"
              value={config.columnsPerTable}
              onChange={(e) => handleChange('columnsPerTable', e.target.value)}
              disabled={busy || workloadStatus.isRunning}
            />
          </div>
        </div>

        <div style={{
          marginTop: '0.75rem',
          background: 'var(--surface)',
          border: '1px solid var(--border)',
          borderRadius: '8px',
          padding: '0.9rem'
        }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', gap: '1rem', flexWrap: 'wrap' }}>
            <div>
              <div style={{ fontSize: '0.8rem', color: 'var(--text-muted)' }}>Existing Tables</div>
              <div style={{ fontWeight: 600 }}>{existingCount} / {config.tableCount}</div>
            </div>
            <div>
              <div style={{ fontSize: '0.8rem', color: 'var(--text-muted)' }}>Schema Ready</div>
              <div style={{ fontWeight: 600, color: schemaStatus?.ready ? 'var(--accent-success)' : 'var(--text-primary)' }}>
                {schemaStatus?.ready ? 'Yes' : 'Not yet'}
              </div>
            </div>
            <div>
              <div style={{ fontSize: '0.8rem', color: 'var(--text-muted)' }}>Workload</div>
              <div style={{ fontWeight: 600, color: workloadStatus?.isRunning ? 'var(--accent-warning)' : 'var(--text-primary)' }}>
                {workloadStatus?.isRunning ? `Running (${workloadStatus.uptime || 0}s)` : 'Stopped'}
              </div>
            </div>
          </div>
        </div>

        <div className="btn-group" style={{ marginTop: '1rem' }}>
          <button className="btn btn-primary" type="button" onClick={handleCreate} disabled={busy || workloadStatus.isRunning}>
            Create Tables
          </button>
          <button className="btn btn-danger" type="button" onClick={handleDrop} disabled={busy || workloadStatus.isRunning}>
            Drop Tables
          </button>
        </div>

        {progress && (
          <div style={{ marginTop: '0.9rem' }}>
            <div style={{ fontSize: '0.85rem', marginBottom: '0.4rem' }}>{progress.step}</div>
            <div className="progress-bar">
              <div className="fill" style={{ width: `${Math.max(0, progress.progress || 0)}%` }}></div>
            </div>
          </div>
        )}

        <div style={{ marginTop: '1.25rem', display: 'grid', gridTemplateColumns: 'repeat(3, minmax(0, 1fr))', gap: '0.85rem' }}>
          <div className="form-group">
            <label htmlFor="ib-sessions">Sessions</label>
            <input
              id="ib-sessions"
              type="number"
              min="1"
              max="256"
              value={config.sessions}
              onChange={(e) => handleChange('sessions', e.target.value)}
              disabled={busy || workloadStatus.isRunning}
            />
          </div>
          <div className="form-group">
            <label htmlFor="ib-duration">Run Time (seconds)</label>
            <input
              id="ib-duration"
              type="number"
              min="1"
              max="86400"
              value={config.durationSeconds}
              onChange={(e) => handleChange('durationSeconds', e.target.value)}
              disabled={busy || workloadStatus.isRunning}
            />
          </div>
          <div className="form-group">
            <label htmlFor="ib-commit-every">Commit Every N Inserts</label>
            <input
              id="ib-commit-every"
              type="number"
              min="1"
              max="1000"
              value={config.commitEvery}
              onChange={(e) => handleChange('commitEvery', e.target.value)}
              disabled={busy || workloadStatus.isRunning}
            />
          </div>
        </div>

        <div className="btn-group" style={{ marginTop: '1rem' }}>
          <button className="btn btn-primary" type="button" onClick={handleStart} disabled={busy || workloadStatus.isRunning || !schemaStatus?.ready}>
            Start Insert Workload
          </button>
          <button className="btn btn-danger" type="button" onClick={handleStop} disabled={busy || !workloadStatus.isRunning}>
            Stop Workload
          </button>
        </div>

        {metrics && (
          <div style={{ marginTop: '1rem', display: 'grid', gap: '1rem' }}>
            <div className="schema-stats">
              <div className="schema-stat">
                <div className="name">Inserts/Sec</div>
                <div className="count">{metrics.perSecond?.inserts || 0}</div>
              </div>
              <div className="schema-stat">
                <div className="name">Total Inserts</div>
                <div className="count">{metrics.total?.inserts || 0}</div>
              </div>
              <div className="schema-stat">
                <div className="name">Commits</div>
                <div className="count">{metrics.total?.commits || 0}</div>
              </div>
              <div className="schema-stat">
                <div className="name">Errors</div>
                <div className="count">{metrics.total?.errors || 0}</div>
              </div>
            </div>

            <div style={{
              background: 'var(--surface)',
              border: '1px solid var(--border)',
              borderRadius: '8px',
              padding: '0.9rem'
            }}>
              <h3 style={{ marginTop: 0 }}>Top Target Tables</h3>
              {tableSummary.length > 0 ? (
                <div style={{ display: 'grid', gap: '0.35rem' }}>
                  {tableSummary.map(([tableName, count]) => (
                    <div key={tableName} style={{ display: 'flex', justifyContent: 'space-between' }}>
                      <span>{tableName}</span>
                      <strong>{count}</strong>
                    </div>
                  ))}
                </div>
              ) : (
                <p style={{ margin: 0, color: 'var(--text-muted)' }}>No inserts recorded yet.</p>
              )}
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

export default InsertBlastPanel;
