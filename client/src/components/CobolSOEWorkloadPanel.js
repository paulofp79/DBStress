import React, { useEffect, useMemo, useState } from 'react';
import axios from 'axios';

const getServerUrl = () => {
  if (window.location.hostname !== 'localhost') {
    return `http://${window.location.host}`;
  }
  return 'http://localhost:3001';
};

const API_BASE = `${getServerUrl()}/api`;

function CobolSOEWorkloadPanel({ dbStatus, socket, onSuccess, onError }) {
  const [loading, setLoading] = useState(false);
  const [status, setStatus] = useState({ isRunning: false });
  const [metrics, setMetrics] = useState(null);
  const [config, setConfig] = useState({
    username: 'soe',
    password: 'soe',
    users: 8,
    minDelay: 5,
    maxDelay: 25,
    interMinDelay: 0,
    interMaxDelay: 10,
    queryTimeout: 120,
    durationSeconds: 0,
    maxTransactions: -1,
    cobolProgram: 'ESORAXA.CBL',
    xaOpenString: 'Oracle_XA+Acc=P/soe/soe+SesTm=60+Threads=true',
    xaMode: 'LOCALTX',
    transactions: []
  });

  const loadDefaults = async () => {
    try {
      const response = await axios.get(`${API_BASE}/cobol/soe/workload/defaults`);
      if (response.data.success) {
        const { success, ...payload } = response.data;
        setConfig((prev) => ({
          ...prev,
          ...payload
        }));
      }
    } catch (err) {
      onError?.(err.response?.data?.message || 'Failed to load COBOL workload defaults');
    }
  };

  const loadStatus = async () => {
    try {
      const response = await axios.get(`${API_BASE}/cobol/soe/workload/status`);
      if (response.data.success) {
        const { success, ...payload } = response.data;
        setStatus(payload);
      }
    } catch (err) {
      onError?.(err.response?.data?.message || 'Failed to load COBOL workload status');
    }
  };

  useEffect(() => {
    if (dbStatus.connected) {
      loadDefaults();
      loadStatus();
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [dbStatus.connected]);

  useEffect(() => {
    if (!socket) {
      return undefined;
    }

    const handleStatus = (data) => setStatus(data);
    const handleMetrics = (data) => setMetrics(data);

    socket.on('cobol-soe-status', handleStatus);
    socket.on('cobol-soe-metrics', handleMetrics);

    return () => {
      socket.off('cobol-soe-status', handleStatus);
      socket.off('cobol-soe-metrics', handleMetrics);
    };
  }, [socket]);

  const handleConfigChange = (field, value) => {
    setConfig((prev) => ({
      ...prev,
      [field]: value
    }));
  };

  const updateTransaction = (shortName, field, value) => {
    setConfig((prev) => ({
      ...prev,
      transactions: prev.transactions.map((txn) => (
        txn.shortName === shortName ? { ...txn, [field]: value } : txn
      ))
    }));
  };

  const handleStart = async () => {
    setLoading(true);
    try {
      const payload = {
        ...config,
        users: Number(config.users),
        minDelay: Number(config.minDelay),
        maxDelay: Number(config.maxDelay),
        interMinDelay: Number(config.interMinDelay),
        interMaxDelay: Number(config.interMaxDelay),
        queryTimeout: Number(config.queryTimeout),
        durationSeconds: Number(config.durationSeconds),
        maxTransactions: Number(config.maxTransactions),
        transactions: config.transactions.map((txn) => ({
          ...txn,
          weight: Number(txn.weight),
          enabled: !!txn.enabled
        }))
      };
      const response = await axios.post(`${API_BASE}/cobol/soe/workload/start`, payload, {
        timeout: 120000
      });
      if (response.data.success) {
        const { success, ...statusPayload } = response.data;
        setStatus(statusPayload);
        onSuccess?.('COBOL XA SOE workload started');
      }
    } catch (err) {
      onError?.(err.response?.data?.message || 'Failed to start COBOL XA SOE workload');
    } finally {
      setLoading(false);
    }
  };

  const handleStop = async () => {
    setLoading(true);
    try {
      const response = await axios.post(`${API_BASE}/cobol/soe/workload/stop`, {}, {
        timeout: 60000
      });
      if (response.data.success) {
        const { success, ...statusPayload } = response.data;
        setStatus(statusPayload);
        onSuccess?.('COBOL XA SOE workload stopped');
      }
    } catch (err) {
      onError?.(err.response?.data?.message || 'Failed to stop COBOL XA SOE workload');
    } finally {
      setLoading(false);
    }
  };

  const enabledCount = useMemo(
    () => config.transactions.filter((txn) => txn.enabled && Number(txn.weight) > 0).length,
    [config.transactions]
  );

  if (!dbStatus.connected) {
    return (
      <div className="panel">
        <div className="panel-header">
          <h2>COBOL XA SOE Workload</h2>
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
        <h2>COBOL XA SOE Workload</h2>
        <span style={{ color: status.isRunning ? 'var(--accent-warning)' : 'var(--text-muted)', fontSize: '0.875rem' }}>
          {status.isRunning ? `Running: ${status.uptime || 0}s` : 'Stopped'}
        </span>
      </div>
      <div className="panel-content">
        <p style={{ marginTop: 0, color: 'var(--text-muted)' }}>
          Simulates a Micro Focus COBOL customer environment using the ESORAXA Oracle XA sample as the app-side model.
          Sessions are tagged as COBOL/ESORAXA clients and execute SOE customer, order, inventory, card, and address transactions.
        </p>

        <div className="form-row">
          <div className="form-group">
            <label htmlFor="cobol-run-user">SOE User</label>
            <input
              id="cobol-run-user"
              value={config.username}
              onChange={(e) => handleConfigChange('username', e.target.value)}
              disabled={status.isRunning || loading}
            />
          </div>
          <div className="form-group">
            <label htmlFor="cobol-run-password">SOE Password</label>
            <input
              id="cobol-run-password"
              type="password"
              value={config.password}
              onChange={(e) => handleConfigChange('password', e.target.value)}
              disabled={status.isRunning || loading}
            />
          </div>
          <div className="form-group">
            <label htmlFor="cobol-run-users">COBOL Regions</label>
            <input
              id="cobol-run-users"
              type="number"
              min="1"
              max="200"
              value={config.users}
              onChange={(e) => handleConfigChange('users', e.target.value)}
              disabled={status.isRunning || loading}
            />
          </div>
        </div>

        <div className="form-row">
          <div className="form-group">
            <label htmlFor="cobol-program">COBOL Program</label>
            <input
              id="cobol-program"
              value={config.cobolProgram}
              onChange={(e) => handleConfigChange('cobolProgram', e.target.value)}
              disabled={status.isRunning || loading}
            />
          </div>
          <div className="form-group">
            <label htmlFor="cobol-xa-mode">XA Mode</label>
            <select
              id="cobol-xa-mode"
              value={config.xaMode}
              onChange={(e) => handleConfigChange('xaMode', e.target.value)}
              disabled={status.isRunning || loading}
            >
              <option value="LOCALTX">LOCALTX</option>
              <option value="FULL-XA">FULL-XA</option>
              <option value="READONLY">READONLY</option>
              <option value="SERIALIZABLE">SERIALIZABLE</option>
            </select>
          </div>
          <div className="form-group">
            <label htmlFor="cobol-query-timeout">Query Timeout (s)</label>
            <input
              id="cobol-query-timeout"
              type="number"
              min="1"
              value={config.queryTimeout}
              onChange={(e) => handleConfigChange('queryTimeout', e.target.value)}
              disabled={status.isRunning || loading}
            />
          </div>
        </div>

        <div className="form-group">
          <label htmlFor="cobol-xa-open-string">Oracle XA Open String</label>
          <input
            id="cobol-xa-open-string"
            value={config.xaOpenString}
            onChange={(e) => handleConfigChange('xaOpenString', e.target.value)}
            disabled={status.isRunning || loading}
          />
        </div>

        <div className="form-row">
          <div className="form-group">
            <label htmlFor="cobol-min-delay">Min Think Time (ms)</label>
            <input
              id="cobol-min-delay"
              type="number"
              min="0"
              value={config.minDelay}
              onChange={(e) => handleConfigChange('minDelay', e.target.value)}
              disabled={status.isRunning || loading}
            />
          </div>
          <div className="form-group">
            <label htmlFor="cobol-max-delay">Max Think Time (ms)</label>
            <input
              id="cobol-max-delay"
              type="number"
              min="0"
              value={config.maxDelay}
              onChange={(e) => handleConfigChange('maxDelay', e.target.value)}
              disabled={status.isRunning || loading}
            />
          </div>
          <div className="form-group">
            <label htmlFor="cobol-inter-min-delay">Inter Min (ms)</label>
            <input
              id="cobol-inter-min-delay"
              type="number"
              min="0"
              value={config.interMinDelay}
              onChange={(e) => handleConfigChange('interMinDelay', e.target.value)}
              disabled={status.isRunning || loading}
            />
          </div>
          <div className="form-group">
            <label htmlFor="cobol-inter-max-delay">Inter Max (ms)</label>
            <input
              id="cobol-inter-max-delay"
              type="number"
              min="0"
              value={config.interMaxDelay}
              onChange={(e) => handleConfigChange('interMaxDelay', e.target.value)}
              disabled={status.isRunning || loading}
            />
          </div>
        </div>

        <div className="form-row">
          <div className="form-group">
            <label htmlFor="cobol-duration">Duration (s, 0 = manual stop)</label>
            <input
              id="cobol-duration"
              type="number"
              min="0"
              value={config.durationSeconds}
              onChange={(e) => handleConfigChange('durationSeconds', e.target.value)}
              disabled={status.isRunning || loading}
            />
          </div>
          <div className="form-group">
            <label htmlFor="cobol-max-txns">Max Transactions (-1 = unlimited)</label>
            <input
              id="cobol-max-txns"
              type="number"
              value={config.maxTransactions}
              onChange={(e) => handleConfigChange('maxTransactions', e.target.value)}
              disabled={status.isRunning || loading}
            />
          </div>
        </div>

        <div style={{
          marginTop: '1rem',
          background: 'var(--surface)',
          border: '1px solid var(--border)',
          borderRadius: '8px',
          padding: '0.9rem'
        }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '0.75rem' }}>
            <h3 style={{ margin: 0 }}>COBOL Transaction Mix</h3>
            <span style={{ fontSize: '0.8rem', color: 'var(--text-muted)' }}>{enabledCount} enabled</span>
          </div>
          <div style={{ display: 'grid', gap: '0.65rem' }}>
            {config.transactions.map((txn) => (
              <div
                key={txn.shortName}
                style={{
                  display: 'grid',
                  gridTemplateColumns: 'minmax(220px, 2fr) 96px minmax(180px, 1fr)',
                  gap: '0.75rem',
                  alignItems: 'center'
                }}
              >
                <label style={{ display: 'flex', gap: '0.5rem', alignItems: 'center' }}>
                  <input
                    type="checkbox"
                    checked={!!txn.enabled}
                    onChange={(e) => updateTransaction(txn.shortName, 'enabled', e.target.checked)}
                    disabled={status.isRunning || loading}
                  />
                  <span>{txn.id} ({txn.shortName})</span>
                </label>
                <input
                  type="number"
                  min="0"
                  value={txn.weight}
                  onChange={(e) => updateTransaction(txn.shortName, 'weight', e.target.value)}
                  disabled={status.isRunning || loading}
                  style={{ width: '96px', minWidth: '96px', justifySelf: 'start' }}
                />
                <span style={{ fontSize: '0.8rem', color: 'var(--text-muted)' }}>{txn.mapsTo}</span>
              </div>
            ))}
          </div>
        </div>

        <div className="btn-group" style={{ marginTop: '1rem' }}>
          <button className="btn btn-primary" type="button" onClick={handleStart} disabled={status.isRunning || loading || enabledCount === 0}>
            {loading && !status.isRunning ? 'Starting...' : 'Start COBOL Workload'}
          </button>
          <button className="btn btn-danger" type="button" onClick={handleStop} disabled={!status.isRunning || loading}>
            {loading && status.isRunning ? 'Stopping...' : 'Stop Workload'}
          </button>
          <button className="btn btn-secondary" type="button" onClick={loadStatus} disabled={loading}>
            Refresh
          </button>
        </div>

        {metrics && (
          <div style={{ marginTop: '1rem', display: 'grid', gap: '1rem' }}>
            <div className="schema-stats">
              <div className="schema-stat">
                <div className="name">TPS</div>
                <div className="count">{metrics.tps}</div>
              </div>
              <div className="schema-stat">
                <div className="name">Transactions</div>
                <div className="count">{metrics.total?.transactions || 0}</div>
              </div>
              <div className="schema-stat">
                <div className="name">Errors</div>
                <div className="count">{metrics.total?.errors || 0}</div>
              </div>
              <div className="schema-stat">
                <div className="name">COBOL Regions</div>
                <div className="count">{metrics.activeWorkers || 0}</div>
              </div>
            </div>

            <div className="schema-stats">
              <div className="schema-stat">
                <div className="name">Selects</div>
                <div className="count">{metrics.total?.selects || 0}</div>
              </div>
              <div className="schema-stat">
                <div className="name">Inserts</div>
                <div className="count">{metrics.total?.inserts || 0}</div>
              </div>
              <div className="schema-stat">
                <div className="name">Updates</div>
                <div className="count">{metrics.total?.updates || 0}</div>
              </div>
              <div className="schema-stat">
                <div className="name">Commits</div>
                <div className="count">{metrics.total?.commits || 0}</div>
              </div>
            </div>

            <div style={{
              background: 'var(--surface)',
              border: '1px solid var(--border)',
              borderRadius: '8px',
              padding: '0.9rem'
            }}>
              <h3 style={{ marginTop: 0 }}>Transactions By COBOL Paragraph</h3>
              <div style={{ display: 'grid', gap: '0.35rem' }}>
                {Object.entries(metrics.byTransaction || {}).map(([shortName, entry]) => (
                  <div key={shortName} style={{ display: 'flex', justifyContent: 'space-between' }}>
                    <span>{entry.name} ({shortName})</span>
                    <strong>{entry.count}</strong>
                  </div>
                ))}
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

export default CobolSOEWorkloadPanel;
