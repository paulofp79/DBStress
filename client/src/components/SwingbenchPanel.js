import React, { useEffect, useMemo, useState } from 'react';
import axios from 'axios';

const getServerUrl = () => {
  if (window.location.hostname !== 'localhost') {
    return `http://${window.location.host}`;
  }
  return 'http://localhost:3001';
};

const API_BASE = `${getServerUrl()}/api`;

const emptyPreview = {
  adminScripts: [],
  ownerScripts: [],
  limitations: [],
  script: ''
};

function SwingbenchPanel({ dbStatus, socket, onSuccess, onError }) {
  const [loadingDefaults, setLoadingDefaults] = useState(false);
  const [refreshingStatus, setRefreshingStatus] = useState(false);
  const [previewing, setPreviewing] = useState(false);
  const [creating, setCreating] = useState(false);
  const [dropping, setDropping] = useState(false);
  const [meta, setMeta] = useState({
    defaults: null,
    supportedModels: {
      compression: [],
      partitioning: [],
      indexing: []
    },
    limitations: []
  });
  const [config, setConfig] = useState({
    username: 'SOE',
    password: 'soe',
    tablespace: 'SOE',
    tempTablespace: 'TEMP',
    createUser: true,
    createTablespace: false,
    replaceExisting: false,
    datafile: '',
    datafileSize: '2G',
    compression: 'none',
    partitioning: 'none',
    indexing: 'all',
    parallelism: 2
  });
  const [status, setStatus] = useState(null);
  const [preview, setPreview] = useState(emptyPreview);
  const [progress, setProgress] = useState(null);

  const loadDefaults = async () => {
    setLoadingDefaults(true);
    try {
      const response = await axios.get(`${API_BASE}/swingbench/soe/defaults`);
      if (response.data.success) {
        setMeta({
          defaults: response.data.defaults,
          supportedModels: response.data.supportedModels,
          limitations: response.data.limitations || []
        });
        setConfig((prev) => ({
          ...response.data.defaults,
          ...prev
        }));
      }
    } catch (err) {
      onError?.(err.response?.data?.message || 'Failed to load Swingbench defaults');
    } finally {
      setLoadingDefaults(false);
    }
  };

  const loadStatus = async (nextConfig = config) => {
    if (!dbStatus.connected) {
      setStatus(null);
      return;
    }

    setRefreshingStatus(true);
    try {
      const response = await axios.get(`${API_BASE}/swingbench/soe/status`, {
        params: { username: nextConfig.username }
      });
      if (response.data.success) {
        setStatus(response.data);
      }
    } catch (err) {
      onError?.(err.response?.data?.message || 'Failed to load SOE schema status');
    } finally {
      setRefreshingStatus(false);
    }
  };

  useEffect(() => {
    if (dbStatus.connected) {
      loadDefaults();
      loadStatus();
    } else {
      setStatus(null);
      setPreview(emptyPreview);
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
        setCreating(false);
        setDropping(false);
        if (data.progress === 100) {
          setTimeout(() => loadStatus(), 500);
        }
      }
    };

    socket.on('swingbench-soe-progress', handleProgress);
    return () => {
      socket.off('swingbench-soe-progress', handleProgress);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [socket, config.username]);

  const scriptList = useMemo(() => {
    return [...(preview.adminScripts || []), ...(preview.ownerScripts || [])];
  }, [preview]);

  const handleChange = (field, value) => {
    setConfig((prev) => ({
      ...prev,
      [field]: value
    }));
  };

  const handlePreview = async () => {
    setPreviewing(true);
    try {
      const response = await axios.post(`${API_BASE}/swingbench/soe/preview`, config, {
        timeout: 60000
      });
      if (response.data.success) {
        setPreview(response.data);
      }
    } catch (err) {
      onError?.(err.response?.data?.message || 'Failed to build SOE install preview');
    } finally {
      setPreviewing(false);
    }
  };

  const handleCreate = async () => {
    setCreating(true);
    setProgress({ step: 'Preparing SOE install...', progress: 0 });
    try {
      const response = await axios.post(`${API_BASE}/swingbench/soe/create`, config, {
        timeout: 600000
      });
      if (response.data.success) {
        onSuccess?.(`Swingbench SOE schema ${response.data.config.username} installed`);
        await loadStatus(config);
      }
    } catch (err) {
      setCreating(false);
      onError?.(err.response?.data?.message || 'Failed to install Swingbench SOE schema');
    }
  };

  const handleDrop = async () => {
    if (!window.confirm(`Drop Swingbench SOE schema '${config.username}'?`)) {
      return;
    }

    setDropping(true);
    setProgress({ step: `Dropping ${config.username}...`, progress: 0 });
    try {
      const response = await axios.post(`${API_BASE}/swingbench/soe/drop`, config, {
        timeout: 300000
      });
      if (response.data.success) {
        onSuccess?.(`Swingbench SOE schema ${config.username} dropped`);
        await loadStatus(config);
      }
    } catch (err) {
      setDropping(false);
      onError?.(err.response?.data?.message || 'Failed to drop Swingbench SOE schema');
    }
  };

  if (!dbStatus.connected) {
    return (
      <div className="panel">
        <div className="panel-header">
          <h2>Swingbench SOE</h2>
        </div>
        <div className="panel-content">
          <p style={{ color: 'var(--text-muted)' }}>Connect to Oracle first. Use a privileged account if you want DBStress to create or drop the SOE user and tablespace.</p>
        </div>
      </div>
    );
  }

  const busy = previewing || creating || dropping;

  return (
    <div className="panel" style={{ maxHeight: '760px', overflow: 'auto' }}>
      <div className="panel-header">
        <h2>Swingbench SOE</h2>
        <button className="btn btn-secondary btn-sm" onClick={() => loadStatus()} type="button" disabled={refreshingStatus || busy}>
          {refreshingStatus ? 'Refreshing...' : 'Refresh Status'}
        </button>
      </div>

      <div className="panel-content">
        <div style={{
          background: 'var(--surface)',
          border: '1px solid var(--border)',
          borderRadius: '8px',
          padding: '0.9rem',
          marginBottom: '1rem'
        }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', gap: '1rem', flexWrap: 'wrap' }}>
            <div>
              <div style={{ fontSize: '0.8rem', color: 'var(--text-muted)' }}>Connected As</div>
              <div style={{ fontWeight: 600 }}>{dbStatus.config?.user}@{dbStatus.config?.connectionString}</div>
            </div>
            <div>
              <div style={{ fontSize: '0.8rem', color: 'var(--text-muted)' }}>SOE User</div>
              <div style={{ fontWeight: 600 }}>{status?.username || config.username}</div>
            </div>
            <div>
              <div style={{ fontSize: '0.8rem', color: 'var(--text-muted)' }}>Schema Ready</div>
              <div style={{ fontWeight: 600, color: status?.ready ? 'var(--accent-success)' : 'var(--text-primary)' }}>
                {status?.ready ? 'Yes' : 'Not yet'}
              </div>
            </div>
          </div>

          {status && (
            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4, minmax(0, 1fr))', gap: '0.5rem', marginTop: '0.9rem', fontSize: '0.85rem' }}>
              <div>Tables: {status.objectCounts?.tables || 0}</div>
              <div>Views: {status.objectCounts?.views || 0}</div>
              <div>Sequences: {status.objectCounts?.sequences || 0}</div>
              <div>Packages: {(status.objectCounts?.packages || 0) + (status.objectCounts?.packageBodies || 0)}</div>
            </div>
          )}
        </div>

        <div style={{ display: 'grid', gap: '0.85rem' }}>
          <div className="form-group">
            <label htmlFor="soe-username">SOE Username</label>
            <input
              id="soe-username"
              value={config.username}
              onChange={(e) => handleChange('username', e.target.value.toUpperCase().replace(/[^A-Z0-9_$#]/g, ''))}
              disabled={busy}
            />
          </div>

          <div className="form-group">
            <label htmlFor="soe-password">SOE Password</label>
            <input
              id="soe-password"
              type="password"
              value={config.password}
              onChange={(e) => handleChange('password', e.target.value.replace(/[^A-Za-z0-9_$#]/g, ''))}
              disabled={busy}
            />
          </div>

          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(2, minmax(0, 1fr))', gap: '0.85rem' }}>
            <div className="form-group">
              <label htmlFor="soe-tablespace">Tablespace</label>
              <input
                id="soe-tablespace"
                value={config.tablespace}
                onChange={(e) => handleChange('tablespace', e.target.value.toUpperCase().replace(/[^A-Z0-9_$#]/g, ''))}
                disabled={busy}
              />
            </div>

            <div className="form-group">
              <label htmlFor="soe-temp-tablespace">Temp Tablespace</label>
              <input
                id="soe-temp-tablespace"
                value={config.tempTablespace}
                onChange={(e) => handleChange('tempTablespace', e.target.value.toUpperCase().replace(/[^A-Z0-9_$#]/g, ''))}
                disabled={busy}
              />
            </div>
          </div>

          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, minmax(0, 1fr))', gap: '0.85rem' }}>
            <div className="form-group">
              <label htmlFor="soe-compression">Compression</label>
              <select
                id="soe-compression"
                value={config.compression}
                onChange={(e) => handleChange('compression', e.target.value)}
                disabled={busy}
              >
                {(meta.supportedModels.compression || []).map((option) => (
                  <option key={option.value} value={option.value}>{option.label}</option>
                ))}
              </select>
            </div>

            <div className="form-group">
              <label htmlFor="soe-partitioning">Partitioning</label>
              <select
                id="soe-partitioning"
                value={config.partitioning}
                onChange={(e) => handleChange('partitioning', e.target.value)}
                disabled={busy}
              >
                {(meta.supportedModels.partitioning || []).map((option) => (
                  <option key={option.value} value={option.value}>{option.label}</option>
                ))}
              </select>
            </div>

            <div className="form-group">
              <label htmlFor="soe-indexing">Indexes</label>
              <select
                id="soe-indexing"
                value={config.indexing}
                onChange={(e) => handleChange('indexing', e.target.value)}
                disabled={busy}
              >
                {(meta.supportedModels.indexing || []).map((option) => (
                  <option key={option.value} value={option.value}>{option.label}</option>
                ))}
              </select>
            </div>
          </div>

          <div className="form-group">
            <label htmlFor="soe-parallelism">Parallelism</label>
            <input
              id="soe-parallelism"
              type="number"
              min="1"
              max="32"
              value={config.parallelism}
              onChange={(e) => handleChange('parallelism', e.target.value)}
              disabled={busy}
            />
          </div>

          <div style={{ display: 'grid', gap: '0.5rem' }}>
            <label style={{ display: 'flex', gap: '0.6rem', alignItems: 'center' }}>
              <input
                type="checkbox"
                checked={config.createUser}
                onChange={(e) => handleChange('createUser', e.target.checked)}
                disabled={busy}
              />
              <span>Create SOE user</span>
            </label>

            <label style={{ display: 'flex', gap: '0.6rem', alignItems: 'center' }}>
              <input
                type="checkbox"
                checked={config.createTablespace}
                onChange={(e) => handleChange('createTablespace', e.target.checked)}
                disabled={busy}
              />
              <span>Create SOE tablespace</span>
            </label>

            <label style={{ display: 'flex', gap: '0.6rem', alignItems: 'center' }}>
              <input
                type="checkbox"
                checked={config.replaceExisting}
                onChange={(e) => handleChange('replaceExisting', e.target.checked)}
                disabled={busy}
              />
              <span>Replace existing SOE user or tablespace first</span>
            </label>
          </div>

          {config.createTablespace && (
            <div style={{ display: 'grid', gridTemplateColumns: '2fr 1fr', gap: '0.85rem' }}>
              <div className="form-group">
                <label htmlFor="soe-datafile">Datafile Path</label>
                <input
                  id="soe-datafile"
                  value={config.datafile}
                  placeholder="/u02/oradata/SOE01.dbf"
                  onChange={(e) => handleChange('datafile', e.target.value)}
                  disabled={busy}
                />
              </div>

              <div className="form-group">
                <label htmlFor="soe-datafilesize">Datafile Size</label>
                <input
                  id="soe-datafilesize"
                  value={config.datafileSize}
                  onChange={(e) => handleChange('datafileSize', e.target.value.toUpperCase())}
                  disabled={busy}
                />
              </div>
            </div>
          )}
        </div>

        <div className="btn-group" style={{ marginTop: '1rem' }}>
          <button className="btn btn-secondary" type="button" onClick={handlePreview} disabled={busy || loadingDefaults}>
            {previewing ? 'Building Preview...' : 'Preview Script'}
          </button>
          <button className="btn btn-primary" type="button" onClick={handleCreate} disabled={busy || loadingDefaults}>
            {creating ? 'Installing...' : 'Install SOE Objects'}
          </button>
          <button className="btn btn-danger" type="button" onClick={handleDrop} disabled={busy || loadingDefaults}>
            {dropping ? 'Dropping...' : 'Drop SOE'}
          </button>
        </div>

        {progress && (
          <div style={{ marginTop: '1rem' }}>
            <div style={{ fontSize: '0.85rem', marginBottom: '0.4rem' }}>{progress.step}</div>
            <div className="progress-bar">
              <div className="fill" style={{ width: `${Math.max(0, progress.progress || 0)}%` }}></div>
            </div>
          </div>
        )}

        <div style={{
          marginTop: '1rem',
          background: 'var(--surface)',
          border: '1px solid var(--border)',
          borderRadius: '8px',
          padding: '0.9rem'
        }}>
          <h3 style={{ marginTop: 0, marginBottom: '0.65rem' }}>Selected SQL Scripts</h3>
          {scriptList.length > 0 ? (
            <div style={{ display: 'grid', gap: '0.35rem', fontSize: '0.85rem' }}>
              {scriptList.map((scriptName) => (
                <div key={scriptName}>{scriptName}</div>
              ))}
            </div>
          ) : (
            <p style={{ margin: 0, color: 'var(--text-muted)' }}>
              Run Preview Script to see the exact Swingbench SQL files that will be executed.
            </p>
          )}
        </div>

        <div style={{
          marginTop: '1rem',
          background: 'var(--surface)',
          border: '1px solid var(--border)',
          borderRadius: '8px',
          padding: '0.9rem'
        }}>
          <h3 style={{ marginTop: 0, marginBottom: '0.65rem' }}>Limitations</h3>
          <div style={{ display: 'grid', gap: '0.45rem', fontSize: '0.85rem' }}>
            {(preview.limitations?.length ? preview.limitations : meta.limitations).map((item) => (
              <div key={item}>{item}</div>
            ))}
          </div>
        </div>

        {preview.script && (
          <div style={{ marginTop: '1rem' }}>
            <h3 style={{ marginBottom: '0.65rem' }}>Generated Install Script</h3>
            <pre style={{
              margin: 0,
              padding: '0.9rem',
              maxHeight: '280px',
              overflow: 'auto',
              background: '#0f172a',
              color: '#e2e8f0',
              borderRadius: '8px',
              fontSize: '0.75rem',
              whiteSpace: 'pre-wrap',
              wordBreak: 'break-word'
            }}>
              {preview.script}
            </pre>
          </div>
        )}
      </div>
    </div>
  );
}

export default SwingbenchPanel;
