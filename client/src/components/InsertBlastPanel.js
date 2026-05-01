import React, { useEffect, useMemo, useState } from 'react';
import axios from 'axios';
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

const getServerUrl = () => {
  if (window.location.hostname !== 'localhost') {
    return `http://${window.location.host}`;
  }
  return 'http://localhost:3001';
};

const API_BASE = `${getServerUrl()}/api`;
const WAIT_COLORS = ['#3b82f6', '#10b981', '#f59e0b', '#ef4444', '#8b5cf6', '#06b6d4', '#f97316', '#14b8a6', '#e11d48', '#84cc16'];
const LMS_USED_COLOR = '#22c55e';
const LMS_ALLOC_COLOR = '#f59e0b';
const INSERT_BLAST_CONFIG_STORAGE_KEY = 'dbstress.insertBlast.config';

const createWorkload = (index = 1, defaultTableCount = 8) => ({
  id: `workload_${Date.now()}_${index}`,
  name: `Workload ${index}`,
  tableCount: defaultTableCount,
  sessions: 8,
  durationSeconds: 60,
  commitEvery: 50,
  sessionMode: 'reuse'
});

const normalizeWorkload = (workload, index) => ({
  id: workload.id || `workload_${index + 1}`,
  name: workload.name || `Workload ${index + 1}`,
  tableCount: workload.tableCount || 8,
  sessions: workload.sessions || 8,
  durationSeconds: workload.durationSeconds || 60,
  commitEvery: workload.commitEvery || 50,
  sessionMode: workload.sessionMode || 'reuse'
});

const createDefaultConfig = () => ({
  tablePrefix: 'IBLAST',
  tableCount: 8,
  columnsPerTable: 24,
  tablespaces: {
    enabled: false,
    tablespacePrefix: 'IBLAST_TS',
    initialSizeMb: 1024,
    autoextendNextMb: 1024,
    datafileLocation: ''
  },
  hwMitigation: {
    enabled: false,
    preallocateOnStart: true,
    extentSizeMb: 128,
    allocateEveryInserts: 100000
  },
  workloads: [createWorkload(1, 8)]
});

const normalizeConfigState = (storedConfig = {}) => {
  const defaults = createDefaultConfig();
  return {
    ...defaults,
    ...storedConfig,
    tablespaces: {
      ...defaults.tablespaces,
      ...(storedConfig.tablespaces || {})
    },
    hwMitigation: {
      ...defaults.hwMitigation,
      ...(storedConfig.hwMitigation || {})
    },
    workloads: Array.isArray(storedConfig.workloads) && storedConfig.workloads.length > 0
      ? storedConfig.workloads.map(normalizeWorkload)
      : defaults.workloads
  };
};

const loadStoredConfig = () => {
  if (typeof window === 'undefined') {
    return createDefaultConfig();
  }

  try {
    const rawValue = window.localStorage.getItem(INSERT_BLAST_CONFIG_STORAGE_KEY);
    return rawValue ? normalizeConfigState(JSON.parse(rawValue)) : createDefaultConfig();
  } catch (error) {
    return createDefaultConfig();
  }
};

const aggregateWaitEvents = (rows = []) => {
  const grouped = new Map();

  rows.forEach((row) => {
    const key = row.event || 'Unknown Event';
    const existing = grouped.get(key) || {
      event: key,
      waitClass: row.waitClass || 'Unknown',
      totalWaits: 0,
      timeWaitedSeconds: 0,
      averageWaitMs: 0
    };

    existing.totalWaits += Number(row.totalWaits || 0);
    existing.timeWaitedSeconds += Number(row.timeWaitedSeconds || 0);
    grouped.set(key, existing);
  });

  return Array.from(grouped.values())
    .map((row) => ({
      ...row,
      averageWaitMs: row.totalWaits > 0 ? (row.timeWaitedSeconds * 1000) / row.totalWaits : 0
    }))
    .sort((a, b) => b.timeWaitedSeconds - a.timeWaitedSeconds)
    .slice(0, 10);
};

function InsertBlastPanel({ dbStatus, socket, onSuccess, onError }) {
  const [config, setConfig] = useState(() => loadStoredConfig());
  const [schemaStatus, setSchemaStatus] = useState(null);
  const [workloadStatus, setWorkloadStatus] = useState({ isRunning: false, workloads: [] });
  const [metrics, setMetrics] = useState(null);
  const [progress, setProgress] = useState(null);
  const [monitorSnapshot, setMonitorSnapshot] = useState(null);
  const [waitEvents, setWaitEvents] = useState([]);
  const [busy, setBusy] = useState(false);
  const [monitorError, setMonitorError] = useState('');

  useEffect(() => {
    if (typeof window === 'undefined') {
      return;
    }

    try {
      window.localStorage.setItem(INSERT_BLAST_CONFIG_STORAGE_KEY, JSON.stringify(config));
    } catch (error) {
      // Ignore storage failures so the panel still works in restricted environments.
    }
  }, [config]);

  const loadStatus = async (nextConfig = config) => {
    if (!dbStatus.connected) {
      return;
    }

    try {
      const response = await axios.get(`${API_BASE}/insert-blast/status`, {
        params: {
          tablePrefix: nextConfig.tablePrefix,
          tableCount: nextConfig.tableCount,
          columnsPerTable: nextConfig.columnsPerTable,
          createTablespaces: nextConfig.tablespaces?.enabled,
          tablespacePrefix: nextConfig.tablespaces?.tablespacePrefix,
          tablespaceInitialSizeMb: nextConfig.tablespaces?.initialSizeMb,
          tablespaceAutoextendNextMb: nextConfig.tablespaces?.autoextendNextMb,
          tablespaceDatafileLocation: nextConfig.tablespaces?.datafileLocation
        }
      });

      if (response.data.success) {
        setSchemaStatus(response.data.schema);
        setWorkloadStatus(response.data.workload || { isRunning: false, workloads: [] });
        if (response.data.workload?.isRunning && response.data.workload?.config) {
          setConfig((prev) => ({
            ...prev,
            hwMitigation: response.data.workload.config.hwMitigation || prev.hwMitigation,
            workloads: (response.data.workload.config.workloads || prev.workloads).map(normalizeWorkload)
          }));
        }
      }
    } catch (err) {
      onError?.(err.response?.data?.message || 'Failed to load insert blast status');
    }
  };

  const loadMonitorSnapshot = async () => {
    if (!dbStatus.connected) {
      return;
    }

    try {
      const response = await axios.get(`${API_BASE}/insert-blast/monitor`);
      if (response.data.success) {
        setMonitorSnapshot(response.data);
        setMonitorError('');
      }
    } catch (err) {
      setMonitorError(err.response?.data?.message || 'Failed to load insert-blast monitor data');
    }
  };

  const loadWaitEvents = async () => {
    if (!dbStatus.connected) {
      return;
    }

    try {
      const response = await axios.get(`${API_BASE}/monitor/waits`, {
        params: { limit: 20 }
      });
      if (response.data.success) {
        setWaitEvents(response.data.waits || []);
      }
    } catch (err) {
      setMonitorError(err.response?.data?.message || 'Failed to load wait events');
    }
  };

  useEffect(() => {
    if (dbStatus.connected) {
      loadStatus();
      loadMonitorSnapshot();
      loadWaitEvents();
    } else {
      setSchemaStatus(null);
      setWorkloadStatus({ isRunning: false, workloads: [] });
      setMetrics(null);
      setProgress(null);
      setMonitorSnapshot(null);
      setWaitEvents([]);
      setMonitorError('');
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [dbStatus.connected]);

  useEffect(() => {
    if (!dbStatus.connected) {
      return undefined;
    }

    const interval = setInterval(() => {
      loadMonitorSnapshot();
      loadWaitEvents();
    }, 5000);

    return () => clearInterval(interval);
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
        setTimeout(() => {
          loadStatus();
          loadMonitorSnapshot();
        }, 500);
      }
    };

    const handleStatus = (data) => {
      setWorkloadStatus(data || { isRunning: false, workloads: [] });
    };

    const handleMetrics = (data) => {
      setMetrics(data);
    };

    socket.on('insert-blast-progress', handleProgress);
    socket.on('insert-blast-status', handleStatus);
    socket.on('insert-blast-metrics', handleMetrics);

    return () => {
      socket.off('insert-blast-progress', handleProgress);
      socket.off('insert-blast-status', handleStatus);
      socket.off('insert-blast-metrics', handleMetrics);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [socket]);

  const handleChange = (field, value) => {
    setConfig((prev) => ({
      ...prev,
      [field]: value
    }));
  };

  const handleTablePrefixChange = (value) => {
    setConfig((prev) => {
      const previousDefaultTablespacePrefix = `${prev.tablePrefix}_TS`;
      const nextDefaultTablespacePrefix = `${value}_TS`;
      const shouldFollowTablePrefix = !prev.tablespaces?.tablespacePrefix
        || prev.tablespaces.tablespacePrefix === previousDefaultTablespacePrefix;

      return {
        ...prev,
        tablePrefix: value,
        tablespaces: {
          ...prev.tablespaces,
          tablespacePrefix: shouldFollowTablePrefix ? nextDefaultTablespacePrefix : prev.tablespaces.tablespacePrefix
        }
      };
    });
  };

  const handleTablespaceChange = (field, value) => {
    setConfig((prev) => ({
      ...prev,
      tablespaces: {
        ...prev.tablespaces,
        [field]: value
      }
    }));
  };

  const handleWorkloadChange = (workloadId, field, value) => {
    setConfig((prev) => ({
      ...prev,
      workloads: prev.workloads.map((workload) => (
        workload.id === workloadId
          ? { ...workload, [field]: value }
          : workload
      ))
    }));
  };

  const addWorkload = () => {
    setConfig((prev) => ({
      ...prev,
      workloads: [...prev.workloads, createWorkload(prev.workloads.length + 1, prev.tableCount)]
    }));
  };

  const removeWorkload = (workloadId) => {
    setConfig((prev) => {
      if (prev.workloads.length <= 1) {
        return prev;
      }

      return {
        ...prev,
        workloads: prev.workloads.filter((workload) => workload.id !== workloadId)
      };
    });
  };

  const handleCreate = async () => {
    setBusy(true);
    setProgress({ step: 'Creating insert-blast tables...', progress: 0 });

    try {
      const response = await axios.post(`${API_BASE}/insert-blast/create`, config, {
        timeout: 600000
      });
      if (response.data.success) {
        const nextConfig = response.data.config
          ? normalizeConfigState({
            ...config,
            ...response.data.config
          })
          : config;
        setConfig(nextConfig);
        onSuccess?.(response.data.message);
        await loadStatus(nextConfig);
      }
    } catch (err) {
      setBusy(false);
      onError?.(err.response?.data?.message || 'Failed to create insert-blast tables');
    }
  };

  const handleDrop = async () => {
    const tablespaceText = config.tablespaces?.enabled
      ? ` and BIGFILE tablespaces for prefix '${config.tablespaces.tablespacePrefix}'`
      : '';

    if (!window.confirm(`Drop insert-blast tables for prefix '${config.tablePrefix}'${tablespaceText}?`)) {
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
      const payload = {
        ...config,
        hwMitigation: {
          enabled: !!config.hwMitigation?.enabled,
          preallocateOnStart: config.hwMitigation?.preallocateOnStart !== false,
          extentSizeMb: Number(config.hwMitigation?.extentSizeMb || 128),
          allocateEveryInserts: Number(config.hwMitigation?.allocateEveryInserts || 100000)
        },
        workloads: config.workloads.map((workload, index) => normalizeWorkload(workload, index))
      };
      const response = await axios.post(`${API_BASE}/insert-blast/start`, payload, {
        timeout: 120000
      });

      if (response.data.success) {
        setConfig((prev) => ({
          ...prev,
          hwMitigation: response.data.config?.hwMitigation || prev.hwMitigation,
          workloads: (response.data.config?.workloads || prev.workloads).map(normalizeWorkload)
        }));
        setWorkloadStatus(response.data);
        await Promise.all([loadMonitorSnapshot(), loadWaitEvents()]);
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
        await Promise.all([loadMonitorSnapshot(), loadWaitEvents()]);
        onSuccess?.('Insert-only workload stopped');
      }
    } catch (err) {
      onError?.(err.response?.data?.message || 'Failed to stop insert-only workload');
    } finally {
      setBusy(false);
    }
  };

  const existingCount = Number(schemaStatus?.existingTableCount || 0);
  const runtimeWorkloadMap = useMemo(() => (
    Object.fromEntries((workloadStatus?.workloads || []).map((workload) => [workload.id, workload]))
  ), [workloadStatus?.workloads]);

  const tableSummary = useMemo(() => {
    const byTable = metrics?.byTable || {};
    return Object.entries(byTable)
      .sort((a, b) => b[1] - a[1])
      .slice(0, 8);
  }, [metrics]);

  const workloadMetrics = useMemo(() => (
    Object.values(metrics?.workloads || {}).sort((a, b) => b.inserts - a.inserts)
  ), [metrics]);

  const totalConfiguredSessions = useMemo(() => (
    config.workloads.reduce((sum, workload) => sum + (Number.parseInt(workload.sessions, 10) || 0), 0)
  ), [config.workloads]);

  const topWaitEvents = useMemo(() => aggregateWaitEvents(waitEvents), [waitEvents]);
  const waitEventsChartData = useMemo(() => ({
    labels: topWaitEvents.map((row) => row.event),
    datasets: [{
      label: 'Time Waited (seconds)',
      data: topWaitEvents.map((row) => Number(Number(row.timeWaitedSeconds || 0).toFixed(2))),
      backgroundColor: topWaitEvents.map((_, index) => WAIT_COLORS[index % WAIT_COLORS.length]),
      borderRadius: 6
    }]
  }), [topWaitEvents]);

  const lmsRows = useMemo(() => (
    [...(monitorSnapshot?.lmsProcessMemory?.rows || [])]
      .sort((a, b) => Number(b.allocMb || 0) - Number(a.allocMb || 0))
      .slice(0, 10)
  ), [monitorSnapshot]);

  const lmsChartData = useMemo(() => ({
    labels: lmsRows.map((row) => `I${row.instId} ${row.processName}/${row.pid} ${row.category}`),
    datasets: [
      {
        label: 'Allocated MB',
        data: lmsRows.map((row) => row.allocMb || 0),
        backgroundColor: LMS_ALLOC_COLOR,
        borderRadius: 6
      },
      {
        label: 'Used MB',
        data: lmsRows.map((row) => row.usedMb || 0),
        backgroundColor: LMS_USED_COLOR,
        borderRadius: 6
      }
    ]
  }), [lmsRows]);

  const horizontalBarOptions = useMemo(() => ({
    responsive: true,
    maintainAspectRatio: false,
    indexAxis: 'y',
    plugins: {
      legend: {
        labels: {
          color: '#cbd5e1'
        }
      }
    },
    scales: {
      x: {
        ticks: {
          color: '#94a3b8'
        },
        grid: {
          color: 'rgba(148, 163, 184, 0.12)'
        }
      },
      y: {
        ticks: {
          color: '#cbd5e1'
        },
        grid: {
          color: 'rgba(148, 163, 184, 0.05)'
        }
      }
    }
  }), []);

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
        <button className="btn btn-secondary btn-sm" type="button" onClick={() => {
          loadStatus();
          loadMonitorSnapshot();
          loadWaitEvents();
        }} disabled={busy}>
          Refresh
        </button>
      </div>
      <div className="panel-content">
        <p style={{ marginTop: 0, color: 'var(--text-muted)' }}>
          Create many wide tables in the current schema, then run one or more insert-only workloads against them with independent session counts, durations, and session modes.
        </p>

        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, minmax(0, 1fr))', gap: '0.85rem' }}>
          <div className="form-group">
            <label htmlFor="ib-prefix">Table Prefix</label>
            <input
              id="ib-prefix"
              value={config.tablePrefix}
              onChange={(e) => handleTablePrefixChange(e.target.value.toUpperCase().replace(/[^A-Z0-9_$#]/g, ''))}
              disabled={busy || workloadStatus.isRunning}
            />
          </div>
          <div className="form-group">
            <label htmlFor="ib-table-count">Tables</label>
            <input
              id="ib-table-count"
              type="number"
              min="1"
              max="5000"
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
          marginTop: '0.9rem',
          background: 'var(--surface)',
          border: '1px solid var(--border)',
          borderRadius: '8px',
          padding: '0.9rem'
        }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', gap: '1rem', alignItems: 'center', flexWrap: 'wrap' }}>
            <div>
              <h3 style={{ margin: 0 }}>Table Tablespaces</h3>
              <div style={{ fontSize: '0.8rem', color: 'var(--text-muted)', marginTop: '0.25rem' }}>
                Create one BIGFILE tablespace per Insert Blast table and place each table in its matching tablespace.
              </div>
            </div>
            <label style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
              <input
                type="checkbox"
                checked={!!config.tablespaces?.enabled}
                onChange={(e) => handleTablespaceChange('enabled', e.target.checked)}
                disabled={busy || workloadStatus.isRunning}
              />
              <span>Enable</span>
            </label>
          </div>

          {config.tablespaces?.enabled && (
            <>
              <div style={{ marginTop: '0.85rem', display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))', gap: '0.85rem' }}>
                <div className="form-group">
                  <label htmlFor="ib-tablespace-prefix">Tablespace Prefix</label>
                  <input
                    id="ib-tablespace-prefix"
                    value={config.tablespaces?.tablespacePrefix || ''}
                    onChange={(e) => handleTablespaceChange('tablespacePrefix', e.target.value.toUpperCase().replace(/[^A-Z0-9_$#]/g, '').slice(0, 27))}
                    disabled={busy || workloadStatus.isRunning}
                  />
                </div>
                <div className="form-group">
                  <label htmlFor="ib-tablespace-location">Datafile Location</label>
                  <input
                    id="ib-tablespace-location"
                    value={config.tablespaces?.datafileLocation || ''}
                    placeholder="OMF default"
                    onChange={(e) => handleTablespaceChange('datafileLocation', e.target.value)}
                    disabled={busy || workloadStatus.isRunning}
                  />
                </div>
                <div className="form-group">
                  <label htmlFor="ib-tablespace-size">Initial Size (MB)</label>
                  <input
                    id="ib-tablespace-size"
                    type="number"
                    min="64"
                    max="1048576"
                    value={config.tablespaces?.initialSizeMb ?? 1024}
                    onChange={(e) => handleTablespaceChange('initialSizeMb', e.target.value)}
                    disabled={busy || workloadStatus.isRunning}
                  />
                </div>
                <div className="form-group">
                  <label htmlFor="ib-tablespace-next">Autoextend Next (MB)</label>
                  <input
                    id="ib-tablespace-next"
                    type="number"
                    min="16"
                    max="65536"
                    value={config.tablespaces?.autoextendNextMb ?? 1024}
                    onChange={(e) => handleTablespaceChange('autoextendNextMb', e.target.value)}
                    disabled={busy || workloadStatus.isRunning}
                  />
                </div>
              </div>

              <div style={{ marginTop: '0.35rem', fontSize: '0.8rem', color: 'var(--text-muted)' }}>
                Names are generated as <code>{`${config.tablespaces?.tablespacePrefix || 'IBLAST_TS'}001`}</code>, <code>{`${config.tablespaces?.tablespacePrefix || 'IBLAST_TS'}002`}</code>, and so on. Leave datafile location blank for Oracle Managed Files, or use a directory/disk group such as `/u01/oradata` or `+DATA`.
              </div>
            </>
          )}
        </div>

        <div style={{
          marginTop: '0.9rem',
          background: 'var(--surface)',
          border: '1px solid var(--border)',
          borderRadius: '8px',
          padding: '0.9rem'
        }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', gap: '1rem', alignItems: 'center', flexWrap: 'wrap' }}>
            <div>
              <h3 style={{ margin: 0 }}>HW Contention Mitigation</h3>
              <div style={{ fontSize: '0.8rem', color: 'var(--text-muted)', marginTop: '0.25rem' }}>
                Pre-allocate table extents before the run, then add another extent after every X inserts per table to reduce `enq: HW - contention`.
              </div>
            </div>
            <label style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
              <input
                type="checkbox"
                checked={!!config.hwMitigation?.enabled}
                onChange={(e) => handleChange('hwMitigation', {
                  ...config.hwMitigation,
                  enabled: e.target.checked
                })}
                disabled={busy || workloadStatus.isRunning}
              />
              <span>Enable</span>
            </label>
          </div>

          {config.hwMitigation?.enabled && (
            <>
              <div style={{ marginTop: '0.85rem', display: 'grid', gridTemplateColumns: 'repeat(3, minmax(0, 1fr))', gap: '0.85rem' }}>
                <div className="form-group">
                  <label htmlFor="ib-hw-extent-size">Extent Size (MB)</label>
                  <input
                    id="ib-hw-extent-size"
                    type="number"
                    min="8"
                    max="1024"
                    value={config.hwMitigation?.extentSizeMb ?? 128}
                    onChange={(e) => handleChange('hwMitigation', {
                      ...config.hwMitigation,
                      extentSizeMb: e.target.value
                    })}
                    disabled={busy || workloadStatus.isRunning}
                  />
                </div>
                <div className="form-group">
                  <label htmlFor="ib-hw-allocate-every">Allocate Every X Inserts</label>
                  <input
                    id="ib-hw-allocate-every"
                    type="number"
                    min="1000"
                    max="10000000"
                    value={config.hwMitigation?.allocateEveryInserts ?? 100000}
                    onChange={(e) => handleChange('hwMitigation', {
                      ...config.hwMitigation,
                      allocateEveryInserts: e.target.value
                    })}
                    disabled={busy || workloadStatus.isRunning}
                  />
                </div>
                <div className="form-group" style={{ display: 'flex', alignItems: 'flex-end' }}>
                  <label style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', marginBottom: '0.6rem' }}>
                    <input
                      type="checkbox"
                      checked={config.hwMitigation?.preallocateOnStart !== false}
                      onChange={(e) => handleChange('hwMitigation', {
                        ...config.hwMitigation,
                        preallocateOnStart: e.target.checked
                      })}
                      disabled={busy || workloadStatus.isRunning}
                    />
                    <span>Pre-allocate on start</span>
                  </label>
                </div>
              </div>

              <div style={{ marginTop: '0.35rem', fontSize: '0.8rem', color: 'var(--text-muted)' }}>
                Recommended starting point: `128 MB` extents and another allocation every `100000` inserts per table. This reduces HW pressure, but it also grows segments faster.
              </div>
            </>
          )}
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
              <div style={{ fontWeight: 600 }}>{existingCount}</div>
              <div style={{ fontSize: '0.75rem', color: 'var(--text-muted)', marginTop: '0.15rem' }}>
                Requested: {config.tableCount}
              </div>
            </div>
            <div>
              <div style={{ fontSize: '0.8rem', color: 'var(--text-muted)' }}>Schema Ready</div>
              <div style={{ fontWeight: 600, color: schemaStatus?.ready ? 'var(--accent-success)' : 'var(--text-primary)' }}>
                {schemaStatus?.ready ? 'Yes' : 'Not yet'}
              </div>
              {schemaStatus?.misplacedTables?.length > 0 && (
                <div style={{ fontSize: '0.75rem', color: 'var(--accent-warning)', marginTop: '0.15rem' }}>
                  {schemaStatus.misplacedTables.length} tablespace mismatch(es)
                </div>
              )}
            </div>
            <div>
              <div style={{ fontSize: '0.8rem', color: 'var(--text-muted)' }}>Configured Workloads</div>
              <div style={{ fontWeight: 600 }}>{config.workloads.length}</div>
            </div>
            <div>
              <div style={{ fontSize: '0.8rem', color: 'var(--text-muted)' }}>Configured Sessions</div>
              <div style={{ fontWeight: 600 }}>{totalConfiguredSessions}</div>
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

        <div style={{ marginTop: '1.25rem', display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: '1rem', flexWrap: 'wrap' }}>
          <h3 style={{ margin: 0 }}>Workloads</h3>
          <button className="btn btn-secondary btn-sm" type="button" onClick={addWorkload} disabled={busy || workloadStatus.isRunning || config.workloads.length >= 20}>
            Add Workload
          </button>
        </div>

        <div style={{ marginTop: '0.85rem', display: 'grid', gap: '0.85rem' }}>
          {config.workloads.map((workload, index) => {
            const runtime = runtimeWorkloadMap[workload.id];

            return (
              <div
                key={workload.id}
                style={{
                  background: 'var(--surface)',
                  border: '1px solid var(--border)',
                  borderRadius: '8px',
                  padding: '0.9rem'
                }}
              >
                <div style={{ display: 'flex', justifyContent: 'space-between', gap: '1rem', alignItems: 'center', flexWrap: 'wrap' }}>
                  <strong>{workload.name || `Workload ${index + 1}`}</strong>
                  <div style={{ display: 'flex', gap: '0.75rem', alignItems: 'center' }}>
                    {runtime && (
                      <span style={{ fontSize: '0.8rem', color: runtime.isRunning ? 'var(--accent-warning)' : 'var(--text-muted)' }}>
                        {runtime.isRunning ? `Running ${runtime.uptime || 0}s` : 'Idle'}
                      </span>
                    )}
                    <button
                      className="btn btn-danger btn-sm"
                      type="button"
                      onClick={() => removeWorkload(workload.id)}
                      disabled={busy || workloadStatus.isRunning || config.workloads.length <= 1}
                    >
                      Remove
                    </button>
                  </div>
                </div>

                <div style={{ marginTop: '0.85rem', display: 'grid', gridTemplateColumns: 'repeat(5, minmax(0, 1fr))', gap: '0.85rem' }}>
                  <div className="form-group">
                    <label htmlFor={`ib-workload-name-${workload.id}`}>Name</label>
                    <input
                      id={`ib-workload-name-${workload.id}`}
                      value={workload.name}
                      onChange={(e) => handleWorkloadChange(workload.id, 'name', e.target.value)}
                      disabled={busy || workloadStatus.isRunning}
                    />
                  </div>
                  <div className="form-group">
                    <label htmlFor={`ib-session-mode-${workload.id}`}>Session Mode</label>
                    <select
                      id={`ib-session-mode-${workload.id}`}
                      value={workload.sessionMode}
                      onChange={(e) => handleWorkloadChange(workload.id, 'sessionMode', e.target.value)}
                      disabled={busy || workloadStatus.isRunning}
                    >
                      <option value="reuse">Logon once, reuse session</option>
                      <option value="reconnect">Logon/insert/logout loop</option>
                    </select>
                  </div>
                  <div className="form-group">
                    <label htmlFor={`ib-table-count-${workload.id}`}>Tables Used</label>
                    <input
                      id={`ib-table-count-${workload.id}`}
                      type="number"
                      min="1"
                      max={config.tableCount}
                      value={workload.tableCount}
                      onChange={(e) => handleWorkloadChange(workload.id, 'tableCount', e.target.value)}
                      disabled={busy || workloadStatus.isRunning}
                    />
                  </div>
                  <div className="form-group">
                    <label htmlFor={`ib-sessions-${workload.id}`}>Sessions</label>
                    <input
                      id={`ib-sessions-${workload.id}`}
                      type="number"
                      min="1"
                      value={workload.sessions}
                      onChange={(e) => handleWorkloadChange(workload.id, 'sessions', e.target.value)}
                      disabled={busy || workloadStatus.isRunning}
                    />
                  </div>
                  <div className="form-group">
                    <label htmlFor={`ib-duration-${workload.id}`}>Run Time (seconds)</label>
                    <input
                      id={`ib-duration-${workload.id}`}
                      type="number"
                      min="1"
                      max="86400"
                      value={workload.durationSeconds}
                      onChange={(e) => handleWorkloadChange(workload.id, 'durationSeconds', e.target.value)}
                      disabled={busy || workloadStatus.isRunning}
                    />
                  </div>
                  <div className="form-group">
                    <label htmlFor={`ib-commit-${workload.id}`}>Commit Every</label>
                    <input
                      id={`ib-commit-${workload.id}`}
                      type="number"
                      min="1"
                      value={workload.commitEvery}
                      onChange={(e) => handleWorkloadChange(workload.id, 'commitEvery', e.target.value)}
                      disabled={busy || workloadStatus.isRunning}
                    />
                  </div>
                </div>

                <div style={{ marginTop: '0.35rem', fontSize: '0.8rem', color: 'var(--text-muted)' }}>
                  {workload.sessionMode === 'reconnect'
                    ? `${workload.sessions} clients will use ${workload.tableCount} table(s), log on, insert, commit, and log off repeatedly for ${workload.durationSeconds} seconds.`
                    : `${workload.sessions} clients will use ${workload.tableCount} table(s), keep the same session open for ${workload.durationSeconds} seconds, and commit every ${workload.commitEvery} inserts.`}
                </div>
              </div>
            );
          })}
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
              <div className="schema-stat">
                <div className="name">Extent Alloc</div>
                <div className="count">{metrics.total?.extentAllocations || 0}</div>
              </div>
            </div>

            {workloadMetrics.length > 0 && (
              <div style={{
                background: 'var(--surface)',
                border: '1px solid var(--border)',
                borderRadius: '8px',
                padding: '0.9rem'
              }}>
                <h3 style={{ marginTop: 0 }}>Workload Breakdown</h3>
                <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(170px, 1fr))', gap: '0.75rem' }}>
                  {workloadMetrics.map((workload) => (
                    <div
                      key={workload.id}
                      style={{
                        border: '1px solid var(--border)',
                        borderRadius: '8px',
                        padding: '0.75rem'
                      }}
                    >
                      <div style={{ fontWeight: 600 }}>{workload.name}</div>
                      <div style={{ marginTop: '0.4rem', fontSize: '0.8rem', color: 'var(--text-muted)' }}>
                        {workload.sessionMode === 'reuse' ? 'Reuse session' : 'Reconnect each cycle'}
                      </div>
                      <div style={{ marginTop: '0.6rem', display: 'grid', gap: '0.3rem', fontSize: '0.9rem' }}>
                        <div>Sessions: <strong>{workload.sessions}</strong></div>
                        <div>Tables Used: <strong>{workload.tableCount}</strong></div>
                        <div>Active Workers: <strong>{workload.activeWorkers}</strong></div>
                        <div>Inserts: <strong>{workload.inserts}</strong></div>
                        <div>Inserts/Sec: <strong>{workload.perSecond?.inserts || 0}</strong></div>
                        <div>Commits: <strong>{workload.commits}</strong></div>
                        <div>Errors: <strong>{workload.errors}</strong></div>
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            )}

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

        <div style={{ marginTop: '1.25rem', display: 'grid', gap: '1rem' }}>
          <div style={{
            background: 'var(--surface)',
            border: '1px solid var(--border)',
            borderRadius: '8px',
            padding: '0.9rem'
          }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', gap: '1rem', flexWrap: 'wrap', alignItems: 'center' }}>
              <h3 style={{ margin: 0 }}>Connected User Sessions Per Instance</h3>
              <span style={{ fontSize: '0.8rem', color: 'var(--text-muted)' }}>
                User: {monitorSnapshot?.userSessions?.username || dbStatus.config?.user || '-'}
              </span>
            </div>
            <div style={{ marginTop: '0.85rem', display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))', gap: '0.75rem' }}>
              {(monitorSnapshot?.userSessions?.sessions || []).length > 0 ? (
                monitorSnapshot.userSessions.sessions.map((row) => (
                  <div
                    key={`inst-${row.instId}`}
                    style={{
                      border: '1px solid var(--border)',
                      borderRadius: '8px',
                      padding: '0.75rem'
                    }}
                  >
                    <div style={{ fontWeight: 600 }}>Instance {row.instId}</div>
                    <div style={{ marginTop: '0.5rem', display: 'grid', gap: '0.25rem', fontSize: '0.9rem' }}>
                      <div>Total: <strong>{row.totalSessions}</strong></div>
                      <div>Active: <strong>{row.activeSessions}</strong></div>
                      <div>Inactive: <strong>{row.inactiveSessions}</strong></div>
                    </div>
                  </div>
                ))
              ) : (
                <p style={{ margin: 0, color: 'var(--text-muted)' }}>No user sessions found for the connected schema yet.</p>
              )}
            </div>
          </div>

          <div style={{
            background: 'var(--surface)',
            border: '1px solid var(--border)',
            borderRadius: '8px',
            padding: '0.9rem'
          }}>
            <h3 style={{ margin: '0 0 0.85rem 0' }}>Top 10 Wait Events</h3>
            {topWaitEvents.length > 0 ? (
              <div style={{ height: '340px' }}>
                <Bar data={waitEventsChartData} options={horizontalBarOptions} />
              </div>
            ) : (
              <p style={{ margin: 0, color: 'var(--text-muted)' }}>Wait event data is not available yet.</p>
            )}
          </div>

          <div style={{
            background: 'var(--surface)',
            border: '1px solid var(--border)',
            borderRadius: '8px',
            padding: '0.9rem'
          }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', gap: '1rem', flexWrap: 'wrap', alignItems: 'center' }}>
              <h3 style={{ margin: 0 }}>LMS Process Memory</h3>
              <span style={{ fontSize: '0.8rem', color: 'var(--text-muted)' }}>
                {monitorSnapshot?.lmsProcessMemory?.source || 'No source yet'}
              </span>
            </div>
            {lmsRows.length > 0 ? (
              <div style={{ marginTop: '0.85rem', height: '360px' }}>
                <Bar data={lmsChartData} options={horizontalBarOptions} />
              </div>
            ) : (
              <p style={{ marginTop: '0.85rem', marginBottom: 0, color: 'var(--text-muted)' }}>
                No LMS rows returned by the database.
              </p>
            )}
          </div>

          {monitorError && (
            <p style={{ margin: 0, color: 'var(--accent-danger)' }}>{monitorError}</p>
          )}
        </div>
      </div>
    </div>
  );
}

export default InsertBlastPanel;
