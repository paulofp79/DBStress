import React, { useEffect, useMemo, useState } from 'react';
import { Line } from 'react-chartjs-2';
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend
} from 'chart.js';

ChartJS.register(
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
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

const RECOMMENDED_WAITS = [
  'gc current block congested',
  'gc cr block congested',
  'gc current block busy',
  'gc cr block busy'
];

const SERIES_COLORS = [
  { border: 'rgb(239, 68, 68)', bg: 'rgba(239, 68, 68, 0.15)' },
  { border: 'rgb(245, 158, 11)', bg: 'rgba(245, 158, 11, 0.15)' },
  { border: 'rgb(59, 130, 246)', bg: 'rgba(59, 130, 246, 0.15)' },
  { border: 'rgb(34, 197, 94)', bg: 'rgba(34, 197, 94, 0.15)' },
  { border: 'rgb(168, 85, 247)', bg: 'rgba(168, 85, 247, 0.15)' },
  { border: 'rgb(14, 165, 233)', bg: 'rgba(14, 165, 233, 0.15)' }
];

function GCCongestionPanel({ dbStatus, socket }) {
  const [config, setConfig] = useState({
    schemaPrefix: 'GCDEMO',
    tableCount: 100,
    scaleFactor: 1,
    baseRowsPerTable: 10000,
    indexPartitioning: 'none',
    indexHashPartitions: 16,
    threads: 120,
    thinkTime: 0,
    hotRows: 200,
    hotTableSpan: 10,
    updatesPerTxn: 4,
    readRatio: 0.15
  });

  const [status, setStatus] = useState({
    message: '',
    progress: null,
    isPreparing: false,
    isRunning: false,
    prepared: false,
    preparedConfig: null
  });

  const [metrics, setMetrics] = useState({
    tps: 0,
    totalTransactions: 0,
    errors: 0,
    uptime: 0
  });

  const [availableWaits, setAvailableWaits] = useState([]);
  const [selectedWaits, setSelectedWaits] = useState([...RECOMMENDED_WAITS]);
  const [dropOptions, setDropOptions] = useState({
    waitForLogoutSec: 30,
    forceLogout: false
  });

  const [waitChart, setWaitChart] = useState({
    labels: [],
    series: {}
  });
  const [latestWaits, setLatestWaits] = useState([]);

  const totalRowsEstimate = useMemo(
    () => config.tableCount * config.baseRowsPerTable * config.scaleFactor,
    [config.tableCount, config.baseRowsPerTable, config.scaleFactor]
  );

  const fetchWaitEvents = async () => {
    try {
      const response = await fetch(`${API_BASE}/gc-congestion/wait-events`);
      const data = await response.json();
      if (response.ok && data.success) {
        const events = data.events || [];
        setAvailableWaits(events);
        setSelectedWaits(prev => {
          if (prev.length > 0) return prev;
          const recommended = RECOMMENDED_WAITS.filter(eventName => events.includes(eventName));
          return recommended.length > 0 ? recommended : events.slice(0, 6);
        });
      }
    } catch (err) {
      // Keep defaults if lookup fails.
    }
  };

  const fetchStatus = async () => {
    try {
      const response = await fetch(`${API_BASE}/gc-congestion/status`);
      const data = await response.json();
      if (!response.ok) return;

      setStatus(prev => ({
        ...prev,
        isPreparing: !!data.isPreparing,
        isRunning: !!data.isRunning,
        prepared: !!data.prepared,
        preparedConfig: data.preparedConfig || null
      }));

      if (data.workloadConfig) {
        setConfig(prev => ({
          ...prev,
          ...data.workloadConfig
        }));
        if (Array.isArray(data.workloadConfig.waitFilters) && data.workloadConfig.waitFilters.length > 0) {
          setSelectedWaits(data.workloadConfig.waitFilters);
        }
      }

      if (data.stats) {
        setMetrics(prev => ({
          ...prev,
          tps: data.stats.tps || 0,
          totalTransactions: data.stats.totalTransactions || 0,
          errors: data.stats.errors || 0,
          uptime: data.stats.uptime || 0
        }));
      }
    } catch (err) {
      // Ignore status fetch errors during startup.
    }
  };

  useEffect(() => {
    if (!dbStatus.connected) return;
    fetchStatus();
    fetchWaitEvents();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [dbStatus.connected]);

  useEffect(() => {
    if (!socket) return;

    const onStatus = (payload) => {
      setStatus(prev => ({
        ...prev,
        message: payload.message || prev.message,
        progress: payload.progress ?? prev.progress,
        isPreparing: !!payload.isPreparing,
        isRunning: !!payload.isRunning,
        prepared: !!payload.prepared,
        preparedConfig: payload.preparedConfig || prev.preparedConfig
      }));
    };

    const onMetrics = (payload) => {
      setMetrics({
        tps: payload.tps || 0,
        totalTransactions: payload.totalTransactions || 0,
        errors: payload.errors || 0,
        uptime: payload.uptime || 0
      });
    };

    const onWaits = (payload) => {
      const events = payload.events || [];
      const timestamp = payload.timestamp || Date.now();
      const label = new Date(timestamp).toLocaleTimeString([], { minute: '2-digit', second: '2-digit' });
      const trackedEvents = (payload.selectedFilters && payload.selectedFilters.length > 0)
        ? payload.selectedFilters
        : events.map(e => e.event);

      setLatestWaits(events);

      setWaitChart(prev => {
        const nextLabels = [...prev.labels, label].slice(-60);
        const previousLabelCount = prev.labels.length;
        const payloadMap = new Map(events.map(e => [e.event, e]));
        const nextSeries = {};

        trackedEvents.forEach(eventName => {
          const previousSeries = prev.series[eventName] || [];
          const padCount = Math.max(0, previousLabelCount - previousSeries.length);
          const paddedSeries = padCount > 0
            ? [...Array(padCount).fill(null), ...previousSeries]
            : previousSeries;
          const nextPoint = (payloadMap.get(eventName)?.deltaWaits ?? 0);
          nextSeries[eventName] = [...paddedSeries, nextPoint].slice(-60);
        });

        return {
          labels: nextLabels,
          series: nextSeries
        };
      });
    };

    socket.on('gc-congestion-status', onStatus);
    socket.on('gc-congestion-metrics', onMetrics);
    socket.on('gc-congestion-waits', onWaits);

    return () => {
      socket.off('gc-congestion-status', onStatus);
      socket.off('gc-congestion-metrics', onMetrics);
      socket.off('gc-congestion-waits', onWaits);
    };
  }, [socket]);

  const updateNumber = (field, value, min, max) => {
    const parsed = Number(value);
    if (!Number.isFinite(parsed)) return;
    setConfig(prev => ({
      ...prev,
      [field]: Math.min(max, Math.max(min, Math.floor(parsed)))
    }));
  };

  const updateFloat = (field, value, min, max) => {
    const parsed = Number(value);
    if (!Number.isFinite(parsed)) return;
    setConfig(prev => ({
      ...prev,
      [field]: Math.min(max, Math.max(min, parsed))
    }));
  };

  const toggleWait = (eventName) => {
    setSelectedWaits(prev => {
      if (prev.includes(eventName)) {
        return prev.filter(item => item !== eventName);
      }
      return [...prev, eventName];
    });
  };

  const handlePrepare = async () => {
    try {
      setStatus(prev => ({
        ...prev,
        message: 'Preparing GC demo tables...',
        progress: 0
      }));

      const response = await fetch(`${API_BASE}/gc-congestion/prepare`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          ...config,
          waitFilters: selectedWaits
        })
      });
      const data = await response.json();
      if (!response.ok) {
        throw new Error(data.error || 'Failed to prepare GC demo tables');
      }

      await fetchStatus();
    } catch (err) {
      setStatus(prev => ({
        ...prev,
        message: `Error: ${err.message}`
      }));
    }
  };

  const handleStart = async () => {
    try {
      setWaitChart({ labels: [], series: {} });
      setLatestWaits([]);
      setStatus(prev => ({
        ...prev,
        message: 'Starting GC congestion workload...'
      }));

      const response = await fetch(`${API_BASE}/gc-congestion/start`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          ...config,
          waitFilters: selectedWaits
        })
      });
      const data = await response.json();
      if (!response.ok) {
        throw new Error(data.error || 'Failed to start workload');
      }

      await fetchStatus();
    } catch (err) {
      setStatus(prev => ({
        ...prev,
        message: `Error: ${err.message}`
      }));
    }
  };

  const handleStop = async () => {
    try {
      const response = await fetch(`${API_BASE}/gc-congestion/stop`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' }
      });
      const data = await response.json();
      if (!response.ok) {
        throw new Error(data.error || 'Failed to stop workload');
      }

      await fetchStatus();
    } catch (err) {
      setStatus(prev => ({
        ...prev,
        message: `Error: ${err.message}`
      }));
    }
  };

  const handleDrop = async () => {
    try {
      const response = await fetch(`${API_BASE}/gc-congestion/drop`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          schemaPrefix: config.schemaPrefix,
          waitForLogoutSec: dropOptions.waitForLogoutSec,
          forceLogout: dropOptions.forceLogout
        })
      });
      const data = await response.json();
      if (!response.ok) {
        throw new Error(data.error || 'Failed to drop demo tables');
      }

      const forceNote = data.forcedLogout
        ? ` | killed sessions: ${data.killedSessions || 0}`
        : '';
      const forceFailNote = data.killFailures > 0
        ? ` | kill failures: ${data.killFailures}`
        : '';
      setStatus(prev => ({
        ...prev,
        message: `Dropped ${data.dropped || 0} demo tables${forceNote}${forceFailNote}`
      }));
      await fetchStatus();
    } catch (err) {
      setStatus(prev => ({
        ...prev,
        message: `Error: ${err.message}`
      }));
    }
  };

  const formatNumber = (value) => {
    const num = Number(value) || 0;
    if (num >= 1000000) return `${(num / 1000000).toFixed(1)}M`;
    if (num >= 1000) return `${(num / 1000).toFixed(1)}K`;
    return String(num);
  };

  const formatUptime = (seconds) => {
    const total = Number(seconds) || 0;
    const hrs = Math.floor(total / 3600);
    const mins = Math.floor((total % 3600) / 60);
    const secs = total % 60;
    if (hrs > 0) return `${hrs}h ${mins}m ${secs}s`;
    if (mins > 0) return `${mins}m ${secs}s`;
    return `${secs}s`;
  };

  const chartSeriesNames = selectedWaits.length > 0
    ? selectedWaits
    : Object.keys(waitChart.series);

  const chartData = {
    labels: waitChart.labels,
    datasets: chartSeriesNames.map((eventName, index) => ({
      label: eventName,
      data: waitChart.series[eventName] || [],
      borderColor: SERIES_COLORS[index % SERIES_COLORS.length].border,
      backgroundColor: SERIES_COLORS[index % SERIES_COLORS.length].bg,
      tension: 0.35,
      fill: false,
      pointRadius: 0,
      pointHoverRadius: 4,
      spanGaps: true
    }))
  };

  const chartOptions = {
    responsive: true,
    maintainAspectRatio: false,
    animation: { duration: 0 },
    interaction: { mode: 'index', intersect: false },
    plugins: {
      legend: {
        position: 'top',
        labels: {
          color: '#a0a0b0',
          usePointStyle: true,
          boxWidth: 10
        }
      },
      tooltip: {
        backgroundColor: 'rgba(26, 26, 46, 0.95)',
        titleColor: '#ffffff',
        bodyColor: '#a0a0b0',
        borderColor: '#2a2a45',
        borderWidth: 1
      }
    },
    scales: {
      x: {
        grid: { color: 'rgba(42, 42, 69, 0.5)' },
        ticks: { color: '#6b6b80', maxTicksLimit: 10 }
      },
      y: {
        beginAtZero: true,
        grid: { color: 'rgba(42, 42, 69, 0.5)' },
        ticks: { color: '#6b6b80' },
        title: {
          display: true,
          text: 'Delta waits / interval',
          color: '#6b6b80'
        }
      }
    }
  };

  const canPrepare = dbStatus.connected && !status.isPreparing && !status.isRunning;
  const canStart = dbStatus.connected && status.prepared && !status.isRunning && !status.isPreparing && selectedWaits.length > 0;
  const canStop = status.isRunning;
  const canDrop = dbStatus.connected && !status.isPreparing && !status.isRunning;

  if (!dbStatus.connected) {
    return (
      <div className="panel">
        <div className="panel-header">
          <h2>GC Congestion Demo</h2>
        </div>
        <div className="panel-content">
          <p style={{ color: 'var(--text-muted)' }}>Connect to database first.</p>
        </div>
      </div>
    );
  }

  return (
    <div style={{ display: 'flex', gap: '1rem', height: '100%', padding: '1rem' }}>
      <div style={{
        width: '360px',
        flexShrink: 0,
        background: 'var(--surface)',
        borderRadius: '8px',
        padding: '1rem',
        display: 'flex',
        flexDirection: 'column',
        gap: '0.8rem',
        overflowY: 'auto'
      }}>
        <h2 style={{ margin: 0, fontSize: '1.1rem', borderBottom: '1px solid var(--border)', paddingBottom: '0.5rem' }}>
          GC Congestion Demo
        </h2>

        <div style={{
          padding: '0.75rem',
          background: 'rgba(239, 68, 68, 0.1)',
          border: '1px solid rgba(239, 68, 68, 0.3)',
          borderRadius: '4px',
          fontSize: '0.75rem',
          color: 'var(--text-muted)'
        }}>
          <strong style={{ color: '#ef4444' }}>Goal:</strong> simulate RAC Global Cache congestion
          with many tables, large scaled data, index hot blocks, and filtered real-time GC waits.
        </div>

        <div className="form-group" style={{ marginBottom: 0 }}>
          <label>Schema Prefix</label>
          <input
            value={config.schemaPrefix}
            onChange={(e) => setConfig(prev => ({
              ...prev,
              schemaPrefix: e.target.value.toUpperCase().replace(/[^A-Z0-9_]/g, '').slice(0, 10)
            }))}
          />
        </div>

        <div className="form-row">
          <div className="form-group" style={{ marginBottom: 0 }}>
            <label>Tables</label>
            <input
              type="number"
              min="1"
              max="200"
              value={config.tableCount}
              onChange={(e) => updateNumber('tableCount', e.target.value, 1, 200)}
            />
          </div>
          <div className="form-group" style={{ marginBottom: 0 }}>
            <label>Scale (x)</label>
            <input
              type="number"
              min="1"
              max="1000"
              value={config.scaleFactor}
              onChange={(e) => updateNumber('scaleFactor', e.target.value, 1, 1000)}
            />
          </div>
        </div>

        <div className="form-row">
          <div className="form-group" style={{ marginBottom: 0 }}>
            <label>Base rows/table</label>
            <input
              type="number"
              min="100"
              max="2000000"
              value={config.baseRowsPerTable}
              onChange={(e) => updateNumber('baseRowsPerTable', e.target.value, 100, 2000000)}
            />
          </div>
          <div className="form-group" style={{ marginBottom: 0 }}>
            <label>Threads</label>
            <input
              type="number"
              min="1"
              max="500"
              value={config.threads}
              onChange={(e) => updateNumber('threads', e.target.value, 1, 500)}
            />
          </div>
        </div>

        <div className="form-row">
          <div className="form-group" style={{ marginBottom: 0 }}>
            <label>Index layout</label>
            <select
              value={config.indexPartitioning}
              onChange={(e) => setConfig(prev => ({ ...prev, indexPartitioning: e.target.value === 'hash' ? 'hash' : 'none' }))}
            >
              <option value="none">Non-partitioned</option>
              <option value="hash">Hash-partitioned</option>
            </select>
          </div>
          <div className="form-group" style={{ marginBottom: 0 }}>
            <label>Hash partitions</label>
            <input
              type="number"
              min="2"
              max="512"
              disabled={config.indexPartitioning !== 'hash'}
              value={config.indexHashPartitions}
              onChange={(e) => updateNumber('indexHashPartitions', e.target.value, 2, 512)}
            />
          </div>
        </div>

        <div className="form-row">
          <div className="form-group" style={{ marginBottom: 0 }}>
            <label>Hot rows</label>
            <input
              type="number"
              min="10"
              max="100000"
              value={config.hotRows}
              onChange={(e) => updateNumber('hotRows', e.target.value, 10, 100000)}
            />
          </div>
          <div className="form-group" style={{ marginBottom: 0 }}>
            <label>Hot table span</label>
            <input
              type="number"
              min="1"
              max={Math.max(1, config.tableCount)}
              value={config.hotTableSpan}
              onChange={(e) => updateNumber('hotTableSpan', e.target.value, 1, Math.max(1, config.tableCount))}
            />
          </div>
        </div>

        <div className="form-row">
          <div className="form-group" style={{ marginBottom: 0 }}>
            <label>Updates/txn</label>
            <input
              type="number"
              min="1"
              max="20"
              value={config.updatesPerTxn}
              onChange={(e) => updateNumber('updatesPerTxn', e.target.value, 1, 20)}
            />
          </div>
          <div className="form-group" style={{ marginBottom: 0 }}>
            <label>Read ratio (0-1)</label>
            <input
              type="number"
              step="0.05"
              min="0"
              max="0.95"
              value={config.readRatio}
              onChange={(e) => updateFloat('readRatio', e.target.value, 0, 0.95)}
            />
          </div>
        </div>

        <div className="form-group" style={{ marginBottom: 0 }}>
          <label>Think time (ms)</label>
          <input
            type="number"
            min="0"
            max="2000"
            value={config.thinkTime}
            onChange={(e) => updateNumber('thinkTime', e.target.value, 0, 2000)}
          />
        </div>

        <div style={{
          background: 'var(--bg-primary)',
          borderRadius: '6px',
          padding: '0.6rem',
          fontSize: '0.78rem',
          color: 'var(--text-secondary)'
        }}>
          Total rows to load: <strong>{formatNumber(totalRowsEstimate)}</strong>
          {status.preparedConfig && (
            <>
              <br />
              Prepared: {status.preparedConfig.tableCount} tables x {formatNumber(status.preparedConfig.rowsPerTable)} rows
              <br />
              Index layout: {status.preparedConfig.indexPartitioning === 'hash'
                ? `hash partitioned (${status.preparedConfig.indexHashPartitions} partitions)`
                : 'non-partitioned'}
            </>
          )}
        </div>

        <div style={{
          border: '1px solid var(--border)',
          borderRadius: '6px',
          padding: '0.6rem'
        }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '0.5rem' }}>
            <strong style={{ fontSize: '0.82rem' }}>GC wait filters</strong>
            <div style={{ display: 'flex', gap: '0.4rem' }}>
              <button
                className="btn btn-sm btn-secondary"
                onClick={() => setSelectedWaits(RECOMMENDED_WAITS.filter(eventName => availableWaits.includes(eventName)))}
                type="button"
              >
                Recommended
              </button>
              <button
                className="btn btn-sm btn-secondary"
                onClick={() => setSelectedWaits(availableWaits.slice(0, 12))}
                type="button"
              >
                Top
              </button>
            </div>
          </div>
          <div style={{ maxHeight: '190px', overflowY: 'auto', fontSize: '0.76rem' }}>
            {availableWaits.length === 0 && (
              <div style={{ color: 'var(--text-muted)' }}>No GC wait list loaded yet.</div>
            )}
            {availableWaits.map(eventName => (
              <label key={eventName} style={{ display: 'flex', gap: '0.45rem', alignItems: 'center', marginBottom: '0.25rem' }}>
                <input
                  type="checkbox"
                  checked={selectedWaits.includes(eventName)}
                  onChange={() => toggleWait(eventName)}
                />
                <span>{eventName}</span>
              </label>
            ))}
          </div>
        </div>

        <div style={{
          border: '1px solid var(--border)',
          borderRadius: '6px',
          padding: '0.6rem'
        }}>
          <strong style={{ fontSize: '0.82rem', display: 'block', marginBottom: '0.45rem' }}>Drop behavior</strong>
          <div className="form-row">
            <div className="form-group" style={{ marginBottom: 0 }}>
              <label>Wait logout (sec)</label>
              <input
                type="number"
                min="0"
                max="600"
                value={dropOptions.waitForLogoutSec}
                onChange={(e) => {
                  const parsed = Number(e.target.value);
                  if (!Number.isFinite(parsed)) return;
                  setDropOptions(prev => ({
                    ...prev,
                    waitForLogoutSec: Math.min(600, Math.max(0, Math.floor(parsed)))
                  }));
                }}
              />
            </div>
            <div className="form-group" style={{ marginBottom: 0, display: 'flex', alignItems: 'end' }}>
              <label style={{ display: 'flex', alignItems: 'center', gap: '0.45rem', marginBottom: '0.35rem' }}>
                <input
                  type="checkbox"
                  checked={dropOptions.forceLogout}
                  onChange={(e) => setDropOptions(prev => ({ ...prev, forceLogout: e.target.checked }))}
                />
                Force logout stuck sessions
              </label>
            </div>
          </div>
        </div>

        <div style={{ display: 'flex', gap: '0.5rem', flexWrap: 'wrap' }}>
          <button className="btn btn-primary" disabled={!canPrepare} onClick={handlePrepare}>
            Prepare Data
          </button>
          <button className="btn btn-success" disabled={!canStart} onClick={handleStart}>
            Start Workload
          </button>
          <button className="btn btn-danger" disabled={!canStop} onClick={handleStop}>
            Stop
          </button>
          <button className="btn btn-secondary" disabled={!canDrop} onClick={handleDrop}>
            Drop Tables
          </button>
        </div>

        {status.message && (
          <div style={{
            padding: '0.55rem',
            background: 'var(--bg-primary)',
            borderRadius: '6px',
            fontSize: '0.8rem',
            color: status.message.startsWith('Error') ? 'var(--accent-danger)' : 'var(--text-secondary)'
          }}>
            {status.message}
          </div>
        )}

        {status.isPreparing && status.progress !== null && (
          <div>
            <div className="progress-bar">
              <div className="fill" style={{ width: `${Math.max(0, Math.min(100, status.progress || 0))}%` }}></div>
            </div>
            <div className="progress-text">{status.progress}%</div>
          </div>
        )}
      </div>

      <div style={{ flex: 1, display: 'flex', flexDirection: 'column', gap: '1rem' }}>
        <div style={{ display: 'flex', gap: '1rem' }}>
          <div className="stat-card" style={{ flex: 1 }}>
            <div className="value highlight">{metrics.tps}</div>
            <div className="label">TPS</div>
          </div>
          <div className="stat-card" style={{ flex: 1 }}>
            <div className="value">{formatNumber(metrics.totalTransactions)}</div>
            <div className="label">Transactions</div>
          </div>
          <div className="stat-card" style={{ flex: 1 }}>
            <div className="value" style={{ color: metrics.errors > 0 ? 'var(--accent-danger)' : 'var(--text-primary)' }}>
              {metrics.errors}
            </div>
            <div className="label">Errors</div>
          </div>
          <div className="stat-card" style={{ flex: 1 }}>
            <div className="value">{formatUptime(metrics.uptime)}</div>
            <div className="label">Uptime</div>
          </div>
        </div>

        <div className="panel" style={{ flex: 1 }}>
          <div className="panel-header">
            <h2>Filtered GC Waits (Real Time)</h2>
            <span style={{ color: 'var(--text-muted)', fontSize: '0.8rem' }}>
              interval deltas for selected wait events
            </span>
          </div>
          <div className="panel-content">
            <div style={{ height: '320px' }}>
              <Line data={chartData} options={chartOptions} />
            </div>
          </div>
        </div>

        <div className="panel">
          <div className="panel-header">
            <h2>Latest GC Wait Snapshot</h2>
          </div>
          <div className="panel-content" style={{ padding: 0 }}>
            <table className="events-table">
              <thead>
                <tr>
                  <th>Event</th>
                  <th>Delta Waits</th>
                  <th>Delta Time (ms)</th>
                  <th>Avg Wait (ms)</th>
                  <th>Total Waits</th>
                </tr>
              </thead>
              <tbody>
                {latestWaits.length === 0 && (
                  <tr>
                    <td colSpan="5" style={{ color: 'var(--text-muted)', textAlign: 'center', padding: '1rem' }}>
                      Start workload to stream filtered GC waits.
                    </td>
                  </tr>
                )}
                {latestWaits.map(event => (
                  <tr key={event.event}>
                    <td>{event.event}</td>
                    <td>{formatNumber(event.deltaWaits)}</td>
                    <td>{Number(event.deltaTimeMs || 0).toFixed(2)}</td>
                    <td>{Number(event.avgWaitMs || 0).toFixed(3)}</td>
                    <td>{formatNumber(event.totalWaits)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </div>
  );
}

export default GCCongestionPanel;
