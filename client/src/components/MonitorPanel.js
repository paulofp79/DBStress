import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
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
const SERIES_COLORS = ['#38bdf8', '#f59e0b', '#ef4444', '#22c55e', '#a855f7', '#f97316'];

function MonitorPanel({ dbStatus }) {
  const [isRunning, setIsRunning] = useState(false);
  const [isLoading, setIsLoading] = useState(false);
  const [waits, setWaits] = useState([]);
  const [source, setSource] = useState('');
  const [lastUpdated, setLastUpdated] = useState(null);
  const [error, setError] = useState('');
  const [chartState, setChartState] = useState({ labels: [], series: {} });
  const [filters, setFilters] = useState({
    scope: 'all',
    waitClass: '',
    search: '',
    chartMetric: 'averageWaitMs'
  });
  const intervalRef = useRef(null);
  const previousSnapshotRef = useRef(new Map());

  const formatNumber = (num) => {
    const value = Number(num) || 0;
    if (value >= 1000000) return `${(value / 1000000).toFixed(2)}M`;
    if (value >= 1000) return `${(value / 1000).toFixed(1)}K`;
    return value.toFixed(0);
  };

  const waitClasses = useMemo(() => {
    const classes = Array.from(new Set(waits.map((item) => item.waitClass).filter(Boolean)));
    classes.sort();
    return classes;
  }, [waits]);

  const aggregateRows = (rows) => {
    const grouped = new Map();
    rows.forEach((row) => {
      const existing = grouped.get(row.event) || {
        event: row.event,
        waitClass: row.waitClass,
        totalWaits: 0,
        timeWaitedSeconds: 0,
        averageWaitMs: 0,
        instances: {}
      };

      existing.totalWaits += Number(row.totalWaits || 0);
      existing.timeWaitedSeconds += Number(row.timeWaitedSeconds || 0);
      existing.instances[row.instId] = {
        totalWaits: Number(row.totalWaits || 0),
        timeWaitedSeconds: Number(row.timeWaitedSeconds || 0),
        averageWaitMs: Number(row.averageWaitMs || 0)
      };
      grouped.set(row.event, existing);
    });

    return Array.from(grouped.values())
      .map((item) => ({
        ...item,
        averageWaitMs: item.totalWaits > 0 ? (item.timeWaitedSeconds * 1000) / item.totalWaits : 0
      }))
      .sort((a, b) => b.timeWaitedSeconds - a.timeWaitedSeconds);
  };

  const loadWaits = useCallback(async () => {
    setIsLoading(true);
    try {
      const params = new URLSearchParams({
        limit: '30',
        scope: filters.scope
      });
      if (filters.waitClass) params.set('waitClass', filters.waitClass);
      if (filters.search) params.set('search', filters.search);

      const response = await fetch(`${API_BASE}/monitor/waits?${params.toString()}`);
      const data = await response.json();
      if (!response.ok || !data.success) {
        throw new Error(data.message || 'Failed to load waits');
      }

      const rows = data.waits || [];
      const currentSnapshot = new Map();
      rows.forEach((row) => {
        currentSnapshot.set(`${row.instId}:${row.event}`, row);
      });

      const previousSnapshot = previousSnapshotRef.current;
      const aggregated = aggregateRows(rows).map((item) => {
        let deltaWaits = 0;
        let deltaTimeSeconds = 0;

        Object.entries(item.instances).forEach(([instId, metrics]) => {
          const key = `${instId}:${item.event}`;
          const previous = previousSnapshot.get(key);
          deltaWaits += Math.max(0, Number(metrics.totalWaits || 0) - Number(previous?.totalWaits || 0));
          deltaTimeSeconds += Math.max(0, Number(metrics.timeWaitedSeconds || 0) - Number(previous?.timeWaitedSeconds || 0));
        });

        return {
          ...item,
          deltaWaits,
          deltaTimeSeconds,
          deltaAverageWaitMs: deltaWaits > 0 ? (deltaTimeSeconds * 1000) / deltaWaits : 0
        };
      });

      previousSnapshotRef.current = currentSnapshot;

      const chartRows = aggregated.slice(0, 6);
      const label = new Date(data.timestamp || Date.now()).toLocaleTimeString([], { minute: '2-digit', second: '2-digit' });
      setChartState((prev) => {
        const nextLabels = [...prev.labels, label].slice(-40);
        const nextSeries = {};
        chartRows.forEach((row) => {
          const prior = prev.series[row.event] || [];
          const metricValue = filters.chartMetric === 'deltaTimeSeconds'
            ? Number(row.deltaTimeSeconds || 0)
            : Number(row.averageWaitMs || 0);
          nextSeries[row.event] = [...prior, metricValue].slice(-40);
        });
        return { labels: nextLabels, series: nextSeries };
      });

      setWaits(aggregated);
      setSource(data.source || '');
      setLastUpdated(data.timestamp || Date.now());
      setError('');
    } catch (err) {
      setError(err.message || 'Failed to load waits');
    } finally {
      setIsLoading(false);
    }
  }, [filters.chartMetric, filters.scope, filters.search, filters.waitClass]);

  const resetMonitoringState = useCallback(() => {
    previousSnapshotRef.current = new Map();
    setChartState({ labels: [], series: {} });
    setWaits([]);
  }, []);

  const handleStart = async () => {
    resetMonitoringState();
    await loadWaits();
    setIsRunning(true);
  };

  const handleStop = () => {
    setIsRunning(false);
  };

  useEffect(() => {
    if (!isRunning) {
      if (intervalRef.current) {
        clearInterval(intervalRef.current);
        intervalRef.current = null;
      }
      return undefined;
    }

    intervalRef.current = setInterval(() => {
      loadWaits();
    }, 2000);

    return () => {
      if (intervalRef.current) {
        clearInterval(intervalRef.current);
        intervalRef.current = null;
      }
    };
  }, [isRunning, loadWaits]);

  useEffect(() => {
    if (!dbStatus.connected) {
      setIsRunning(false);
      resetMonitoringState();
      setError('');
    }
  }, [dbStatus.connected, resetMonitoringState]);

  useEffect(() => {
    if (isRunning) {
      resetMonitoringState();
      loadWaits();
    }
  }, [filters.scope, filters.waitClass, filters.search, filters.chartMetric, isRunning, loadWaits, resetMonitoringState]);

  const chartEvents = waits.slice(0, 6).map((item) => item.event);
  const chartData = {
    labels: chartState.labels,
    datasets: chartEvents.map((eventName, index) => ({
      label: eventName,
      data: chartState.series[eventName] || [],
      borderColor: SERIES_COLORS[index % SERIES_COLORS.length],
      backgroundColor: SERIES_COLORS[index % SERIES_COLORS.length],
      tension: 0.3,
      pointRadius: 0,
      pointHoverRadius: 4,
      spanGaps: true
    }))
  };

  const chartOptions = {
    responsive: true,
    maintainAspectRatio: false,
    animation: { duration: 0 },
    plugins: {
      legend: {
        position: 'top',
        labels: { color: '#a0a0b0', usePointStyle: true, boxWidth: 10 }
      }
    },
    scales: {
      x: {
        grid: { color: 'rgba(42, 42, 69, 0.5)' },
        ticks: { color: '#6b6b80', maxTicksLimit: 8 }
      },
      y: {
        beginAtZero: true,
        grid: { color: 'rgba(42, 42, 69, 0.5)' },
        ticks: { color: '#6b6b80' },
        title: {
          display: true,
          text: filters.chartMetric === 'deltaTimeSeconds' ? 'Delta Time Waited (s)' : 'Avg Wait (ms)',
          color: '#6b6b80'
        }
      }
    }
  };

  if (!dbStatus.connected) {
    return (
      <div className="panel" style={{ minHeight: '100%' }}>
        <div className="panel-header">
          <h2>Monitor</h2>
        </div>
        <div className="panel-content">
          <p style={{ color: 'var(--text-muted)' }}>
            Use the connection panel on this tab to connect to the database, then start monitoring waits.
          </p>
        </div>
      </div>
    );
  }

  return (
    <div className="panel" style={{ minHeight: '100%' }}>
      <div className="panel-header">
        <h2>Monitor</h2>
        <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem', flexWrap: 'wrap' }}>
          {source && (
            <span style={{ fontSize: '0.8rem', color: 'var(--text-muted)' }}>
              Source: {source}
            </span>
          )}
          <button className="btn btn-success btn-sm" disabled={isRunning || isLoading} onClick={handleStart}>
            Start
          </button>
          <button className="btn btn-secondary btn-sm" disabled={!isRunning} onClick={handleStop}>
            Stop
          </button>
        </div>
      </div>

      <div className="panel-content">
        <div className="form-row" style={{ marginBottom: '1rem' }}>
          <div className="form-group" style={{ marginBottom: 0 }}>
            <label>Scope</label>
            <select value={filters.scope} onChange={(e) => setFilters((prev) => ({ ...prev, scope: e.target.value }))}>
              <option value="all">All waits</option>
              <option value="gc">GC waits only</option>
            </select>
          </div>
          <div className="form-group" style={{ marginBottom: 0 }}>
            <label>Wait class</label>
            <select value={filters.waitClass} onChange={(e) => setFilters((prev) => ({ ...prev, waitClass: e.target.value }))}>
              <option value="">All classes</option>
              {waitClasses.map((item) => (
                <option key={item} value={item}>{item}</option>
              ))}
            </select>
          </div>
          <div className="form-group" style={{ marginBottom: 0 }}>
            <label>Search event</label>
            <input
              value={filters.search}
              onChange={(e) => setFilters((prev) => ({ ...prev, search: e.target.value }))}
              placeholder="gc current, log file, enq..."
            />
          </div>
          <div className="form-group" style={{ marginBottom: 0 }}>
            <label>Chart metric</label>
            <select value={filters.chartMetric} onChange={(e) => setFilters((prev) => ({ ...prev, chartMetric: e.target.value }))}>
              <option value="averageWaitMs">Avg wait (ms)</option>
              <option value="deltaTimeSeconds">Delta time waited</option>
            </select>
          </div>
        </div>

        <div style={{
          marginBottom: '1rem',
          padding: '0.85rem 1rem',
          borderRadius: '10px',
          background: 'var(--bg-tertiary)',
          border: '1px solid var(--border-color)',
          color: 'var(--text-secondary)'
        }}>
          Real-time top waits from Oracle. Includes per-instance breakdown from `gv$` when available and delta charting between polls.
          <span style={{ display: 'block', marginTop: '0.4rem' }}>
            Observation: Avg wait in this tab is cumulative since instance startup, so it is better for understanding overall database behavior than the immediate effect of one workload.
          </span>
          {lastUpdated && (
            <span style={{ marginLeft: '0.75rem', color: 'var(--text-muted)' }}>
              Last updated: {new Date(lastUpdated).toLocaleTimeString()}
            </span>
          )}
        </div>

        {error && (
          <div className="alert alert-danger">{error}</div>
        )}

        <div className="panel" style={{ marginBottom: '1rem' }}>
          <div className="panel-header">
            <h2>Wait Trend</h2>
          </div>
          <div className="panel-content">
            <div style={{ height: '280px' }}>
              <Line data={chartData} options={chartOptions} />
            </div>
          </div>
        </div>

        <div style={{ padding: 0 }}>
          <table className="events-table">
            <thead>
              <tr>
                <th>#</th>
                <th>Event Name</th>
                <th>Wait Class</th>
                <th>Total Waits</th>
                <th>Delta Waits</th>
                <th>Time Waited (s)</th>
                <th>Delta Time (s)</th>
                <th>Avg Wait (ms)</th>
                <th>Instances</th>
              </tr>
            </thead>
            <tbody>
              {waits.length === 0 && (
                <tr>
                  <td colSpan="9" style={{ textAlign: 'center', padding: '1.5rem', color: 'var(--text-muted)' }}>
                    {isRunning || isLoading ? 'Loading wait events...' : 'Click Start to monitor top wait events from gv$.'}
                  </td>
                </tr>
              )}
              {waits.map((event, index) => (
                <tr key={`${event.event}-${index}`}>
                  <td>{index + 1}</td>
                  <td className="event-name" title={event.event}>{event.event}</td>
                  <td>{event.waitClass}</td>
                  <td>{formatNumber(event.totalWaits)}</td>
                  <td>{formatNumber(event.deltaWaits)}</td>
                  <td>{Number(event.timeWaitedSeconds || 0).toFixed(2)}</td>
                  <td>{Number(event.deltaTimeSeconds || 0).toFixed(2)}</td>
                  <td>{Number(event.averageWaitMs || 0).toFixed(2)}</td>
                  <td>
                    {Object.entries(event.instances || {}).map(([instId, metrics]) => (
                      <div key={instId} style={{ fontSize: '0.75rem', whiteSpace: 'nowrap' }}>
                        INST {instId}: {formatNumber(metrics.totalWaits)} waits / {Number(metrics.timeWaitedSeconds || 0).toFixed(2)}s
                      </div>
                    ))}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}

export default MonitorPanel;
