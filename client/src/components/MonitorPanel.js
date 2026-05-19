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
  const [tpsChartState, setTpsChartState] = useState({ labels: [], total: [], instances: {} });
  const [transactionSource, setTransactionSource] = useState('');
  const [currentTps, setCurrentTps] = useState(0);
  const [totalTransactions, setTotalTransactions] = useState(0);
  const [filters, setFilters] = useState({
    scope: 'all',
    waitClass: '',
    search: '',
    chartMetric: 'averageWaitMs'
  });
  const intervalRef = useRef(null);
  const previousSnapshotRef = useRef(new Map());
  const previousTransactionSnapshotRef = useRef(null);

  const formatNumber = (num) => {
    const value = Number(num) || 0;
    if (value >= 1000000) return `${(value / 1000000).toFixed(2)}M`;
    if (value >= 1000) return `${(value / 1000).toFixed(1)}K`;
    return value.toFixed(0);
  };

  const formatIntegerTps = (num) => String(Math.round(Number(num) || 0));

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
      const transactionResponse = await fetch(`${API_BASE}/monitor/transactions`);
      const data = await response.json();
      const transactionData = await transactionResponse.json();
      if (!response.ok || !data.success) {
        throw new Error(data.message || 'Failed to load waits');
      }
      if (!transactionResponse.ok || !transactionData.success) {
        throw new Error(transactionData.message || 'Failed to load transaction metrics');
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

      const transactionSnapshot = new Map();
      (transactionData.instances || []).forEach((row) => {
        transactionSnapshot.set(String(row.instId || 1), Number(row.totalTransactions || 0));
      });

      const previousTransactionSnapshot = previousTransactionSnapshotRef.current;
      const elapsedSeconds = previousTransactionSnapshot
        ? Math.max(0.001, (Number(transactionData.timestamp || Date.now()) - previousTransactionSnapshot.timestamp) / 1000)
        : 0;
      let nextTotalTps = 0;
      const nextInstanceTps = {};

      transactionSnapshot.forEach((total, instId) => {
        const previousTotal = previousTransactionSnapshot?.totals.get(instId) || 0;
        const tps = elapsedSeconds > 0 ? Math.max(0, total - previousTotal) / elapsedSeconds : 0;
        const roundedTps = Number(tps.toFixed(2));
        nextInstanceTps[instId] = roundedTps;
        nextTotalTps += roundedTps;
      });

      const roundedTotalTps = Number(nextTotalTps.toFixed(2));
      previousTransactionSnapshotRef.current = {
        timestamp: Number(transactionData.timestamp || Date.now()),
        totals: transactionSnapshot
      };

      setTpsChartState((prev) => {
        const nextLabels = [...prev.labels, label].slice(-40);
        const nextInstances = {};
        Object.entries(nextInstanceTps).forEach(([instId, tps]) => {
          const prior = prev.instances[instId] || [];
          nextInstances[instId] = [...prior, tps].slice(-40);
        });
        return {
          labels: nextLabels,
          total: [...prev.total, roundedTotalTps].slice(-40),
          instances: nextInstances
        };
      });
      setCurrentTps(roundedTotalTps);
      setTotalTransactions(Number(transactionData.totalTransactions || 0));
      setTransactionSource(transactionData.source || '');
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
    previousTransactionSnapshotRef.current = null;
    setChartState({ labels: [], series: {} });
    setTpsChartState({ labels: [], total: [], instances: {} });
    setCurrentTps(0);
    setTotalTransactions(0);
    setTransactionSource('');
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

  const instanceIds = Object.keys(tpsChartState.instances).sort((a, b) => Number(a) - Number(b));
  const tpsChartData = {
    labels: tpsChartState.labels,
    datasets: [
      {
        label: 'Total TPS',
        data: tpsChartState.total,
        borderColor: '#22c55e',
        backgroundColor: 'rgba(34, 197, 94, 0.16)',
        fill: true,
        tension: 0.3,
        pointRadius: 0,
        pointHoverRadius: 4
      },
      ...instanceIds.map((instId, index) => ({
        label: `INST ${instId}`,
        data: tpsChartState.instances[instId] || [],
        borderColor: SERIES_COLORS[index % SERIES_COLORS.length],
        backgroundColor: SERIES_COLORS[index % SERIES_COLORS.length],
        fill: false,
        tension: 0.3,
        pointRadius: 0,
        pointHoverRadius: 4,
        borderDash: [5, 5]
      }))
    ]
  };

  const tpsChartOptions = {
    responsive: true,
    maintainAspectRatio: false,
    animation: { duration: 0 },
    interaction: {
      intersect: false,
      mode: 'index'
    },
    plugins: {
      legend: {
        position: 'top',
        labels: { color: '#a0a0b0', usePointStyle: true, boxWidth: 10 }
      },
      tooltip: {
        backgroundColor: 'rgba(26, 26, 46, 0.95)',
        titleColor: '#ffffff',
        bodyColor: '#a0a0b0',
        borderColor: '#2a2a45',
        borderWidth: 1,
        callbacks: {
          label: (context) => `${context.dataset.label}: ${Number(context.raw || 0).toFixed(2)} TPS`
        }
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
          text: 'Transactions / Second',
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
              {transactionSource ? `, ${transactionSource}` : ''}
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
            TPS is calculated from deltas in Oracle user commits plus user rollbacks.
          </span>
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

        <div className="grid-2" style={{ marginBottom: '1rem' }}>
          <div className="panel">
            <div className="panel-header">
              <h2>Transactions per Second</h2>
              <div style={{ display: 'flex', gap: '0.75rem', flexWrap: 'wrap', color: 'var(--text-muted)', fontSize: '0.85rem' }}>
                <span>Current: <strong style={{ color: '#22c55e' }}>{formatIntegerTps(currentTps)} TPS</strong></span>
                <span>Total: {formatNumber(totalTransactions)}</span>
              </div>
            </div>
            <div className="panel-content">
              <div style={{ height: '280px' }}>
                <Line data={tpsChartData} options={tpsChartOptions} />
              </div>
            </div>
          </div>

          <div className="panel">
            <div className="panel-header">
              <h2>Wait Trend</h2>
            </div>
            <div className="panel-content">
              <div style={{ height: '280px' }}>
                <Line data={chartData} options={chartOptions} />
              </div>
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
