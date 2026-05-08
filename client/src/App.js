import React, { useState, useEffect, useCallback } from 'react';
import { io } from 'socket.io-client';
import axios from 'axios';
import ConnectionPanel from './components/ConnectionPanel';
import SchemaPanel from './components/SchemaPanel';
import StressConfigPanel from './components/StressConfigPanel';
import MetricsPanel from './components/MetricsPanel';
import WaitEventsPanel from './components/WaitEventsPanel';
import TPSChart from './components/TPSChart';
import OperationsChart from './components/OperationsChart';
import GCWaitChart from './components/GCWaitChart';
import IndexContentionPanel from './components/IndexContentionPanel';
import LibraryCacheLockPanel from './components/LibraryCacheLockPanel';
import StatsComparisonPanel from './components/StatsComparisonPanel';
import MetricExplorerPanel from './components/MetricExplorerPanel';
import HWContentionPanel from './components/HWContentionPanel';
import SkewDetectionPanel from './components/SkewDetectionPanel';
import TDEComparisonPanel from './components/TDEComparisonPanel';
import GCCongestionPanel from './components/GCCongestionPanel';
import GCBenchmarkPanel from './components/GCBenchmarkPanel';
import HomePanel from './components/HomePanel';
import MonitorPanel from './components/MonitorPanel';
import SwingbenchPanel from './components/SwingbenchPanel';
import SwingbenchWorkloadPanel from './components/SwingbenchWorkloadPanel';
import InsertBlastPanel from './components/InsertBlastPanel';
import DatafileGrowthPanel from './components/DatafileGrowthPanel';

// Auto-detect server URL based on where the page is loaded from
const getServerUrl = () => {
  // In production, use the same host as the page
  if (window.location.hostname !== 'localhost') {
    return `http://${window.location.host}`;
  }
  // In development, use localhost:3001
  return 'http://localhost:3001';
};

const SERVER_URL = getServerUrl();
const API_BASE = `${SERVER_URL}/api`;

// Configure axios defaults
axios.defaults.timeout = 30000; // 30 second timeout

function App() {
  const [socket, setSocket] = useState(null);
  const [connected, setConnected] = useState(false);
  const [dbStatus, setDbStatus] = useState({ connected: false });
  const [stressStatus, setStressStatus] = useState({ isRunning: false });
  const [schemas, setSchemas] = useState([]);
  const [activeTab, setActiveTab] = useState('home');
  const [metrics, setMetrics] = useState({
    tps: [],
    operations: [],
    waitEvents: [],
    gcWaitEvents: [],
    systemStats: {},
    // Multi-schema metrics
    tpsBySchema: {},
    operationsBySchema: {}
  });
  const [error, setError] = useState(null);
  const [success, setSuccess] = useState(null);

  // Initialize socket connection
  useEffect(() => {
    const newSocket = io(SERVER_URL);

    newSocket.on('connect', () => {
      console.log('Socket connected');
      setConnected(true);
    });

    newSocket.on('disconnect', () => {
      console.log('Socket disconnected');
      setConnected(false);
    });

    newSocket.on('stress-status', (status) => {
      setStressStatus(status);
    });

    // Handle multi-schema metrics
    newSocket.on('stress-metrics-by-schema', (schemaMetrics) => {
      setMetrics(prev => {
        const newTpsBySchema = { ...prev.tpsBySchema };
        const newOpsBySchema = { ...prev.operationsBySchema };

        for (const schemaId of Object.keys(schemaMetrics)) {
          const data = schemaMetrics[schemaId];

          // TPS data
          if (!newTpsBySchema[schemaId]) {
            newTpsBySchema[schemaId] = [];
          }
          newTpsBySchema[schemaId] = [
            ...newTpsBySchema[schemaId].slice(-59),
            { time: new Date(), value: data.tps }
          ];

          // Operations data
          if (!newOpsBySchema[schemaId]) {
            newOpsBySchema[schemaId] = [];
          }
          newOpsBySchema[schemaId] = [
            ...newOpsBySchema[schemaId].slice(-59),
            {
              time: new Date(),
              inserts: data.perSecond.inserts,
              updates: data.perSecond.updates,
              deletes: data.perSecond.deletes
            }
          ];
        }

        // Get first schema for backward-compatible single metrics
        const firstSchemaId = Object.keys(schemaMetrics)[0];
        const firstData = firstSchemaId ? schemaMetrics[firstSchemaId] : null;

        return {
          ...prev,
          tpsBySchema: newTpsBySchema,
          operationsBySchema: newOpsBySchema,
          // Always update perSecond and total for single-schema MetricsPanel view
          perSecond: firstData ? firstData.perSecond : prev.perSecond,
          total: firstData ? firstData.total : prev.total,
          // Also update single-schema arrays
          tps: firstData
            ? [...prev.tps.slice(-59), { time: new Date(), value: firstData.tps }]
            : prev.tps,
          operations: firstData
            ? [...prev.operations.slice(-59), {
                time: new Date(),
                inserts: firstData.perSecond.inserts,
                updates: firstData.perSecond.updates,
                deletes: firstData.perSecond.deletes
              }]
            : prev.operations
        };
      });
    });

    // Keep backward compatible single metric handler
    newSocket.on('stress-metrics', (data) => {
      if (!data.schemas) {
        // Single schema mode
        setMetrics(prev => ({
          ...prev,
          tps: [...prev.tps.slice(-59), { time: new Date(), value: data.tps }],
          operations: [...prev.operations.slice(-59), {
            time: new Date(),
            inserts: data.perSecond.inserts,
            updates: data.perSecond.updates,
            deletes: data.perSecond.deletes
          }],
          total: data.total,
          perSecond: data.perSecond
        }));
      }
    });

    newSocket.on('db-metrics', (data) => {
      setMetrics(prev => ({
        ...prev,
        waitEvents: data.waitEvents,
        gcWaitEvents: data.gcWaitEvents || [],
        systemStats: data.systemStats,
        sessionStats: data.sessionStats
      }));
    });

    newSocket.on('stress-stopped', (finalStats) => {
      setStressStatus({ isRunning: false });
      const total = finalStats.transactions || 0;
      showSuccess(`Stress test completed. Total transactions: ${total}`);
    });

    setSocket(newSocket);

    return () => {
      newSocket.close();
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // Fetch initial status
  useEffect(() => {
    fetchDbStatus();
    fetchStressStatus();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const fetchDbStatus = async () => {
    try {
      const response = await axios.get(`${API_BASE}/db/status`);
      setDbStatus(response.data);
      if (response.data.connected) {
        fetchSchemas();
      }
    } catch (err) {
      console.error('Error fetching DB status:', err);
    }
  };

  const fetchStressStatus = async () => {
    try {
      const response = await axios.get(`${API_BASE}/stress/status`);
      setStressStatus(response.data);
    } catch (err) {
      console.error('Error fetching stress status:', err);
    }
  };

  const fetchSchemas = useCallback(async () => {
    try {
      const response = await axios.get(`${API_BASE}/schemas/list`);
      if (response.data.success) {
        setSchemas(response.data.schemas || []);
      }
    } catch (err) {
      console.error('Error fetching schemas:', err);
    }
  }, []);

  const showError = useCallback((message) => {
    setError(message);
    setTimeout(() => setError(null), 5000);
  }, []);

  const showSuccess = useCallback((message) => {
    setSuccess(message);
    setTimeout(() => setSuccess(null), 5000);
  }, []);

  const handleConnect = async (credentials) => {
    try {
      setError(null);
      const response = await axios.post(`${API_BASE}/db/connect`, credentials);
      if (response.data.success) {
        setDbStatus({ connected: true, config: { user: credentials.user, connectionString: credentials.connectionString } });
        showSuccess('Connected to Oracle database');
        fetchSchemas();
        return true;
      }
      return false;
    } catch (err) {
      showError(err.response?.data?.message || 'Failed to connect to database');
      return false;
    }
  };

  const handleDisconnect = async () => {
    try {
      await axios.post(`${API_BASE}/db/disconnect`);
      setDbStatus({ connected: false });
      setSchemas([]);
      showSuccess('Disconnected from database');
    } catch (err) {
      showError(err.response?.data?.message || 'Failed to disconnect');
    }
  };

  const handleCreateSchema = async (options) => {
    try {
      setError(null);
      // Calculate timeout based on schema size: base 2 min + 30s per RAC table pair + 1 min per scale factor
      const racTableCount = options.racTableCount || 1;
      const scaleFactor = options.scaleFactor || 1;
      const timeoutMs = (120 + (racTableCount * 30) + (scaleFactor * 60)) * 1000; // in ms
      const maxTimeout = 600000; // 10 minutes max

      await axios.post(`${API_BASE}/schema/create`, options, {
        timeout: Math.min(timeoutMs, maxTimeout)
      });
      showSuccess(`Schema${options.prefix ? ` '${options.prefix}'` : ''} created successfully`);
      fetchSchemas();
    } catch (err) {
      showError(err.response?.data?.message || 'Failed to create schema');
    }
  };

  const handleDropSchema = async (prefix) => {
    try {
      // Longer timeout for drop (could have many RAC tables)
      await axios.post(`${API_BASE}/schema/drop`, { prefix }, {
        timeout: 300000  // 5 minutes
      });
      showSuccess(`Schema${prefix ? ` '${prefix}'` : ''} dropped successfully`);
      fetchSchemas();
    } catch (err) {
      showError(err.response?.data?.message || 'Failed to drop schema');
    }
  };

  const handleStartStress = async (config) => {
    try {
      setError(null);
      // Clear previous metrics
      setMetrics(prev => ({
        ...prev,
        tps: [],
        operations: [],
        tpsBySchema: {},
        operationsBySchema: {}
      }));
      const response = await axios.post(`${API_BASE}/stress/start`, config);
      if (response.data.success) {
        setStressStatus({ isRunning: true, config });
        showSuccess('Stress test started');
      }
    } catch (err) {
      showError(err.response?.data?.message || 'Failed to start stress test');
    }
  };

  const handleStopStress = async () => {
    try {
      await axios.post(`${API_BASE}/stress/stop`);
      setStressStatus({ isRunning: false });
      showSuccess('Stress test stopped');
    } catch (err) {
      showError(err.response?.data?.message || 'Failed to stop stress test');
    }
  };

  const handleUpdateConfig = async (newConfig) => {
    try {
      await axios.put(`${API_BASE}/stress/config`, newConfig);
    } catch (err) {
      showError(err.response?.data?.message || 'Failed to update configuration');
    }
  };

  // Determine if multi-schema mode
  const schemaIds = Object.keys(metrics.tpsBySchema);
  const isMultiSchema = schemaIds.length > 1;

  return (
    <div className="app">
      <header className="header">
        <h1>DBStress - Oracle Database Stress Testing</h1>
        <div className="status">
          <span className={`status-dot ${connected ? 'connected' : ''}`}></span>
          <span>{connected ? 'Connected to server' : 'Disconnected'}</span>
          {dbStatus.connected && (
            <>
              <span style={{ margin: '0 0.5rem' }}>|</span>
              <span>DB: {dbStatus.config?.user}@{dbStatus.config?.connectionString}</span>
            </>
          )}
        </div>
      </header>

      <main className="main">
        {error && <div className="alert alert-danger">{error}</div>}
        {success && <div className="alert alert-success">{success}</div>}

        {/* Tab Navigation */}
        <div style={{
          display: 'flex',
          gap: '0',
          marginBottom: '1rem',
          borderBottom: '2px solid var(--border)',
          overflowX: 'auto'
        }}>
          <button
            onClick={() => setActiveTab('home')}
            style={{
              padding: '0.75rem 1.5rem',
              background: activeTab === 'home' ? 'var(--surface)' : 'transparent',
              border: 'none',
              borderBottom: activeTab === 'home' ? '2px solid #22c55e' : '2px solid transparent',
              marginBottom: '-2px',
              color: activeTab === 'home' ? '#22c55e' : 'var(--text-muted)',
              fontWeight: activeTab === 'home' ? '600' : '400',
              cursor: 'pointer',
              fontSize: '0.95rem',
              whiteSpace: 'nowrap'
            }}
          >
            Home
          </button>
          <button
            onClick={() => setActiveTab('monitor')}
            style={{
              padding: '0.75rem 1.5rem',
              background: activeTab === 'monitor' ? 'var(--surface)' : 'transparent',
              border: 'none',
              borderBottom: activeTab === 'monitor' ? '2px solid #38bdf8' : '2px solid transparent',
              marginBottom: '-2px',
              color: activeTab === 'monitor' ? '#38bdf8' : 'var(--text-muted)',
              fontWeight: activeTab === 'monitor' ? '600' : '400',
              cursor: 'pointer',
              fontSize: '0.95rem',
              whiteSpace: 'nowrap'
            }}
          >
            Monitor
          </button>
          <button
            onClick={() => setActiveTab('stress')}
            style={{
              padding: '0.75rem 1.5rem',
              background: activeTab === 'stress' ? 'var(--surface)' : 'transparent',
              border: 'none',
              borderBottom: activeTab === 'stress' ? '2px solid var(--accent-primary)' : '2px solid transparent',
              marginBottom: '-2px',
              color: activeTab === 'stress' ? 'var(--accent-primary)' : 'var(--text-muted)',
              fontWeight: activeTab === 'stress' ? '600' : '400',
              cursor: 'pointer',
              fontSize: '0.95rem'
            }}
          >
            Stress Test
          </button>
          <button
            onClick={() => setActiveTab('index-contention')}
            style={{
              padding: '0.75rem 1.5rem',
              background: activeTab === 'index-contention' ? 'var(--surface)' : 'transparent',
              border: 'none',
              borderBottom: activeTab === 'index-contention' ? '2px solid var(--accent-warning)' : '2px solid transparent',
              marginBottom: '-2px',
              color: activeTab === 'index-contention' ? 'var(--accent-warning)' : 'var(--text-muted)',
              fontWeight: activeTab === 'index-contention' ? '600' : '400',
              cursor: 'pointer',
              fontSize: '0.95rem'
            }}
          >
            Index Contention Demo
          </button>
          <button
            onClick={() => setActiveTab('library-cache-lock')}
            style={{
              padding: '0.75rem 1.5rem',
              background: activeTab === 'library-cache-lock' ? 'var(--surface)' : 'transparent',
              border: 'none',
              borderBottom: activeTab === 'library-cache-lock' ? '2px solid #ef4444' : '2px solid transparent',
              marginBottom: '-2px',
              color: activeTab === 'library-cache-lock' ? '#ef4444' : 'var(--text-muted)',
              fontWeight: activeTab === 'library-cache-lock' ? '600' : '400',
              cursor: 'pointer',
              fontSize: '0.95rem'
            }}
          >
            Library Cache Lock
          </button>
          <button
            onClick={() => setActiveTab('hw-contention')}
            style={{
              padding: '0.75rem 1.5rem',
              background: activeTab === 'hw-contention' ? 'var(--surface)' : 'transparent',
              border: 'none',
              borderBottom: activeTab === 'hw-contention' ? '2px solid #f59e0b' : '2px solid transparent',
              marginBottom: '-2px',
              color: activeTab === 'hw-contention' ? '#f59e0b' : 'var(--text-muted)',
              fontWeight: activeTab === 'hw-contention' ? '600' : '400',
              cursor: 'pointer',
              fontSize: '0.95rem'
            }}
          >
            HW Contention Demo
          </button>
          <button
            onClick={() => setActiveTab('stats-comparison')}
            style={{
              padding: '0.75rem 1.5rem',
              background: activeTab === 'stats-comparison' ? 'var(--surface)' : 'transparent',
              border: 'none',
              borderBottom: activeTab === 'stats-comparison' ? '2px solid #3b82f6' : '2px solid transparent',
              marginBottom: '-2px',
              color: activeTab === 'stats-comparison' ? '#3b82f6' : 'var(--text-muted)',
              fontWeight: activeTab === 'stats-comparison' ? '600' : '400',
              cursor: 'pointer',
              fontSize: '0.95rem'
            }}
          >
            Stats Comparison
          </button>
          <button
            onClick={() => setActiveTab('skew-detection')}
            style={{
              padding: '0.75rem 1.5rem',
              background: activeTab === 'skew-detection' ? 'var(--surface)' : 'transparent',
              border: 'none',
              borderBottom: activeTab === 'skew-detection' ? '2px solid #8b5cf6' : '2px solid transparent',
              marginBottom: '-2px',
              color: activeTab === 'skew-detection' ? '#8b5cf6' : 'var(--text-muted)',
              fontWeight: activeTab === 'skew-detection' ? '600' : '400',
              cursor: 'pointer',
              fontSize: '0.95rem'
            }}
          >
            Skew Detection
          </button>
          <button
            onClick={() => setActiveTab('metric-explorer')}
            style={{
              padding: '0.75rem 1.5rem',
              background: activeTab === 'metric-explorer' ? 'var(--surface)' : 'transparent',
              border: 'none',
              borderBottom: activeTab === 'metric-explorer' ? '2px solid var(--accent-secondary)' : '2px solid transparent',
              marginBottom: '-2px',
              color: activeTab === 'metric-explorer' ? 'var(--accent-secondary)' : 'var(--text-muted)',
              fontWeight: activeTab === 'metric-explorer' ? '600' : '400',
              cursor: 'pointer',
              fontSize: '0.95rem'
            }}
          >
            Metric Explorer
          </button>

          <button
            onClick={() => setActiveTab('swingbench-soe')}
            style={{
              padding: '0.75rem 1.5rem',
              background: activeTab === 'swingbench-soe' ? 'var(--surface)' : 'transparent',
              border: 'none',
              borderBottom: activeTab === 'swingbench-soe' ? '2px solid #f97316' : '2px solid transparent',
              marginBottom: '-2px',
              color: activeTab === 'swingbench-soe' ? '#f97316' : 'var(--text-muted)',
              fontWeight: activeTab === 'swingbench-soe' ? '600' : '400',
              cursor: 'pointer',
              fontSize: '0.95rem',
              whiteSpace: 'nowrap'
            }}
          >
            Swingbench SOE
          </button>

          <button
            onClick={() => setActiveTab('insert-blast')}
            style={{
              padding: '0.75rem 1.5rem',
              background: activeTab === 'insert-blast' ? 'var(--surface)' : 'transparent',
              border: 'none',
              borderBottom: activeTab === 'insert-blast' ? '2px solid #14b8a6' : '2px solid transparent',
              marginBottom: '-2px',
              color: activeTab === 'insert-blast' ? '#14b8a6' : 'var(--text-muted)',
              fontWeight: activeTab === 'insert-blast' ? '600' : '400',
              cursor: 'pointer',
              fontSize: '0.95rem',
              whiteSpace: 'nowrap'
            }}
          >
            Insert Blast
          </button>

          <button
            onClick={() => setActiveTab('datafile-growth')}
            style={{
              padding: '0.75rem 1.5rem',
              background: activeTab === 'datafile-growth' ? 'var(--surface)' : 'transparent',
              border: 'none',
              borderBottom: activeTab === 'datafile-growth' ? '2px solid #facc15' : '2px solid transparent',
              marginBottom: '-2px',
              color: activeTab === 'datafile-growth' ? '#facc15' : 'var(--text-muted)',
              fontWeight: activeTab === 'datafile-growth' ? '600' : '400',
              cursor: 'pointer',
              fontSize: '0.95rem',
              whiteSpace: 'nowrap'
            }}
          >
            Datafile Growth
          </button>

          <button
            onClick={() => setActiveTab('tde-comparison')}
            style={{
              padding: '0.75rem 1.5rem',
              background: activeTab === 'tde-comparison' ? 'var(--surface)' : 'transparent',
              border: 'none',
              borderBottom: activeTab === 'tde-comparison' ? '2px solid #ef4444' : '2px solid transparent',
              marginBottom: '-2px',
              color: activeTab === 'tde-comparison' ? '#ef4444' : 'var(--text-muted)',
              fontWeight: activeTab === 'tde-comparison' ? '600' : '400',
              cursor: 'pointer',
              fontSize: '0.95rem'
            }}
          >
            TDE Comparison
          </button>

          <button
            onClick={() => setActiveTab('gc-congestion')}
            style={{
              padding: '0.75rem 1.5rem',
              background: activeTab === 'gc-congestion' ? 'var(--surface)' : 'transparent',
              border: 'none',
              borderBottom: activeTab === 'gc-congestion' ? '2px solid #ef4444' : '2px solid transparent',
              marginBottom: '-2px',
              color: activeTab === 'gc-congestion' ? '#ef4444' : 'var(--text-muted)',
              fontWeight: activeTab === 'gc-congestion' ? '600' : '400',
              cursor: 'pointer',
              fontSize: '0.95rem'
            }}
          >
            GC Congestion Demo
          </button>
          <button
            onClick={() => setActiveTab('gc-benchmark')}
            style={{
              padding: '0.75rem 1.5rem',
              background: activeTab === 'gc-benchmark' ? 'var(--surface)' : 'transparent',
              border: 'none',
              borderBottom: activeTab === 'gc-benchmark' ? '2px solid #22c55e' : '2px solid transparent',
              marginBottom: '-2px',
              color: activeTab === 'gc-benchmark' ? '#22c55e' : 'var(--text-muted)',
              fontWeight: activeTab === 'gc-benchmark' ? '600' : '400',
              cursor: 'pointer',
              fontSize: '0.95rem',
              whiteSpace: 'nowrap'
            }}
          >
            GC Benchmark
          </button>
        </div>

        {activeTab === 'home' && (
          <HomePanel onOpenTab={setActiveTab} />
        )}

        <div style={{ display: activeTab === 'monitor' ? 'block' : 'none' }}>
          <div className="monitor-layout">
            <ConnectionPanel
              dbStatus={dbStatus}
              onConnect={handleConnect}
              onDisconnect={handleDisconnect}
            />
            <MonitorPanel dbStatus={dbStatus} />
          </div>
        </div>

        {/* Stress Test Tab */}
        {activeTab === 'stress' && (
          <>
            <div className="grid-3">
              <ConnectionPanel
                dbStatus={dbStatus}
                onConnect={handleConnect}
                onDisconnect={handleDisconnect}
              />

              <SchemaPanel
                dbStatus={dbStatus}
                schemas={schemas}
                onCreateSchema={handleCreateSchema}
                onDropSchema={handleDropSchema}
                onRefreshSchemas={fetchSchemas}
                socket={socket}
              />

              <StressConfigPanel
                dbStatus={dbStatus}
                schemas={schemas}
                stressStatus={stressStatus}
                onStart={handleStartStress}
                onStop={handleStopStress}
                onUpdateConfig={handleUpdateConfig}
              />
            </div>

            {stressStatus.isRunning && (
              <>
                <MetricsPanel metrics={metrics} stressStatus={stressStatus} />

                <div className="grid-2">
                  <TPSChart
                    data={metrics.tps}
                    schemaData={isMultiSchema ? metrics.tpsBySchema : null}
                  />
                  <OperationsChart
                    data={metrics.operations}
                    schemaData={isMultiSchema ? metrics.operationsBySchema : null}
                  />
                </div>

                <div className="grid-2">
                  <WaitEventsPanel waitEvents={metrics.waitEvents} />
                  <GCWaitChart gcWaitEvents={metrics.gcWaitEvents} />
                </div>
              </>
            )}
          </>
        )}

        {/* Index Contention Demo Tab */}
        {activeTab === 'index-contention' && (
          <IndexContentionPanel
            dbStatus={dbStatus}
            socket={socket}
            schemas={schemas}
          />
        )}

        {/* Library Cache Lock Tab */}
        {activeTab === 'library-cache-lock' && (
          <LibraryCacheLockPanel
            dbStatus={dbStatus}
            socket={socket}
            schemas={schemas}
          />
        )}

        {/* HW Contention Demo Tab */}
        {activeTab === 'hw-contention' && (
          <HWContentionPanel
            dbStatus={dbStatus}
            socket={socket}
            schemas={schemas}
          />
        )}

        {/* Stats Comparison Tab */}
        {activeTab === 'stats-comparison' && (
          <StatsComparisonPanel
            dbStatus={dbStatus}
            socket={socket}
          />
        )}

        {/* Skew Detection Tab */}
        {activeTab === 'skew-detection' && (
          <SkewDetectionPanel
            dbStatus={dbStatus}
            socket={socket}
          />
        )}

        {/* Metric Explorer Tab */}
        {activeTab === 'metric-explorer' && (
          <MetricExplorerPanel />
        )}

        {activeTab === 'swingbench-soe' && (
          <>
            <div className="grid-2">
              <ConnectionPanel
                dbStatus={dbStatus}
                onConnect={handleConnect}
                onDisconnect={handleDisconnect}
              />
              <SwingbenchPanel
                dbStatus={dbStatus}
                socket={socket}
                onSuccess={showSuccess}
                onError={showError}
              />
            </div>
            <div style={{ marginTop: '1.5rem' }}>
              <SwingbenchWorkloadPanel
                dbStatus={dbStatus}
                socket={socket}
                onSuccess={showSuccess}
                onError={showError}
              />
            </div>
          </>
        )}

        {activeTab === 'insert-blast' && (
          <div className="grid-2">
            <ConnectionPanel
              dbStatus={dbStatus}
              onConnect={handleConnect}
              onDisconnect={handleDisconnect}
            />
            <InsertBlastPanel
              dbStatus={dbStatus}
              socket={socket}
              onSuccess={showSuccess}
              onError={showError}
            />
          </div>
        )}

        {activeTab === 'datafile-growth' && (
          <div className="grid-2">
            <ConnectionPanel
              dbStatus={dbStatus}
              onConnect={handleConnect}
              onDisconnect={handleDisconnect}
            />
            <DatafileGrowthPanel
              dbStatus={dbStatus}
              socket={socket}
              onSuccess={showSuccess}
              onError={showError}
            />
          </div>
        )}

        {/* TDE Comparison Tab */}
        {activeTab === 'tde-comparison' && (
          <TDEComparisonPanel
            dbStatus={dbStatus}
            socket={socket}
          />
        )}

        {activeTab === 'gc-congestion' && (
          <GCCongestionPanel
            dbStatus={dbStatus}
            socket={socket}
          />
        )}

        <div style={{ display: activeTab === 'gc-benchmark' ? 'block' : 'none' }}>
          <GCBenchmarkPanel />
        </div>
      </main>
    </div>
  );
}

export default App;
