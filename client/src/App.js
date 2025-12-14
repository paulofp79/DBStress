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

const API_BASE = 'http://localhost:3001/api';

function App() {
  const [socket, setSocket] = useState(null);
  const [connected, setConnected] = useState(false);
  const [dbStatus, setDbStatus] = useState({ connected: false });
  const [stressStatus, setStressStatus] = useState({ isRunning: false });
  const [schemaInfo, setSchemaInfo] = useState(null);
  const [metrics, setMetrics] = useState({
    tps: [],
    operations: [],
    waitEvents: [],
    systemStats: {}
  });
  const [error, setError] = useState(null);
  const [success, setSuccess] = useState(null);

  // Initialize socket connection
  useEffect(() => {
    const newSocket = io('http://localhost:3001');

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

    newSocket.on('stress-metrics', (data) => {
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
    });

    newSocket.on('db-metrics', (data) => {
      setMetrics(prev => ({
        ...prev,
        waitEvents: data.waitEvents,
        systemStats: data.systemStats,
        sessionStats: data.sessionStats
      }));
    });

    newSocket.on('stress-stopped', (finalStats) => {
      setStressStatus({ isRunning: false });
      showSuccess(`Stress test completed. Total transactions: ${finalStats.transactions}`);
    });

    setSocket(newSocket);

    return () => {
      newSocket.close();
    };
  }, []);

  // Fetch initial status
  useEffect(() => {
    fetchDbStatus();
    fetchStressStatus();
  }, []);

  const fetchDbStatus = async () => {
    try {
      const response = await axios.get(`${API_BASE}/db/status`);
      setDbStatus(response.data);
      if (response.data.connected) {
        fetchSchemaInfo();
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

  const fetchSchemaInfo = async () => {
    try {
      const response = await axios.get(`${API_BASE}/schema/info`);
      setSchemaInfo(response.data);
    } catch (err) {
      console.error('Error fetching schema info:', err);
    }
  };

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
        fetchSchemaInfo();
      }
    } catch (err) {
      showError(err.response?.data?.message || 'Failed to connect to database');
    }
  };

  const handleDisconnect = async () => {
    try {
      await axios.post(`${API_BASE}/db/disconnect`);
      setDbStatus({ connected: false });
      setSchemaInfo(null);
      showSuccess('Disconnected from database');
    } catch (err) {
      showError(err.response?.data?.message || 'Failed to disconnect');
    }
  };

  const handleCreateSchema = async (scaleFactor) => {
    try {
      setError(null);
      await axios.post(`${API_BASE}/schema/create`, { scaleFactor });
      showSuccess('Schema created successfully');
      fetchSchemaInfo();
    } catch (err) {
      showError(err.response?.data?.message || 'Failed to create schema');
    }
  };

  const handleDropSchema = async () => {
    try {
      await axios.post(`${API_BASE}/schema/drop`);
      showSuccess('Schema dropped successfully');
      fetchSchemaInfo();
    } catch (err) {
      showError(err.response?.data?.message || 'Failed to drop schema');
    }
  };

  const handleStartStress = async (config) => {
    try {
      setError(null);
      setMetrics(prev => ({ ...prev, tps: [], operations: [] }));
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

        <div className="grid-3">
          <ConnectionPanel
            dbStatus={dbStatus}
            onConnect={handleConnect}
            onDisconnect={handleDisconnect}
          />

          <SchemaPanel
            dbStatus={dbStatus}
            schemaInfo={schemaInfo}
            onCreateSchema={handleCreateSchema}
            onDropSchema={handleDropSchema}
            socket={socket}
          />

          <StressConfigPanel
            dbStatus={dbStatus}
            schemaInfo={schemaInfo}
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
              <TPSChart data={metrics.tps} />
              <OperationsChart data={metrics.operations} />
            </div>

            <WaitEventsPanel waitEvents={metrics.waitEvents} />
          </>
        )}
      </main>
    </div>
  );
}

export default App;
