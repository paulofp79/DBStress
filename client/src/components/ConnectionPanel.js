import React, { useState } from 'react';

// Auto-detect server URL
const getServerUrl = () => {
  if (window.location.hostname !== 'localhost') {
    return `http://${window.location.host}`;
  }
  return 'http://localhost:3001';
};

function ConnectionPanel({ dbStatus, onConnect, onDisconnect }) {
  const [credentials, setCredentials] = useState({
    user: '',
    password: '',
    connectionString: 'localhost:1521/ORCLPDB1'
  });
  const [testing, setTesting] = useState(false);
  const [connecting, setConnecting] = useState(false);

  const handleChange = (e) => {
    setCredentials(prev => ({
      ...prev,
      [e.target.name]: e.target.value
    }));
  };

  const handleConnect = async (e) => {
    e.preventDefault();
    setConnecting(true);
    await onConnect(credentials);
    setConnecting(false);
  };

  const handleDisconnect = async () => {
    await onDisconnect();
  };

  const handleTest = async () => {
    setTesting(true);
    try {
      const response = await fetch(`${getServerUrl()}/api/db/test-connection`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(credentials)
      });
      const data = await response.json();
      if (data.success) {
        alert(`Connection successful!\n\nVersion: ${data.version}\nInstance: ${data.instance}\nHost: ${data.host}`);
      } else {
        alert(`Connection failed: ${data.message}`);
      }
    } catch (err) {
      alert(`Connection test failed: ${err.message}`);
    }
    setTesting(false);
  };

  return (
    <div className="panel">
      <div className="panel-header">
        <h2>Database Connection</h2>
        {dbStatus.connected && (
          <span style={{ color: 'var(--accent-success)', fontSize: '0.875rem' }}>
            Connected
          </span>
        )}
      </div>
      <div className="panel-content">
        {!dbStatus.connected ? (
          <form onSubmit={handleConnect}>
            <div className="form-group">
              <label htmlFor="user">Username</label>
              <input
                type="text"
                id="user"
                name="user"
                value={credentials.user}
                onChange={handleChange}
                placeholder="oracle_user"
                required
              />
            </div>

            <div className="form-group">
              <label htmlFor="password">Password</label>
              <input
                type="password"
                id="password"
                name="password"
                value={credentials.password}
                onChange={handleChange}
                placeholder="********"
                required
              />
            </div>

            <div className="form-group">
              <label htmlFor="connectionString">Connection String</label>
              <input
                type="text"
                id="connectionString"
                name="connectionString"
                value={credentials.connectionString}
                onChange={handleChange}
                placeholder="host:port/service_name"
                required
              />
            </div>

            <div className="btn-group">
              <button
                type="button"
                className="btn btn-secondary"
                onClick={handleTest}
                disabled={testing || !credentials.user || !credentials.password}
              >
                {testing ? 'Testing...' : 'Test Connection'}
              </button>
              <button
                type="submit"
                className="btn btn-primary"
                disabled={connecting || !credentials.user || !credentials.password}
              >
                {connecting ? 'Connecting...' : 'Connect'}
              </button>
            </div>
          </form>
        ) : (
          <div>
            <div className="schema-stats" style={{ marginBottom: '1rem' }}>
              <div className="schema-stat">
                <div className="name">User</div>
                <div className="count">{dbStatus.config?.user}</div>
              </div>
              <div className="schema-stat">
                <div className="name">Connection</div>
                <div className="count" style={{ fontSize: '0.75rem' }}>
                  {dbStatus.config?.connectionString}
                </div>
              </div>
              {dbStatus.pool && (
                <>
                  <div className="schema-stat">
                    <div className="name">Pool Open</div>
                    <div className="count">{dbStatus.pool.connectionsOpen}</div>
                  </div>
                  <div className="schema-stat">
                    <div className="name">Pool In Use</div>
                    <div className="count">{dbStatus.pool.connectionsInUse}</div>
                  </div>
                </>
              )}
            </div>
            <button className="btn btn-danger" onClick={handleDisconnect}>
              Disconnect
            </button>
          </div>
        )}
      </div>
    </div>
  );
}

export default ConnectionPanel;
