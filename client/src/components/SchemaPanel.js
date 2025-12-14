import React, { useState, useEffect } from 'react';

function SchemaPanel({ dbStatus, schemaInfo, onCreateSchema, onDropSchema, socket }) {
  const [scaleFactor, setScaleFactor] = useState(1);
  const [creating, setCreating] = useState(false);
  const [progress, setProgress] = useState({ step: '', progress: 0 });

  useEffect(() => {
    if (socket) {
      socket.on('schema-progress', (data) => {
        setProgress(data);
        if (data.progress === 100 || data.progress === -1) {
          setCreating(false);
        }
      });
    }
  }, [socket]);

  const handleCreate = async () => {
    setCreating(true);
    setProgress({ step: 'Starting...', progress: 0 });
    await onCreateSchema(scaleFactor);
  };

  const handleDrop = async () => {
    if (window.confirm('Are you sure you want to drop the schema? All data will be lost.')) {
      await onDropSchema();
    }
  };

  const formatNumber = (num) => {
    if (num >= 1000000) return (num / 1000000).toFixed(1) + 'M';
    if (num >= 1000) return (num / 1000).toFixed(1) + 'K';
    return num?.toString() || '0';
  };

  if (!dbStatus.connected) {
    return (
      <div className="panel">
        <div className="panel-header">
          <h2>Sales Schema</h2>
        </div>
        <div className="panel-content">
          <p style={{ color: 'var(--text-muted)' }}>Connect to database first</p>
        </div>
      </div>
    );
  }

  return (
    <div className="panel">
      <div className="panel-header">
        <h2>Sales Schema</h2>
        {schemaInfo?.schemaExists && (
          <span style={{ color: 'var(--accent-success)', fontSize: '0.875rem' }}>
            {schemaInfo.totalSizeMB?.toFixed(1)} MB
          </span>
        )}
      </div>
      <div className="panel-content">
        {creating ? (
          <div>
            <p style={{ marginBottom: '0.5rem', fontSize: '0.875rem' }}>{progress.step}</p>
            <div className="progress-bar">
              <div className="fill" style={{ width: `${Math.max(0, progress.progress)}%` }}></div>
            </div>
            <p className="progress-text">{progress.progress}% complete</p>
          </div>
        ) : schemaInfo?.schemaExists ? (
          <div>
            <div className="schema-stats">
              <div className="schema-stat">
                <div className="name">Products</div>
                <div className="count">{formatNumber(schemaInfo.counts?.products)}</div>
              </div>
              <div className="schema-stat">
                <div className="name">Customers</div>
                <div className="count">{formatNumber(schemaInfo.counts?.customers)}</div>
              </div>
              <div className="schema-stat">
                <div className="name">Orders</div>
                <div className="count">{formatNumber(schemaInfo.counts?.orders)}</div>
              </div>
              <div className="schema-stat">
                <div className="name">Order Items</div>
                <div className="count">{formatNumber(schemaInfo.counts?.order_items)}</div>
              </div>
              <div className="schema-stat">
                <div className="name">Inventory</div>
                <div className="count">{formatNumber(schemaInfo.counts?.inventory)}</div>
              </div>
              <div className="schema-stat">
                <div className="name">Payments</div>
                <div className="count">{formatNumber(schemaInfo.counts?.payments)}</div>
              </div>
            </div>
            <button
              className="btn btn-danger btn-sm"
              onClick={handleDrop}
              style={{ marginTop: '1rem' }}
            >
              Drop Schema
            </button>
          </div>
        ) : (
          <div>
            <div className="form-group">
              <label>Scale Factor</label>
              <div className="slider-group">
                <input
                  type="range"
                  min="1"
                  max="100"
                  value={scaleFactor}
                  onChange={(e) => setScaleFactor(parseInt(e.target.value))}
                />
                <span className="slider-value">{scaleFactor}x</span>
              </div>
              <p style={{ fontSize: '0.75rem', color: 'var(--text-muted)', marginTop: '0.5rem' }}>
                {scaleFactor}x = ~{formatNumber(1000 * scaleFactor)} customers,
                ~{formatNumber(500 * scaleFactor)} products,
                ~{formatNumber(5000 * scaleFactor)} orders
              </p>
            </div>
            <button className="btn btn-success" onClick={handleCreate}>
              Create Schema
            </button>
          </div>
        )}
      </div>
    </div>
  );
}

export default SchemaPanel;
