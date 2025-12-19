import React, { useState, useEffect } from 'react';

function SchemaPanel({ dbStatus, schemas, onCreateSchema, onDropSchema, onRefreshSchemas, socket }) {
  const [scaleFactor, setScaleFactor] = useState(1);
  const [prefix, setPrefix] = useState('');
  const [compress, setCompress] = useState(false);
  const [parallelism, setParallelism] = useState(10);
  const [creating, setCreating] = useState({});
  const [progress, setProgress] = useState({});

  useEffect(() => {
    if (socket) {
      socket.on('schema-progress', (data) => {
        const schemaId = data.schemaId || 'default';
        setProgress(prev => ({ ...prev, [schemaId]: data }));
        if (data.progress === 100 || data.progress === -1) {
          setCreating(prev => ({ ...prev, [schemaId]: false }));
          if (data.progress === 100) {
            // Refresh schemas list after creation
            setTimeout(() => onRefreshSchemas?.(), 1000);
          }
        }
      });
    }
  }, [socket, onRefreshSchemas]);

  const handleCreate = async () => {
    const schemaId = prefix || 'default';
    setCreating(prev => ({ ...prev, [schemaId]: true }));
    setProgress(prev => ({ ...prev, [schemaId]: { step: 'Starting...', progress: 0 } }));
    await onCreateSchema({ scaleFactor, prefix, compress, parallelism });
  };

  const handleDrop = async (schemaPrefix) => {
    const displayName = schemaPrefix || 'default';
    if (window.confirm(`Are you sure you want to drop schema '${displayName}'? All data will be lost.`)) {
      await onDropSchema(schemaPrefix);
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
          <h2>Schema Management</h2>
        </div>
        <div className="panel-content">
          <p style={{ color: 'var(--text-muted)' }}>Connect to database first</p>
        </div>
      </div>
    );
  }

  const isCreatingAny = Object.values(creating).some(v => v);
  const existingSchemas = schemas || [];

  return (
    <div className="panel" style={{ maxHeight: '600px', overflow: 'auto' }}>
      <div className="panel-header">
        <h2>Schema Management</h2>
        <span style={{ color: 'var(--accent-success)', fontSize: '0.875rem' }}>
          {existingSchemas.length} schema(s)
        </span>
      </div>
      <div className="panel-content">
        {/* Existing Schemas */}
        {existingSchemas.length > 0 && (
          <div style={{ marginBottom: '1.5rem' }}>
            <h3 style={{ fontSize: '0.875rem', marginBottom: '0.75rem', color: 'var(--text-muted)' }}>
              Existing Schemas
            </h3>
            {existingSchemas.map((schema) => {
              const schemaId = schema.prefix || 'default';
              const schemaProgress = progress[schemaId];
              const isCreating = creating[schemaId];

              return (
                <div key={schemaId} className="schema-card" style={{
                  background: 'var(--surface)',
                  padding: '0.75rem',
                  borderRadius: '6px',
                  marginBottom: '0.5rem',
                  border: '1px solid var(--border)'
                }}>
                  <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '0.5rem' }}>
                    <span style={{ fontWeight: '500' }}>
                      {schemaId}
                      {schema.compress && (
                        <span style={{ marginLeft: '0.5rem', fontSize: '0.7rem', background: 'var(--accent-primary)', padding: '2px 6px', borderRadius: '4px' }}>
                          COMPRESSED
                        </span>
                      )}
                    </span>
                    <span style={{ fontSize: '0.8rem', color: 'var(--text-muted)' }}>
                      {schema.totalSizeMB?.toFixed(1)} MB
                    </span>
                  </div>

                  {isCreating ? (
                    <div>
                      <p style={{ marginBottom: '0.5rem', fontSize: '0.75rem' }}>{schemaProgress?.step}</p>
                      <div className="progress-bar">
                        <div className="fill" style={{ width: `${Math.max(0, schemaProgress?.progress || 0)}%` }}></div>
                      </div>
                    </div>
                  ) : (
                    <>
                      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: '0.25rem', fontSize: '0.75rem' }}>
                        <div>Products: {formatNumber(schema.counts?.products)}</div>
                        <div>Customers: {formatNumber(schema.counts?.customers)}</div>
                        <div>Orders: {formatNumber(schema.counts?.orders)}</div>
                      </div>
                      <button
                        className="btn btn-danger btn-sm"
                        onClick={() => handleDrop(schema.prefix)}
                        style={{ marginTop: '0.5rem', fontSize: '0.75rem', padding: '4px 8px' }}
                      >
                        Drop
                      </button>
                    </>
                  )}
                </div>
              );
            })}
          </div>
        )}

        {/* Create New Schema */}
        <div style={{ borderTop: existingSchemas.length > 0 ? '1px solid var(--border)' : 'none', paddingTop: existingSchemas.length > 0 ? '1rem' : 0 }}>
          <h3 style={{ fontSize: '0.875rem', marginBottom: '0.75rem', color: 'var(--text-muted)' }}>
            Create New Schema
          </h3>

          <div className="form-group" style={{ marginBottom: '0.75rem' }}>
            <label>Schema Prefix (optional)</label>
            <input
              type="text"
              value={prefix}
              onChange={(e) => setPrefix(e.target.value.toLowerCase().replace(/[^a-z0-9]/g, ''))}
              placeholder="e.g., comp, nocomp, test1"
              style={{
                width: '100%',
                padding: '0.5rem',
                background: 'var(--surface)',
                border: '1px solid var(--border)',
                borderRadius: '4px',
                color: 'var(--text-primary)'
              }}
              disabled={isCreatingAny}
            />
            <p style={{ fontSize: '0.7rem', color: 'var(--text-muted)', marginTop: '0.25rem' }}>
              Leave empty for default schema. Use prefixes to create multiple schemas.
            </p>
          </div>

          <div className="form-group" style={{ marginBottom: '0.75rem' }}>
            <label style={{ display: 'flex', alignItems: 'center', cursor: 'pointer' }}>
              <input
                type="checkbox"
                checked={compress}
                onChange={(e) => setCompress(e.target.checked)}
                disabled={isCreatingAny}
                style={{ marginRight: '0.5rem' }}
              />
              Enable Compression (COMPRESS FOR OLTP)
            </label>
          </div>

          <div className="form-group" style={{ marginBottom: '0.75rem' }}>
            <label>Scale Factor</label>
            <div className="slider-group">
              <input
                type="range"
                min="1"
                max="100"
                value={scaleFactor}
                onChange={(e) => setScaleFactor(parseInt(e.target.value))}
                disabled={isCreatingAny}
              />
              <span className="slider-value">{scaleFactor}x</span>
            </div>
            <p style={{ fontSize: '0.7rem', color: 'var(--text-muted)', marginTop: '0.25rem' }}>
              ~{formatNumber(1000 * scaleFactor)} customers,
              ~{formatNumber(500 * scaleFactor)} products,
              ~{formatNumber(5000 * scaleFactor)} orders
            </p>
          </div>

          <div className="form-group" style={{ marginBottom: '0.75rem' }}>
            <label>Insert Parallelism</label>
            <div className="slider-group">
              <input
                type="range"
                min="1"
                max="50"
                value={parallelism}
                onChange={(e) => setParallelism(parseInt(e.target.value))}
                disabled={isCreatingAny}
              />
              <span className="slider-value">{parallelism}</span>
            </div>
          </div>

          <button
            className="btn btn-success"
            onClick={handleCreate}
            disabled={isCreatingAny}
          >
            {isCreatingAny ? 'Creating...' : 'Create Schema'}
          </button>
        </div>
      </div>
    </div>
  );
}

export default SchemaPanel;
