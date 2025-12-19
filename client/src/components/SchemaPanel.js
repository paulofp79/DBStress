import React, { useState, useEffect } from 'react';

function SchemaPanel({ dbStatus, schemas, onCreateSchema, onDropSchema, onRefreshSchemas, socket }) {
  const [scaleFactor, setScaleFactor] = useState(1);
  const [parallelism, setParallelism] = useState(10);
  const [creating, setCreating] = useState({});
  const [progress, setProgress] = useState({});

  // Batch schema definitions - create multiple schemas at once
  const [schemaDefinitions, setSchemaDefinitions] = useState([
    { prefix: 'nocomp', compress: false, enabled: true },
    { prefix: 'comp', compress: true, enabled: true }
  ]);

  useEffect(() => {
    if (socket) {
      socket.on('schema-progress', (data) => {
        const schemaId = data.schemaId || 'default';
        setProgress(prev => ({ ...prev, [schemaId]: data }));
        if (data.progress === 100 || data.progress === -1) {
          setCreating(prev => ({ ...prev, [schemaId]: false }));
          if (data.progress === 100) {
            setTimeout(() => onRefreshSchemas?.(), 1000);
          }
        }
      });
    }
  }, [socket, onRefreshSchemas]);

  const handleCreateAll = async () => {
    const enabledSchemas = schemaDefinitions.filter(s => s.enabled);

    if (enabledSchemas.length === 0) {
      alert('Please enable at least one schema to create');
      return;
    }

    // Mark all as creating
    const newCreating = {};
    enabledSchemas.forEach(s => {
      const schemaId = s.prefix || 'default';
      newCreating[schemaId] = true;
    });
    setCreating(newCreating);

    // Create all schemas in parallel
    const promises = enabledSchemas.map(schema => {
      const schemaId = schema.prefix || 'default';
      setProgress(prev => ({ ...prev, [schemaId]: { step: 'Starting...', progress: 0 } }));
      return onCreateSchema({
        scaleFactor,
        prefix: schema.prefix,
        compress: schema.compress,
        parallelism
      });
    });

    await Promise.all(promises);
  };

  const handleDrop = async (schemaPrefix) => {
    const displayName = schemaPrefix || 'default';
    if (window.confirm(`Are you sure you want to drop schema '${displayName}'? All data will be lost.`)) {
      await onDropSchema(schemaPrefix);
    }
  };

  const updateSchemaDefinition = (index, field, value) => {
    setSchemaDefinitions(prev => {
      const updated = [...prev];
      updated[index] = { ...updated[index], [field]: value };
      return updated;
    });
  };

  const addSchemaDefinition = () => {
    setSchemaDefinitions(prev => [
      ...prev,
      { prefix: `schema${prev.length + 1}`, compress: false, enabled: true }
    ]);
  };

  const removeSchemaDefinition = (index) => {
    if (schemaDefinitions.length <= 1) return;
    setSchemaDefinitions(prev => prev.filter((_, i) => i !== index));
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

  const isCreatingAny = Object.values(creating).some(v => v);
  const existingSchemas = schemas || [];

  return (
    <div className="panel" style={{ maxHeight: '700px', overflow: 'auto' }}>
      <div className="panel-header">
        <h2>Sales Schema</h2>
        {existingSchemas.length > 0 && (
          <span style={{ color: 'var(--accent-success)', fontSize: '0.875rem' }}>
            {existingSchemas.length} schema(s) exist
          </span>
        )}
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
                <div key={schemaId} style={{
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

        {/* Create New Schemas - Batch Mode */}
        <div style={{ borderTop: existingSchemas.length > 0 ? '1px solid var(--border)' : 'none', paddingTop: existingSchemas.length > 0 ? '1rem' : 0 }}>
          <h3 style={{ fontSize: '0.875rem', marginBottom: '0.75rem', color: 'var(--text-muted)' }}>
            Create Schema(s)
          </h3>

          {/* Global Settings */}
          <div className="form-group" style={{ marginBottom: '1rem' }}>
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
              {scaleFactor}x = ~{formatNumber(1000 * scaleFactor)} customers,
              ~{formatNumber(500 * scaleFactor)} products,
              ~{formatNumber(5000 * scaleFactor)} orders
            </p>
          </div>

          {/* Schema Definitions */}
          <div style={{ marginBottom: '1rem' }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '0.5rem' }}>
              <label style={{ fontSize: '0.875rem' }}>Table Compression Options</label>
              <button
                type="button"
                onClick={addSchemaDefinition}
                disabled={isCreatingAny}
                style={{
                  background: 'var(--accent-primary)',
                  border: 'none',
                  color: 'white',
                  padding: '4px 8px',
                  borderRadius: '4px',
                  fontSize: '0.75rem',
                  cursor: isCreatingAny ? 'not-allowed' : 'pointer',
                  opacity: isCreatingAny ? 0.5 : 1
                }}
              >
                + Add Schema
              </button>
            </div>

            {schemaDefinitions.map((schema, index) => {
              const schemaId = schema.prefix || 'default';
              const schemaProgress = progress[schemaId];
              const isCreating = creating[schemaId];

              return (
                <div key={index} style={{
                  background: 'var(--surface)',
                  padding: '0.75rem',
                  borderRadius: '6px',
                  marginBottom: '0.5rem',
                  border: schema.enabled ? '2px solid var(--accent-primary)' : '1px solid var(--border)',
                  opacity: schema.enabled ? 1 : 0.6
                }}>
                  {isCreating ? (
                    <div>
                      <div style={{ fontWeight: '500', marginBottom: '0.5rem' }}>{schemaId}</div>
                      <p style={{ marginBottom: '0.5rem', fontSize: '0.75rem' }}>{schemaProgress?.step}</p>
                      <div className="progress-bar">
                        <div className="fill" style={{ width: `${Math.max(0, schemaProgress?.progress || 0)}%` }}></div>
                      </div>
                      <p style={{ fontSize: '0.7rem', color: 'var(--text-muted)', marginTop: '0.25rem' }}>
                        {schemaProgress?.progress}% complete
                      </p>
                    </div>
                  ) : (
                    <div style={{ display: 'flex', gap: '0.75rem', alignItems: 'center', flexWrap: 'wrap' }}>
                      <label style={{ display: 'flex', alignItems: 'center', cursor: 'pointer' }}>
                        <input
                          type="checkbox"
                          checked={schema.enabled}
                          onChange={(e) => updateSchemaDefinition(index, 'enabled', e.target.checked)}
                          disabled={isCreatingAny}
                          style={{ marginRight: '0.5rem' }}
                        />
                      </label>

                      <div style={{ flex: '1', minWidth: '100px' }}>
                        <input
                          type="text"
                          value={schema.prefix}
                          onChange={(e) => updateSchemaDefinition(index, 'prefix', e.target.value.toLowerCase().replace(/[^a-z0-9]/g, ''))}
                          placeholder="prefix"
                          disabled={isCreatingAny}
                          style={{
                            width: '100%',
                            padding: '0.4rem',
                            background: 'var(--bg-primary)',
                            border: '1px solid var(--border)',
                            borderRadius: '4px',
                            color: 'var(--text-primary)',
                            fontSize: '0.8rem'
                          }}
                        />
                      </div>

                      <select
                        value={schema.compress ? 'compress' : 'nocompress'}
                        onChange={(e) => updateSchemaDefinition(index, 'compress', e.target.value === 'compress')}
                        disabled={isCreatingAny}
                        style={{
                          padding: '0.4rem',
                          background: 'var(--bg-primary)',
                          border: '1px solid var(--border)',
                          borderRadius: '4px',
                          color: 'var(--text-primary)',
                          fontSize: '0.8rem',
                          minWidth: '140px'
                        }}
                      >
                        <option value="nocompress">No Compression</option>
                        <option value="compress">COMPRESS FOR OLTP</option>
                      </select>

                      {schemaDefinitions.length > 1 && (
                        <button
                          type="button"
                          onClick={() => removeSchemaDefinition(index)}
                          disabled={isCreatingAny}
                          style={{
                            background: 'var(--accent-danger)',
                            border: 'none',
                            color: 'white',
                            padding: '4px 8px',
                            borderRadius: '4px',
                            fontSize: '0.75rem',
                            cursor: isCreatingAny ? 'not-allowed' : 'pointer'
                          }}
                        >
                          X
                        </button>
                      )}
                    </div>
                  )}
                </div>
              );
            })}

            <p style={{ fontSize: '0.7rem', color: 'var(--text-muted)', marginTop: '0.5rem' }}>
              Create multiple schemas with different compression to compare performance.
              Requires corresponding Oracle license/features for compression.
            </p>
          </div>

          <button
            className="btn btn-success"
            onClick={handleCreateAll}
            disabled={isCreatingAny || schemaDefinitions.filter(s => s.enabled).length === 0}
            style={{ width: '100%' }}
          >
            {isCreatingAny
              ? 'Creating...'
              : `Create ${schemaDefinitions.filter(s => s.enabled).length} Schema(s)`}
          </button>
        </div>
      </div>
    </div>
  );
}

export default SchemaPanel;
