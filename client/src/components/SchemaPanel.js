import React, { useState, useEffect } from 'react';

// Auto-detect server URL
const getServerUrl = () => {
  if (window.location.hostname !== 'localhost') {
    return `http://${window.location.host}`;
  }
  return 'http://localhost:3001';
};

function SchemaPanel({ dbStatus, schemas, onCreateSchema, onDropSchema, onRefreshSchemas, socket }) {
  const [scaleFactor, setScaleFactor] = useState(1);
  const parallelism = 10; // Fixed parallelism for batch inserts
  const [creating, setCreating] = useState({});
  const [progress, setProgress] = useState({});

  // Compression type options
  const COMPRESSION_OPTIONS = [
    { value: 'none', label: 'No Compression' },
    { value: 'basic', label: 'ROW STORE COMPRESS BASIC' },
    { value: 'advanced', label: 'ROW STORE COMPRESS ADVANCED' },
    { value: 'query_low', label: 'COLUMN STORE FOR QUERY LOW' },
    { value: 'query_high', label: 'COLUMN STORE FOR QUERY HIGH' },
    { value: 'archive_low', label: 'COLUMN STORE FOR ARCHIVE LOW' },
    { value: 'archive_high', label: 'COLUMN STORE FOR ARCHIVE HIGH' }
  ];

  // Batch schema definitions - create multiple schemas at once
  const [schemaDefinitions, setSchemaDefinitions] = useState([
    { prefix: 'nocomp', compressionType: 'none', racTableCount: 1, enabled: true },
    { prefix: 'rowadv', compressionType: 'advanced', racTableCount: 1, enabled: true }
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
        compressionType: schema.compressionType,
        racTableCount: schema.racTableCount || 1,
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

  const handleDropAll = async () => {
    if (existingSchemas.length === 0) return;

    const schemaNames = existingSchemas.map(s => s.prefix || 'default').join(', ');
    if (window.confirm(`Are you sure you want to drop ALL schemas (${schemaNames})? All data will be lost.`)) {
      // Drop all schemas in sequence
      for (const schema of existingSchemas) {
        await onDropSchema(schema.prefix);
      }
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
      { prefix: `schema${prev.length + 1}`, compressionType: 'none', racTableCount: 1, enabled: true }
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

  // Download SQL script for a schema
  const handleDownloadScript = async (schema) => {
    try {
      const response = await fetch(`${getServerUrl()}/api/schema/generate-script`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          prefix: schema.prefix,
          compressionType: schema.compressionType || 'none',
          scaleFactor: scaleFactor,
          racTableCount: schema.racTableCount || 1
        })
      });

      if (!response.ok) throw new Error('Failed to generate script');

      const blob = await response.blob();
      const filename = `dbstress_schema${schema.prefix ? `_${schema.prefix}` : ''}_${scaleFactor}x.sql`;

      // Create download link
      const url = window.URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = filename;
      document.body.appendChild(a);
      a.click();
      window.URL.revokeObjectURL(url);
      document.body.removeChild(a);
    } catch (err) {
      alert('Failed to download script: ' + err.message);
    }
  };

  // Download all enabled schemas as scripts
  const handleDownloadAllScripts = async () => {
    const enabledSchemas = schemaDefinitions.filter(s => s.enabled);
    for (const schema of enabledSchemas) {
      await handleDownloadScript(schema);
      // Small delay between downloads
      await new Promise(resolve => setTimeout(resolve, 500));
    }
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
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '0.75rem' }}>
              <h3 style={{ fontSize: '0.875rem', color: 'var(--text-muted)', margin: 0 }}>
                Existing Schemas
              </h3>
              <button
                className="btn btn-danger btn-sm"
                onClick={handleDropAll}
                disabled={isCreatingAny}
                style={{ fontSize: '0.75rem', padding: '4px 8px' }}
              >
                Drop All Schemas
              </button>
            </div>
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
                      {schema.compressionType && schema.compressionType !== 'none' && (
                        <span style={{ marginLeft: '0.5rem', fontSize: '0.65rem', background: 'var(--accent-primary)', padding: '2px 6px', borderRadius: '4px' }}>
                          {COMPRESSION_OPTIONS.find(o => o.value === schema.compressionType)?.label || schema.compressionType}
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
                        value={schema.compressionType || 'none'}
                        onChange={(e) => updateSchemaDefinition(index, 'compressionType', e.target.value)}
                        disabled={isCreatingAny}
                        style={{
                          padding: '0.4rem',
                          background: 'var(--bg-primary)',
                          border: '1px solid var(--border)',
                          borderRadius: '4px',
                          color: 'var(--text-primary)',
                          fontSize: '0.75rem',
                          minWidth: '180px'
                        }}
                      >
                        {COMPRESSION_OPTIONS.map(opt => (
                          <option key={opt.value} value={opt.value}>{opt.label}</option>
                        ))}
                      </select>

                      <div style={{ display: 'flex', alignItems: 'center', gap: '0.25rem' }}>
                        <label style={{ fontSize: '0.7rem', color: 'var(--text-muted)', whiteSpace: 'nowrap' }}>
                          RAC Tables:
                        </label>
                        <input
                          type="number"
                          min="1"
                          max="100"
                          value={schema.racTableCount || 1}
                          onChange={(e) => updateSchemaDefinition(index, 'racTableCount', Math.max(1, Math.min(100, parseInt(e.target.value) || 1)))}
                          disabled={isCreatingAny}
                          style={{
                            width: '50px',
                            padding: '0.4rem',
                            background: 'var(--bg-primary)',
                            border: '1px solid var(--border)',
                            borderRadius: '4px',
                            color: 'var(--text-primary)',
                            fontSize: '0.75rem',
                            textAlign: 'center'
                          }}
                        />
                      </div>

                      <button
                        type="button"
                        onClick={() => handleDownloadScript(schema)}
                        disabled={isCreatingAny}
                        title="Download SQL script"
                        style={{
                          background: 'var(--accent-primary)',
                          border: 'none',
                          color: 'white',
                          padding: '4px 8px',
                          borderRadius: '4px',
                          fontSize: '0.75rem',
                          cursor: isCreatingAny ? 'not-allowed' : 'pointer'
                        }}
                      >
                        SQL
                      </button>

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

          <div style={{ display: 'flex', gap: '0.5rem', flexWrap: 'wrap' }}>
            <button
              className="btn btn-success"
              onClick={handleCreateAll}
              disabled={isCreatingAny || schemaDefinitions.filter(s => s.enabled).length === 0}
              style={{ flex: 1, minWidth: '150px' }}
            >
              {isCreatingAny
                ? 'Creating...'
                : `Create ${schemaDefinitions.filter(s => s.enabled).length} Schema(s)`}
            </button>

            <button
              className="btn"
              onClick={handleDownloadAllScripts}
              disabled={isCreatingAny || schemaDefinitions.filter(s => s.enabled).length === 0}
              style={{
                background: 'var(--accent-primary)',
                whiteSpace: 'nowrap'
              }}
              title="Download SQL scripts to run from SQL*Plus"
            >
              Download SQL Scripts
            </button>

            {existingSchemas.length > 0 && (
              <button
                className="btn btn-danger"
                onClick={handleDropAll}
                disabled={isCreatingAny}
                style={{ whiteSpace: 'nowrap' }}
              >
                Drop All ({existingSchemas.length})
              </button>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}

export default SchemaPanel;
