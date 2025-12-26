import React, { useEffect, useState } from 'react';

const COMPRESSION_OPTIONS = [
  { value: 'NONE', label: 'No Compression (default)' },
  { value: 'ROW_STORE_COMPRESS_BASIC', label: 'ROW STORE COMPRESS BASIC' },
  { value: 'ROW_STORE_COMPRESS_ADVANCED', label: 'ROW STORE COMPRESS ADVANCED' },
  { value: 'COLUMN_STORE_COMPRESS_FOR_QUERY_LOW', label: 'COLUMN STORE COMPRESS FOR QUERY LOW' },
  { value: 'COLUMN_STORE_COMPRESS_FOR_QUERY_HIGH', label: 'COLUMN STORE COMPRESS FOR QUERY HIGH' },
  { value: 'COLUMN_STORE_COMPRESS_FOR_ARCHIVE_LOW', label: 'COLUMN STORE COMPRESS FOR ARCHIVE LOW' },
  { value: 'COLUMN_STORE_COMPRESS_FOR_ARCHIVE_HIGH', label: 'COLUMN STORE COMPRESS FOR ARCHIVE HIGH' }
];

function ComparePanel({ dbStatus, stressStatus, onCreateDual, onStartDual, onStopDual, socket }) {
  const [schemaA, setSchemaA] = useState({
    tablePrefix: 'CMP_A',
    scaleFactor: 1,
    compressionMode: 'ROW_STORE_COMPRESS_BASIC'
  });
  const [schemaB, setSchemaB] = useState({
    tablePrefix: 'CMP_B',
    scaleFactor: 1,
    compressionMode: 'NONE'
  });

  const [config, setConfig] = useState({
    sessions: 50,
    insertsPerSecond: 50,
    updatesPerSecond: 30,
    deletesPerSecond: 10,
    selectsPerSecond: 100,
    thinkTime: 50
  });

  const [creating, setCreating] = useState(false);
  const [progressA, setProgressA] = useState({ step: '', progress: 0 });
  const [progressB, setProgressB] = useState({ step: '', progress: 0 });

  useEffect(() => {
    if (!socket) return;

    const handler = (data) => {
      const tp = (data && data.tablePrefix) || '';
      if (tp && tp.toUpperCase() === (schemaA.tablePrefix || '').toUpperCase()) {
        setProgressA({ step: data.step, progress: data.progress });
        if (data.progress === 100 || data.progress === -1) {
          setCreating(false);
        }
      } else if (tp && tp.toUpperCase() === (schemaB.tablePrefix || '').toUpperCase()) {
        setProgressB({ step: data.step, progress: data.progress });
        if (data.progress === 100 || data.progress === -1) {
          setCreating(false);
        }
      } else {
        // Fallback if no prefix tagged: show same message in both
        setProgressA({ step: data.step, progress: data.progress });
        setProgressB({ step: data.step, progress: data.progress });
        if (data.progress === 100 || data.progress === -1) {
          setCreating(false);
        }
      }
    };

    socket.on('schema-progress', handler);
    return () => {
      socket.off('schema-progress', handler);
    };
  }, [socket, schemaA.tablePrefix, schemaB.tablePrefix]);

  const canCreate = dbStatus.connected && !creating;
  const dualRunning = !!stressStatus?.multi?.isRunning;
  const canStartDual = dbStatus.connected && !dualRunning;
  const canStopDual = dualRunning;

  const onChangeSchema = (which, field, value) => {
    if (which === 'A') {
      setSchemaA(prev => ({ ...prev, [field]: value }));
    } else {
      setSchemaB(prev => ({ ...prev, [field]: value }));
    }
  };

  const handleCreateBoth = async () => {
    setCreating(true);
    setProgressA({ step: 'Starting...', progress: 0 });
    setProgressB({ step: 'Starting...', progress: 0 });

    await onCreateDual({
      a: {
        tablePrefix: (schemaA.tablePrefix || '').trim(),
        scaleFactor: schemaA.scaleFactor,
        compressionMode: schemaA.compressionMode
      },
      b: {
        tablePrefix: (schemaB.tablePrefix || '').trim(),
        scaleFactor: schemaB.scaleFactor,
        compressionMode: schemaB.compressionMode
      }
    });
  };

  const handleStartDual = () => {
    const runA = {
      ...config,
      tablePrefix: (schemaA.tablePrefix || '').trim(),
      runId: (schemaA.tablePrefix || 'RUN_A').trim()
    };
    const runB = {
      ...config,
      tablePrefix: (schemaB.tablePrefix || '').trim(),
      runId: (schemaB.tablePrefix || 'RUN_B').trim()
    };
    onStartDual({ runA, runB });
  };

  return (
    <div className="panel" style={{ gridColumn: '1 / -1' }}>
      <div className="panel-header">
        <h2>Compare Schemas (A vs B)</h2>
        {dualRunning && (
          <span style={{ color: 'var(--accent-warning)', fontSize: '0.875rem' }}>
            Dual stress running
          </span>
        )}
      </div>

      <div className="panel-content">
        {!dbStatus.connected ? (
          <p style={{ color: 'var(--text-muted)' }}>Connect to database first</p>
        ) : (
          <>
            <div className="form-row">
              <div className="form-group" style={{ flex: 1, borderRight: '1px solid var(--border-color)', paddingRight: '1rem' }}>
                <h3 style={{ marginTop: 0 }}>Schema A</h3>
                <label>Table Prefix</label>
                <input
                  type="text"
                  className="input"
                  value={schemaA.tablePrefix}
                  onChange={(e) => onChangeSchema('A', 'tablePrefix', e.target.value.toUpperCase().replace(/[^A-Z0-9_]/g, ''))}
                  placeholder="e.g., CMP_A"
                />
                <div className="form-group" style={{ marginTop: '0.75rem' }}>
                  <label>Scale Factor</label>
                  <div className="slider-group">
                    <input
                      type="range"
                      min="1"
                      max="100"
                      value={schemaA.scaleFactor}
                      onChange={(e) => onChangeSchema('A', 'scaleFactor', parseInt(e.target.value))}
                      disabled={creating}
                    />
                    <span className="slider-value">{schemaA.scaleFactor}x</span>
                  </div>
                </div>
                <div className="form-group" style={{ marginTop: '0.75rem' }}>
                  <label>Table Compression</label>
                  <select
                    value={schemaA.compressionMode}
                    onChange={(e) => onChangeSchema('A', 'compressionMode', e.target.value)}
                    className="select"
                    disabled={creating}
                  >
                    {COMPRESSION_OPTIONS.map((option) => (
                      <option key={option.value} value={option.value}>
                        {option.label}
                      </option>
                    ))}
                  </select>
                </div>

                {creating && (
                  <div style={{ marginTop: '0.75rem' }}>
                    <p style={{ marginBottom: '0.5rem', fontSize: '0.875rem' }}>{progressA.step}</p>
                    <div className="progress-bar">
                      <div className="fill" style={{ width: `${Math.max(0, progressA.progress)}%` }}></div>
                    </div>
                    <p className="progress-text">{progressA.progress}% complete</p>
                  </div>
                )}
              </div>

              <div className="form-group" style={{ flex: 1, paddingLeft: '1rem' }}>
                <h3 style={{ marginTop: 0 }}>Schema B</h3>
                <label>Table Prefix</label>
                <input
                  type="text"
                  className="input"
                  value={schemaB.tablePrefix}
                  onChange={(e) => onChangeSchema('B', 'tablePrefix', e.target.value.toUpperCase().replace(/[^A-Z0-9_]/g, ''))}
                  placeholder="e.g., CMP_B"
                />
                <div className="form-group" style={{ marginTop: '0.75rem' }}>
                  <label>Scale Factor</label>
                  <div className="slider-group">
                    <input
                      type="range"
                      min="1"
                      max="100"
                      value={schemaB.scaleFactor}
                      onChange={(e) => onChangeSchema('B', 'scaleFactor', parseInt(e.target.value))}
                      disabled={creating}
                    />
                    <span className="slider-value">{schemaB.scaleFactor}x</span>
                  </div>
                </div>
                <div className="form-group" style={{ marginTop: '0.75rem' }}>
                  <label>Table Compression</label>
                  <select
                    value={schemaB.compressionMode}
                    onChange={(e) => onChangeSchema('B', 'compressionMode', e.target.value)}
                    className="select"
                    disabled={creating}
                  >
                    {COMPRESSION_OPTIONS.map((option) => (
                      <option key={option.value} value={option.value}>
                        {option.label}
                      </option>
                    ))}
                  </select>
                </div>

                {creating && (
                  <div style={{ marginTop: '0.75rem' }}>
                    <p style={{ marginBottom: '0.5rem', fontSize: '0.875rem' }}>{progressB.step}</p>
                    <div className="progress-bar">
                      <div className="fill" style={{ width: `${Math.max(0, progressB.progress)}%` }}></div>
                    </div>
                    <p className="progress-text">{progressB.progress}% complete</p>
                  </div>
                )}
              </div>
            </div>

            <div className="btn-group" style={{ marginTop: '0.75rem' }}>
              <button className="btn btn-success" onClick={handleCreateBoth} disabled={!canCreate}>
                Create Both Schemas
              </button>
            </div>

            <hr style={{ borderColor: 'var(--border-color)', margin: '1rem 0' }} />

            <h3 style={{ marginTop: 0 }}>Dual Stress Configuration</h3>

            <div className="form-group">
              <label>Concurrent Sessions (per run)</label>
              <div className="slider-group">
                <input
                  type="range"
                  min="1"
                  max="1000"
                  step="5"
                  value={config.sessions}
                  onChange={(e) => setConfig(prev => ({ ...prev, sessions: parseInt(e.target.value) }))}
                  disabled={dualRunning}
                />
                <span className="slider-value">{config.sessions}</span>
              </div>
            </div>

            <div className="form-row">
              <div className="form-group">
                <label>INSERTs/sec</label>
                <div className="slider-group">
                  <input
                    type="range"
                    min="0"
                    max="500"
                    step="10"
                    value={config.insertsPerSecond}
                    onChange={(e) => setConfig(prev => ({ ...prev, insertsPerSecond: parseInt(e.target.value) }))}
                  />
                  <span className="slider-value">{config.insertsPerSecond}</span>
                </div>
              </div>

              <div className="form-group">
                <label>UPDATEs/sec</label>
                <div className="slider-group">
                  <input
                    type="range"
                    min="0"
                    max="500"
                    step="10"
                    value={config.updatesPerSecond}
                    onChange={(e) => setConfig(prev => ({ ...prev, updatesPerSecond: parseInt(e.target.value) }))}
                  />
                  <span className="slider-value">{config.updatesPerSecond}</span>
                </div>
              </div>
            </div>

            <div className="form-row">
              <div className="form-group">
                <label>DELETEs/sec</label>
                <div className="slider-group">
                  <input
                    type="range"
                    min="0"
                    max="200"
                    step="5"
                    value={config.deletesPerSecond}
                    onChange={(e) => setConfig(prev => ({ ...prev, deletesPerSecond: parseInt(e.target.value) }))}
                  />
                  <span className="slider-value">{config.deletesPerSecond}</span>
                </div>
              </div>

              <div className="form-group">
                <label>SELECTs/sec</label>
                <div className="slider-group">
                  <input
                    type="range"
                    min="0"
                    max="1000"
                    step="10"
                    value={config.selectsPerSecond}
                    onChange={(e) => setConfig(prev => ({ ...prev, selectsPerSecond: parseInt(e.target.value) }))}
                  />
                  <span className="slider-value">{config.selectsPerSecond}</span>
                </div>
              </div>
            </div>

            <div className="form-group">
              <label>Think Time (ms)</label>
              <div className="slider-group">
                <input
                  type="range"
                  min="0"
                  max="500"
                  step="10"
                  value={config.thinkTime}
                  onChange={(e) => setConfig(prev => ({ ...prev, thinkTime: parseInt(e.target.value) }))}
                />
                <span className="slider-value">{config.thinkTime}ms</span>
              </div>
            </div>

            <div className="btn-group">
              {canStartDual && (
                <button className="btn btn-success" onClick={handleStartDual}>
                  Start Dual Stress
                </button>
              )}
              {canStopDual && (
                <button className="btn btn-danger" onClick={onStopDual}>
                  Stop Dual Stress
                </button>
              )}
            </div>
          </>
        )}
      </div>
    </div>
  );
}

export default ComparePanel;
