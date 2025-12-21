import React, { useState, useEffect, useRef } from 'react';

function StressConfigPanel({ dbStatus, schemas, stressStatus, onStart, onStop, onUpdateConfig }) {
  const [config, setConfig] = useState({
    sessions: 10,
    insertsPerSecond: 50,
    updatesPerSecond: 30,
    deletesPerSecond: 10,
    selectsPerSecond: 100,
    thinkTime: 50
  });

  // Schema selection for stress testing
  const [selectedSchemas, setSelectedSchemas] = useState([]);

  // Local timer for uptime display
  const [localUptime, setLocalUptime] = useState(0);
  const timerRef = useRef(null);
  const startTimeRef = useRef(null);

  // Start local timer when stress test is running
  useEffect(() => {
    if (stressStatus.isRunning) {
      // Initialize start time if not set
      if (!startTimeRef.current) {
        startTimeRef.current = Date.now();
      }

      // Start the timer
      timerRef.current = setInterval(() => {
        const elapsed = Math.floor((Date.now() - startTimeRef.current) / 1000);
        setLocalUptime(elapsed);
      }, 1000);
    } else {
      // Clear timer when stopped
      if (timerRef.current) {
        clearInterval(timerRef.current);
        timerRef.current = null;
      }
      startTimeRef.current = null;
      setLocalUptime(0);
    }

    return () => {
      if (timerRef.current) {
        clearInterval(timerRef.current);
      }
    };
  }, [stressStatus.isRunning]);

  useEffect(() => {
    if (stressStatus.config) {
      setConfig(stressStatus.config);
    }
  }, [stressStatus.config]);

  // Auto-select all schemas when schemas list changes
  useEffect(() => {
    if (schemas && schemas.length > 0 && selectedSchemas.length === 0) {
      setSelectedSchemas(schemas.map(s => s.prefix || ''));
    }
  }, [schemas, selectedSchemas.length]);

  const handleChange = (field, value) => {
    const newConfig = { ...config, [field]: parseInt(value) };
    setConfig(newConfig);

    // Live update if running
    if (stressStatus.isRunning) {
      onUpdateConfig(newConfig);
    }
  };

  const toggleSchema = (prefix) => {
    setSelectedSchemas(prev => {
      if (prev.includes(prefix)) {
        return prev.filter(p => p !== prefix);
      }
      return [...prev, prefix];
    });
  };

  const handleStart = () => {
    startTimeRef.current = Date.now();
    setLocalUptime(0);

    const schemasToTest = selectedSchemas.map(prefix => ({ prefix }));
    onStart({ ...config, schemas: schemasToTest });
  };

  const existingSchemas = schemas || [];
  const hasSchemas = existingSchemas.length > 0;
  const canStart = dbStatus.connected && hasSchemas && !stressStatus.isRunning && selectedSchemas.length > 0;
  const canStop = stressStatus.isRunning;

  const formatUptime = (seconds) => {
    const hrs = Math.floor(seconds / 3600);
    const mins = Math.floor((seconds % 3600) / 60);
    const secs = seconds % 60;
    if (hrs > 0) return `${hrs}h ${mins}m ${secs}s`;
    if (mins > 0) return `${mins}m ${secs}s`;
    return `${secs}s`;
  };

  return (
    <div className="panel">
      <div className="panel-header">
        <h2>Stress Test Configuration</h2>
        {stressStatus.isRunning && (
          <span style={{ color: 'var(--accent-warning)', fontSize: '0.875rem' }}>
            Running: {formatUptime(localUptime)}
          </span>
        )}
      </div>
      <div className="panel-content">
        {!dbStatus.connected ? (
          <p style={{ color: 'var(--text-muted)' }}>Connect to database first</p>
        ) : !hasSchemas ? (
          <p style={{ color: 'var(--text-muted)' }}>Create schema first</p>
        ) : (
          <>
            {/* Schema Selection */}
            <div className="form-group" style={{ marginBottom: '1rem' }}>
              <label>Select Schemas to Test</label>
              <div style={{ display: 'flex', flexWrap: 'wrap', gap: '0.5rem', marginTop: '0.5rem' }}>
                {existingSchemas.map((schema) => {
                  const prefix = schema.prefix || '';
                  const displayName = prefix || 'default';
                  const isSelected = selectedSchemas.includes(prefix);

                  return (
                    <button
                      key={displayName}
                      onClick={() => toggleSchema(prefix)}
                      disabled={stressStatus.isRunning}
                      style={{
                        padding: '0.4rem 0.75rem',
                        borderRadius: '4px',
                        border: isSelected ? '2px solid var(--accent-primary)' : '1px solid var(--border)',
                        background: isSelected ? 'var(--accent-primary)' : 'var(--surface)',
                        color: isSelected ? 'white' : 'var(--text-primary)',
                        cursor: stressStatus.isRunning ? 'not-allowed' : 'pointer',
                        fontSize: '0.8rem',
                        opacity: stressStatus.isRunning ? 0.6 : 1
                      }}
                    >
                      {displayName}
                      {schema.compress && ' (C)'}
                    </button>
                  );
                })}
              </div>
              <p style={{ fontSize: '0.7rem', color: 'var(--text-muted)', marginTop: '0.25rem' }}>
                Select multiple schemas to run side-by-side comparison tests. (C) = compressed
              </p>
            </div>

            <div className="form-group">
              <label>Concurrent Sessions (per schema)</label>
              <div className="slider-group">
                <input
                  type="range"
                  min="1"
                  max="100"
                  value={config.sessions}
                  onChange={(e) => handleChange('sessions', e.target.value)}
                  disabled={stressStatus.isRunning}
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
                    onChange={(e) => handleChange('insertsPerSecond', e.target.value)}
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
                    onChange={(e) => handleChange('updatesPerSecond', e.target.value)}
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
                    onChange={(e) => handleChange('deletesPerSecond', e.target.value)}
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
                    onChange={(e) => handleChange('selectsPerSecond', e.target.value)}
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
                  onChange={(e) => handleChange('thinkTime', e.target.value)}
                />
                <span className="slider-value">{config.thinkTime}ms</span>
              </div>
              <p style={{ fontSize: '0.75rem', color: 'var(--text-muted)', marginTop: '0.25rem' }}>
                Lower = more aggressive workload
              </p>
            </div>

            <div className="btn-group">
              {canStart && (
                <button className="btn btn-success" onClick={handleStart}>
                  Start Stress Test ({selectedSchemas.length} schema{selectedSchemas.length > 1 ? 's' : ''})
                </button>
              )}
              {canStop && (
                <button className="btn btn-danger" onClick={onStop}>
                  Stop Stress Test
                </button>
              )}
            </div>
          </>
        )}
      </div>
    </div>
  );
}

export default StressConfigPanel;
