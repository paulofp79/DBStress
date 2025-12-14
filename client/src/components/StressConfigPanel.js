import React, { useState, useEffect } from 'react';

function StressConfigPanel({ dbStatus, schemaInfo, stressStatus, onStart, onStop, onUpdateConfig }) {
  const [config, setConfig] = useState({
    sessions: 10,
    insertsPerSecond: 50,
    updatesPerSecond: 30,
    deletesPerSecond: 10,
    selectsPerSecond: 100,
    thinkTime: 50
  });

  useEffect(() => {
    if (stressStatus.config) {
      setConfig(stressStatus.config);
    }
  }, [stressStatus.config]);

  const handleChange = (field, value) => {
    const newConfig = { ...config, [field]: parseInt(value) };
    setConfig(newConfig);

    // Live update if running
    if (stressStatus.isRunning) {
      onUpdateConfig(newConfig);
    }
  };

  const handleStart = () => {
    onStart(config);
  };

  const canStart = dbStatus.connected && schemaInfo?.schemaExists && !stressStatus.isRunning;
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
            Running: {formatUptime(stressStatus.uptime || 0)}
          </span>
        )}
      </div>
      <div className="panel-content">
        {!dbStatus.connected ? (
          <p style={{ color: 'var(--text-muted)' }}>Connect to database first</p>
        ) : !schemaInfo?.schemaExists ? (
          <p style={{ color: 'var(--text-muted)' }}>Create schema first</p>
        ) : (
          <>
            <div className="form-group">
              <label>Concurrent Sessions</label>
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
                  Start Stress Test
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
