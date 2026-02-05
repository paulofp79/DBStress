import React, { useState, useEffect } from 'react';

// Auto-detect server URL
const getServerUrl = () => {
  if (window.location.hostname !== 'localhost') {
    return `http://${window.location.host}`;
  }
  return 'http://localhost:3001';
};

const API_BASE = `${getServerUrl()}/api`;

function SkewDetectionPanel({ dbStatus, socket }) {
  // State
  const [tablesCreated, setTablesCreated] = useState(false);
  const [isCreating, setIsCreating] = useState(false);
  const [isDropping, setIsDropping] = useState(false);
  const [isAnalyzing, setIsAnalyzing] = useState(false);
  const [isGatheringStats, setIsGatheringStats] = useState(false);
  const [analysisResults, setAnalysisResults] = useState([]);
  const [histogramInfo, setHistogramInfo] = useState({});
  const [statusMessage, setStatusMessage] = useState('');
  const [progress, setProgress] = useState(0);
  const [selectedTable, setSelectedTable] = useState('');
  const [methodOpt, setMethodOpt] = useState('SIZE AUTO');

  // Test tables info from backend
  const [testTables, setTestTables] = useState([]);

  // Fetch status on mount
  useEffect(() => {
    fetchStatus();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [dbStatus.connected]);

  // Socket listeners
  useEffect(() => {
    if (socket) {
      socket.on('skew-detection-status', (data) => {
        setStatusMessage(data.message || '');
        setProgress(data.progress || 0);
        if (data.tablesCreated !== undefined) {
          setTablesCreated(data.tablesCreated);
        }
      });

      socket.on('skew-detection-analysis-results', (results) => {
        setAnalysisResults(results);
        setIsAnalyzing(false);
      });

      socket.on('skew-detection-histogram-info', (data) => {
        setHistogramInfo(prev => ({
          ...prev,
          [data.tableName]: data
        }));
      });
    }

    return () => {
      if (socket) {
        socket.off('skew-detection-status');
        socket.off('skew-detection-analysis-results');
        socket.off('skew-detection-histogram-info');
      }
    };
  }, [socket]);

  const fetchStatus = async () => {
    if (!dbStatus.connected) return;

    try {
      const response = await fetch(`${API_BASE}/skew-detection/status`);
      const data = await response.json();
      setTablesCreated(data.tablesCreated || false);
      setTestTables(data.testTables || []);
      if (data.testTables && data.testTables.length > 0) {
        setSelectedTable(data.testTables[0].name);
      }
    } catch (err) {
      console.error('Error fetching skew detection status:', err);
    }
  };

  const handleCreateTables = async () => {
    try {
      setIsCreating(true);
      setStatusMessage('Creating test tables...');
      setProgress(0);
      setAnalysisResults([]);
      setHistogramInfo({});

      const response = await fetch(`${API_BASE}/skew-detection/create-tables`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' }
      });

      const data = await response.json();

      if (!response.ok) {
        throw new Error(data.error || 'Failed to create tables');
      }

      setTablesCreated(true);
      setStatusMessage('Test tables created successfully!');
      fetchStatus();
    } catch (err) {
      setStatusMessage(`Error: ${err.message}`);
    } finally {
      setIsCreating(false);
    }
  };

  const handleDropTables = async () => {
    try {
      setIsDropping(true);
      setStatusMessage('Dropping test tables...');

      const response = await fetch(`${API_BASE}/skew-detection/drop-tables`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' }
      });

      const data = await response.json();

      if (!response.ok) {
        throw new Error(data.error || 'Failed to drop tables');
      }

      setTablesCreated(false);
      setAnalysisResults([]);
      setHistogramInfo({});
      setStatusMessage('Test tables dropped');
    } catch (err) {
      setStatusMessage(`Error: ${err.message}`);
    } finally {
      setIsDropping(false);
    }
  };

  const handleAnalyzeSkew = async () => {
    try {
      setIsAnalyzing(true);
      setStatusMessage('Analyzing skew patterns...');
      setProgress(0);

      const response = await fetch(`${API_BASE}/skew-detection/analyze`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' }
      });

      const data = await response.json();

      if (!response.ok) {
        throw new Error(data.error || 'Failed to analyze skew');
      }

      setAnalysisResults(data.results || []);
      setStatusMessage('Analysis complete!');
    } catch (err) {
      setStatusMessage(`Error: ${err.message}`);
    } finally {
      setIsAnalyzing(false);
    }
  };

  const handleGatherStats = async () => {
    if (!selectedTable) return;

    try {
      setIsGatheringStats(true);
      setStatusMessage(`Gathering stats for ${selectedTable}...`);

      const response = await fetch(`${API_BASE}/skew-detection/gather-stats`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ tableName: selectedTable, methodOpt })
      });

      const data = await response.json();

      if (!response.ok) {
        throw new Error(data.error || 'Failed to gather stats');
      }

      setStatusMessage(`Stats gathered for ${selectedTable} in ${data.elapsed}s`);

      // Fetch histogram info after gathering stats
      await fetchHistogramInfo(selectedTable);
    } catch (err) {
      setStatusMessage(`Error: ${err.message}`);
    } finally {
      setIsGatheringStats(false);
    }
  };

  const fetchHistogramInfo = async (tableName) => {
    try {
      const response = await fetch(`${API_BASE}/skew-detection/histogram-info?tableName=${tableName}`);
      const data = await response.json();

      if (response.ok) {
        setHistogramInfo(prev => ({
          ...prev,
          [tableName]: data
        }));
      }
    } catch (err) {
      console.error('Error fetching histogram info:', err);
    }
  };

  const getClassificationColor = (classification) => {
    switch (classification) {
      case 'EXTREME': return '#ef4444';
      case 'HIGH': return '#f97316';
      case 'MODERATE': return '#eab308';
      case 'LOW': return '#22c55e';
      default: return 'var(--text-muted)';
    }
  };

  const getClassificationBgColor = (classification) => {
    switch (classification) {
      case 'EXTREME': return 'rgba(239, 68, 68, 0.1)';
      case 'HIGH': return 'rgba(249, 115, 22, 0.1)';
      case 'MODERATE': return 'rgba(234, 179, 8, 0.1)';
      case 'LOW': return 'rgba(34, 197, 94, 0.1)';
      default: return 'transparent';
    }
  };

  if (!dbStatus.connected) {
    return (
      <div className="panel" style={{ height: '100%' }}>
        <div className="panel-header">
          <h2>Skew Detection Demo</h2>
        </div>
        <div className="panel-content">
          <p style={{ color: 'var(--text-muted)' }}>Connect to database first</p>
        </div>
      </div>
    );
  }

  const currentHistInfo = histogramInfo[selectedTable];

  return (
    <div style={{ display: 'flex', gap: '1rem', height: '100%', padding: '1rem' }}>
      {/* Left Panel - Controls */}
      <div style={{
        width: '320px',
        flexShrink: 0,
        background: 'var(--surface)',
        borderRadius: '8px',
        padding: '1rem',
        display: 'flex',
        flexDirection: 'column',
        gap: '1rem',
        overflowY: 'auto'
      }}>
        <h2 style={{ margin: 0, fontSize: '1.1rem', borderBottom: '1px solid var(--border)', paddingBottom: '0.5rem' }}>
          Skew Detection Demo
        </h2>

        {/* Info Box */}
        <div style={{
          padding: '0.75rem',
          background: 'rgba(139, 92, 246, 0.1)',
          border: '1px solid rgba(139, 92, 246, 0.3)',
          borderRadius: '4px',
          fontSize: '0.75rem',
          color: 'var(--text-muted)'
        }}>
          <strong style={{ color: '#8b5cf6' }}>Data Skew Detection</strong>
          <div style={{ marginTop: '0.25rem' }}>
            Analyzes column value distributions to detect skew and recommend optimal DBMS_STATS histogram settings.
          </div>
          <ul style={{ margin: '0.5rem 0 0 0', paddingLeft: '1.2rem' }}>
            <li><strong>Skew Ratio:</strong> MAX/AVG frequency (&gt;2 = skew)</li>
            <li><strong>Max %:</strong> Dominant value percentage (&gt;30% = skew)</li>
            <li><strong>CV%:</strong> Coefficient of variation (&gt;50% = high)</li>
          </ul>
        </div>

        {/* Create/Drop Tables */}
        <div style={{ display: 'flex', gap: '0.5rem' }}>
          <button
            onClick={handleCreateTables}
            disabled={isCreating || tablesCreated}
            className="btn btn-success"
            style={{ flex: 1 }}
          >
            {isCreating ? 'Creating...' : 'Create Test Tables'}
          </button>
          <button
            onClick={handleDropTables}
            disabled={isDropping || !tablesCreated}
            className="btn btn-danger"
            style={{ flex: 1 }}
          >
            {isDropping ? 'Dropping...' : 'Drop Tables'}
          </button>
        </div>

        {/* Progress Bar */}
        {(isCreating || isAnalyzing) && progress >= 0 && (
          <div style={{ width: '100%' }}>
            <div style={{
              height: '8px',
              background: 'var(--bg-primary)',
              borderRadius: '4px',
              overflow: 'hidden'
            }}>
              <div style={{
                height: '100%',
                width: `${progress}%`,
                background: '#8b5cf6',
                transition: 'width 0.3s ease'
              }} />
            </div>
            <div style={{ fontSize: '0.75rem', color: 'var(--text-muted)', marginTop: '0.25rem', textAlign: 'center' }}>
              {progress}%
            </div>
          </div>
        )}

        {/* Analyze Button */}
        <button
          onClick={handleAnalyzeSkew}
          disabled={!tablesCreated || isAnalyzing}
          style={{
            width: '100%',
            padding: '0.75rem',
            background: tablesCreated && !isAnalyzing ? '#8b5cf6' : 'var(--surface)',
            border: 'none',
            borderRadius: '4px',
            color: '#fff',
            cursor: (!tablesCreated || isAnalyzing) ? 'not-allowed' : 'pointer',
            opacity: (!tablesCreated || isAnalyzing) ? 0.5 : 1,
            fontSize: '0.9rem',
            fontWeight: '600'
          }}
        >
          {isAnalyzing ? 'Analyzing...' : 'Analyze Skew'}
        </button>

        {/* Status Message */}
        {statusMessage && (
          <div style={{
            padding: '0.5rem',
            background: 'var(--bg-primary)',
            borderRadius: '4px',
            fontSize: '0.8rem',
            color: statusMessage.startsWith('Error') ? 'var(--accent-danger)' : 'var(--text-muted)'
          }}>
            {statusMessage}
          </div>
        )}

        {/* Gather Statistics Section */}
        {tablesCreated && (
          <div style={{
            padding: '0.75rem',
            background: 'rgba(59, 130, 246, 0.1)',
            border: '1px solid rgba(59, 130, 246, 0.3)',
            borderRadius: '4px'
          }}>
            <h4 style={{ margin: '0 0 0.5rem 0', fontSize: '0.85rem', color: '#3b82f6' }}>Gather Statistics</h4>

            {/* Table Selection */}
            <div style={{ marginBottom: '0.5rem' }}>
              <label style={{ fontSize: '0.75rem', color: 'var(--text-muted)' }}>Table:</label>
              <select
                value={selectedTable}
                onChange={(e) => setSelectedTable(e.target.value)}
                disabled={isGatheringStats}
                style={{
                  width: '100%',
                  padding: '0.4rem',
                  background: 'var(--bg-primary)',
                  border: '1px solid var(--border)',
                  borderRadius: '4px',
                  color: 'var(--text-primary)',
                  fontSize: '0.8rem',
                  marginTop: '0.25rem'
                }}
              >
                {testTables.map(t => (
                  <option key={t.name} value={t.name}>{t.name}</option>
                ))}
              </select>
            </div>

            {/* METHOD_OPT Selection */}
            <div style={{ marginBottom: '0.5rem' }}>
              <label style={{ fontSize: '0.75rem', color: 'var(--text-muted)' }}>METHOD_OPT:</label>
              <select
                value={methodOpt}
                onChange={(e) => setMethodOpt(e.target.value)}
                disabled={isGatheringStats}
                style={{
                  width: '100%',
                  padding: '0.4rem',
                  background: 'var(--bg-primary)',
                  border: '1px solid var(--border)',
                  borderRadius: '4px',
                  color: 'var(--text-primary)',
                  fontSize: '0.8rem',
                  marginTop: '0.25rem'
                }}
              >
                <option value="SIZE AUTO">SIZE AUTO</option>
                <option value="SIZE 254">SIZE 254</option>
                <option value="SIZE 1">SIZE 1 (No histograms)</option>
              </select>
            </div>

            <button
              onClick={handleGatherStats}
              disabled={isGatheringStats || !selectedTable}
              style={{
                width: '100%',
                padding: '0.5rem',
                background: isGatheringStats ? 'var(--surface)' : '#3b82f6',
                border: 'none',
                borderRadius: '4px',
                color: '#fff',
                cursor: isGatheringStats ? 'not-allowed' : 'pointer',
                opacity: isGatheringStats ? 0.5 : 1,
                fontSize: '0.85rem',
                fontWeight: '500'
              }}
            >
              {isGatheringStats ? 'Gathering...' : 'DBMS_STATS.GATHER_TABLE_STATS'}
            </button>

            {/* Histogram Info Summary */}
            {currentHistInfo && currentHistInfo.tableStats && (
              <div style={{ marginTop: '0.5rem', fontSize: '0.75rem' }}>
                <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                  <span>Rows:</span>
                  <span style={{ fontWeight: '600' }}>{(currentHistInfo.tableStats.numRows || 0).toLocaleString()}</span>
                </div>
                <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                  <span>Blocks:</span>
                  <span style={{ fontWeight: '600' }}>{(currentHistInfo.tableStats.blocks || 0).toLocaleString()}</span>
                </div>
              </div>
            )}
          </div>
        )}

        {/* Test Tables Info */}
        {testTables.length > 0 && (
          <div style={{
            padding: '0.75rem',
            background: 'var(--bg-primary)',
            borderRadius: '4px'
          }}>
            <h4 style={{ margin: '0 0 0.5rem 0', fontSize: '0.85rem', color: 'var(--text-muted)' }}>Test Tables</h4>
            {testTables.map(t => (
              <div key={t.name} style={{
                display: 'flex',
                justifyContent: 'space-between',
                fontSize: '0.75rem',
                padding: '0.25rem 0',
                borderBottom: '1px solid var(--border)'
              }}>
                <span>{t.name.replace('SKEW_TEST_', '')}</span>
                <span style={{ color: 'var(--text-muted)' }}>{t.rows.toLocaleString()} rows</span>
              </div>
            ))}
          </div>
        )}
      </div>

      {/* Right Panel - Results */}
      <div style={{ flex: 1, display: 'flex', flexDirection: 'column', gap: '1rem', overflowY: 'auto' }}>
        {/* Analysis Results Table */}
        {analysisResults.length > 0 && (
          <div style={{
            background: 'var(--surface)',
            borderRadius: '8px',
            padding: '1rem'
          }}>
            <h3 style={{ margin: '0 0 0.75rem 0', fontSize: '0.9rem', color: '#8b5cf6' }}>
              Skew Analysis Results
            </h3>
            <div style={{ overflowX: 'auto' }}>
              <table style={{
                width: '100%',
                borderCollapse: 'collapse',
                fontSize: '0.8rem'
              }}>
                <thead>
                  <tr style={{ borderBottom: '2px solid var(--border)' }}>
                    <th style={{ padding: '0.5rem', textAlign: 'left', color: 'var(--text-muted)' }}>Table</th>
                    <th style={{ padding: '0.5rem', textAlign: 'left', color: 'var(--text-muted)' }}>Column</th>
                    <th style={{ padding: '0.5rem', textAlign: 'right', color: 'var(--text-muted)' }}>Distinct</th>
                    <th style={{ padding: '0.5rem', textAlign: 'right', color: 'var(--text-muted)' }}>Skew Ratio</th>
                    <th style={{ padding: '0.5rem', textAlign: 'right', color: 'var(--text-muted)' }}>Max %</th>
                    <th style={{ padding: '0.5rem', textAlign: 'right', color: 'var(--text-muted)' }}>CV%</th>
                    <th style={{ padding: '0.5rem', textAlign: 'left', color: 'var(--text-muted)' }}>Max Value</th>
                    <th style={{ padding: '0.5rem', textAlign: 'center', color: 'var(--text-muted)' }}>Classification</th>
                    <th style={{ padding: '0.5rem', textAlign: 'left', color: 'var(--text-muted)' }}>Recommendation</th>
                  </tr>
                </thead>
                <tbody>
                  {analysisResults.map((row, idx) => (
                    <tr key={`${row.tableName}-${row.columnName}`} style={{
                      borderBottom: '1px solid var(--border)',
                      background: getClassificationBgColor(row.classification)
                    }}>
                      <td style={{ padding: '0.4rem 0.5rem', fontWeight: '500' }}>
                        {row.tableName.replace('SKEW_TEST_', '')}
                      </td>
                      <td style={{ padding: '0.4rem 0.5rem' }}>{row.columnName}</td>
                      <td style={{ padding: '0.4rem 0.5rem', textAlign: 'right' }}>{row.distinctValues}</td>
                      <td style={{
                        padding: '0.4rem 0.5rem',
                        textAlign: 'right',
                        fontWeight: row.skewRatio > 2 ? '600' : '400',
                        color: row.skewRatio > 10 ? '#ef4444' : row.skewRatio > 5 ? '#f97316' : row.skewRatio > 2 ? '#eab308' : 'inherit'
                      }}>
                        {row.skewRatio.toFixed(2)}
                      </td>
                      <td style={{
                        padding: '0.4rem 0.5rem',
                        textAlign: 'right',
                        fontWeight: row.maxValuePercent > 30 ? '600' : '400',
                        color: row.maxValuePercent > 80 ? '#ef4444' : row.maxValuePercent > 50 ? '#f97316' : row.maxValuePercent > 30 ? '#eab308' : 'inherit'
                      }}>
                        {row.maxValuePercent.toFixed(1)}%
                      </td>
                      <td style={{
                        padding: '0.4rem 0.5rem',
                        textAlign: 'right',
                        color: row.cvPercent > 100 ? '#ef4444' : row.cvPercent > 50 ? '#f97316' : 'inherit'
                      }}>
                        {row.cvPercent.toFixed(1)}%
                      </td>
                      <td style={{ padding: '0.4rem 0.5rem', maxWidth: '100px', overflow: 'hidden', textOverflow: 'ellipsis' }}>
                        {row.maxValue}
                      </td>
                      <td style={{ padding: '0.4rem 0.5rem', textAlign: 'center' }}>
                        <span style={{
                          display: 'inline-block',
                          padding: '0.2rem 0.5rem',
                          borderRadius: '4px',
                          background: getClassificationColor(row.classification),
                          color: '#fff',
                          fontSize: '0.7rem',
                          fontWeight: '600'
                        }}>
                          {row.classification}
                        </span>
                      </td>
                      <td style={{ padding: '0.4rem 0.5rem', fontSize: '0.75rem', color: '#3b82f6' }}>
                        {row.recommendation.methodOpt}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        )}

        {/* Skew Classification Legend */}
        {analysisResults.length > 0 && (
          <div style={{
            background: 'var(--surface)',
            borderRadius: '8px',
            padding: '1rem'
          }}>
            <h3 style={{ margin: '0 0 0.75rem 0', fontSize: '0.9rem', color: 'var(--text-muted)' }}>
              Classification Guide
            </h3>
            <div style={{ display: 'flex', gap: '2rem', flexWrap: 'wrap' }}>
              <div>
                <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', marginBottom: '0.5rem' }}>
                  <span style={{
                    display: 'inline-block',
                    width: '12px',
                    height: '12px',
                    borderRadius: '2px',
                    background: '#ef4444'
                  }} />
                  <span style={{ fontSize: '0.8rem' }}><strong>EXTREME</strong>: Ratio &gt;10 or Max% &gt;80%</span>
                </div>
                <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', marginBottom: '0.5rem' }}>
                  <span style={{
                    display: 'inline-block',
                    width: '12px',
                    height: '12px',
                    borderRadius: '2px',
                    background: '#f97316'
                  }} />
                  <span style={{ fontSize: '0.8rem' }}><strong>HIGH</strong>: Ratio &gt;5 or Max% &gt;50%</span>
                </div>
              </div>
              <div>
                <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', marginBottom: '0.5rem' }}>
                  <span style={{
                    display: 'inline-block',
                    width: '12px',
                    height: '12px',
                    borderRadius: '2px',
                    background: '#eab308'
                  }} />
                  <span style={{ fontSize: '0.8rem' }}><strong>MODERATE</strong>: Ratio &gt;2 or Max% &gt;30%</span>
                </div>
                <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', marginBottom: '0.5rem' }}>
                  <span style={{
                    display: 'inline-block',
                    width: '12px',
                    height: '12px',
                    borderRadius: '2px',
                    background: '#22c55e'
                  }} />
                  <span style={{ fontSize: '0.8rem' }}><strong>LOW</strong>: Evenly distributed data</span>
                </div>
              </div>
            </div>
          </div>
        )}

        {/* Histogram Info for Selected Table */}
        {currentHistInfo && currentHistInfo.histogramInfo && currentHistInfo.histogramInfo.length > 0 && (
          <div style={{
            background: 'var(--surface)',
            borderRadius: '8px',
            padding: '1rem'
          }}>
            <h3 style={{ margin: '0 0 0.75rem 0', fontSize: '0.9rem', color: '#3b82f6' }}>
              Histogram Info: {selectedTable.replace('SKEW_TEST_', '')}
            </h3>
            <div style={{ overflowX: 'auto' }}>
              <table style={{
                width: '100%',
                borderCollapse: 'collapse',
                fontSize: '0.8rem'
              }}>
                <thead>
                  <tr style={{ borderBottom: '2px solid var(--border)' }}>
                    <th style={{ padding: '0.5rem', textAlign: 'left', color: 'var(--text-muted)' }}>Column</th>
                    <th style={{ padding: '0.5rem', textAlign: 'right', color: 'var(--text-muted)' }}>Distinct</th>
                    <th style={{ padding: '0.5rem', textAlign: 'right', color: 'var(--text-muted)' }}>Nulls</th>
                    <th style={{ padding: '0.5rem', textAlign: 'right', color: 'var(--text-muted)' }}>Buckets</th>
                    <th style={{ padding: '0.5rem', textAlign: 'left', color: 'var(--text-muted)' }}>Histogram Type</th>
                    <th style={{ padding: '0.5rem', textAlign: 'right', color: 'var(--text-muted)' }}>Density</th>
                  </tr>
                </thead>
                <tbody>
                  {currentHistInfo.histogramInfo.map((col, idx) => (
                    <tr key={col.columnName} style={{
                      borderBottom: '1px solid var(--border)',
                      background: idx % 2 === 0 ? 'transparent' : 'rgba(255,255,255,0.02)'
                    }}>
                      <td style={{ padding: '0.4rem 0.5rem', fontWeight: '500' }}>{col.columnName}</td>
                      <td style={{ padding: '0.4rem 0.5rem', textAlign: 'right' }}>{(col.numDistinct || 0).toLocaleString()}</td>
                      <td style={{ padding: '0.4rem 0.5rem', textAlign: 'right' }}>{(col.numNulls || 0).toLocaleString()}</td>
                      <td style={{ padding: '0.4rem 0.5rem', textAlign: 'right', color: col.numBuckets > 1 ? '#3b82f6' : 'var(--text-muted)' }}>
                        {col.numBuckets || 0}
                      </td>
                      <td style={{
                        padding: '0.4rem 0.5rem',
                        color: col.histogramType === 'NONE' ? 'var(--text-muted)' :
                               col.histogramType === 'FREQUENCY' ? '#10b981' :
                               col.histogramType === 'HEIGHT BALANCED' ? '#f59e0b' :
                               col.histogramType === 'HYBRID' ? '#a855f7' : '#3b82f6'
                      }}>
                        {col.histogramType || 'NONE'}
                      </td>
                      <td style={{ padding: '0.4rem 0.5rem', textAlign: 'right' }}>
                        {col.density ? col.density.toExponential(2) : '-'}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        )}

        {/* Empty State */}
        {!tablesCreated && (
          <div style={{
            background: 'var(--surface)',
            borderRadius: '8px',
            padding: '2rem',
            textAlign: 'center',
            color: 'var(--text-muted)'
          }}>
            <h3 style={{ margin: '0 0 1rem 0', fontSize: '1.1rem', color: '#8b5cf6' }}>
              Skew Detection Demo
            </h3>
            <p>Click "Create Test Tables" to generate 4 test tables with various skew patterns:</p>
            <ul style={{ textAlign: 'left', maxWidth: '500px', margin: '1rem auto', paddingLeft: '2rem' }}>
              <li><strong>ORDERS</strong>: 100K rows with EXTREME (STATUS), NONE (ORDER_TYPE), MODERATE (REGION) skew</li>
              <li><strong>AUDIT_LOGS</strong>: 50K rows with EXTREME skew (99% INFO, 99% LOW)</li>
              <li><strong>PRODUCTS</strong>: 20K rows with evenly distributed data (NO skew)</li>
              <li><strong>TRANSACTIONS</strong>: 80K rows with HIGH skew (90% USD, 85% USA)</li>
            </ul>
          </div>
        )}

        {tablesCreated && analysisResults.length === 0 && (
          <div style={{
            background: 'var(--surface)',
            borderRadius: '8px',
            padding: '2rem',
            textAlign: 'center',
            color: 'var(--text-muted)'
          }}>
            <p>Test tables created. Click "Analyze Skew" to detect skew patterns in all columns.</p>
          </div>
        )}
      </div>
    </div>
  );
}

export default SkewDetectionPanel;
