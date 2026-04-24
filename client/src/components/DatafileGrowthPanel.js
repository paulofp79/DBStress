import React, { useEffect, useMemo, useState } from 'react';
import axios from 'axios';

const getServerUrl = () => {
  if (window.location.hostname !== 'localhost') {
    return `http://${window.location.host}`;
  }
  return 'http://localhost:3001';
};

const API_BASE = `${getServerUrl()}/api`;

function DatafileGrowthPanel({ dbStatus, socket, onSuccess, onError }) {
  const [snapshot, setSnapshot] = useState({ tablespaces: [], scheduler: { schedules: [] } });
  const [loading, setLoading] = useState(false);
  const [selectedFileNames, setSelectedFileNames] = useState([]);
  const [config, setConfig] = useState({
    incrementMb: 100,
    intervalSeconds: 60
  });

  const loadStatus = async () => {
    if (!dbStatus.connected) {
      return;
    }

    try {
      const response = await axios.get(`${API_BASE}/datafiles/status`);
      if (response.data.success) {
        setSnapshot({
          tablespaces: response.data.tablespaces || [],
          source: response.data.source,
          scheduler: response.data.scheduler || { schedules: [] }
        });
      }
    } catch (err) {
      onError?.(err.response?.data?.message || 'Failed to load tablespaces/datafiles');
    }
  };

  useEffect(() => {
    if (dbStatus.connected) {
      loadStatus();
    } else {
      setSnapshot({ tablespaces: [], scheduler: { schedules: [] } });
      setSelectedFileNames([]);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [dbStatus.connected]);

  useEffect(() => {
    if (!dbStatus.connected) {
      return undefined;
    }

    const interval = setInterval(() => {
      loadStatus();
    }, 5000);

    return () => clearInterval(interval);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [dbStatus.connected]);

  useEffect(() => {
    if (!socket) {
      return undefined;
    }

    const handleStatus = () => {
      loadStatus();
    };

    socket.on('datafile-growth-status', handleStatus);
    return () => {
      socket.off('datafile-growth-status', handleStatus);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [socket, dbStatus.connected]);

  const datafiles = useMemo(() => (
    (snapshot.tablespaces || []).flatMap((tablespace) => (
      (tablespace.datafiles || []).map((datafile) => ({
        ...datafile,
        tablespaceName: tablespace.tablespaceName
      }))
    ))
  ), [snapshot.tablespaces]);

  useEffect(() => {
    if (selectedFileNames.length === 0 && datafiles.length > 0) {
      setSelectedFileNames([datafiles[0].fileName]);
    }
  }, [datafiles, selectedFileNames]);

  useEffect(() => {
    setSelectedFileNames((prev) => prev.filter((fileName) => datafiles.some((item) => item.fileName === fileName)));
  }, [datafiles]);

  const schedulesByFile = useMemo(() => (
    Object.fromEntries((snapshot.scheduler?.schedules || []).map((schedule) => [schedule.fileName, schedule]))
  ), [snapshot.scheduler]);

  const selectedDatafiles = useMemo(() => (
    datafiles.filter((item) => selectedFileNames.includes(item.fileName))
  ), [datafiles, selectedFileNames]);

  const selectedDatafile = selectedDatafiles[0] || null;

  const toggleFileSelection = (fileName) => {
    setSelectedFileNames((prev) => (
      prev.includes(fileName)
        ? prev.filter((item) => item !== fileName)
        : [...prev, fileName]
    ));
  };

  const handleSelectAll = () => {
    setSelectedFileNames(datafiles.map((item) => item.fileName));
  };

  const handleClearSelection = () => {
    setSelectedFileNames([]);
  };

  const handleStart = async () => {
    if (selectedDatafiles.length === 0) {
      onError?.('Choose at least one datafile first.');
      return;
    }

    setLoading(true);
    try {
      for (const datafile of selectedDatafiles) {
        const response = await axios.post(`${API_BASE}/datafiles/growth/start`, {
          fileName: datafile.fileName,
          tablespaceName: datafile.tablespaceName,
          incrementMb: Number(config.incrementMb),
          intervalSeconds: Number(config.intervalSeconds)
        });
        if (!response.data.success) {
          throw new Error(`Failed to start schedule for ${datafile.fileName}`);
        }
      }
      onSuccess?.(`Scheduled ${selectedDatafiles.length} datafile(s) to grow by ${config.incrementMb} MB every ${config.intervalSeconds} seconds`);
      await loadStatus();
    } catch (err) {
      onError?.(err.response?.data?.message || 'Failed to start datafile growth schedule');
    } finally {
      setLoading(false);
    }
  };

  const handleStop = async (fileNames = selectedFileNames) => {
    const list = Array.isArray(fileNames) ? fileNames.filter(Boolean) : [fileNames].filter(Boolean);
    if (list.length === 0) {
      return;
    }

    setLoading(true);
    try {
      for (const fileName of list) {
        const response = await axios.post(`${API_BASE}/datafiles/growth/stop`, { fileName });
        if (!response.data.success) {
          throw new Error(`Failed to stop schedule for ${fileName}`);
        }
      }
      onSuccess?.(`Stopped ${list.length} datafile growth schedule(s)`);
      await loadStatus();
    } catch (err) {
      onError?.(err.response?.data?.message || 'Failed to stop datafile growth schedule');
    } finally {
      setLoading(false);
    }
  };

  const handleStopAll = async () => {
    setLoading(true);
    try {
      const response = await axios.post(`${API_BASE}/datafiles/growth/stop-all`, {});
      if (response.data.success) {
        onSuccess?.('Stopped all datafile growth schedules');
        await loadStatus();
      }
    } catch (err) {
      onError?.(err.response?.data?.message || 'Failed to stop datafile growth schedules');
    } finally {
      setLoading(false);
    }
  };

  if (!dbStatus.connected) {
    return (
      <div className="panel">
        <div className="panel-header">
          <h2>Datafile Growth</h2>
        </div>
        <div className="panel-content">
          <p style={{ color: 'var(--text-muted)' }}>Connect to Oracle first.</p>
        </div>
      </div>
    );
  }

  return (
    <div className="panel">
      <div className="panel-header">
        <h2>Datafile Growth Scheduler</h2>
        <button className="btn btn-secondary btn-sm" type="button" onClick={loadStatus} disabled={loading}>
          Refresh
        </button>
      </div>
      <div className="panel-content">
        <p style={{ marginTop: 0, color: 'var(--text-muted)' }}>
          Review tablespaces and datafiles, then start a timed resize schedule such as growing one or more selected datafiles by `100 MB` every `60` seconds.
        </p>

        <div style={{
          background: 'var(--surface)',
          border: '1px solid var(--border)',
          borderRadius: '8px',
          padding: '0.9rem'
        }}>
          <div style={{ display: 'grid', gridTemplateColumns: 'minmax(0, 2fr) repeat(2, minmax(0, 1fr))', gap: '0.85rem' }}>
            <div className="form-group">
              <label htmlFor="df-file-select">Selected Datafiles</label>
              <select
                id="df-file-select"
                multiple
                value={selectedFileNames}
                onChange={(e) => setSelectedFileNames(Array.from(e.target.selectedOptions).map((option) => option.value))}
                disabled={loading}
                style={{ minHeight: '140px' }}
              >
                {datafiles.map((datafile) => (
                  <option key={datafile.fileName} value={datafile.fileName}>
                    {datafile.tablespaceName} - {datafile.fileName}
                  </option>
                ))}
              </select>
            </div>
            <div className="form-group">
              <label htmlFor="df-increment-mb">Increment (MB)</label>
              <input
                id="df-increment-mb"
                type="number"
                min="1"
                value={config.incrementMb}
                onChange={(e) => setConfig((prev) => ({ ...prev, incrementMb: e.target.value }))}
                disabled={loading}
              />
            </div>
            <div className="form-group">
              <label htmlFor="df-interval-seconds">Interval (seconds)</label>
              <input
                id="df-interval-seconds"
                type="number"
                min="1"
                value={config.intervalSeconds}
                onChange={(e) => setConfig((prev) => ({ ...prev, intervalSeconds: e.target.value }))}
                disabled={loading}
              />
            </div>
          </div>

          {selectedDatafiles.length > 0 && (
            <div style={{ marginTop: '0.35rem', fontSize: '0.8rem', color: 'var(--text-muted)' }}>
              {selectedDatafiles.length} selected
              {selectedDatafile ? ` | First selected size: ${selectedDatafile.sizeMb} MB` : ''}
              {selectedDatafile?.maxSizeMb > 0 ? ` | First selected max: ${selectedDatafile.maxSizeMb} MB` : ''}
            </div>
          )}

          <div className="btn-group" style={{ marginTop: '0.75rem' }}>
            <button className="btn btn-secondary btn-sm" type="button" onClick={handleSelectAll} disabled={loading || datafiles.length === 0}>
              Select All
            </button>
            <button className="btn btn-secondary btn-sm" type="button" onClick={handleClearSelection} disabled={loading || selectedFileNames.length === 0}>
              Clear Selection
            </button>
          </div>

          <div className="btn-group" style={{ marginTop: '1rem' }}>
            <button className="btn btn-primary" type="button" onClick={handleStart} disabled={loading || selectedDatafiles.length === 0}>
              Start Schedule
            </button>
            <button
              className="btn btn-danger"
              type="button"
              onClick={() => handleStop()}
              disabled={loading || selectedFileNames.length === 0 || !selectedFileNames.some((fileName) => schedulesByFile[fileName])}
            >
              Stop Selected
            </button>
            <button className="btn btn-secondary" type="button" onClick={handleStopAll} disabled={loading || (snapshot.scheduler?.schedules || []).length === 0}>
              Stop All
            </button>
          </div>
        </div>

        <div style={{ marginTop: '1rem', display: 'grid', gap: '1rem' }}>
          <div style={{
            background: 'var(--surface)',
            border: '1px solid var(--border)',
            borderRadius: '8px',
            padding: '0.9rem'
          }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', gap: '1rem', alignItems: 'center', flexWrap: 'wrap' }}>
              <h3 style={{ margin: 0 }}>Active Schedules</h3>
              <span style={{ fontSize: '0.8rem', color: 'var(--text-muted)' }}>
                {(snapshot.scheduler?.schedules || []).length} active
              </span>
            </div>

            {(snapshot.scheduler?.schedules || []).length > 0 ? (
              <div style={{ marginTop: '0.85rem', display: 'grid', gap: '0.75rem' }}>
                {snapshot.scheduler.schedules.map((schedule) => (
                  <div
                    key={schedule.fileName}
                    style={{
                      border: '1px solid var(--border)',
                      borderRadius: '8px',
                      padding: '0.75rem'
                    }}
                  >
                    <div style={{ fontWeight: 600 }}>{schedule.tablespaceName}</div>
                    <div style={{ marginTop: '0.25rem', fontSize: '0.8rem', color: 'var(--text-muted)', wordBreak: 'break-all' }}>
                      {schedule.fileName}
                    </div>
                    <div style={{ marginTop: '0.5rem', display: 'grid', gap: '0.25rem', fontSize: '0.9rem' }}>
                      <div>Grow by: <strong>{schedule.incrementMb} MB</strong></div>
                      <div>Interval: <strong>{schedule.intervalSeconds}s</strong></div>
                      <div>Resize count: <strong>{schedule.resizeCount}</strong></div>
                      <div>Current size: <strong>{schedule.currentSizeMb || 0} MB</strong></div>
                      <div>Next run: <strong>{schedule.nextRunAt ? new Date(schedule.nextRunAt).toLocaleTimeString() : '-'}</strong></div>
                      {schedule.lastError && (
                        <div style={{ color: 'var(--accent-danger)' }}>Last error: {schedule.lastError}</div>
                      )}
                    </div>
                    <button className="btn btn-danger btn-sm" type="button" style={{ marginTop: '0.75rem' }} onClick={() => handleStop(schedule.fileName)} disabled={loading}>
                      Stop
                    </button>
                  </div>
                ))}
              </div>
            ) : (
              <p style={{ marginTop: '0.85rem', marginBottom: 0, color: 'var(--text-muted)' }}>
                No active growth schedules.
              </p>
            )}
          </div>

          <div style={{
            background: 'var(--surface)',
            border: '1px solid var(--border)',
            borderRadius: '8px',
            padding: '0.9rem'
          }}>
            <h3 style={{ marginTop: 0 }}>Tablespaces and Datafiles</h3>
            <div style={{ display: 'grid', gap: '1rem' }}>
              {(snapshot.tablespaces || []).map((tablespace) => (
                <div
                  key={tablespace.tablespaceName}
                  style={{
                    border: '1px solid var(--border)',
                    borderRadius: '8px',
                    padding: '0.9rem'
                  }}
                >
                  <div style={{ display: 'flex', justifyContent: 'space-between', gap: '1rem', alignItems: 'center', flexWrap: 'wrap' }}>
                    <div>
                      <strong>{tablespace.tablespaceName}</strong>
                      <div style={{ marginTop: '0.25rem', fontSize: '0.8rem', color: 'var(--text-muted)' }}>
                        Status: {tablespace.status} | Contents: {tablespace.contents || '-'} | Bigfile: {tablespace.bigfile || '-'}
                      </div>
                    </div>
                    <span style={{ fontSize: '0.8rem', color: 'var(--text-muted)' }}>
                      {tablespace.datafiles.length} datafile(s)
                    </span>
                  </div>

                  <div style={{ marginTop: '0.85rem', overflowX: 'auto' }}>
                    <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.85rem' }}>
                      <thead>
                        <tr style={{ borderBottom: '1px solid var(--border)' }}>
                          <th style={{ textAlign: 'center', padding: '0.45rem' }}>Pick</th>
                          <th style={{ textAlign: 'left', padding: '0.45rem' }}>File</th>
                          <th style={{ textAlign: 'right', padding: '0.45rem' }}>Size MB</th>
                          <th style={{ textAlign: 'right', padding: '0.45rem' }}>Max MB</th>
                          <th style={{ textAlign: 'center', padding: '0.45rem' }}>Autoextend</th>
                          <th style={{ textAlign: 'center', padding: '0.45rem' }}>Schedule</th>
                        </tr>
                      </thead>
                      <tbody>
                        {tablespace.datafiles.map((datafile) => {
                          const schedule = schedulesByFile[datafile.fileName];

                          return (
                            <tr key={datafile.fileName} style={{ borderBottom: '1px solid var(--border)' }}>
                              <td style={{ padding: '0.45rem', textAlign: 'center' }}>
                                <input
                                  type="checkbox"
                                  checked={selectedFileNames.includes(datafile.fileName)}
                                  onChange={() => toggleFileSelection(datafile.fileName)}
                                  disabled={loading}
                                />
                              </td>
                              <td style={{ padding: '0.45rem', wordBreak: 'break-all' }}>{datafile.fileName}</td>
                              <td style={{ padding: '0.45rem', textAlign: 'right' }}>{datafile.sizeMb}</td>
                              <td style={{ padding: '0.45rem', textAlign: 'right' }}>{datafile.maxSizeMb}</td>
                              <td style={{ padding: '0.45rem', textAlign: 'center' }}>{datafile.autoextensible ? 'Yes' : 'No'}</td>
                              <td style={{ padding: '0.45rem', textAlign: 'center' }}>
                                {schedule ? `${schedule.incrementMb} MB / ${schedule.intervalSeconds}s` : 'Stopped'}
                              </td>
                            </tr>
                          );
                        })}
                      </tbody>
                    </table>
                  </div>
                </div>
              ))}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

export default DatafileGrowthPanel;
