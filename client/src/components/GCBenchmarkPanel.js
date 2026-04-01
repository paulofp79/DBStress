import React, { useEffect, useState } from 'react';

const getServerUrl = () => {
  if (window.location.hostname !== 'localhost') {
    return `http://${window.location.host}`;
  }
  return 'http://localhost:3001';
};

const API_BASE = `${getServerUrl()}/api`;

function GCBenchmarkPanel() {
  const [status, setStatus] = useState({
    loading: true,
    reachable: false,
    targetUrl: 'http://localhost:8000',
    message: ''
  });

  const loadStatus = async () => {
    setStatus(prev => ({ ...prev, loading: true }));
    try {
      const response = await fetch(`${API_BASE}/gc-benchmark/status`);
      const data = await response.json();
      setStatus({
        loading: false,
        reachable: !!data.reachable,
        targetUrl: data.targetUrl || 'http://localhost:8000',
        message: data.message || ''
      });
    } catch (err) {
      setStatus(prev => ({
        ...prev,
        loading: false,
        reachable: false,
        message: err.message || 'Could not reach GC benchmark service'
      }));
    }
  };

  useEffect(() => {
    loadStatus();
  }, []);

  return (
    <div className="panel" style={{ minHeight: 'calc(100vh - 210px)' }}>
      <div className="panel-header">
        <h2>GC Benchmark</h2>
        <div style={{ display: 'flex', gap: '0.5rem' }}>
          <button className="btn btn-secondary btn-sm" onClick={loadStatus}>
            Refresh Status
          </button>
          <a
            className="btn btn-primary btn-sm"
            href={status.targetUrl}
            target="_blank"
            rel="noreferrer"
            style={{ textDecoration: 'none' }}
          >
            Open Standalone
          </a>
        </div>
      </div>

      <div className="panel-content" style={{ display: 'grid', gap: '1rem' }}>
        <div style={{
          padding: '0.9rem 1rem',
          borderRadius: '8px',
          border: '1px solid var(--border-color)',
          background: 'var(--bg-tertiary)',
          color: 'var(--text-secondary)',
          fontSize: '0.9rem'
        }}>
          Embedded standalone tool from `/home/paportug/DBStress/gc_benchmark`.
          Server target: <strong style={{ color: 'var(--text-primary)' }}> {status.targetUrl}</strong>
        </div>

        {status.loading && (
          <div className="alert alert-success">Checking GC benchmark service...</div>
        )}

        {!status.loading && !status.reachable && (
          <div style={{
            display: 'grid',
            gap: '1rem',
            padding: '1.25rem',
            borderRadius: '10px',
            border: '1px solid rgba(245, 158, 11, 0.35)',
            background: 'rgba(245, 158, 11, 0.08)'
          }}>
            <div style={{ color: '#fbbf24', fontWeight: 600 }}>
              GC benchmark service is not running on port 8000.
            </div>
            <div style={{ color: 'var(--text-secondary)', fontSize: '0.9rem' }}>
              Start it from `/home/paportug/DBStress/gc_benchmark/gc_benchmark` with:
            </div>
            <pre style={{
              margin: 0,
              padding: '1rem',
              borderRadius: '8px',
              background: 'var(--bg-primary)',
              color: 'var(--text-primary)',
              overflowX: 'auto'
            }}>
cd /home/paportug/DBStress/gc_benchmark/gc_benchmark
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
            </pre>
            {status.message && (
              <div style={{ color: 'var(--text-muted)', fontSize: '0.85rem' }}>
                Last check: {status.message}
              </div>
            )}
          </div>
        )}

        {!status.loading && status.reachable && (
          <div style={{
            height: 'calc(100vh - 360px)',
            minHeight: '720px',
            border: '1px solid var(--border-color)',
            borderRadius: '10px',
            overflow: 'hidden',
            background: '#0b1020'
          }}>
            <iframe
              title="GC Benchmark"
              src={status.targetUrl}
              style={{
                width: '100%',
                height: '100%',
                border: 'none',
                background: '#0b1020'
              }}
            />
          </div>
        )}
      </div>
    </div>
  );
}

export default GCBenchmarkPanel;
