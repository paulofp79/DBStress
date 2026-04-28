import React, { useEffect, useState } from 'react';

const getServerUrl = () => {
  if (window.location.hostname !== 'localhost') {
    return `http://${window.location.host}`;
  }
  return 'http://localhost:3001';
};

const API_BASE = `${getServerUrl()}/api`;

function ServiceCard({ title, port, url, reachable, description, actions = [] }) {
  return (
    <div className="launcher-card">
      <div className="launcher-card__top">
        <div>
          <div className="launcher-card__title">{title}</div>
          <div className="launcher-card__desc">{description}</div>
        </div>
        <div className={`launcher-badge ${reachable ? 'up' : 'down'}`}>
          {reachable ? 'Running' : 'Down'}
        </div>
      </div>

      <div className="launcher-meta">
        <span>Port</span>
        <strong>{port}</strong>
      </div>
      <div className="launcher-meta">
        <span>URL</span>
        <strong>{url}</strong>
      </div>

      <div className="launcher-actions">
        {actions.map(action => (
          action.href ? (
            <a
              key={action.label}
              className="btn btn-primary"
              href={action.href}
              target={action.target || '_self'}
              rel={action.target === '_blank' ? 'noreferrer' : undefined}
              style={{ color: 'inherit', textDecoration: 'none' }}
            >
              {action.label}
            </a>
          ) : (
            <button
              key={action.label}
              className="btn btn-secondary"
              onClick={action.onClick}
              type="button"
            >
                {action.label}
            </button>
          )
        ))}
      </div>
    </div>
  );
}

function HomePanel({ onOpenTab }) {
  const [status, setStatus] = useState({
    loading: true,
    gcBenchmarkReachable: false,
    gcBenchmarkUrl: 'http://localhost:8000'
  });

  const loadStatus = async () => {
    setStatus(prev => ({ ...prev, loading: true }));
    try {
      const [healthResp, gcResp] = await Promise.all([
        fetch(`${API_BASE}/health`),
        fetch(`${API_BASE}/gc-benchmark/status`)
      ]);
      const health = await healthResp.json();
      const gc = await gcResp.json();

      setStatus({
        loading: false,
        dbStressReachable: health.status === 'ok',
        gcBenchmarkReachable: !!gc.reachable,
        gcBenchmarkUrl: gc.targetUrl || 'http://localhost:8000'
      });
    } catch (err) {
      setStatus({
        loading: false,
        dbStressReachable: true,
        gcBenchmarkReachable: false,
        gcBenchmarkUrl: 'http://localhost:8000'
      });
    }
  };

  useEffect(() => {
    loadStatus();
    const timer = setInterval(loadStatus, 5000);
    return () => clearInterval(timer);
  }, []);

  return (
    <div style={{ display: 'grid', gap: '1.25rem' }}>
      <div className="panel">
        <div className="panel-header">
          <h2>Launcher</h2>
          <button className="btn btn-secondary btn-sm" onClick={loadStatus} type="button">
            {status.loading ? 'Checking...' : 'Refresh'}
          </button>
        </div>
        <div className="panel-content">
          <div className="launcher-hero">
            <div>
              <h3>DBStress Home</h3>
              <p>Use this page as the entrypoint for all local tools. Start both services with `npm run start-all` and stop them with `npm run stop-all`.</p>
            </div>
          </div>

          <div className="launcher-grid">
            <ServiceCard
              title="DBStress"
              description="Main launcher, schema tools, contention demos, and embedded tool access."
              port="3001"
              url="http://localhost:3001"
              reachable={status.dbStressReachable !== false}
              actions={[
                { label: 'Open Monitor', onClick: () => onOpenTab('monitor') },
                { label: 'Open Stress Tab', onClick: () => onOpenTab('stress') },
                { label: 'Open SOE Tool', onClick: () => onOpenTab('swingbench-soe') },
                { label: 'Open COBOL SOE', onClick: () => onOpenTab('cobol-soe') },
                { label: 'Open GC Benchmark Tab', onClick: () => onOpenTab('gc-benchmark') }
              ]}
            />

            <ServiceCard
              title="GC Benchmark"
              description="Standalone FastAPI tool. Available embedded in DBStress or directly on its own port."
              port="8000"
              url={status.gcBenchmarkUrl}
              reachable={status.gcBenchmarkReachable}
              actions={[
                { label: 'Open Embedded', onClick: () => onOpenTab('gc-benchmark') },
                { label: 'Open Standalone', href: status.gcBenchmarkUrl, target: '_blank' }
              ]}
            />
          </div>

          <div className="launcher-note">
            Launcher commands:
            <code>npm run start-all</code>
            <code>npm run stop-all</code>
            <code>npm run status-all</code>
          </div>
        </div>
      </div>
    </div>
  );
}

export default HomePanel;
