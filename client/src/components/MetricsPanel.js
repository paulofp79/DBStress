import React from 'react';

function MetricsPanel({ metrics, stressStatus }) {
  const formatNumber = (num) => {
    if (num === undefined || num === null) return '0';
    if (num >= 1000000) return (num / 1000000).toFixed(2) + 'M';
    if (num >= 1000) return (num / 1000).toFixed(1) + 'K';
    return num.toString();
  };

  return (
    <div className="panel">
      <div className="panel-header">
        <h2>Real-Time Metrics</h2>
      </div>
      <div className="panel-content">
        <div className="stats-grid">
          <div className="stat-card">
            <div className="value highlight">
              {metrics.perSecond?.transactions || 0}
            </div>
            <div className="label">TPS</div>
          </div>

          <div className="stat-card">
            <div className="value" style={{ color: 'var(--accent-success)' }}>
              {metrics.perSecond?.inserts || 0}
            </div>
            <div className="label">Inserts/sec</div>
          </div>

          <div className="stat-card">
            <div className="value" style={{ color: 'var(--accent-info)' }}>
              {metrics.perSecond?.updates || 0}
            </div>
            <div className="label">Updates/sec</div>
          </div>

          <div className="stat-card">
            <div className="value" style={{ color: 'var(--accent-danger)' }}>
              {metrics.perSecond?.deletes || 0}
            </div>
            <div className="label">Deletes/sec</div>
          </div>

          <div className="stat-card">
            <div className="value" style={{ color: 'var(--accent-secondary)' }}>
              {metrics.perSecond?.selects || 0}
            </div>
            <div className="label">Selects/sec</div>
          </div>

          <div className="stat-card">
            <div className="value" style={{ color: 'var(--accent-warning)' }}>
              {metrics.perSecond?.errors || 0}
            </div>
            <div className="label">Errors/sec</div>
          </div>
        </div>

        <div style={{ marginTop: '1.5rem' }}>
          <h3 style={{ fontSize: '0.875rem', marginBottom: '1rem', color: 'var(--text-secondary)' }}>
            Cumulative Totals
          </h3>
          <div className="stats-grid">
            <div className="stat-card">
              <div className="value">{formatNumber(metrics.total?.transactions)}</div>
              <div className="label">Total TXN</div>
            </div>
            <div className="stat-card">
              <div className="value">{formatNumber(metrics.total?.inserts)}</div>
              <div className="label">Total INS</div>
            </div>
            <div className="stat-card">
              <div className="value">{formatNumber(metrics.total?.updates)}</div>
              <div className="label">Total UPD</div>
            </div>
            <div className="stat-card">
              <div className="value">{formatNumber(metrics.total?.deletes)}</div>
              <div className="label">Total DEL</div>
            </div>
            <div className="stat-card">
              <div className="value">{formatNumber(metrics.total?.selects)}</div>
              <div className="label">Total SEL</div>
            </div>
            <div className="stat-card">
              <div className="value">{formatNumber(metrics.total?.errors)}</div>
              <div className="label">Total ERR</div>
            </div>
          </div>
        </div>

        {metrics.sessionStats && (
          <div style={{ marginTop: '1.5rem' }}>
            <h3 style={{ fontSize: '0.875rem', marginBottom: '1rem', color: 'var(--text-secondary)' }}>
              Database Sessions
            </h3>
            <div className="stats-grid">
              <div className="stat-card">
                <div className="value">{metrics.sessionStats.totalSessions || 0}</div>
                <div className="label">Total</div>
              </div>
              <div className="stat-card">
                <div className="value" style={{ color: 'var(--accent-success)' }}>
                  {metrics.sessionStats.activeSessions || 0}
                </div>
                <div className="label">Active</div>
              </div>
              <div className="stat-card">
                <div className="value">{metrics.sessionStats.inactiveSessions || 0}</div>
                <div className="label">Inactive</div>
              </div>
              <div className="stat-card">
                <div className="value" style={{ color: 'var(--accent-danger)' }}>
                  {metrics.sessionStats.blockedSessions || 0}
                </div>
                <div className="label">Blocked</div>
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

export default MetricsPanel;
