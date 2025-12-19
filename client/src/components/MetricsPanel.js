import React from 'react';

// Color palette for schemas
const SCHEMA_COLORS = [
  { primary: 'rgb(99, 102, 241)', bg: 'rgba(99, 102, 241, 0.15)' },   // Indigo
  { primary: 'rgb(34, 197, 94)', bg: 'rgba(34, 197, 94, 0.15)' },     // Green
  { primary: 'rgb(249, 115, 22)', bg: 'rgba(249, 115, 22, 0.15)' },   // Orange
  { primary: 'rgb(236, 72, 153)', bg: 'rgba(236, 72, 153, 0.15)' },   // Pink
];

function MetricsPanel({ metrics, stressStatus }) {
  const formatNumber = (num) => {
    if (num === undefined || num === null) return '0';
    if (num >= 1000000) return (num / 1000000).toFixed(2) + 'M';
    if (num >= 1000) return (num / 1000).toFixed(1) + 'K';
    return num.toString();
  };

  // Get schema metrics from tpsBySchema (contains latest data)
  const schemaIds = Object.keys(metrics.tpsBySchema || {});
  const isMultiSchema = schemaIds.length > 1;

  // Build per-schema metrics from the latest data points
  const schemaMetrics = {};
  schemaIds.forEach(schemaId => {
    const tpsData = metrics.tpsBySchema[schemaId] || [];
    const opsData = metrics.operationsBySchema?.[schemaId] || [];
    const latestTps = tpsData[tpsData.length - 1];
    const latestOps = opsData[opsData.length - 1];

    // Calculate totals from all data points
    const totalTps = tpsData.reduce((sum, d) => sum + (d.value || 0), 0);
    const totalInserts = opsData.reduce((sum, d) => sum + (d.inserts || 0), 0);
    const totalUpdates = opsData.reduce((sum, d) => sum + (d.updates || 0), 0);
    const totalDeletes = opsData.reduce((sum, d) => sum + (d.deletes || 0), 0);

    schemaMetrics[schemaId] = {
      perSecond: {
        transactions: latestTps?.value || 0,
        inserts: latestOps?.inserts || 0,
        updates: latestOps?.updates || 0,
        deletes: latestOps?.deletes || 0,
      },
      total: {
        transactions: totalTps,
        inserts: totalInserts,
        updates: totalUpdates,
        deletes: totalDeletes,
      }
    };
  });

  // Single schema view (backward compatible)
  if (!isMultiSchema) {
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

  // Multi-schema comparison view
  return (
    <div className="panel">
      <div className="panel-header">
        <h2>Real-Time Metrics (Comparison)</h2>
      </div>
      <div className="panel-content">
        {/* Schema comparison grid */}
        <div style={{
          display: 'grid',
          gridTemplateColumns: `repeat(${schemaIds.length}, 1fr)`,
          gap: '1rem'
        }}>
          {schemaIds.map((schemaId, index) => {
            const color = SCHEMA_COLORS[index % SCHEMA_COLORS.length];
            const data = schemaMetrics[schemaId];

            return (
              <div
                key={schemaId}
                style={{
                  background: color.bg,
                  borderRadius: '8px',
                  padding: '1rem',
                  border: `2px solid ${color.primary}`
                }}
              >
                {/* Schema Header */}
                <div style={{
                  fontSize: '1rem',
                  fontWeight: '600',
                  color: color.primary,
                  marginBottom: '1rem',
                  textAlign: 'center',
                  borderBottom: `1px solid ${color.primary}`,
                  paddingBottom: '0.5rem'
                }}>
                  {schemaId}
                </div>

                {/* TPS - Main metric */}
                <div style={{ textAlign: 'center', marginBottom: '1rem' }}>
                  <div style={{
                    fontSize: '2.5rem',
                    fontWeight: 'bold',
                    color: color.primary,
                    fontFamily: 'JetBrains Mono, monospace'
                  }}>
                    {data.perSecond.transactions}
                  </div>
                  <div style={{ fontSize: '0.8rem', color: 'var(--text-muted)' }}>TPS</div>
                </div>

                {/* Per-second metrics */}
                <div style={{
                  display: 'grid',
                  gridTemplateColumns: 'repeat(2, 1fr)',
                  gap: '0.5rem',
                  marginBottom: '1rem'
                }}>
                  <div style={{ textAlign: 'center', padding: '0.5rem', background: 'rgba(0,0,0,0.2)', borderRadius: '4px' }}>
                    <div style={{ fontSize: '1.25rem', fontWeight: '600', color: 'rgb(16, 185, 129)' }}>
                      {data.perSecond.inserts}
                    </div>
                    <div style={{ fontSize: '0.7rem', color: 'var(--text-muted)' }}>INS/s</div>
                  </div>
                  <div style={{ textAlign: 'center', padding: '0.5rem', background: 'rgba(0,0,0,0.2)', borderRadius: '4px' }}>
                    <div style={{ fontSize: '1.25rem', fontWeight: '600', color: 'rgb(59, 130, 246)' }}>
                      {data.perSecond.updates}
                    </div>
                    <div style={{ fontSize: '0.7rem', color: 'var(--text-muted)' }}>UPD/s</div>
                  </div>
                  <div style={{ textAlign: 'center', padding: '0.5rem', background: 'rgba(0,0,0,0.2)', borderRadius: '4px' }}>
                    <div style={{ fontSize: '1.25rem', fontWeight: '600', color: 'rgb(239, 68, 68)' }}>
                      {data.perSecond.deletes}
                    </div>
                    <div style={{ fontSize: '0.7rem', color: 'var(--text-muted)' }}>DEL/s</div>
                  </div>
                  <div style={{ textAlign: 'center', padding: '0.5rem', background: 'rgba(0,0,0,0.2)', borderRadius: '4px' }}>
                    <div style={{ fontSize: '1.25rem', fontWeight: '600', color: 'var(--text-secondary)' }}>
                      {formatNumber(data.total.transactions)}
                    </div>
                    <div style={{ fontSize: '0.7rem', color: 'var(--text-muted)' }}>Total TXN</div>
                  </div>
                </div>
              </div>
            );
          })}
        </div>

        {/* Database Sessions - shared */}
        {metrics.sessionStats && (
          <div style={{ marginTop: '1.5rem' }}>
            <h3 style={{ fontSize: '0.875rem', marginBottom: '1rem', color: 'var(--text-secondary)' }}>
              Database Sessions (Shared)
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
