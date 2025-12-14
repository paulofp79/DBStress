import React from 'react';

function WaitEventsPanel({ waitEvents }) {
  const getWaitClassBadge = (waitClass) => {
    const classMap = {
      'System I/O': 'system-io',
      'User I/O': 'user-io',
      'Concurrency': 'concurrency',
      'Application': 'application',
      'Network': 'network',
      'Configuration': 'configuration',
      'Commit': 'system-io',
      'Other': 'other'
    };
    return classMap[waitClass] || 'other';
  };

  const formatNumber = (num) => {
    if (num >= 1000000) return (num / 1000000).toFixed(2) + 'M';
    if (num >= 1000) return (num / 1000).toFixed(1) + 'K';
    return num?.toFixed(0) || '0';
  };

  if (!waitEvents || waitEvents.length === 0) {
    return (
      <div className="panel">
        <div className="panel-header">
          <h2>Top 10 Wait Events</h2>
        </div>
        <div className="panel-content">
          <p style={{ color: 'var(--text-muted)', textAlign: 'center', padding: '2rem' }}>
            Collecting wait events data...
          </p>
        </div>
      </div>
    );
  }

  return (
    <div className="panel">
      <div className="panel-header">
        <h2>Top 10 Wait Events</h2>
        <span style={{ fontSize: '0.75rem', color: 'var(--text-muted)' }}>
          Non-idle waits from V$SYSTEM_EVENT
        </span>
      </div>
      <div className="panel-content" style={{ padding: 0 }}>
        <table className="events-table">
          <thead>
            <tr>
              <th>#</th>
              <th>Event Name</th>
              <th>Wait Class</th>
              <th>Total Waits</th>
              <th>Time Waited (s)</th>
              <th>Avg Wait (ms)</th>
            </tr>
          </thead>
          <tbody>
            {waitEvents.map((event, index) => (
              <tr key={event.event || index}>
                <td>{index + 1}</td>
                <td className="event-name" title={event.event}>
                  {event.event}
                </td>
                <td>
                  <span className={`wait-class-badge ${getWaitClassBadge(event.waitClass)}`}>
                    {event.waitClass}
                  </span>
                </td>
                <td>{formatNumber(event.totalWaits)}</td>
                <td>{event.timeWaitedSeconds?.toFixed(2) || '0.00'}</td>
                <td>{event.averageWaitMs?.toFixed(2) || '0.00'}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

export default WaitEventsPanel;
