import React, { useMemo } from 'react';
import { Bar } from 'react-chartjs-2';
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  BarElement,
  Title,
  Tooltip,
  Legend
} from 'chart.js';

ChartJS.register(
  CategoryScale,
  LinearScale,
  BarElement,
  Title,
  Tooltip,
  Legend
);

function GCWaitChart({ gcWaitEvents = [] }) {
  // Group events by instance and aggregate
  const chartData = useMemo(() => {
    if (!gcWaitEvents || gcWaitEvents.length === 0) {
      return null;
    }

    // Group by event name, sum across instances
    const eventMap = new Map();
    gcWaitEvents.forEach(e => {
      const existing = eventMap.get(e.event) || { totalWaits: 0, timeWaitedMs: 0, avgWaitMs: 0, count: 0 };
      existing.totalWaits += e.totalWaits;
      existing.timeWaitedMs += e.timeWaitedMs;
      existing.avgWaitMs += e.avgWaitMs;
      existing.count += 1;
      eventMap.set(e.event, existing);
    });

    // Sort by time waited and take top 10
    const sortedEvents = Array.from(eventMap.entries())
      .map(([event, data]) => ({
        event: event.replace('gc ', ''),  // Shorten label
        totalWaits: data.totalWaits,
        timeWaitedMs: data.timeWaitedMs,
        avgWaitMs: data.avgWaitMs / data.count
      }))
      .sort((a, b) => b.timeWaitedMs - a.timeWaitedMs)
      .slice(0, 10);

    const labels = sortedEvents.map(e => e.event);
    const waits = sortedEvents.map(e => e.totalWaits);
    const times = sortedEvents.map(e => e.timeWaitedMs);

    return {
      labels,
      datasets: [
        {
          label: 'Total Waits',
          data: waits,
          backgroundColor: 'rgba(239, 68, 68, 0.7)',
          borderColor: 'rgb(239, 68, 68)',
          borderWidth: 1,
          xAxisID: 'x'
        },
        {
          label: 'Time Waited (ms)',
          data: times,
          backgroundColor: 'rgba(59, 130, 246, 0.7)',
          borderColor: 'rgb(59, 130, 246)',
          borderWidth: 1,
          xAxisID: 'x1'
        }
      ]
    };
  }, [gcWaitEvents]);

  const options = {
    responsive: true,
    maintainAspectRatio: false,
    indexAxis: 'y',  // Horizontal bar chart
    plugins: {
      legend: {
        position: 'top',
        labels: {
          color: '#9ca3af',
          font: { size: 10 }
        }
      },
      title: {
        display: false
      },
      tooltip: {
        callbacks: {
          label: function(context) {
            const label = context.dataset.label || '';
            const value = context.parsed.x;
            if (label.includes('Time')) {
              return `${label}: ${value.toFixed(2)} ms`;
            }
            return `${label}: ${value.toLocaleString()}`;
          }
        }
      }
    },
    scales: {
      x: {
        type: 'linear',
        position: 'bottom',
        ticks: { color: '#9ca3af', font: { size: 9 } },
        grid: { color: 'rgba(255,255,255,0.1)' },
        title: {
          display: true,
          text: 'Total Waits',
          color: '#ef4444',
          font: { size: 10 }
        }
      },
      x1: {
        type: 'linear',
        position: 'top',
        ticks: { color: '#9ca3af', font: { size: 9 } },
        grid: { display: false },
        title: {
          display: true,
          text: 'Time Waited (ms)',
          color: '#3b82f6',
          font: { size: 10 }
        }
      },
      y: {
        type: 'category',
        position: 'left',
        ticks: { color: '#9ca3af', font: { size: 9 } },
        grid: { color: 'rgba(255,255,255,0.1)' }
      }
    }
  };

  // Show per-instance breakdown
  const instanceBreakdown = useMemo(() => {
    if (!gcWaitEvents || gcWaitEvents.length === 0) return [];

    const instances = new Map();
    gcWaitEvents.forEach(e => {
      const key = `Instance ${e.instId}`;
      const existing = instances.get(key) || { totalWaits: 0, timeWaitedMs: 0 };
      existing.totalWaits += e.totalWaits;
      existing.timeWaitedMs += e.timeWaitedMs;
      instances.set(key, existing);
    });

    return Array.from(instances.entries()).map(([inst, data]) => ({
      instance: inst,
      ...data
    }));
  }, [gcWaitEvents]);

  // Find key events of interest
  const keyEvents = useMemo(() => {
    if (!gcWaitEvents) return [];
    return gcWaitEvents.filter(e =>
      e.event.includes('congested') ||
      e.event.includes('busy') ||
      e.event.includes('2-way') ||
      e.event.includes('3-way')
    ).slice(0, 5);
  }, [gcWaitEvents]);

  if (!chartData) {
    return (
      <div className="panel">
        <div className="panel-header">
          <h2>GC Wait Events (RAC)</h2>
        </div>
        <div className="panel-content">
          <p style={{ color: 'var(--text-muted)', textAlign: 'center', padding: '2rem 0' }}>
            No GC wait events detected. Run stress test with RAC Contention Mode enabled.
          </p>
        </div>
      </div>
    );
  }

  return (
    <div className="panel">
      <div className="panel-header">
        <h2>GC Wait Events (RAC)</h2>
        <span style={{ fontSize: '0.75rem', color: 'var(--text-muted)' }}>
          {gcWaitEvents.length} events
        </span>
      </div>
      <div className="panel-content">
        {/* Instance Summary */}
        {instanceBreakdown.length > 1 && (
          <div style={{
            display: 'flex',
            gap: '1rem',
            marginBottom: '1rem',
            flexWrap: 'wrap'
          }}>
            {instanceBreakdown.map(inst => (
              <div key={inst.instance} style={{
                background: 'var(--surface)',
                padding: '0.5rem 1rem',
                borderRadius: '6px',
                border: '1px solid var(--border)'
              }}>
                <div style={{ fontSize: '0.7rem', color: 'var(--text-muted)' }}>{inst.instance}</div>
                <div style={{ fontSize: '1rem', fontWeight: '600' }}>
                  {inst.totalWaits.toLocaleString()} waits
                </div>
                <div style={{ fontSize: '0.75rem', color: 'var(--accent-primary)' }}>
                  {(inst.timeWaitedMs / 1000).toFixed(2)}s waited
                </div>
              </div>
            ))}
          </div>
        )}

        {/* Key Events Alert */}
        {keyEvents.length > 0 && (
          <div style={{
            background: 'rgba(239, 68, 68, 0.1)',
            border: '1px solid var(--accent-danger)',
            borderRadius: '6px',
            padding: '0.75rem',
            marginBottom: '1rem'
          }}>
            <div style={{ fontSize: '0.8rem', fontWeight: '600', color: 'var(--accent-danger)', marginBottom: '0.5rem' }}>
              Key Contention Events Detected
            </div>
            {keyEvents.map((e, i) => (
              <div key={i} style={{ fontSize: '0.75rem', marginBottom: '0.25rem' }}>
                <span style={{ color: 'var(--text-primary)' }}>{e.event}</span>
                <span style={{ color: 'var(--text-muted)', marginLeft: '0.5rem' }}>
                  {e.totalWaits.toLocaleString()} waits, avg {e.avgWaitMs.toFixed(2)}ms
                </span>
              </div>
            ))}
          </div>
        )}

        {/* Chart */}
        <div style={{ height: '300px' }}>
          <Bar data={chartData} options={options} />
        </div>
      </div>
    </div>
  );
}

export default GCWaitChart;
