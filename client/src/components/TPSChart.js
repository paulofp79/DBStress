import React from 'react';
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend,
  Filler
} from 'chart.js';
import { Line } from 'react-chartjs-2';

ChartJS.register(
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend,
  Filler
);

// Color palette for different schemas
const SCHEMA_COLORS = [
  { border: 'rgb(99, 102, 241)', bg: 'rgba(99, 102, 241, 0.1)' },   // Indigo
  { border: 'rgb(34, 197, 94)', bg: 'rgba(34, 197, 94, 0.1)' },     // Green
  { border: 'rgb(249, 115, 22)', bg: 'rgba(249, 115, 22, 0.1)' },   // Orange
  { border: 'rgb(236, 72, 153)', bg: 'rgba(236, 72, 153, 0.1)' },   // Pink
  { border: 'rgb(14, 165, 233)', bg: 'rgba(14, 165, 233, 0.1)' },   // Sky blue
];

function TPSChart({ data, schemaData }) {
  // If schemaData is provided, show multiple schema comparison
  const hasMultiSchema = schemaData && Object.keys(schemaData).length > 0;

  let chartData;
  let currentTpsDisplay;

  if (hasMultiSchema) {
    const schemaIds = Object.keys(schemaData);
    const maxLength = Math.max(...schemaIds.map(id => schemaData[id]?.length || 0));

    chartData = {
      labels: Array.from({ length: maxLength }, (_, i) => i + 1),
      datasets: schemaIds.map((schemaId, index) => {
        const color = SCHEMA_COLORS[index % SCHEMA_COLORS.length];
        const schemaPoints = schemaData[schemaId] || [];

        return {
          label: schemaId || 'default',
          data: schemaPoints.map(d => d.value),
          borderColor: color.border,
          backgroundColor: color.bg,
          fill: false,
          tension: 0.4,
          pointRadius: 0,
          pointHoverRadius: 4
        };
      })
    };

    // Build current TPS display for all schemas
    currentTpsDisplay = schemaIds.map((schemaId, index) => {
      const schemaPoints = schemaData[schemaId] || [];
      const currentTps = schemaPoints[schemaPoints.length - 1]?.value || 0;
      const color = SCHEMA_COLORS[index % SCHEMA_COLORS.length];
      return (
        <span key={schemaId} style={{ marginRight: '1rem' }}>
          <span style={{ color: color.border, fontWeight: '500' }}>{schemaId || 'default'}:</span>{' '}
          <span style={{ fontFamily: 'JetBrains Mono' }}>{currentTps}</span>
        </span>
      );
    });
  } else {
    // Single schema mode (backward compatible)
    chartData = {
      labels: data.map((_, i) => i + 1),
      datasets: [
        {
          label: 'Transactions per Second',
          data: data.map(d => d.value),
          borderColor: 'rgb(99, 102, 241)',
          backgroundColor: 'rgba(99, 102, 241, 0.1)',
          fill: true,
          tension: 0.4,
          pointRadius: 0,
          pointHoverRadius: 4
        }
      ]
    };

    currentTpsDisplay = (
      <span style={{ fontFamily: 'JetBrains Mono', color: 'var(--accent-primary)' }}>
        Current: {data[data.length - 1]?.value || 0} TPS
      </span>
    );
  }

  const options = {
    responsive: true,
    maintainAspectRatio: false,
    animation: {
      duration: 0
    },
    interaction: {
      intersect: false,
      mode: 'index'
    },
    plugins: {
      legend: {
        display: hasMultiSchema,
        position: 'top',
        labels: {
          color: '#a0a0b0',
          usePointStyle: true,
          padding: 15
        }
      },
      title: {
        display: false
      },
      tooltip: {
        backgroundColor: 'rgba(26, 26, 46, 0.95)',
        titleColor: '#ffffff',
        bodyColor: '#a0a0b0',
        borderColor: '#2a2a45',
        borderWidth: 1,
        padding: 12,
        displayColors: true,
        callbacks: {
          title: () => 'Transactions per Second',
          label: (context) => `${context.dataset.label}: ${context.raw} TPS`
        }
      }
    },
    scales: {
      x: {
        display: true,
        grid: {
          color: 'rgba(42, 42, 69, 0.5)',
          drawBorder: false
        },
        ticks: {
          color: '#6b6b80',
          maxTicksLimit: 10
        },
        title: {
          display: true,
          text: 'Time (seconds)',
          color: '#6b6b80'
        }
      },
      y: {
        display: true,
        beginAtZero: true,
        grid: {
          color: 'rgba(42, 42, 69, 0.5)',
          drawBorder: false
        },
        ticks: {
          color: '#6b6b80'
        },
        title: {
          display: true,
          text: 'TPS',
          color: '#6b6b80'
        }
      }
    }
  };

  const hasData = hasMultiSchema
    ? Object.values(schemaData).some(arr => arr && arr.length > 0)
    : data.length > 0;

  return (
    <div className="panel">
      <div className="panel-header">
        <h2>Transactions per Second {hasMultiSchema && '(Comparison)'}</h2>
        {hasData && (
          <div style={{ display: 'flex', flexWrap: 'wrap', gap: '0.5rem' }}>
            {currentTpsDisplay}
          </div>
        )}
      </div>
      <div className="panel-content">
        <div className="chart-container">
          <Line data={chartData} options={options} />
        </div>
      </div>
    </div>
  );
}

export default TPSChart;
