import React from 'react';
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend
} from 'chart.js';
import { Line } from 'react-chartjs-2';

ChartJS.register(
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend
);

// Color palette for different schemas with operation type variations
const SCHEMA_COLORS = [
  { inserts: 'rgb(16, 185, 129)', updates: 'rgb(59, 130, 246)', deletes: 'rgb(239, 68, 68)' },  // Schema 1
  { inserts: 'rgb(34, 197, 94)', updates: 'rgb(99, 102, 241)', deletes: 'rgb(249, 115, 22)' },  // Schema 2
];

function OperationsChart({ data, schemaData }) {
  const hasMultiSchema = schemaData && Object.keys(schemaData).length > 0;

  let chartData;
  let latestStats;

  if (hasMultiSchema) {
    const schemaIds = Object.keys(schemaData);
    const maxLength = Math.max(...schemaIds.map(id => schemaData[id]?.length || 0));

    const datasets = [];
    schemaIds.forEach((schemaId, schemaIndex) => {
      const colors = SCHEMA_COLORS[schemaIndex % SCHEMA_COLORS.length];
      const schemaPoints = schemaData[schemaId] || [];
      const label = schemaId || 'default';

      // Add datasets for each operation type
      datasets.push({
        label: `${label} - INSERTs`,
        data: schemaPoints.map(d => d.inserts),
        borderColor: colors.inserts,
        borderDash: schemaIndex > 0 ? [5, 5] : [],
        tension: 0.4,
        pointRadius: 0,
        pointHoverRadius: 4
      });

      datasets.push({
        label: `${label} - UPDATEs`,
        data: schemaPoints.map(d => d.updates),
        borderColor: colors.updates,
        borderDash: schemaIndex > 0 ? [5, 5] : [],
        tension: 0.4,
        pointRadius: 0,
        pointHoverRadius: 4
      });

      datasets.push({
        label: `${label} - DELETEs`,
        data: schemaPoints.map(d => d.deletes),
        borderColor: colors.deletes,
        borderDash: schemaIndex > 0 ? [5, 5] : [],
        tension: 0.4,
        pointRadius: 0,
        pointHoverRadius: 4
      });
    });

    chartData = {
      labels: Array.from({ length: maxLength }, (_, i) => i + 1),
      datasets
    };

    // Build latest stats for all schemas
    latestStats = schemaIds.map((schemaId, index) => {
      const schemaPoints = schemaData[schemaId] || [];
      const latest = schemaPoints[schemaPoints.length - 1] || { inserts: 0, updates: 0, deletes: 0 };
      const colors = SCHEMA_COLORS[index % SCHEMA_COLORS.length];
      return (
        <div key={schemaId} style={{ marginRight: '1.5rem' }}>
          <div style={{ fontSize: '0.7rem', color: 'var(--text-muted)', marginBottom: '0.25rem' }}>
            {schemaId || 'default'}
          </div>
          <div style={{ display: 'flex', gap: '0.5rem', fontSize: '0.7rem', fontFamily: 'JetBrains Mono' }}>
            <span style={{ color: colors.inserts }}>I:{latest.inserts}</span>
            <span style={{ color: colors.updates }}>U:{latest.updates}</span>
            <span style={{ color: colors.deletes }}>D:{latest.deletes}</span>
          </div>
        </div>
      );
    });
  } else {
    // Single schema mode
    chartData = {
      labels: data.map((_, i) => i + 1),
      datasets: [
        {
          label: 'INSERTs',
          data: data.map(d => d.inserts),
          borderColor: 'rgb(16, 185, 129)',
          backgroundColor: 'rgba(16, 185, 129, 0.1)',
          tension: 0.4,
          pointRadius: 0,
          pointHoverRadius: 4
        },
        {
          label: 'UPDATEs',
          data: data.map(d => d.updates),
          borderColor: 'rgb(59, 130, 246)',
          backgroundColor: 'rgba(59, 130, 246, 0.1)',
          tension: 0.4,
          pointRadius: 0,
          pointHoverRadius: 4
        },
        {
          label: 'DELETEs',
          data: data.map(d => d.deletes),
          borderColor: 'rgb(239, 68, 68)',
          backgroundColor: 'rgba(239, 68, 68, 0.1)',
          tension: 0.4,
          pointRadius: 0,
          pointHoverRadius: 4
        }
      ]
    };

    const latest = data[data.length - 1] || { inserts: 0, updates: 0, deletes: 0 };
    latestStats = (
      <div style={{ display: 'flex', gap: '1rem', fontSize: '0.75rem', fontFamily: 'JetBrains Mono' }}>
        <span style={{ color: 'rgb(16, 185, 129)' }}>INS: {latest.inserts}</span>
        <span style={{ color: 'rgb(59, 130, 246)' }}>UPD: {latest.updates}</span>
        <span style={{ color: 'rgb(239, 68, 68)' }}>DEL: {latest.deletes}</span>
      </div>
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
        position: 'top',
        align: 'end',
        labels: {
          color: '#a0a0b0',
          usePointStyle: true,
          pointStyle: 'circle',
          padding: 10,
          font: {
            size: 10
          }
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
        padding: 12
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
          text: 'Operations/sec',
          color: '#6b6b80'
        }
      }
    }
  };

  return (
    <div className="panel">
      <div className="panel-header">
        <h2>DML Operations/sec {hasMultiSchema && '(Comparison)'}</h2>
        <div style={{ display: 'flex', flexWrap: 'wrap' }}>
          {latestStats}
        </div>
      </div>
      <div className="panel-content">
        <div className="chart-container">
          <Line data={chartData} options={options} />
        </div>
      </div>
    </div>
  );
}

export default OperationsChart;
