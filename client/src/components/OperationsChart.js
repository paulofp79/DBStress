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

function OperationsChart({ data }) {
  const chartData = {
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
          padding: 15,
          font: {
            size: 11
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

  const latest = data[data.length - 1] || { inserts: 0, updates: 0, deletes: 0 };

  return (
    <div className="panel">
      <div className="panel-header">
        <h2>DML Operations per Second</h2>
        <div style={{ display: 'flex', gap: '1rem', fontSize: '0.75rem', fontFamily: 'JetBrains Mono' }}>
          <span style={{ color: 'rgb(16, 185, 129)' }}>INS: {latest.inserts}</span>
          <span style={{ color: 'rgb(59, 130, 246)' }}>UPD: {latest.updates}</span>
          <span style={{ color: 'rgb(239, 68, 68)' }}>DEL: {latest.deletes}</span>
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
