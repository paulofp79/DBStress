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

function TPSChart({ data }) {
  const chartData = {
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
        display: false
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
        displayColors: false,
        callbacks: {
          title: () => 'Transactions per Second',
          label: (context) => `TPS: ${context.raw}`
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

  return (
    <div className="panel">
      <div className="panel-header">
        <h2>Transactions per Second</h2>
        {data.length > 0 && (
          <span style={{ fontFamily: 'JetBrains Mono', color: 'var(--accent-primary)' }}>
            Current: {data[data.length - 1]?.value || 0} TPS
          </span>
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
