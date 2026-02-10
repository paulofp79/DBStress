import React, { useCallback, useMemo, useState } from 'react';
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

const DATASET_COLORS = [
  { border: 'rgb(14, 165, 233)', bg: 'rgba(14, 165, 233, 0.15)' },
  { border: 'rgb(34, 197, 94)', bg: 'rgba(34, 197, 94, 0.15)' },
  { border: 'rgb(249, 115, 22)', bg: 'rgba(249, 115, 22, 0.15)' },
  { border: 'rgb(236, 72, 153)', bg: 'rgba(236, 72, 153, 0.15)' },
  { border: 'rgb(99, 102, 241)', bg: 'rgba(99, 102, 241, 0.15)' },
  { border: 'rgb(52, 211, 153)', bg: 'rgba(52, 211, 153, 0.15)' },
  { border: 'rgb(249, 168, 212)', bg: 'rgba(249, 168, 212, 0.15)' }
];

const TIMESTAMP_REGEX = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}[+-]\d{2}:\d{2}$/;
const METRIC_REGEX = /^[A-Z]+_[A-Z0-9_]+$/;

const collectUniqueSorted = (values) => Array.from(new Set(values)).sort((a, b) => a.localeCompare(b));

const parseValue = (raw) => {
  if (!raw) {
    return { numericValue: null, unit: '' };
  }

  const cleaned = raw.replace(/,/g, '').trim();
  const match = cleaned.match(/-?\d+(?:\.\d+)?/);

  if (!match) {
    return { numericValue: null, unit: cleaned };
  }

  const numericValue = parseFloat(match[0]);
  const unit = cleaned.slice(match[0].length).trim();

  return { numericValue, unit };
};

const parseMetricFile = (content) => {
  const tokens = content.split(/\s+/).filter(Boolean);
  const entries = [];
  const warnings = [];

  let currentTimestamp = null;
  let index = 0;

  while (index < tokens.length) {
    const token = tokens[index];

    if (TIMESTAMP_REGEX.test(token)) {
      currentTimestamp = token;
      index += 1;
      continue;
    }

    if (!METRIC_REGEX.test(token)) {
      index += 1;
      continue;
    }

    const metric = token;
    index += 1;

    if (index >= tokens.length) {
      warnings.push(`Metric '${metric}' has no target/value data.`);
      break;
    }

    const targetToken = tokens[index];

    if (!targetToken || METRIC_REGEX.test(targetToken) || TIMESTAMP_REGEX.test(targetToken)) {
      warnings.push(`Metric '${metric}' is missing a target; skipping entry.`);
      continue;
    }

    const target = targetToken;
    index += 1;

    const valueTokens = [];
    let explicitTimestamp = null;

    while (index < tokens.length) {
      const candidate = tokens[index];

      if (TIMESTAMP_REGEX.test(candidate)) {
        explicitTimestamp = candidate;
        index += 1;
        break;
      }

      if (METRIC_REGEX.test(candidate)) {
        if (valueTokens.length === 0) {
          warnings.push(`Metric '${metric}' has no value before next metric; skipping.`);
        }
        break;
      }

      valueTokens.push(candidate);
      index += 1;
    }

    if (valueTokens.length === 0) {
      continue;
    }

    const rawValue = valueTokens.join(' ');
    const timestamp = explicitTimestamp || currentTimestamp;

    if (!timestamp) {
      warnings.push(`Metric '${metric}' missing timestamp; skipping entry.`);
      continue;
    }

    const parsedTime = Date.parse(timestamp);

    if (Number.isNaN(parsedTime)) {
      warnings.push(`Metric '${metric}' has invalid timestamp '${timestamp}'; skipping entry.`);
      continue;
    }

    const { numericValue, unit } = parseValue(rawValue);

    entries.push({
      metric,
      target,
      rawValue,
      numericValue,
      unit,
      timestamp,
      timestampMs: parsedTime
    });

    if (explicitTimestamp) {
      currentTimestamp = explicitTimestamp;
    }
  }

  return { entries, warnings };
};

function MetricExplorerPanel() {
  const [entries, setEntries] = useState([]);
  const [warnings, setWarnings] = useState([]);
  const [selectedMetric, setSelectedMetric] = useState('');
  const [selectedTargets, setSelectedTargets] = useState([]);
  const [fileInfo, setFileInfo] = useState(null);
  const [error, setError] = useState('');

  const metricsMap = useMemo(() => {
    const map = new Map();

    entries.forEach((entry) => {
      if (!map.has(entry.metric)) {
        map.set(entry.metric, {
          targets: new Map(),
          entries: [],
          units: new Set()
        });
      }

      const metricData = map.get(entry.metric);
      metricData.entries.push(entry);

      if (!metricData.targets.has(entry.target)) {
        metricData.targets.set(entry.target, []);
      }

      metricData.targets.get(entry.target).push(entry);

      if (entry.unit) {
        metricData.units.add(entry.unit);
      }
    });

    return map;
  }, [entries]);

  const sortedMetricNames = useMemo(
    () => Array.from(metricsMap.keys()).sort((a, b) => a.localeCompare(b)),
    [metricsMap]
  );

  const metricData = selectedMetric ? metricsMap.get(selectedMetric) : null;

  const allTargetsForMetric = useMemo(() => {
    if (!metricData) {
      return [];
    }
    return Array.from(metricData.targets.keys()).sort((a, b) => a.localeCompare(b));
  }, [metricData]);

  const selectedTargetSet = useMemo(() => new Set(selectedTargets), [selectedTargets]);
  const hasTargetsSelected = selectedTargetSet.size > 0;

  const chartData = useMemo(() => {
    if (!metricData || !hasTargetsSelected) {
      return null;
    }

    const timestamps = new Set();
    metricData.entries.forEach((entry) => {
      timestamps.add(entry.timestamp);
    });
    const labels = Array.from(timestamps).sort((a, b) => new Date(a) - new Date(b));

    const datasets = [];
    let colorIndex = 0;

    const targets = Array.from(selectedTargetSet.values());

    targets.forEach((target) => {
      const targetEntries = metricData.targets.get(target) || [];
      const numericEntries = targetEntries.filter((entry) => entry.numericValue !== null);

      if (!numericEntries.length) {
        return;
      }

      const lookup = new Map(numericEntries.map((entry) => [entry.timestamp, entry.numericValue]));
      const colors = DATASET_COLORS[colorIndex % DATASET_COLORS.length];

      datasets.push({
        label: target,
        data: labels.map((label) => (lookup.has(label) ? lookup.get(label) : null)),
        borderColor: colors.border,
        backgroundColor: colors.bg,
        fill: false,
        tension: 0.35,
        spanGaps: true,
        pointRadius: 0,
        pointHoverRadius: 4
      });

      colorIndex += 1;
    });

    if (!datasets.length) {
      return null;
    }

    return { labels, datasets };
  }, [metricData, selectedTargetSet, hasTargetsSelected]);

  const filteredEntries = useMemo(() => {
    if (!metricData || !hasTargetsSelected) {
      return [];
    }

    return metricData.entries
      .filter((entry) => selectedTargetSet.has(entry.target))
      .slice()
      .sort((a, b) => a.timestampMs - b.timestampMs);
  }, [metricData, selectedTargetSet, hasTargetsSelected]);

  const summaryStats = useMemo(() => {
    if (!metricData) {
      return null;
    }

    const numericEntries = metricData.entries.filter((entry) => entry.numericValue !== null);

    if (!numericEntries.length) {
      return null;
    }

    const values = numericEntries.map((entry) => entry.numericValue);
    const min = Math.min(...values);
    const max = Math.max(...values);
    const avg = values.reduce((sum, value) => sum + value, 0) / values.length;

    return {
      count: metricData.entries.length,
      numericCount: numericEntries.length,
      min,
      max,
      avg,
      units: Array.from(metricData.units)
    };
  }, [metricData]);

  const handleFileChange = useCallback((event) => {
    const file = event.target.files?.[0];

    if (!file) {
      return;
    }

    const reader = new FileReader();

    reader.onload = (loadEvent) => {
      try {
        const text = typeof loadEvent.target?.result === 'string' ? loadEvent.target.result : '';
        const { entries: parsedEntries, warnings: parseWarnings } = parseMetricFile(text);

        setEntries(parsedEntries);
        setWarnings(parseWarnings);
        setError('');
        setFileInfo({
          name: file.name,
          size: file.size,
          parsedCount: parsedEntries.length
        });

        if (parsedEntries.length) {
          const orderedMetrics = collectUniqueSorted(parsedEntries.map((entry) => entry.metric));
          const defaultMetric = orderedMetrics[0] || '';
          setSelectedMetric(defaultMetric);
          const defaultTargets = defaultMetric
            ? collectUniqueSorted(
                parsedEntries
                  .filter((entry) => entry.metric === defaultMetric)
                  .map((entry) => entry.target)
              )
            : [];
          setSelectedTargets(defaultTargets);
        } else {
          setSelectedMetric('');
          setSelectedTargets([]);
        }
      } catch (loadError) {
        setEntries([]);
        setWarnings([]);
        setSelectedMetric('');
        setSelectedTargets([]);
        setFileInfo({
          name: file.name,
          size: file.size,
          parsedCount: 0
        });
        setError(loadError instanceof Error ? loadError.message : 'Failed to parse file.');
      }
    };

    reader.onerror = () => {
      setEntries([]);
      setWarnings([]);
      setSelectedMetric('');
      setSelectedTargets([]);
      setFileInfo(null);
      setError('Unable to read the selected file.');
    };

    reader.readAsText(file);
  }, []);

  const handleMetricSelect = useCallback(
    (event) => {
      const value = event.target.value;
      setSelectedMetric(value);

      if (!value) {
        setSelectedTargets([]);
        return;
      }

      const metricInfo = metricsMap.get(value);
      if (!metricInfo) {
        setSelectedTargets([]);
        return;
      }

      setSelectedTargets(collectUniqueSorted(Array.from(metricInfo.targets.keys())));
    },
    [metricsMap]
  );

  const handleTargetToggle = useCallback((target) => {
    setSelectedTargets((prev) => {
      if (prev.includes(target)) {
        return prev.filter((item) => item !== target);
      }
      return [...prev, target];
    });
  }, []);

  const handleSelectAllTargets = useCallback(() => {
    setSelectedTargets(allTargetsForMetric);
  }, [allTargetsForMetric]);

  const handleClearTargets = useCallback(() => {
    setSelectedTargets([]);
  }, []);

  const showChart = Boolean(chartData);

  return (
    <div className="panel" style={{ gridColumn: '1 / -1' }}>
      <div className="panel-header">
        <h2>Metric Explorer</h2>
        {fileInfo && (
          <span style={{ fontSize: '0.85rem', color: 'var(--text-secondary)' }}>
            Loaded {fileInfo.name} ({(fileInfo.size / 1024).toFixed(1)} KB) — {fileInfo.parsedCount} entries
          </span>
        )}
      </div>

      <div className="panel-content metric-explorer">
        <div className="metric-explorer__section">
          <label className="metric-explorer__label">Upload Metric File</label>
          <input
            type="file"
            accept=".txt,.log,.lst,.csv"
            onChange={handleFileChange}
            className="metric-explorer__input metric-explorer__input--file"
          />
          {error && (
            <div className="alert alert-danger" style={{ marginTop: '0.75rem' }}>
              {error}
            </div>
          )}
          {!error && warnings.length > 0 && (
            <div className="alert alert-warning" style={{ marginTop: '0.75rem' }}>
              <div>
                Parsed with {warnings.length} warning{warnings.length === 1 ? '' : 's'}.
              </div>
              <details className="metric-explorer__warnings">
                <summary>Show details</summary>
                <ul>
                  {warnings.slice(0, 10).map((warning, index) => (
                    <li key={index}>{warning}</li>
                  ))}
                  {warnings.length > 10 && <li>…and {warnings.length - 10} more</li>}
                </ul>
              </details>
            </div>
          )}
        </div>

        {!entries.length && (
          <p className="metric-explorer__empty">
            Upload a metric export (for example <code>metric_db.txt</code>) to explore values and plot trends.
          </p>
        )}

        {entries.length > 0 && (
          <>
            <div className="metric-explorer__section">
              <label className="metric-explorer__label">Select Metric</label>
              <select
                value={selectedMetric}
                onChange={handleMetricSelect}
                className="metric-explorer__input"
              >
                <option value="">-- Choose a metric --</option>
                {sortedMetricNames.map((metric) => (
                  <option key={metric} value={metric}>
                    {metric}
                  </option>
                ))}
              </select>
            </div>

            {metricData && (
              <>
                <div className="metric-explorer__section">
                  <div className="metric-explorer__targets-header">
                    <span>Targets</span>
                    <div className="metric-explorer__targets-actions">
                      <button
                        type="button"
                        className="metric-explorer__link"
                        onClick={handleSelectAllTargets}
                        disabled={!allTargetsForMetric.length}
                      >
                        Select all
                      </button>
                      <button
                        type="button"
                        className="metric-explorer__link"
                        onClick={handleClearTargets}
                        disabled={!hasTargetsSelected}
                      >
                        Clear
                      </button>
                    </div>
                  </div>
                  <div className="metric-explorer__targets-list">
                    {allTargetsForMetric.map((target) => (
                      <label key={target} className="metric-explorer__checkbox">
                        <input
                          type="checkbox"
                          checked={hasTargetsSelected ? selectedTargetSet.has(target) : false}
                          onChange={() => handleTargetToggle(target)}
                        />
                        <span>{target}</span>
                      </label>
                    ))}
                  </div>
                </div>

                {summaryStats && (
                  <div className="metric-explorer__summary">
                    <div>
                      <span className="metric-explorer__summary-label">Entries:</span> {summaryStats.count}
                    </div>
                    <div>
                      <span className="metric-explorer__summary-label">Numeric points:</span> {summaryStats.numericCount}
                    </div>
                    <div>
                      <span className="metric-explorer__summary-label">Min:</span> {summaryStats.min.toFixed(3)}
                    </div>
                    <div>
                      <span className="metric-explorer__summary-label">Max:</span> {summaryStats.max.toFixed(3)}
                    </div>
                    <div>
                      <span className="metric-explorer__summary-label">Avg:</span> {summaryStats.avg.toFixed(3)}
                    </div>
                    {summaryStats.units.length > 0 && (
                      <div>
                        <span className="metric-explorer__summary-label">Units:</span> {summaryStats.units.join(', ')}
                      </div>
                    )}
                  </div>
                )}

                {!hasTargetsSelected && (
                  <p className="metric-explorer__empty">Select at least one target to display data.</p>
                )}

                {hasTargetsSelected && showChart && (
                  <div className="metric-explorer__chart">
                    <Line
                      data={chartData}
                      options={{
                        responsive: true,
                        maintainAspectRatio: false,
                        interaction: { intersect: false, mode: 'index' },
                        animation: { duration: 0 },
                        plugins: {
                          legend: {
                            position: 'top',
                            labels: { color: '#a0a0b0', usePointStyle: true }
                          },
                          tooltip: {
                            backgroundColor: 'rgba(26, 26, 46, 0.95)',
                            titleColor: '#ffffff',
                            bodyColor: '#a0a0b0',
                            borderColor: '#2a2a45',
                            borderWidth: 1,
                            callbacks: {
                              title: (items) => (items[0] ? items[0].label : ''),
                              label: (item) => `${item.dataset.label}: ${item.formattedValue}`
                            }
                          }
                        },
                        scales: {
                          x: {
                            grid: { color: 'rgba(42, 42, 69, 0.5)' },
                            ticks: { color: '#6b6b80', maxRotation: 45, minRotation: 45 }
                          },
                          y: {
                            grid: { color: 'rgba(42, 42, 69, 0.5)' },
                            ticks: { color: '#6b6b80' }
                          }
                        }
                      }}
                    />
                  </div>
                )}

                {hasTargetsSelected && !showChart && (
                  <p className="metric-explorer__empty">
                    No numeric values available for the selected metric/targets.
                  </p>
                )}

                {filteredEntries.length > 0 && (
                  <div className="metric-explorer__table-wrapper">
                    <table className="metric-explorer__table">
                      <thead>
                        <tr>
                          <th>Timestamp</th>
                          <th>Target</th>
                          <th>Value</th>
                          <th>Unit</th>
                        </tr>
                      </thead>
                      <tbody>
                        {filteredEntries.slice(0, 200).map((entry, index) => (
                          <tr key={`${entry.metric}-${entry.target}-${entry.timestamp}-${index}`}>
                            <td>{entry.timestamp}</td>
                            <td>{entry.target}</td>
                            <td>
                              {entry.numericValue !== null
                                ? entry.numericValue.toLocaleString(undefined, { maximumFractionDigits: 4 })
                                : entry.rawValue}
                            </td>
                            <td>{entry.numericValue !== null ? entry.unit : ''}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                    {filteredEntries.length > 200 && (
                      <div className="metric-explorer__note">
                        Showing first 200 rows. Refine target selection to narrow the dataset.
                      </div>
                    )}
                  </div>
                )}
              </>
            )}
          </>
        )}
      </div>
    </div>
  );
}

export default MetricExplorerPanel;
