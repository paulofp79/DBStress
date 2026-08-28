# Oracle RAC GC Wait Benchmark Tool

A full-stack web application for simulating and measuring Oracle RAC Global Cache (GC) wait events. Drive concurrent DML workloads against a configurable multi-table schema and capture before/after GC metrics to compare the impact of partitioning, compression, and concurrency settings.

## Tracked GC Events

| Event | Description |
|---|---|
| `gc current block congested` | Current block transfers delayed by LMS congestion |
| `gc current block 3-way` | Current block served from a third instance's cache |
| `gc cr grant congested` | CR grant delayed by LMS congestion |
| `gc current block 2-way` | Current block served directly from the holder |
| `gc cr block congested` | CR block transfer delayed by congestion |
| `gc cr grant 2-way` | CR grant served directly |

## Requirements

- **Python 3.10+**
- **Oracle RAC** environment (multi-instance) — single-instance will work but GC waits will be minimal
- **Oracle user** with SELECT on `V$SYSTEM_EVENT`, `GV$SYSTEM_EVENT`, `V$SEGMENT_STATISTICS`
- **python-oracledb** in thin mode (default) — no Oracle Instant Client needed
  - For thick mode: install [Oracle Instant Client](https://www.oracle.com/database/technologies/instant-client.html) and set the lib path

## Quick Start

```bash
cd gc_benchmark

# Create a virtual environment (recommended)
python3 -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Start the server
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

Open **http://localhost:8000** in your browser.

## Usage

### 1. Connection Tab
Enter your Oracle RAC connection details (host, port, service name, username, password). Click **Test Connection** to verify, then **Check Privileges** to confirm access to the required V$ views. Click **Save** to persist settings (password is never saved to disk).

### 2. Schema Setup Tab
Configure the benchmark schema:
- **Tables**: 5–50 order-like tables with indexes
- **Partitioning**: None, Hash (4/8/16/32), Range (monthly/quarterly), or List
- **Compression**: None, Basic, Advanced, or HCC variants

Use **Preview DDL** to review the SQL before executing. Click **Create Schema** to build the tables — progress streams live to the log panel.

### 3. Run Workload Tab
Configure the concurrent workload:
- **Threads**: 2–10000 concurrent sessions
- **Duration**: run time in seconds
- **Workload Seed Rows**: rows per table to cache / top up for the run
- **Hot Row %**: percentage of rows receiving disproportionate updates

Click **Start Run**. The live dashboard shows:
- Elapsed time progress bar
- Real-time INSERT/UPDATE/DELETE/ERROR counters
- Chart.js line chart of GC wait event deltas updating every 5 seconds

### 4. Results & Comparison Tab
All completed runs are stored in SQLite. This tab shows:
- Sortable table of all runs with GC delta metrics
- Color-coded rows (green = lowest waits, red = highest)
- Multi-select comparison with a grouped bar chart
- CSV export

## Configuration

Connection settings are stored in `config.ini`:

```ini
[oracle]
host = myrachost
port = 1521
service_name = orclpdb1
user = benchmark_user
mode = thin
```

The password is entered per session and never written to disk.

## HCC Compression

HCC (Hybrid Columnar Compression) options generate valid Oracle DDL but require **Exadata**, **ZFS Storage**, or compatible storage. On standard systems, the CREATE TABLE statement will fail with `ORA-64307`. The tool displays a warning badge when HCC is selected.

## Architecture

```
gc_benchmark/
  main.py        FastAPI app, routes, WebSocket streaming
  schema.py      DDL generation (tables, indexes, partitions, compression)
  workload.py    Threaded DML engine with oracledb connection pool
  metrics.py     GC snapshot queries (GV$SYSTEM_EVENT, V$SEGMENT_STATISTICS)
  report.py      Async SQLite storage and comparison logic
  config.ini     Oracle connection settings
  results.db     SQLite database (auto-created)
  static/
    index.html   Single-page application
    app.js       Frontend logic + Chart.js integration
    style.css    Dark theme styling
```

## Troubleshooting

| Issue | Solution |
|---|---|
| No GC waits observed | Ensure you are running against a **multi-instance RAC** cluster. Single-instance databases do not generate GC waits. |
| `ORA-00942: table or view does not exist` on V$ views | Grant SELECT on the required views to your benchmark user: `GRANT SELECT ON V_$SYSTEM_EVENT TO benchmark_user;` |
| `ORA-64307` with HCC compression | HCC requires Exadata or compatible storage. Use NONE, BASIC, or ADVANCED compression instead. |
| `DPI-1047: Cannot locate Oracle Client library` | You are in thick mode but Oracle Instant Client is not installed or not in the library path. Switch to thin mode or install the client. |
| WebSocket disconnects during long runs | The workload continues in the background. Refresh the page — the Results tab will show the completed run. |
