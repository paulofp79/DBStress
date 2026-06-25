# DBStress

DBStress is an Oracle workload lab for creating repeatable database pressure, watching the database while it is under load, and comparing behavior across schema, storage, optimizer, and RAC global-cache choices.

The repository contains two web applications:

- **DBStress** on `http://localhost:3001`: the main React and Node.js application.
- **GC Benchmark** on `http://localhost:8000`: a standalone FastAPI application focused on Oracle RAC Global Cache wait experiments.

The main application can also launch or embed the GC Benchmark tool, so the usual workflow is to start both services together from the repository root.

## What This Tool Does

Use DBStress when you need to:

- connect to an Oracle database and create/drop repeatable test schemas
- run configurable mixed DML/SELECT stress workloads
- drive multiple schemas at the same time for comparison runs
- observe TPS, operation rates, sessions, top waits, and GC waits through the UI
- reproduce specific contention patterns such as index hot blocks, library cache locks, high-water-mark contention, and GC congestion
- compare optimizer statistics, skew, TDE encryption overhead, and datafile growth behavior
- create and run Swingbench SOE-style schemas/workloads from the browser
- generate wide-table insert/select storms with optional per-table BIGFILE tablespaces
- run a separate RAC GC benchmark with stored results and comparisons

This is a lab tool. It creates objects, opens concurrent sessions, can allocate storage, and can intentionally generate contention. Run it against disposable schemas or test databases unless you are deliberately diagnosing a controlled non-production environment.

## Repository Layout

```text
client/                         React frontend for DBStress
server/                         Node.js/Express backend and Socket.IO server
server/db/                      Oracle connection and schema/workload managers
server/stress/                  Workload engines for the DBStress labs
server/metrics/                 Oracle metric collection
gc_benchmark/gc_benchmark/      FastAPI RAC GC Benchmark application
swingbench/                     SOE DDL, package, Java, and workload assets
scripts/start-all.sh            Build and start DBStress plus GC Benchmark
scripts/status-all.sh           Show service status, ports, PIDs, and logs
scripts/stop-all.sh             Stop both services
scripts/gc-wait-live.sh         SQL*Plus live GC wait delta sampler
scripts/datafile-growth.sh      Datafile growth helper
scripts/insert-blast.sh         Insert Blast helper
startall.sh                     Root wrapper for npm run start-all
stopall.sh                      Root wrapper for npm run stop-all
DATA/                           Local analysis samples and artifacts
```

Main entry points:

- `server/index.js`
- `client/src/App.js`
- `gc_benchmark/gc_benchmark/main.py`

## Requirements

### Main DBStress App

- Node.js 18 or newer
- npm
- Oracle Database
- Oracle Instant Client or another valid setup for the Node.js `oracledb` driver
- an Oracle user that can create/drop the lab objects you choose to use

Useful database privileges depend on the panel. The monitor and RAC-oriented panels query views such as `GV$SYSTEM_EVENT`, `GV$SESSION`, `GV$SYSSTAT`, `V$SEGMENT_STATISTICS`, and related dynamic performance views.

### GC Benchmark App

- Python 3.10 or newer
- `python-oracledb`
- Oracle RAC for meaningful GC wait results
- SELECT access to the dynamic performance views used by the benchmark

The bundled start script prefers `python3.12`, then `python3`, then `python`.

## Installation

From this repository root:

```bash
npm run install-all
```

This installs the root backend dependencies and the React frontend dependencies.

The GC Benchmark virtual environment is created automatically by `npm run start-all` if it does not already exist. To prepare it manually:

```bash
cd /Users/pporacle/Documents/GitHub/DBStress/gc_benchmark/gc_benchmark
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Start, Stop, And Status

Recommended startup:

```bash
npm run start-all
npm run status-all
npm run stop-all
```

`npm run start-all` does the following:

- builds the React frontend
- starts DBStress on `http://localhost:3001`
- creates or refreshes the GC Benchmark virtual environment when needed
- starts GC Benchmark on `http://localhost:8000`
- writes PID files and logs under `.run/`

Logs:

```text
.run/logs/dbstress.log
.run/logs/gc-benchmark.log
```

Short wrappers are also available:

```bash
./startall.sh
./stopall.sh
```

### Run DBStress Only

Development mode with backend and React dev server:

```bash
npm run dev
```

Production-style local start:

```bash
npm run build
npm start
```

The backend serves the built React app on port `3001`.

### Run GC Benchmark Only

```bash
cd /Users/pporacle/Documents/GitHub/DBStress/gc_benchmark/gc_benchmark
source .venv/bin/activate
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

Open `http://localhost:8000`.

## DBStress UI

### Home

Launcher for the full toolset. It checks whether the main DBStress app and standalone GC Benchmark service are reachable and provides shortcuts into common tabs.

### Monitor

Live database observation panel. It has its own connection panel and explicit start/stop controls. It can show:

- transaction rate
- response time
- wait trends
- top wait events
- GC-only scoped waits
- wait-class and event-name filters

The average wait shown here is cumulative since instance startup, so it is best for environment observation rather than measuring only one workload run.

### Stress Test

The original general-purpose workload runner. It supports:

- Oracle connection management
- schema creation and drop
- multiple schemas in a single run
- configurable concurrent sessions per schema
- configurable INSERT/UPDATE/DELETE/SELECT rates
- think time
- RAC hot-table mode
- index-contention mode
- live TPS, operation, wait-event, and GC-wait charts

### Index Contention Demo

Lab for generating index hot-block behavior. It can start and stop index contention workloads, change index strategy, change sequence cache behavior, and run sequence-cache A/B tests.

### Library Cache Lock

Lab for library cache lock experiments. It can install a procedure, start/stop the contention workload, and test AWR snapshot creation from the UI.

### HW Contention Demo

Lab for high-water-mark and segment-extension style insert contention. It supports multiple test modes, configurable concurrent sessions, extent pre-allocation, partition count controls, wait-event charts, segment statistics, and statistics gathering.

### Stats Comparison

Tool for comparing optimizer statistics behavior and performance effects. It can start/stop comparison workloads, gather stats, and inspect histogram information.

### Skew Detection

Creates test tables with skewed distributions, analyzes skew, gathers statistics with configurable `METHOD_OPT`, and displays histogram/table-stat information.

### Metric Explorer

General metric exploration panel for inspecting database metrics outside the fixed workload views.

### Swingbench SOE

Browser-driven Swingbench Order Entry schema and workload tooling. It uses the assets under `swingbench/` and includes:

- default SOE settings
- schema preview
- schema create/drop
- workload default settings
- workload start/stop
- live status through the backend

### Insert Blast

Wide-table insert/select storm tool. It can:

- create many wide tables with a configurable prefix
- optionally create one BIGFILE tablespace per table
- configure datafile location, initial size, autoextend size, and TDE tablespace encryption
- run one or more independent workloads
- choose insert or select workload type
- choose connect/disconnect or persistent-session mode
- configure sessions, duration, tables used, and commit frequency
- optionally pre-allocate and periodically add extents to reduce `enq: HW - contention`
- monitor workload breakdown, sessions per instance, top waits, and LMS process memory

### Datafile Growth

Tool for inspecting tablespaces/datafiles and scheduling datafile growth operations through the backend.

### TDE Comparison

Compares AES-256 encrypted tables against plain tables. It can create encrypted/plain test tables, run selectable SELECT/INSERT/UPDATE phases, gather stats after load, and report comparative timings.

### GC Congestion Demo

RAC-focused lab for preparing, running, stopping, and dropping a GC congestion workload. It also exposes wait-event data for the relevant GC events.

### GC Acquire/Release Lab

RAC global-cache acquire/release lab with validation, setup, workload start/stop, live monitor, and cleanup operations. It includes the standard hot-index/hot-block paths, the manual LGNN repro, and a paced `file_@443` singleton insert test that:

- runs against a chosen service with a global inserts/sec target
- preserves the existing normal table
- commits every inserted row
- monitors `gc buffer busy acquire` and related wait rows in the existing live chart
- can create a DBStress-managed reverse-key clone for normal vs reverse-key comparison
- stops and kills only DBStress-tagged insert-test sessions

### GC Benchmark

Embedded view of the standalone FastAPI GC Benchmark tool. Open it from the DBStress tab or directly at `http://localhost:8000`.

## Standalone GC Benchmark

The GC Benchmark app is a full-stack FastAPI tool for simulating and measuring Oracle RAC Global Cache waits. It drives concurrent DML against a configurable multi-table schema and stores completed run results in SQLite.

Main tabs:

- **Connection**: enter host, port, service, username, password, test the connection, and check privileges. Passwords are not saved to disk.
- **Schema Setup**: configure table count, partitioning, and compression; preview DDL; create/drop the benchmark schema.
- **Run Workload**: configure threads, duration, seed rows, and hot-row percentage; watch live counters and GC wait deltas.
- **Results & Comparison**: review saved runs, compare selected runs with charts, and export CSV.

Tracked GC wait events include:

- `gc current block congested`
- `gc current block 3-way`
- `gc cr grant congested`
- `gc current block 2-way`
- `gc cr block congested`
- `gc cr grant 2-way`

HCC compression choices generate valid Oracle DDL but require Exadata, ZFS Storage, or compatible storage. On unsupported storage, Oracle can reject the DDL with `ORA-64307`.

## Backend API Families

The Node backend exposes REST endpoints under `/api` and streams live events with Socket.IO.

Major API groups:

- `/api/health`
- `/api/db/*`
- `/api/schema/*`
- `/api/schemas/list`
- `/api/monitor/*`
- `/api/stress/*`
- `/api/metrics/reset-gc-baseline`
- `/api/swingbench/soe/*`
- `/api/insert-blast/*`
- `/api/datafiles/*`
- `/api/index-contention/*`
- `/api/library-cache-lock/*`
- `/api/stats-comparison/*`
- `/api/hw-contention/*`
- `/api/skew-detection/*`
- `/api/tde-comparison/*`
- `/api/gc-congestion/*`
- `/api/gc-acquire-release/*`
- `/api/gc-benchmark/status`

The backend also serves the React production build from `client/build`.

## Helper Scripts

### Service Management

```bash
npm run start-all
npm run status-all
npm run stop-all
```

These scripts manage the two local services and their `.run/` PID/log files.

### Live GC Wait Sampler

Use `scripts/gc-wait-live.sh` for a terminal-side RAC GC wait delta sampler through SQL*Plus:

```bash
scripts/gc-wait-live.sh 'user/password@host:1521/service' 5
scripts/gc-wait-live.sh 'user/password@host:1521/service' 5 'gc current block congested' 1
```

Arguments:

```text
scripts/gc-wait-live.sh <user/password@host:port/service> [interval_seconds] [event_filter] [min_avg_wait_ms]
```

The script samples `GV$SYSTEM_EVENT`, keeps a baseline from script start, and prints per-instance delta waits plus average wait milliseconds.

## Development

Useful commands:

```bash
npm run server
npm run client
npm run dev
npm run build
node --check server/index.js
```

Frontend-only build:

```bash
cd client
npm run build
```

Backend source is CommonJS JavaScript. Frontend source is React 18 with Chart.js and Socket.IO client.

## Validation Checklist

After backend JavaScript edits:

```bash
node --check server/index.js
```

After frontend edits:

```bash
cd client
npm run build
```

For README-only edits, no application build is required, but running a build is still useful if the documentation change was made alongside UI work.

## Operational Notes

- Use dedicated test schemas. Many panels create and drop objects.
- Verify privileges before running monitor or RAC labs.
- RAC and GC labs are meaningful only when connected to a multi-instance database.
- Insert Blast and Datafile Growth can allocate significant storage.
- TDE tests require a database wallet/keystore configured for encrypted tablespaces or encrypted objects.
- HCC options require compatible storage.
- Some monitor values are cumulative database counters rather than workload-window-only measurements.
