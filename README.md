# DBStress

DBStress is an Oracle workload lab with two web applications in the same repository:

- `DBStress` on port `3001`: the main React/Node.js application and launcher
- `GC Benchmark` on port `8000`: the FastAPI-based Oracle RAC GC wait benchmark tool

The repository also includes helper scripts to start and stop both services together and shell tooling for direct database-side GC wait observation.

## What Is In This Repo

### Main Applications

#### 1. DBStress

`DBStress` is the original multi-tool Oracle stress testing application.

Main UI tabs:

- `Home`
- `Monitor`
- `Stress Test`
- `Index Contention Demo`
- `Library Cache Lock Demo`
- `HW Contention Demo`
- `Stats Comparison`
- `Skew Detection`
- `Metric Explorer`
- `TDE Comparison`
- `GC Congestion Demo`
- `GC Benchmark`

#### 2. GC Benchmark

`GC Benchmark` is the newer workload tool focused on Oracle RAC global cache waits.

Main GC Benchmark tabs:

- `Connection`
- `Schema Setup`
- `Run Workload`
- `Results & Comparison`

This is the tool used to:

- create many benchmark tables
- create indexed schemas for block and index hot-spot tests
- compare partitioned and non-partitioned index behavior
- run large concurrent workloads
- observe GC waits in real time
- compare completed runs across schema and workload variations

## Repository Structure

```text
client/                         React frontend for DBStress
server/                         Node.js backend for DBStress
gc_benchmark/gc_benchmark/      FastAPI GC Benchmark application
scripts/start-all.sh            Start DBStress and GC Benchmark
scripts/status-all.sh           Show current service state and ports
scripts/stop-all.sh             Stop DBStress and GC Benchmark
scripts/gc-wait-live.sh         Shell tool to watch live GC wait avg ms from SQL*Plus
startall.sh                     Root wrapper for npm run start-all
stopall.sh                      Root wrapper for npm run stop-all
DATA/                           Local data samples and analysis artifacts
```

## Requirements

### DBStress

- Node.js `18+`
- Oracle Database
- Oracle Instant Client for the Node.js `oracledb` driver

### GC Benchmark

- Python `3.10+`
- `python-oracledb`
- Oracle RAC recommended for meaningful GC waits

If your machine does not have `python3` but does have `python3.12`, use `python3.12` explicitly.

## Installation

### DBStress dependencies

```bash
cd /home/paportug/DBStress
npm run install-all
```

### GC Benchmark dependencies

If you use the bundled start scripts, the GC Benchmark virtual environment is created automatically when needed.

If you want to run GC Benchmark manually:

```bash
cd /home/paportug/DBStress/gc_benchmark/gc_benchmark
python3.12 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Start And Stop

### Recommended: start both applications together

From the repository root:

```bash
npm run start-all
npm run status-all
npm run stop-all
```

What they do:

- `npm run start-all`
  - builds the React frontend
  - starts `DBStress` on `http://localhost:3001`
  - starts `GC Benchmark` on `http://localhost:8000`
- `npm run status-all`
  - shows whether each service is running and its port
- `npm run stop-all`
  - stops both services

### Root wrapper scripts

You also have two short wrapper scripts in the repository root:

```bash
./startall.sh
./stopall.sh
```

They simply run:

```bash
npm run start-all
npm run stop-all
```

Use them from `/home/paportug/DBStress` if you want shorter commands.

### Run DBStress only

Development mode:

```bash
npm run dev
```

Or separately:

```bash
npm run server
npm run client
```

Production-style start:

```bash
npm run build
npm start
```

Access DBStress at:

```text
http://localhost:3001
```

### Run GC Benchmark only

```bash
cd /home/paportug/DBStress/gc_benchmark/gc_benchmark
source .venv/bin/activate
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

Access GC Benchmark at:

```text
http://localhost:8000
```

## DBStress Tools

### Home

Acts as the launcher page for the full toolset and provides navigation to the DBStress tools and GC Benchmark.

### Monitor

Real-time environment monitoring from Oracle `GV$` views.

Current behavior:

- starts and stops explicitly with buttons
- keeps running even if you leave the tab until you click `Stop`
- shows top wait events in real time
- chart Y-axis is average wait in milliseconds
- note: this tab shows cumulative avg wait since instance startup, so it is better for overall environment observation than for measuring one specific workload run

### Stress Test

The original general-purpose stress engine.

Main functions:

- connect to Oracle
- create the online sales schema
- run mixed DML stress tests
- track TPS and DML rates
- view wait events and GC waits while the test is active
- save and reuse recent connection definitions

### Index Contention Demo

Tool for generating index hot-block behavior and observing index-related contention effects.

### Library Cache Lock Demo

Tool for library cache lock contention experiments.

### HW Contention Demo

Tool focused on high-water-mark and segment extension style contention experiments.

### Stats Comparison

Tool for comparing optimizer statistics strategies and related performance effects.

### Skew Detection

Tool to create test tables, detect skew, inspect histograms, and compare distribution behavior across columns and tables.

### Metric Explorer

Utility tab for exploring and charting metrics from parsed input data.

### TDE Comparison

Compares encrypted and non-encrypted table behavior using equivalent workloads.

### GC Congestion Demo

A dedicated RAC demo focused on creating many tables, loading larger data sets, and generating GC congestion scenarios.

### DBStress Schema

The online sales schema in DBStress includes tables such as:

- `regions`
- `countries`
- `warehouses`
- `categories`
- `products`
- `inventory`
- `customers`
- `orders`
- `order_items`
- `payments`
- `order_history`
- `product_reviews`

## GC Benchmark

GC Benchmark is the main Oracle RAC contention tool in this repository.

Tracked GC events include:

- `gc current block congested`
- `gc current block 2-way`
- `gc current block 3-way`
- `gc cr block congested`
- `gc cr grant congested`
- `gc cr grant 2-way`

### GC Benchmark Tab: Connection

Purpose:

- connect to the benchmark target database
- save and reuse recent connections
- configure a separate CDB connection when required for CDB-only views

Current features:

- test Oracle connection
- save recent benchmark connections without passwords
- reuse recent connections
- choose `thin` or `thick` mode
- configure and test `CDB Connection`
- save and reuse recent CDB connections

Notes:

- passwords are session-only
- CDB Connection is used for operations such as connection pool stats and PDB restart

### GC Benchmark Tab: Schema Setup

Purpose:

- create and manage the benchmark schema used by the workload engine

Current features:

- choose benchmark table prefix
- choose number of tables
- choose rows per table scale
- create very large schemas, including multi-TB targets depending on chosen size
- create indexed benchmark tables
- compare non-partitioned versus hash-partitioned index designs
- preview DDL
- create tables in parallel using one session per table
- drop current benchmark schema
- drop matching tables by prefix
- kill sessions by user before retrying cleanup or starting a new run
- show the exact kill command in the log area
- use parallel killer sessions when many sessions must be removed

Live operational panels in this tab:

- database activity from `GV$` views
  - sessions
  - processes
  - transactions
- connection pool stats from `GV$CPOOL_STATS`
  - using the configured CDB connection when required

### GC Benchmark Tab: Run Workload

Purpose:

- run Oracle RAC workloads against a selected benchmark schema
- observe live workload progress and GC waits
- compare requested concurrency versus actual active workers

Current workload controls:

- choose an existing schema from Oracle
- choose contention mode
- set custom Normal-mode mix for `INSERT`, `UPDATE`, `DELETE`, and `SELECT`
- set lock hold time for Hammer mode
- set concurrent threads
- type or slide the thread count directly
- set duration
- set workload seed rows
- set hot row percentage
- restart a PDB as a separate action using the configured CDB connection

Current contention modes:

- `NORMAL`
- `HAMMER`
- `LMS_STRESS`
- `EXTREME_LMS`

Current live behavior:

- shows active schema
- shows running workload banner
- shows requested threads and physical workers or sessions
- shows current phase
  - `PREPARING`
  - `WARMING`
  - `RUNNING`
- preparation happens before the timed run starts
- worker sessions are warmed before the timed run starts
- the UI now shows preparation status instead of pretending the workload has already started
- the timed workload window starts only after preparation is complete
- stop works during preparation and during the run

Current live dashboard:

- inserts, updates, deletes, selects, errors
- per-second operation rates
- workload progress bar
- `GC Wait Events (live avg ms)` chart
- live database activity panel with username filter
- live connection pool stats panel

Important note about `Concurrent Threads`:

- the UI accepts values up to `10000`
- the backend currently still has an internal physical worker cap, so requested threads and actual physical workers may differ
- the current UI shows both values explicitly

Important note about `GC Wait Events (live avg ms)`:

- this chart uses delta average wait since workload start
- it is better for seeing what the current run is doing in real time
- it is not the same as Oracle cumulative startup average

### GC Benchmark Tab: Results & Comparison

Purpose:

- store completed workload runs
- compare runs across schema and workload changes

Current stored run data includes:

- run id
- date
- schema selected in `Run Workload`
- table count
- partition information
- compression
- thread count
- duration
- GC event deltas

Current results view includes:

- benchmark results table
- schema column from the selected workload schema dropdown
- comparison of primary GC metrics
- compare all runs or selected runs
- CSV export

## Direct Shell Tools

### `scripts/gc-wait-live.sh`

This helper script lets you query Oracle directly from the shell and print the same style of delta avg-ms metric used by the GC Benchmark chart.

It uses `sqlplus` and `gv$system_event`.

Usage:

```bash
./scripts/gc-wait-live.sh '<user/password@host:port/service>' [interval_seconds] [event_filter] [min_avg_wait_ms]
```

Examples:

```bash
./scripts/gc-wait-live.sh 'pp/pp123@host:1521/service'
./scripts/gc-wait-live.sh 'pp/pp123@host:1521/service' 5
./scripts/gc-wait-live.sh 'pp/pp123@host:1521/service' 5 'gc current block congested'
./scripts/gc-wait-live.sh 'pp/pp123@host:1521/service' 5 'gc current block congested' 2
```

Current behavior:

- loops continuously until `Ctrl+C`
- shows output per `inst_id`
- supports filtering by event name
- supports filtering by minimum `avg_wait_ms`
- now works in `since-start` mode so it matches the GC Benchmark chart more closely

## API Highlights

### DBStress backend

Examples:

- `POST /api/db/test-connection`
- `POST /api/db/connect`
- `POST /api/db/disconnect`
- `GET /api/db/status`
- `POST /api/schema/create`
- `POST /api/schema/drop`
- `GET /api/schemas/list`
- `POST /api/stress/start`
- `POST /api/stress/stop`
- `GET /api/stress/status`
- `PUT /api/stress/config`

### GC Benchmark backend

Examples:

- `POST /api/connect/test`
- `POST /api/connect/save`
- `GET /api/connect/recent`
- `POST /api/db/cpool-connection/test`
- `GET /api/cpool-connections/recent`
- `POST /api/db/pdb-restart`
- `POST /api/schema/create`
- `POST /api/schema/drop`
- `DELETE /api/schema/drop-prefix`
- `POST /api/schema/kill-sessions`
- `GET /api/schema/list`
- `GET /api/schema/state`
- `POST /api/workload/start`
- `POST /api/workload/stop`
- `GET /api/workload/status`
- `GET /api/db/activity`
- `GET /api/db/cpool-stats`
- `GET /api/results`
- `GET /api/results/compare`
- `GET /api/results/export/csv`

## Troubleshooting

### GC Benchmark starts but sessions appear late

The workload now has explicit startup phases:

- schema preparation
- worker session warm-up
- timed run

So sessions should begin appearing much earlier and the UI should show what phase is in progress.

### GC Benchmark requested threads do not match database sessions exactly

This can happen because:

- the current backend still distinguishes requested threads from physical workers
- Oracle services may be pooled depending on your service configuration
- resource limits on the database may prevent the requested concurrency from being realized

### Monitor and GC Benchmark show different avg wait values

That is expected:

- `Monitor` shows cumulative startup-based avg wait from Oracle views
- `GC Benchmark` shows delta avg wait since the workload started

### Connection pool stats show no rows

Check:

- whether you are using a pooled or DRCP service
- whether the stats must be queried from CDB in your environment
- whether the configured CDB connection is correct

### Drop tables fails

Common causes:

- sessions are still connected to the target tables
- workload owner sessions are still alive

Use `Kill Sessions by User`, then retry the drop.
