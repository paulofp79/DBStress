# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

DBStress is an Oracle Database stress testing and performance demonstration tool with real-time monitoring. It provides multiple demonstration modes for teaching database performance concepts (contention, locking, histogram statistics) via a React frontend and Node.js backend.

## Build & Development Commands

```bash
npm run install-all      # Install all dependencies (root + client)
npm run dev              # Run backend + frontend concurrently (dev mode)
npm run server           # Backend only (port 3001)
npm run client           # Frontend only (port 3000)
npm run build            # Build React frontend for production
npm start                # Build + start server (serves React from /client/build)
```

## Architecture

```
React Frontend (port 3000)     →  Socket.IO + REST API  →  Node.js Backend (port 3001)
    ↓                                                            ↓
5 Tabs:                                                     oracledb driver
- Stress Test                                                    ↓
- Index Contention Demo                                   Oracle Database
- Library Cache Lock Demo                                 (V$ views for metrics)
- HW Contention Demo
- Stats Comparison
```

### Backend Structure (`server/`)
- **index.js** - Express app, Socket.IO server, all API routes
- **db/oracle.js** - OracleDB connection pool wrapper (singleton pattern)
- **db/schemaManager.js** - Schema DDL & data generation (multi-schema support via table prefixes)
- **metrics/collector.js** - Periodic V$ view polling (2-second intervals), emits `db-metrics` events
- **stress/engine.js** - Main workload engine with async worker tasks, multi-schema support
- **stress/indexContentionEngine.js** - B-tree index contention demo (sequence cache tuning, A/B testing)
- **stress/libraryCacheLockEngine.js** - DDL lock contention demo
- **stress/hwContentionEngine.js** - CPU cache coherency contention demo
- **stress/statsComparisonEngine.js** - DBMS_STATS histogram comparison (SIZE 254 vs SIZE AUTO)

### Frontend Structure (`client/src/`)
- **App.js** - Main component with tab navigation, Socket.IO + Axios setup
- **components/** - ConnectionPanel, SchemaPanel, StressConfigPanel, MetricsPanel, TPSChart, OperationsChart, WaitEventsPanel, GCWaitChart, plus demo-specific panels

## Key Patterns

- **Singleton**: OracleDatabase class exported as single instance
- **Multi-schema**: Tables use prefixes, metrics tracked per prefix
- **Real-time**: Socket.IO for bidirectional communication, metrics emitted every 2 seconds
- **RAC-aware**: Code queries GV$ views for cluster-wide metrics

## Environment Configuration

Copy `.env.example` to `.env`:
```env
ORACLE_USER=your_username
ORACLE_PASSWORD=your_password
ORACLE_CONNECTION_STRING=localhost:1521/ORCLPDB1
PORT=3001
```

## Database Requirements

- Oracle 19c+ recommended (12c+ minimum)
- Requires Oracle Instant Client
- User needs: CREATE SESSION, CREATE TABLE, CREATE SEQUENCE, UNLIMITED TABLESPACE
- For monitoring: SELECT on V_$SESSION, V_$SYSTEM_EVENT, V_$SYSSTAT, V_$SQL, GV_$* views, DBA_TAB_COLS

## Development Notes

- No test suite or linting configuration present
- No CI/CD pipeline
- Feature branches used (e.g., `claude/oracle-sales-stress-test-...`)
- Frontend uses Chart.js with 60-second rolling windows for metrics visualization
- Dark theme styling in `client/src/index.css`