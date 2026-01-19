// Library Cache Lock Demo Engine
// Simulates library cache lock contention through DDL invalidation pattern
//
// Correct pattern to generate library cache lock:
// - Role 1 (Execution workers): Many sessions executing ONE shared procedure in tight loop
// - Role 2 (DDL invalidator): Separate session continuously recompiling the procedure
//
// This causes: library cache lock, cursor: pin S wait on X, hard parse storms

class LibraryCacheLockEngine {
  constructor() {
    this.isRunning = false;
    this.config = null;
    this.pool = null;
    this.io = null;
    this.db = null;
    this.workers = [];
    this.invalidatorWorker = null;
    this.schemaPrefix = '';

    // Performance metrics
    this.stats = {
      totalCalls: 0,
      totalInvalidations: 0,
      errors: 0,
      responseTimes: [],
      tps: 0
    };

    this.previousStats = {
      totalCalls: 0
    };

    this.statsInterval = null;
    this.waitEventsInterval = null;
    this._lastStatsTime = Date.now();
  }

  async start(db, config, io) {
    if (this.isRunning) {
      throw new Error('Library Cache Lock demo already running');
    }

    this.config = {
      threads: config.threads || 50,              // Number of execution workers
      invalidateInterval: config.invalidateInterval || 1000,  // How often to recompile (ms)
      enableInvalidator: config.enableInvalidator !== false,  // Toggle DDL invalidator
      ...config
    };

    this.schemaPrefix = config.schemaPrefix || '';
    this.io = io;
    this.db = db;
    this.isRunning = true;

    // Reset stats
    this.stats = {
      totalCalls: 0,
      totalInvalidations: 0,
      errors: 0,
      responseTimes: [],
      tps: 0
    };
    this.previousStats = { totalCalls: 0 };
    this._lastStatsTime = Date.now();

    console.log(`Starting Library Cache Lock Demo with ${this.config.threads} execution workers`);
    console.log(`DDL Invalidator: ${this.config.enableInvalidator ? 'ENABLED' : 'DISABLED'} (interval: ${this.config.invalidateInterval}ms)`);

    // Emit running status immediately
    this.io?.emit('library-cache-lock-status', { running: true, message: 'Starting...' });

    // Create connection pool (extra connections for invalidator)
    this.pool = await db.createStressPool(this.config.threads + 10);

    // Create the simple procedure (no ALTER SESSION - just simple work)
    await this.createProcedure(db);

    // Start execution workers (Role 1) - tight loop calling the procedure
    for (let i = 0; i < this.config.threads; i++) {
      this.workers.push(this.runExecutionWorker(i));
    }

    // Start DDL invalidator worker (Role 2) - continuously recompiles the procedure
    if (this.config.enableInvalidator) {
      this.invalidatorWorker = this.runInvalidatorWorker();
    }

    // Start stats reporting (every second)
    this.statsInterval = setInterval(() => this.reportStats(), 1000);

    // Start wait events monitoring (every 5 seconds)
    this.waitEventsInterval = setInterval(() => this.reportWaitEvents(), 5000);

    this.io?.emit('library-cache-lock-status', { running: true, message: 'Running...' });
  }

  async createProcedure(db) {
    const p = this.schemaPrefix ? `${this.schemaPrefix}_` : '';
    const procName = `${p}LIB_CACHE_STRESS_PROC`;

    this.io?.emit('library-cache-lock-status', { running: true, message: 'Creating procedure...' });

    try {
      // Drop existing procedure if exists
      try {
        await db.execute(`DROP PROCEDURE ${procName}`);
      } catch (err) {
        // Procedure might not exist
      }

      // Create a SIMPLE procedure - NO ALTER SESSION, NO DDL
      // Just does some simple work that execution workers will call
      await db.execute(`
        CREATE OR REPLACE PROCEDURE ${procName} (
          p_input NUMBER,
          p_output OUT NUMBER
        )
        IS
          v_temp NUMBER;
        BEGIN
          -- Simple computation work
          v_temp := p_input * 2;
          v_temp := v_temp + DBMS_RANDOM.VALUE(1, 100);
          v_temp := SQRT(v_temp * v_temp);
          p_output := v_temp;
        END;
      `);

      console.log(`Created procedure: ${procName}`);
      this.io?.emit('library-cache-lock-status', { running: true, message: 'Procedure ready' });
    } catch (err) {
      console.error('Error creating procedure:', err);
      this.io?.emit('library-cache-lock-status', { running: true, message: `Procedure error: ${err.message}` });
      throw err;
    }
  }

  // Role 1: Execution workers - call the procedure in tight loop
  async runExecutionWorker(workerId) {
    const p = this.schemaPrefix ? `${this.schemaPrefix}_` : '';
    const procName = `${p}LIB_CACHE_STRESS_PROC`;

    while (this.isRunning) {
      if (!this.pool) {
        await this.sleep(100);
        continue;
      }

      let connection;

      try {
        connection = await this.pool.getConnection();

        // Tight loop - keep calling the procedure as fast as possible
        while (this.isRunning) {
          const startTime = Date.now();

          try {
            // Call the shared procedure
            const result = await connection.execute(
              `BEGIN ${procName}(:input, :output); END;`,
              {
                input: workerId + Date.now() % 1000,
                output: { dir: require('oracledb').BIND_OUT, type: require('oracledb').NUMBER }
              }
            );

            const responseTime = Date.now() - startTime;
            this.stats.totalCalls++;
            this.stats.responseTimes.push(responseTime);

            // Keep only last 1000 response times
            if (this.stats.responseTimes.length > 1000) {
              this.stats.responseTimes.shift();
            }

          } catch (err) {
            this.stats.errors++;
            // ORA-04068: existing state of packages has been discarded (expected during recompile)
            // ORA-04061: existing state of <object> has been invalidated (expected during recompile)
            if (!err.message.includes('ORA-04068') &&
                !err.message.includes('ORA-04061') &&
                this.stats.errors % 100 === 1) {
              console.log(`Execution worker ${workerId} error:`, err.message);
            }
            // Continue in the loop - don't break on invalidation errors
          }

          // No sleep - tight loop for maximum contention
        }

      } catch (err) {
        this.stats.errors++;
        if (!err.message.includes('pool is terminating') &&
            !err.message.includes('NJS-003') &&
            !err.message.includes('NJS-500')) {
          if (this.stats.errors % 100 === 1) {
            console.log(`Execution worker ${workerId} connection error:`, err.message);
          }
        }
      } finally {
        if (connection) {
          try { await connection.close(); } catch (e) {}
        }
      }

      // Small delay before reconnecting after error
      if (this.isRunning) {
        await this.sleep(100);
      }
    }
  }

  // Role 2: DDL invalidator - continuously recompiles the procedure
  async runInvalidatorWorker() {
    const p = this.schemaPrefix ? `${this.schemaPrefix}_` : '';
    const procName = `${p}LIB_CACHE_STRESS_PROC`;

    console.log(`DDL Invalidator started - recompiling ${procName} every ${this.config.invalidateInterval}ms`);

    while (this.isRunning) {
      if (!this.pool) {
        await this.sleep(100);
        continue;
      }

      let connection;

      try {
        connection = await this.pool.getConnection();

        while (this.isRunning) {
          try {
            // Recompile the procedure - this invalidates it in the library cache
            // All executing sessions will need to wait for library cache lock
            await connection.execute(`ALTER PROCEDURE ${procName} COMPILE`);
            this.stats.totalInvalidations++;

          } catch (err) {
            // Ignore compilation errors - procedure might be in use
            if (this.stats.errors % 100 === 1) {
              console.log(`Invalidator error:`, err.message);
            }
          }

          // Wait before next recompile
          if (this.isRunning) {
            await this.sleep(this.config.invalidateInterval);
          }
        }

      } catch (err) {
        if (!err.message.includes('pool is terminating')) {
          console.log(`Invalidator connection error:`, err.message);
        }
      } finally {
        if (connection) {
          try { await connection.close(); } catch (e) {}
        }
      }

      if (this.isRunning) {
        await this.sleep(1000);
      }
    }

    console.log('DDL Invalidator stopped');
  }

  reportStats() {
    const now = Date.now();
    const elapsedSeconds = (now - this._lastStatsTime) / 1000;
    this._lastStatsTime = now;

    // Calculate TPS
    const currentTotalCalls = this.stats.totalCalls;
    const callsDelta = currentTotalCalls - this.previousStats.totalCalls;
    const tps = elapsedSeconds > 0 ? Math.round(callsDelta / elapsedSeconds) : 0;
    this.stats.tps = tps;
    this.previousStats.totalCalls = currentTotalCalls;

    const avgResponseTime = this.stats.responseTimes.length > 0
      ? this.stats.responseTimes.reduce((a, b) => a + b, 0) / this.stats.responseTimes.length
      : 0;

    const metrics = {
      tps,
      avgResponseTime,
      totalCalls: this.stats.totalCalls,
      totalInvalidations: this.stats.totalInvalidations,
      errors: this.stats.errors
    };

    if (this.io) {
      this.io.emit('library-cache-lock-metrics', metrics);
    }
  }

  async reportWaitEvents() {
    if (!this.isRunning || !this.db) return;

    try {
      // Query GV$SESSION_EVENT for library cache related wait events (RAC-aware)
      const result = await this.db.execute(`
        SELECT * FROM (
          SELECT event, SUM(time_waited_micro)/1000000 as time_seconds, SUM(total_waits) as total_waits
          FROM gv$session_event
          WHERE wait_class != 'Idle'
          GROUP BY event
          ORDER BY time_seconds DESC
        ) WHERE ROWNUM <= 10
      `);

      const top10WaitEvents = [];
      for (const row of result.rows) {
        top10WaitEvents.push({
          event: row.EVENT,
          timeSeconds: row.TIME_SECONDS || 0,
          totalWaits: row.TOTAL_WAITS || 0
        });
      }

      // Also get the specific library cache contention events
      const libCacheResult = await this.db.execute(`
        SELECT event, SUM(time_waited_micro)/1000000 as time_seconds, SUM(total_waits) as total_waits
        FROM gv$session_event
        WHERE event IN (
          'library cache lock',
          'library cache pin',
          'library cache load lock',
          'cursor: pin S wait on X',
          'cursor: pin S',
          'cursor: mutex S',
          'cursor: mutex X',
          'latch: shared pool',
          'latch: library cache',
          'row cache lock'
        )
        GROUP BY event
      `);

      const libraryCacheEvents = {};
      for (const row of libCacheResult.rows) {
        libraryCacheEvents[row.EVENT] = {
          timeSeconds: row.TIME_SECONDS || 0,
          totalWaits: row.TOTAL_WAITS || 0
        };
      }

      // Get hard parse count from GV$SYSSTAT
      let hardParses = 0;
      let parseCount = 0;
      try {
        const parseResult = await this.db.execute(`
          SELECT name, SUM(value) as total
          FROM gv$sysstat
          WHERE name IN ('parse count (hard)', 'parse count (total)')
          GROUP BY name
        `);
        for (const row of parseResult.rows) {
          if (row.NAME === 'parse count (hard)') {
            hardParses = row.TOTAL || 0;
          } else if (row.NAME === 'parse count (total)') {
            parseCount = row.TOTAL || 0;
          }
        }
      } catch (e) {
        // Silently fail
      }

      if (this.io) {
        this.io.emit('library-cache-lock-wait-events', {
          top10WaitEvents,
          libraryCacheEvents,
          hardParses,
          parseCount
        });
      }
    } catch (err) {
      console.log('Cannot query wait events:', err.message);
    }
  }

  async stop() {
    console.log('Stopping Library Cache Lock Demo...');
    this.isRunning = false;

    if (this.statsInterval) {
      clearInterval(this.statsInterval);
      this.statsInterval = null;
    }

    if (this.waitEventsInterval) {
      clearInterval(this.waitEventsInterval);
      this.waitEventsInterval = null;
    }

    await this.sleep(500);

    if (this.pool) {
      try {
        await this.pool.close(2);
      } catch (err) {
        console.log('Pool close warning:', err.message);
      }
      this.pool = null;
    }

    this.workers = [];
    this.invalidatorWorker = null;

    if (this.io) {
      this.io.emit('library-cache-lock-stopped', {
        totalCalls: this.stats.totalCalls,
        totalInvalidations: this.stats.totalInvalidations,
        errors: this.stats.errors
      });
    }

    console.log('Library Cache Lock Demo stopped');
    return this.stats;
  }

  sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }
}

module.exports = new LibraryCacheLockEngine();
