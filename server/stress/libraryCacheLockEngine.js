// Library Cache Lock Demo Engine
// Simulates library cache lock contention through repeated ALTER SESSION calls
// Reproduces: library cache lock, cursor: pin S wait on X, hard parse storms

class LibraryCacheLockEngine {
  constructor() {
    this.isRunning = false;
    this.config = null;
    this.pool = null;
    this.io = null;
    this.db = null;
    this.workers = [];
    this.schemaPrefix = '';

    // Performance metrics
    this.stats = {
      totalCalls: 0,
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
      threads: config.threads || 50,
      thinkTime: config.thinkTime || 0,
      callsPerSession: config.callsPerSession || 100,  // Calls before reconnect
      useAlterSession: config.useAlterSession !== false,  // Toggle ALTER SESSION statements
      useSetModule: config.useSetModule !== false,  // Toggle DBMS_APPLICATION_INFO
      useSetIdentifier: config.useSetIdentifier !== false,  // Toggle DBMS_SESSION
      ...config
    };

    this.schemaPrefix = config.schemaPrefix || '';
    this.io = io;
    this.db = db;
    this.isRunning = true;

    // Reset stats
    this.stats = {
      totalCalls: 0,
      errors: 0,
      responseTimes: [],
      tps: 0
    };
    this.previousStats = { totalCalls: 0 };
    this._lastStatsTime = Date.now();

    console.log(`Starting Library Cache Lock Demo with ${this.config.threads} threads`);

    // Emit running status immediately
    this.io?.emit('library-cache-lock-status', { running: true, message: 'Starting...' });

    // Create connection pool
    this.pool = await db.createStressPool(this.config.threads + 5);

    // Create the procedure
    await this.createProcedure(db);

    // Start workers
    for (let i = 0; i < this.config.threads; i++) {
      this.workers.push(this.runWorker(i));
    }

    // Start stats reporting (every second)
    this.statsInterval = setInterval(() => this.reportStats(), 1000);

    // Start wait events monitoring (every 2 seconds)
    this.waitEventsInterval = setInterval(() => this.reportWaitEvents(db), 2000);

    this.io?.emit('library-cache-lock-status', { running: true, message: 'Running...' });
  }

  async createProcedure(db) {
    const p = this.schemaPrefix ? `${this.schemaPrefix}_` : '';
    const procName = `${p}STRESS_SESSION_PROC`;

    this.io?.emit('library-cache-lock-status', { message: 'Creating procedure...' });

    try {
      // Drop existing procedure if exists
      try {
        await db.execute(`DROP PROCEDURE ${procName}`);
      } catch (err) {
        // Procedure might not exist
      }

      // Create the procedure that causes library cache lock contention
      // This mimics the GRAV_SESSION_MFES_ONLINE procedure pattern
      await db.execute(`
        CREATE OR REPLACE PROCEDURE ${procName} (
          pModuleName VARCHAR2
        )
        IS
          pActionName      VARCHAR2(14);
          pModuleName_mod  VARCHAR2(48);
        BEGIN
          -- Build action name
          pActionName := 'STRESS_ONLINE';

          -- Build modified module name (similar to original pattern)
          pModuleName_mod := SUBSTR(pModuleName, 1, 22)
                            || '0000000'
                            || SUBSTR(pModuleName, 30);

          -- DBMS_APPLICATION_INFO.SET_MODULE - populates v$session module and action
          DBMS_APPLICATION_INFO.SET_MODULE(pModuleName_mod, pActionName);

          -- DBMS_SESSION.SET_IDENTIFIER - populates client_identifier
          DBMS_SESSION.SET_IDENTIFIER(pModuleName);

          -- Multiple ALTER SESSION statements - these cause library cache lock contention
          -- Each ALTER SESSION requires exclusive access to the library cache

          -- Setup optimizer_mode
          EXECUTE IMMEDIATE 'ALTER SESSION SET OPTIMIZER_MODE = first_rows_1';

          -- Disable optimizer features (causes hard parsing)
          EXECUTE IMMEDIATE 'ALTER SESSION SET "_optimizer_use_feedback" = false';
          EXECUTE IMMEDIATE 'ALTER SESSION SET "_optimizer_adaptive_cursor_sharing" = false';
          EXECUTE IMMEDIATE 'ALTER SESSION SET "_optimizer_extended_cursor_sharing_rel" = none';

          -- Additional ALTER SESSION to increase contention
          EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_DATE_FORMAT = ''YYYY-MM-DD HH24:MI:SS''';

        END;
      `);

      console.log(`Created procedure: ${procName}`);
      this.io?.emit('library-cache-lock-status', { message: 'Procedure ready' });
    } catch (err) {
      console.error('Error creating procedure:', err);
      this.io?.emit('library-cache-lock-status', { message: `Procedure error: ${err.message}` });
      throw err;
    }
  }

  async runWorker(workerId) {
    const p = this.schemaPrefix ? `${this.schemaPrefix}_` : '';
    const procName = `${p}STRESS_SESSION_PROC`;

    while (this.isRunning) {
      if (!this.pool) {
        await this.sleep(100);
        continue;
      }

      let connection;
      let callCount = 0;

      try {
        connection = await this.pool.getConnection();

        // Make multiple calls on same session before reconnecting
        // This simulates "Same session repeatedly calling a procedure"
        while (this.isRunning && callCount < this.config.callsPerSession) {
          const startTime = Date.now();

          try {
            // Generate a unique module name for each call
            const timestamp = Date.now().toString(36);
            const moduleName = `STRESS_W${workerId.toString().padStart(3, '0')}_${timestamp}_TESTMODULE`;

            if (this.config.useAlterSession) {
              // Call the procedure with ALTER SESSION statements
              await connection.execute(
                `BEGIN ${procName}(:moduleName); END;`,
                { moduleName }
              );
            } else {
              // Light version - only SET_MODULE, no ALTER SESSION
              await connection.execute(
                `BEGIN
                   DBMS_APPLICATION_INFO.SET_MODULE(:moduleName, 'STRESS_LIGHT');
                 END;`,
                { moduleName }
              );
            }

            const responseTime = Date.now() - startTime;
            this.stats.totalCalls++;
            this.stats.responseTimes.push(responseTime);

            // Keep only last 1000 response times
            if (this.stats.responseTimes.length > 1000) {
              this.stats.responseTimes.shift();
            }

            callCount++;

          } catch (err) {
            this.stats.errors++;
            if (this.stats.errors % 100 === 1) {
              console.log(`Library cache lock worker ${workerId} error:`, err.message);
            }
          }

          // Think time between calls
          if (this.isRunning && this.config.thinkTime > 0) {
            await this.sleep(this.config.thinkTime);
          }
        }

      } catch (err) {
        this.stats.errors++;
        if (!err.message.includes('pool is terminating') &&
            !err.message.includes('NJS-003') &&
            !err.message.includes('NJS-500')) {
          if (this.stats.errors % 100 === 1) {
            console.log(`Library cache lock worker ${workerId} connection error:`, err.message);
          }
        }
      } finally {
        if (connection) {
          try { await connection.close(); } catch (e) {}
        }
      }

      // Small delay before getting new connection
      if (this.isRunning) {
        await this.sleep(10);
      }
    }
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
      errors: this.stats.errors
    };

    if (this.io) {
      this.io.emit('library-cache-lock-metrics', metrics);
    }
  }

  async reportWaitEvents(db) {
    if (!this.isRunning) return;

    try {
      // Query GV$SESSION_EVENT for library cache related wait events (RAC-aware)
      const result = await db.execute(`
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
      const libCacheResult = await db.execute(`
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
        const parseResult = await db.execute(`
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

    if (this.io) {
      this.io.emit('library-cache-lock-stopped', {
        totalCalls: this.stats.totalCalls,
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
