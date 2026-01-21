// Library Cache Lock Demo Engine
// Simulates library cache lock contention through non-existent sequence lookups
//
// Pattern based on real customer issue:
// - Multiple sessions executing SELECT statements against non-existent sequences
// - 3 SELECTs in sequence that reference objects that don't exist
// - This causes: library cache lock (not pin) as Oracle tries to resolve non-existent objects
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
    this.schemaPrefix = '';

    // Performance metrics
    this.stats = {
      totalCalls: 0,
      totalSelects: 0,
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
      sequenceCount: config.sequenceCount || 3,   // Number of non-existent sequences to query
      loopDelay: config.loopDelay || 0,           // Delay between loops (ms), 0 = tight loop
      ...config
    };

    this.schemaPrefix = config.schemaPrefix || '';
    this.io = io;
    this.db = db;
    this.isRunning = true;

    // Reset stats
    this.stats = {
      totalCalls: 0,
      totalSelects: 0,
      errors: 0,
      responseTimes: [],
      tps: 0
    };
    this.previousStats = { totalCalls: 0 };
    this._lastStatsTime = Date.now();

    console.log(`Starting Library Cache Lock Demo with ${this.config.threads} workers`);
    console.log(`Pattern: ${this.config.sequenceCount} SELECTs against non-existent sequences per iteration`);

    // Emit running status immediately
    this.io?.emit('library-cache-lock-status', { running: true, message: 'Starting...' });

    // Create connection pool
    this.pool = await db.createStressPool(this.config.threads + 10);

    // Start execution workers - each will query non-existent sequences
    for (let i = 0; i < this.config.threads; i++) {
      this.workers.push(this.runWorker(i));
    }

    // Start stats reporting (every second)
    this.statsInterval = setInterval(() => this.reportStats(), 1000);

    // Start wait events monitoring (every 5 seconds)
    this.waitEventsInterval = setInterval(() => this.reportWaitEvents(), 5000);

    this.io?.emit('library-cache-lock-status', { running: true, message: 'Running...' });
  }

  // Worker that executes SELECTs against non-existent sequences
  // This pattern causes library cache lock as Oracle tries to resolve the objects
  async runWorker(workerId) {
    const p = this.schemaPrefix ? `${this.schemaPrefix}_` : '';

    // Generate unique non-existent sequence names for this worker
    // Using timestamp + random to ensure they don't exist
    const getSequenceNames = () => {
      const names = [];
      const ts = Date.now().toString(36);
      for (let i = 0; i < this.config.sequenceCount; i++) {
        names.push(`${p}NONEXIST_SEQ_${workerId}_${ts}_${i}`);
      }
      return names;
    };

    while (this.isRunning) {
      if (!this.pool) {
        await this.sleep(100);
        continue;
      }

      let connection;

      try {
        connection = await this.pool.getConnection();

        // Tight loop - keep executing SELECTs against non-existent sequences
        while (this.isRunning) {
          const startTime = Date.now();
          const sequenceNames = getSequenceNames();

          try {
            // Execute 3 SELECTs in sequence against non-existent sequences
            // This is the pattern that caused library cache lock at customer
            for (const seqName of sequenceNames) {
              try {
                await connection.execute(`SELECT ${seqName}.NEXTVAL FROM DUAL`);
              } catch (err) {
                // ORA-02289: sequence does not exist - THIS IS EXPECTED
                // The error itself causes library cache lock contention
                if (err.message.includes('ORA-02289')) {
                  this.stats.totalSelects++;
                  // This is the desired behavior - the lookup causes contention
                } else {
                  throw err;
                }
              }
            }

            const responseTime = Date.now() - startTime;
            this.stats.totalCalls++;
            this.stats.responseTimes.push(responseTime);

            // Keep only last 1000 response times
            if (this.stats.responseTimes.length > 1000) {
              this.stats.responseTimes.shift();
            }

          } catch (err) {
            this.stats.errors++;
            if (this.stats.errors % 100 === 1) {
              console.log(`Worker ${workerId} error:`, err.message);
            }
          }

          // Optional delay between loops
          if (this.isRunning && this.config.loopDelay > 0) {
            await this.sleep(this.config.loopDelay);
          }
        }

      } catch (err) {
        this.stats.errors++;
        if (!err.message.includes('pool is terminating') &&
            !err.message.includes('NJS-003') &&
            !err.message.includes('NJS-500')) {
          if (this.stats.errors % 100 === 1) {
            console.log(`Worker ${workerId} connection error:`, err.message);
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

  reportStats() {
    const now = Date.now();
    const elapsedSeconds = (now - this._lastStatsTime) / 1000;
    this._lastStatsTime = now;

    // Calculate TPS (iterations per second)
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
      totalSelects: this.stats.totalSelects,
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

    if (this.io) {
      this.io.emit('library-cache-lock-stopped', {
        totalCalls: this.stats.totalCalls,
        totalSelects: this.stats.totalSelects,
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
