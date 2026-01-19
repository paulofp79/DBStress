// Library Cache Lock Demo Engine (Correct Oracle Internals Model)
// Reproduces:
//   - library cache lock
//   - cursor: pin S wait on X
// Optional:
//   - hard parse storms (without ALTER SESSION abuse)

class LibraryCacheLockEngine {
  constructor() {
    this.isRunning = false;
    this.config = null;
    this.pool = null;
    this.db = null;
    this.io = null;

    this.workers = [];
    this.ddlWorker = null;
    this.schemaPrefix = '';

    this.stats = {
      totalCalls: 0,
      errors: 0,
      tps: 0,
      responseTimes: []
    };

    this.previousStats = { totalCalls: 0 };
    this._lastStatsTime = Date.now();

    this.statsInterval = null;
    this.waitEventsInterval = null;
  }

  /* =======================
     START
     ======================= */
  async start(db, config, io) {
    if (this.isRunning) {
      throw new Error('Library Cache Lock demo already running');
    }

    this.config = {
      threads: config.threads || 50,
      callInterval: config.callInterval || 0,
      ddlIntervalMs: config.ddlIntervalMs || 100,
      enableHardParseStorm: config.enableHardParseStorm || false,
      schemaPrefix: config.schemaPrefix || ''
    };

    this.db = db;
    this.io = io;
    this.schemaPrefix = this.config.schemaPrefix;
    this.isRunning = true;

    this.resetStats();

    this.io?.emit('library-cache-lock-status', { running: true, message: 'Starting...' });

    // Create pool
    this.pool = await db.createStressPool(this.config.threads + 2);

    // Create shared procedure
    await this.createProcedure();

    // Start execution workers
    for (let i = 0; i < this.config.threads; i++) {
      this.workers.push(this.runWorker(i));
    }

    // Start DDL invalidator
    this.ddlWorker = this.runDDLInvalidator();

    // Stats + waits
    this.statsInterval = setInterval(() => this.reportStats(), 1000);
    this.waitEventsInterval = setInterval(() => this.reportWaitEvents(), 5000);

    this.io?.emit('library-cache-lock-status', { running: true, message: 'Running' });
  }

  /* =======================
     SHARED PROCEDURE
     ======================= */
  async createProcedure() {
    const p = this.schemaPrefix ? `${this.schemaPrefix}_` : '';
    const procName = `${p}STRESS_SHARED_PROC`;

    await this.db.execute(`BEGIN
      EXECUTE IMMEDIATE 'DROP PROCEDURE ${procName}';
    EXCEPTION WHEN OTHERS THEN NULL;
    END;`);

    await this.db.execute(`
      CREATE OR REPLACE PROCEDURE ${procName}(p_module VARCHAR2) AS
        v_dummy NUMBER;
      BEGIN
        DBMS_APPLICATION_INFO.SET_MODULE(p_module, 'STRESS');

        ${this.config.enableHardParseStorm ? `
        EXECUTE IMMEDIATE
          'SELECT COUNT(*) FROM dual WHERE dummy = ''' ||
          DBMS_RANDOM.STRING('A', 5) || ''''
        INTO v_dummy;
        ` : `
        SELECT COUNT(*) INTO v_dummy FROM dual;
        `}
      END;
    `);
  }

  /* =======================
     WORKER SESSIONS
     ======================= */
  async runWorker(workerId) {
    const p = this.schemaPrefix ? `${this.schemaPrefix}_` : '';
    const procName = `${p}STRESS_SHARED_PROC`;

    while (this.isRunning) {
      let conn;
      try {
        conn = await this.pool.getConnection();

        while (this.isRunning) {
          const start = Date.now();
          const moduleName = `STRESS_W${workerId}_${Date.now().toString(36)}`;

          try {
            await conn.execute(
              `BEGIN ${procName}(:p); END;`,
              { p: moduleName }
            );

            const rt = Date.now() - start;
            this.stats.totalCalls++;
            this.stats.responseTimes.push(rt);
            if (this.stats.responseTimes.length > 1000) {
              this.stats.responseTimes.shift();
            }

          } catch (err) {
            // Expected invalidation errors
            if ([4068, 4065, 6508].includes(err.errorNum)) {
              continue;
            }
            this.stats.errors++;
            break;
          }

          if (this.config.callInterval > 0) {
            await this.sleep(this.config.callInterval);
          }
        }
      } finally {
        if (conn) {
          try { await conn.close(); } catch {}
        }
      }

      await this.sleep(500);
    }
  }

  /* =======================
     DDL INVALIDATOR
     ======================= */
  async runDDLInvalidator() {
    const p = this.schemaPrefix ? `${this.schemaPrefix}_` : '';
    const procName = `${p}STRESS_SHARED_PROC`;

    while (this.isRunning) {
      let conn;
      try {
        conn = await this.pool.getConnection();
        await conn.execute(`
          CREATE OR REPLACE PROCEDURE ${procName}(p_module VARCHAR2) AS
            v_dummy NUMBER;
          BEGIN
            DBMS_APPLICATION_INFO.SET_MODULE(p_module, 'STRESS');
            SELECT COUNT(*) INTO v_dummy FROM dual;
          END;
        `);
      } catch {
        // Ignore
      } finally {
        if (conn) {
          try { await conn.close(); } catch {}
        }
      }
      await this.sleep(this.config.ddlIntervalMs);
    }
  }

  /* =======================
     STATS
     ======================= */
  resetStats() {
    this.stats = { totalCalls: 0, errors: 0, tps: 0, responseTimes: [] };
    this.previousStats = { totalCalls: 0 };
    this._lastStatsTime = Date.now();
  }

  reportStats() {
    const now = Date.now();
    const elapsed = (now - this._lastStatsTime) / 1000;
    this._lastStatsTime = now;

    const delta = this.stats.totalCalls - this.previousStats.totalCalls;
    this.stats.tps = elapsed > 0 ? Math.round(delta / elapsed) : 0;
    this.previousStats.totalCalls = this.stats.totalCalls;

    const avgRT = this.stats.responseTimes.length
      ? Math.round(this.stats.responseTimes.reduce((a, b) => a + b, 0) / this.stats.responseTimes.length)
      : 0;

    this.io?.emit('library-cache-lock-metrics', {
      tps: this.stats.tps,
      avgResponseTime: avgRT,
      totalCalls: this.stats.totalCalls,
      errors: this.stats.errors
    });
  }

  /* =======================
     WAIT EVENTS
     ======================= */
  async reportWaitEvents() {
    if (!this.isRunning) return;

    try {
      const result = await this.db.execute(`
        SELECT event, SUM(time_waited_micro)/1e6 time_s, SUM(total_waits) waits
        FROM gv$session_event
        WHERE event IN (
          'library cache lock',
          'cursor: pin S wait on X',
          'library cache pin',
          'cursor: mutex S',
          'cursor: mutex X'
        )
        GROUP BY event
      `);

      const events = {};
      for (const r of result.rows) {
        events[r.EVENT] = {
          timeSeconds: r.TIME_S || 0,
          totalWaits: r.WAITS || 0
        };
      }

      this.io?.emit('library-cache-lock-wait-events', events);
    } catch {}
  }

  /* =======================
     STOP
     ======================= */
  async stop() {
    this.isRunning = false;

    clearInterval(this.statsInterval);
    clearInterval(this.waitEventsInterval);

    await this.sleep(500);

    if (this.pool) {
      try { await this.pool.close(2); } catch {}
    }

    this.io?.emit('library-cache-lock-stopped', this.stats);
    return this.stats;
  }

  sleep(ms) {
    return new Promise(r => setTimeout(r, ms));
  }
}

module.exports = new LibraryCacheLockEngine();
