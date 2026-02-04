// HW (High Water Mark) Contention Demo Engine
// Simulates enq: HW - contention wait event in Oracle
//
// This wait event occurs when multiple sessions try to allocate space
// in the same segment simultaneously (High Water Mark contention)
//
// Three test modes:
// 1. No pre-allocation: Maximum HW contention
// 2. Pre-allocate extents: Reduced HW contention
// 3. Partitioned table: Distributed HW contention across partitions

class HWContentionEngine {
  constructor() {
    this.isRunning = false;
    this.config = null;
    this.pool = null;
    this.io = null;
    this.db = null;
    this.workers = [];
    this.schemaPrefix = '';
    this.extentAllocatorInterval = null;

    // Performance metrics
    this.stats = {
      totalInserts: 0,
      errors: 0,
      responseTimes: [],
      tps: 0
    };

    this.previousStats = {
      totalInserts: 0
    };

    this.statsInterval = null;
    this.waitEventsInterval = null;
    this._lastStatsTime = Date.now();
  }

  async start(db, config, io) {
    if (this.isRunning) {
      throw new Error('HW Contention demo already running');
    }

    this.config = {
      threads: config.threads || 50,                    // Number of inserting sessions
      testMode: config.testMode || 'no_prealloc',       // 'no_prealloc', 'prealloc', 'partitioned'
      preAllocExtents: config.preAllocExtents || 100,   // Number of extents to pre-allocate
      partitionCount: config.partitionCount || 8,       // Number of partitions
      batchSize: config.batchSize || 1,                 // Inserts per commit
      loopDelay: config.loopDelay || 0,                 // Delay between inserts (ms)
      ...config
    };

    this.schemaPrefix = config.schemaPrefix || '';
    this.io = io;
    this.db = db;
    this.isRunning = true;

    // Reset stats
    this.stats = {
      totalInserts: 0,
      errors: 0,
      responseTimes: [],
      tps: 0
    };
    this.previousStats = { totalInserts: 0 };
    this._lastStatsTime = Date.now();

    console.log(`Starting HW Contention Demo with ${this.config.threads} workers`);
    console.log(`Test mode: ${this.config.testMode}`);

    // Emit running status immediately
    this.io?.emit('hw-contention-status', { running: true, message: 'Starting...' });

    // Create connection pool
    this.pool = await db.createStressPool(this.config.threads + 10);

    // Setup table based on test mode
    await this.setupTable(db);

    // Start insert workers
    for (let i = 0; i < this.config.threads; i++) {
      this.workers.push(this.runInsertWorker(i));
    }

    // Start stats reporting (every second)
    this.statsInterval = setInterval(() => this.reportStats(), 1000);

    // Start wait events monitoring (every 5 seconds)
    this.waitEventsInterval = setInterval(() => this.reportWaitEvents(), 5000);

    this.io?.emit('hw-contention-status', { running: true, message: 'Running...' });
  }

  async setupTable(db) {
    const p = this.schemaPrefix ? `${this.schemaPrefix}_` : '';
    const tableName = `${p}HW_STRESS_TAB`;
    const partTableName = `${p}HW_STRESS_TAB_PART`;

    this.io?.emit('hw-contention-status', { running: true, message: 'Dropping existing tables...' });

    // Always drop BOTH tables to ensure clean start
    try {
      await db.execute(`DROP TABLE ${tableName} PURGE`);
      console.log(`Dropped table: ${tableName}`);
    } catch (err) {
      // Table might not exist
    }

    try {
      await db.execute(`DROP TABLE ${partTableName} PURGE`);
      console.log(`Dropped table: ${partTableName}`);
    } catch (err) {
      // Table might not exist
    }

    this.io?.emit('hw-contention-status', { running: true, message: 'Creating fresh table...' });

    try {
      if (this.config.testMode === 'partitioned') {
        // Create partitioned table by hash on timestamp
        const partitions = [];
        for (let i = 0; i < this.config.partitionCount; i++) {
          partitions.push(`PARTITION p${i}`);
        }

        await db.execute(`
          CREATE TABLE ${partTableName} (
            TCL_ID NUMBER GENERATED ALWAYS AS IDENTITY,
            TCL_TIMESTAMP TIMESTAMP DEFAULT SYSTIMESTAMP,
            TCL_TRANSACCION VARCHAR2(100),
            TCL_USUARIO VARCHAR2(100),
            TCL_CENTRO_CONTA VARCHAR2(50),
            TCL_TERMINAL_CONTA VARCHAR2(50),
            TCL_SYSID_CICS VARCHAR2(50),
            TCL_DAT_ORIGINALES VARCHAR2(4000)
          )
          PARTITION BY HASH (TCL_ID)
          PARTITIONS ${this.config.partitionCount}
        `);

        console.log(`Created partitioned table: ${partTableName} with ${this.config.partitionCount} partitions`);
        this.io?.emit('hw-contention-status', { running: true, message: `Partitioned table ready (${this.config.partitionCount} partitions)` });

      } else {
        // Create regular table with small initial extent to force HW contention
        await db.execute(`
          CREATE TABLE ${tableName} (
            TCL_ID NUMBER GENERATED ALWAYS AS IDENTITY,
            TCL_TIMESTAMP TIMESTAMP DEFAULT SYSTIMESTAMP,
            TCL_TRANSACCION VARCHAR2(100),
            TCL_USUARIO VARCHAR2(100),
            TCL_CENTRO_CONTA VARCHAR2(50),
            TCL_TERMINAL_CONTA VARCHAR2(50),
            TCL_SYSID_CICS VARCHAR2(50),
            TCL_DAT_ORIGINALES VARCHAR2(4000)
          )
          STORAGE (INITIAL 64K NEXT 64K)
        `);

        console.log(`Created table: ${tableName}`);

        // Pre-allocate extents if requested
        if (this.config.testMode === 'prealloc') {
          this.io?.emit('hw-contention-status', { running: true, message: `Pre-allocating ${this.config.preAllocExtents} extents...` });

          for (let i = 0; i < this.config.preAllocExtents; i++) {
            await db.execute(`ALTER TABLE ${tableName} ALLOCATE EXTENT`);
            if (i % 10 === 0) {
              this.io?.emit('hw-contention-status', { running: true, message: `Pre-allocated ${i + 1}/${this.config.preAllocExtents} extents...` });
            }
          }

          console.log(`Pre-allocated ${this.config.preAllocExtents} extents`);
          this.io?.emit('hw-contention-status', { running: true, message: `Table ready with ${this.config.preAllocExtents} pre-allocated extents` });
        } else {
          this.io?.emit('hw-contention-status', { running: true, message: 'Table ready (no pre-allocation)' });
        }
      }

    } catch (err) {
      console.error('Error setting up table:', err);
      this.io?.emit('hw-contention-status', { running: true, message: `Table error: ${err.message}` });
      throw err;
    }
  }

  async runInsertWorker(workerId) {
    const p = this.schemaPrefix ? `${this.schemaPrefix}_` : '';
    const tableName = this.config.testMode === 'partitioned'
      ? `${p}HW_STRESS_TAB_PART`
      : `${p}HW_STRESS_TAB`;

    while (this.isRunning) {
      if (!this.pool) {
        await this.sleep(100);
        continue;
      }

      let connection;

      try {
        connection = await this.pool.getConnection();

        while (this.isRunning) {
          const startTime = Date.now();

          try {
            // Generate random data for insert
            const transaccion = `TXN${workerId}_${Date.now()}`;
            const usuario = `USER${workerId}`;
            const centroConta = `CC${Math.floor(Math.random() * 100)}`;
            const terminalConta = `TERM${Math.floor(Math.random() * 50)}`;
            const sysidCics = `CICS${Math.floor(Math.random() * 10)}`;
            const datOriginales = 'X'.repeat(Math.floor(Math.random() * 500) + 100);

            // Execute insert - this is where HW contention occurs
            await connection.execute(`
              INSERT INTO ${tableName} (
                TCL_TIMESTAMP,
                TCL_TRANSACCION,
                TCL_USUARIO,
                TCL_CENTRO_CONTA,
                TCL_TERMINAL_CONTA,
                TCL_SYSID_CICS,
                TCL_DAT_ORIGINALES
              ) VALUES (
                SYSTIMESTAMP,
                :b1,
                :b2,
                :b3,
                :b4,
                :b5,
                :b6
              )
            `, {
              b1: transaccion,
              b2: usuario,
              b3: centroConta,
              b4: terminalConta,
              b5: sysidCics,
              b6: datOriginales
            }, { autoCommit: true });

            const responseTime = Date.now() - startTime;
            this.stats.totalInserts++;
            this.stats.responseTimes.push(responseTime);

            // Keep only last 1000 response times
            if (this.stats.responseTimes.length > 1000) {
              this.stats.responseTimes.shift();
            }

          } catch (err) {
            this.stats.errors++;
            if (this.stats.errors % 100 === 1) {
              console.log(`Insert worker ${workerId} error:`, err.message);
            }
          }

          // Optional delay between inserts
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
            console.log(`Insert worker ${workerId} connection error:`, err.message);
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

    // Calculate TPS (inserts per second)
    const currentTotalInserts = this.stats.totalInserts;
    const insertsDelta = currentTotalInserts - this.previousStats.totalInserts;
    const tps = elapsedSeconds > 0 ? Math.round(insertsDelta / elapsedSeconds) : 0;
    this.stats.tps = tps;
    this.previousStats.totalInserts = currentTotalInserts;

    const avgResponseTime = this.stats.responseTimes.length > 0
      ? this.stats.responseTimes.reduce((a, b) => a + b, 0) / this.stats.responseTimes.length
      : 0;

    const metrics = {
      tps,
      avgResponseTime,
      totalInserts: this.stats.totalInserts,
      errors: this.stats.errors,
      testMode: this.config.testMode
    };

    if (this.io) {
      this.io.emit('hw-contention-metrics', metrics);
    }
  }

  async reportWaitEvents() {
    if (!this.isRunning || !this.db) return;

    try {
      // Query GV$SESSION_EVENT for HW and space-related wait events (RAC-aware)
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

      // Get specific HW contention events
      const hwResult = await this.db.execute(`
        SELECT event, SUM(time_waited_micro)/1000000 as time_seconds, SUM(total_waits) as total_waits
        FROM gv$session_event
        WHERE event IN (
          'enq: HW - contention',
          'enq: TM - contention',
          'enq: TX - row lock contention',
          'enq: TX - index contention',
          'enq: TX - allocate ITL entry',
          'buffer busy waits',
          'free buffer waits',
          'log file sync',
          'log buffer space',
          'db file sequential read',
          'db file scattered read'
        )
        GROUP BY event
      `);

      const hwEvents = {};
      for (const row of hwResult.rows) {
        hwEvents[row.EVENT] = {
          timeSeconds: row.TIME_SECONDS || 0,
          totalWaits: row.TOTAL_WAITS || 0
        };
      }

      // Get segment statistics if available
      let segmentStats = {};
      try {
        const p = this.schemaPrefix ? `${this.schemaPrefix}_` : '';
        const tableName = this.config.testMode === 'partitioned'
          ? `${p}HW_STRESS_TAB_PART`
          : `${p}HW_STRESS_TAB`;

        const segResult = await this.db.execute(`
          SELECT
            segment_name,
            bytes/1024/1024 as size_mb,
            extents,
            blocks
          FROM user_segments
          WHERE segment_name LIKE :tabName
        `, { tabName: tableName + '%' });

        let totalSizeMB = 0;
        let totalExtents = 0;
        let totalBlocks = 0;

        for (const row of segResult.rows) {
          totalSizeMB += row.SIZE_MB || 0;
          totalExtents += row.EXTENTS || 0;
          totalBlocks += row.BLOCKS || 0;
        }

        segmentStats = {
          sizeMB: totalSizeMB,
          extents: totalExtents,
          blocks: totalBlocks
        };
      } catch (e) {
        // Silently fail
      }

      if (this.io) {
        this.io.emit('hw-contention-wait-events', {
          top10WaitEvents,
          hwEvents,
          segmentStats
        });
      }
    } catch (err) {
      console.log('Cannot query wait events:', err.message);
    }
  }

  // Gather statistics with histogram options
  // methodOpt: 'SIZE_254' = FOR ALL COLUMNS SIZE 254
  //            'AUTO' = FOR ALL COLUMNS SIZE AUTO
  async gatherStats(methodOpt = 'AUTO') {
    if (!this.db) {
      throw new Error('Database not connected');
    }

    const p = this.schemaPrefix ? `${this.schemaPrefix}_` : '';
    const tableName = this.config?.testMode === 'partitioned'
      ? `${p}HW_STRESS_TAB_PART`
      : `${p}HW_STRESS_TAB`;

    const methodOptStr = methodOpt === 'SIZE_254'
      ? 'FOR ALL COLUMNS SIZE 254'
      : 'FOR ALL COLUMNS SIZE AUTO';

    console.log(`Gathering statistics on ${tableName} with METHOD_OPT => '${methodOptStr}'`);
    this.io?.emit('hw-contention-status', { running: this.isRunning, message: `Gathering stats (${methodOpt})...` });

    const startTime = Date.now();

    try {
      await this.db.execute(`
        BEGIN
          DBMS_STATS.GATHER_TABLE_STATS(
            ownname => USER,
            tabname => :tabName,
            method_opt => :methodOpt,
            cascade => TRUE,
            degree => 4
          );
        END;
      `, {
        tabName: tableName,
        methodOpt: methodOptStr
      });

      const elapsed = ((Date.now() - startTime) / 1000).toFixed(2);
      console.log(`Statistics gathered in ${elapsed}s`);
      this.io?.emit('hw-contention-status', { running: this.isRunning, message: `Stats gathered in ${elapsed}s` });

      // Fetch and emit histogram info
      await this.reportHistogramInfo();

      return { success: true, elapsed: parseFloat(elapsed) };
    } catch (err) {
      console.error('Error gathering stats:', err.message);
      this.io?.emit('hw-contention-status', { running: this.isRunning, message: `Stats error: ${err.message}` });
      throw err;
    }
  }

  // Report histogram information from the database
  async reportHistogramInfo() {
    if (!this.db) return;

    const p = this.schemaPrefix ? `${this.schemaPrefix}_` : '';
    const tableName = this.config?.testMode === 'partitioned'
      ? `${p}HW_STRESS_TAB_PART`
      : `${p}HW_STRESS_TAB`;

    try {
      // Query histogram info from USER_TAB_COL_STATISTICS (safer than SYS.HISTGRM$)
      const histResult = await this.db.execute(`
        SELECT
          column_name,
          num_distinct,
          num_nulls,
          num_buckets,
          histogram
        FROM user_tab_col_statistics
        WHERE table_name = :tabName
        ORDER BY column_name
      `, { tabName: tableName });

      const histogramInfo = [];
      for (const row of histResult.rows) {
        histogramInfo.push({
          columnName: row.COLUMN_NAME,
          numDistinct: row.NUM_DISTINCT || 0,
          numNulls: row.NUM_NULLS || 0,
          numBuckets: row.NUM_BUCKETS || 0,
          histogramType: row.HISTOGRAM || 'NONE'
        });
      }

      // Query row count from SYS.HISTGRM$ for histogram bucket details (if accessible)
      let histgramRows = 0;
      try {
        const histgramResult = await this.db.execute(`
          SELECT COUNT(*) as cnt
          FROM sys.histgrm$ h, sys.obj$ o, sys.col$ c
          WHERE o.name = :tabName
            AND o.obj# = h.obj#
            AND h.obj# = c.obj#
            AND h.intcol# = c.intcol#
        `, { tabName: tableName });
        histgramRows = histgramResult.rows[0]?.CNT || 0;
      } catch (e) {
        // User might not have access to SYS tables
        console.log('Cannot access SYS.HISTGRM$ (requires DBA privileges)');
      }

      // Get table stats summary
      let tableStats = {};
      try {
        const tabStatResult = await this.db.execute(`
          SELECT
            num_rows,
            blocks,
            avg_row_len,
            last_analyzed
          FROM user_tables
          WHERE table_name = :tabName
        `, { tabName: tableName });

        if (tabStatResult.rows.length > 0) {
          const row = tabStatResult.rows[0];
          tableStats = {
            numRows: row.NUM_ROWS || 0,
            blocks: row.BLOCKS || 0,
            avgRowLen: row.AVG_ROW_LEN || 0,
            lastAnalyzed: row.LAST_ANALYZED
          };
        }
      } catch (e) {
        // Silently fail
      }

      if (this.io) {
        this.io.emit('hw-contention-histogram-info', {
          tableName,
          histogramInfo,
          histgramRows,
          tableStats
        });
      }

      return { histogramInfo, histgramRows, tableStats };
    } catch (err) {
      console.log('Cannot query histogram info:', err.message);
      return null;
    }
  }

  async stop() {
    console.log('Stopping HW Contention Demo...');
    this.isRunning = false;

    if (this.statsInterval) {
      clearInterval(this.statsInterval);
      this.statsInterval = null;
    }

    if (this.waitEventsInterval) {
      clearInterval(this.waitEventsInterval);
      this.waitEventsInterval = null;
    }

    if (this.extentAllocatorInterval) {
      clearInterval(this.extentAllocatorInterval);
      this.extentAllocatorInterval = null;
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
      this.io.emit('hw-contention-stopped', {
        totalInserts: this.stats.totalInserts,
        errors: this.stats.errors
      });
    }

    console.log('HW Contention Demo stopped');
    return this.stats;
  }

  sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }
}

module.exports = new HWContentionEngine();
