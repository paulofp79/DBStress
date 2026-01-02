// Index Contention Demo Engine
// Simulates and demonstrates B-tree index contention scenarios
const oracledb = require('oracledb');

class IndexContentionEngine {
  constructor() {
    this.isRunning = false;
    this.config = null;
    this.pool = null;
    this.io = null;
    this.workers = [];
    this.schemaPrefix = '';

    // Performance metrics
    this.stats = {
      totalTransactions: 0,
      errors: 0,
      responseTimes: [],  // Rolling window for avg calculation
      tps: 0
    };

    this.previousStats = { totalTransactions: 0 };
    this.statsInterval = null;
    this.waitEventsInterval = null;
  }

  async start(db, config, io) {
    if (this.isRunning) {
      throw new Error('Index Contention demo already running');
    }

    this.config = {
      threads: config.threads || 50,
      thinkTime: config.thinkTime || 10,
      indexType: config.indexType || 'standard',
      tableCount: config.tableCount || 1,
      // Sequence cache size (0 = NOCACHE), configurable at start or runtime
      sequenceCache: (config.sequenceCache !== undefined ? config.sequenceCache : 0),
      // Default hash partitions for hash_partition index type
      hashPartitions: config.hashPartitions || 4,
      ...config
    };

    this.schemaPrefix = config.schemaPrefix || '';
    this.io = io;
    this.isRunning = true;

    // Reset stats
    this.stats = {
      totalTransactions: 0,
      errors: 0,
      responseTimes: [],
      tps: 0
    };
    this.previousStats = { totalTransactions: 0 };

    console.log(`Starting Index Contention Demo with ${this.config.threads} threads, index type: ${this.config.indexType}`);

    // Create connection pool
    this.pool = await db.createStressPool(this.config.threads + 5);

    // Create tables if they don't exist
    await this.createTables(db);

    // Ensure tables and index exist with correct type
    await this.setupIndex(db);

    // Start workers
    for (let i = 0; i < this.config.threads; i++) {
      this.workers.push(this.runWorker(i));
    }

    // Start stats reporting (every second)
    this.statsInterval = setInterval(() => this.reportStats(), 1000);

    // Start wait events monitoring (every 2 seconds)
    this.waitEventsInterval = setInterval(() => this.reportWaitEvents(db), 2000);

    this.io?.emit('index-contention-status', { running: true, message: 'Running...' });
  }

  async createTables(db) {
    const p = this.schemaPrefix ? `${this.schemaPrefix}_` : '';

    this.io?.emit('index-contention-status', { message: 'Creating tables...' });

    try {
      for (let i = 1; i <= this.config.tableCount; i++) {
        const suffix = i > 1 ? `_${i}` : '';
        const tableName = `${p}txn_history${suffix}`;
        const seqName = `${p}seq_txn_history${suffix}`;

        // Create sequence if not exists (configurable cache for sequence contention tests)
        try {
          const cacheClause = (this.config.sequenceCache && this.config.sequenceCache > 0) ? `CACHE ${this.config.sequenceCache}` : 'NOCACHE';
          await db.execute(`
            CREATE SEQUENCE ${seqName}
              START WITH 1
              INCREMENT BY 1
              ${cacheClause}
              NOORDER
          `);
          console.log(`Created sequence: ${seqName} (${cacheClause})`);
        } catch (err) {
          if (err.errorNum !== 955) { // ORA-00955: name is already used
            console.log(`Sequence ${seqName} might already exist or error: ${err.message}`);
          }
        }

        // Create table if not exists
        try {
          await db.execute(`
            CREATE TABLE ${tableName} (
              txn_id        NUMBER NOT NULL,
              session_id    NUMBER,
              instance_id   NUMBER,
              txn_type      VARCHAR2(20),
              txn_amount    NUMBER(12,2),
              txn_data      VARCHAR2(100),
              created_at    TIMESTAMP DEFAULT SYSTIMESTAMP
            )
          `);
          console.log(`Created table: ${tableName}`);
        } catch (err) {
          if (err.errorNum !== 955) { // ORA-00955: name is already used
            console.log(`Table ${tableName} might already exist or error: ${err.message}`);
          }
        }
      }

      this.io?.emit('index-contention-status', { message: 'Tables ready' });
    } catch (err) {
      console.error('Error creating tables:', err);
      this.io?.emit('index-contention-status', { message: `Table creation error: ${err.message}` });
      throw err;
    }
  }

  async setupIndex(db) {
    const p = this.schemaPrefix ? `${this.schemaPrefix}_` : '';
    const indexType = this.config.indexType;
    // Use configured sequence cache when creating/recreating sequences
    const cacheClause = (this.config.sequenceCache && this.config.sequenceCache > 0) ? `CACHE ${this.config.sequenceCache}` : 'NOCACHE';

    this.io?.emit('index-contention-status', { message: `Setting up ${indexType} index...` });

    try {
      // For each table, set up the appropriate index
      for (let i = 1; i <= this.config.tableCount; i++) {
        const suffix = i > 1 ? `_${i}` : '';
        const tableName = `${p}txn_history${suffix}`;
        const pkName = `${p}pk_txn_history${suffix}`;
        const seqName = `${p}seq_txn_history${suffix}`;

        // Drop existing primary key constraint if exists
        try {
          await db.execute(`ALTER TABLE ${tableName} DROP CONSTRAINT ${pkName}`);
        } catch (err) {
          // Constraint might not exist
        }

        // Truncate table to remove any duplicate data from previous runs
        try {
          await db.execute(`TRUNCATE TABLE ${tableName}`);
          console.log(`Table ${tableName}: Truncated`);
        } catch (err) {
          // Table might not exist or other issue
        }

        // Create new index based on type
        switch (indexType) {
          case 'none_no_seq':
            // No index, no sequence - uses random ID
            console.log(`Table ${tableName}: No index, no sequence (random ID)`);
            break;

          case 'none_cached_seq':
            // No index, but cached sequence
            try {
              await db.execute(`DROP SEQUENCE ${seqName}`);
            } catch (e) { /* might not exist */ }
            try {
              await db.execute(`
                CREATE SEQUENCE ${seqName}
                  START WITH 1
                  INCREMENT BY 1
                  CACHE 1000
              `);
              console.log(`Table ${tableName}: No index, cached sequence (CACHE 1000)`);
            } catch (err) {
              console.log(`Table ${tableName}: Sequence error: ${err.message}`);
            }
            break;

          case 'standard':
            // Standard B-tree index - maximum contention
            // Ensure sequence has NOCACHE NOORDER for maximum contention
            try {
              await db.execute(`DROP SEQUENCE ${seqName}`);
            } catch (e) { /* might not exist */ }
            await db.execute(`
              CREATE SEQUENCE ${seqName}
                START WITH 1
                INCREMENT BY 1
                NOCACHE
                NOORDER
            `);
            await db.execute(`ALTER TABLE ${tableName} ADD CONSTRAINT ${pkName} PRIMARY KEY (txn_id)`);
            console.log(`Table ${tableName}: Standard B-tree PK with NOCACHE NOORDER sequence`);
            break;

          case 'reverse':
            // Reverse key index - distributes inserts but increases I/O
            // Ensure sequence has NOCACHE NOORDER
            try {
              await db.execute(`DROP SEQUENCE ${seqName}`);
            } catch (e) { /* might not exist */ }
            await db.execute(`
              CREATE SEQUENCE ${seqName}
                START WITH 1
                INCREMENT BY 1
                NOCACHE
                NOORDER
            `);
            await db.execute(`
              ALTER TABLE ${tableName} ADD CONSTRAINT ${pkName} PRIMARY KEY (txn_id)
              USING INDEX (CREATE UNIQUE INDEX ${pkName} ON ${tableName}(txn_id) REVERSE)
            `);
            console.log(`Table ${tableName}: Reverse key index with NOCACHE NOORDER sequence`);
            break;

          case 'hash_partition':
            // Hash partitioned index - try GLOBAL HASH partitioned unique index (requires partitioning / EE)
            try {
              const partitions = this.config.hashPartitions || 4;
              await db.execute(`
                ALTER TABLE ${tableName} ADD CONSTRAINT ${pkName} PRIMARY KEY (txn_id)
                USING INDEX (CREATE UNIQUE INDEX ${pkName}_idx ON ${tableName}(txn_id)
                  GLOBAL PARTITION BY HASH (txn_id) PARTITIONS ${partitions})
              `);
              console.log(`Table ${tableName}: Created PK with GLOBAL HASH partitioned index (${partitions} partitions)`);
            } catch (err) {
              // Fallback to standard PK without partitioning
              try {
                await db.execute(`ALTER TABLE ${tableName} ADD CONSTRAINT ${pkName} PRIMARY KEY (txn_id)`);
                console.log(`Table ${tableName}: Using standard PK (partitioning not available)`);
              } catch (e) {
                console.log(`Table ${tableName}: Failed to create PK: ${e.message}`);
              }
            }
            break;

          case 'scalable_sequence':
            // Scalable sequence - Oracle 18c+ feature
            // Recreate sequence with SCALE option if supported
            try {
              await db.execute(`DROP SEQUENCE ${seqName}`);
              await db.execute(`
                CREATE SEQUENCE ${seqName}
                  START WITH 1
                  INCREMENT BY 1
                  ${cacheClause}
                  SCALE EXTEND
              `);
              await db.execute(`ALTER TABLE ${tableName} ADD CONSTRAINT ${pkName} PRIMARY KEY (txn_id)`);
              console.log(`Table ${tableName}: Scalable sequence with standard PK (${cacheClause})`);
            } catch (err) {
              // Fall back to standard sequence if SCALE not supported
              try {
                await db.execute(`
                  CREATE SEQUENCE ${seqName}
                    START WITH 1
                    INCREMENT BY 1
                    ${cacheClause}
                    NOORDER
                `);
              } catch (e) {
                // Sequence might already exist
              }
              await db.execute(`ALTER TABLE ${tableName} ADD CONSTRAINT ${pkName} PRIMARY KEY (txn_id)`);
              console.log(`Table ${tableName}: Standard sequence (SCALE not available, ${cacheClause})`);
            }
            break;

          default:
            await db.execute(`ALTER TABLE ${tableName} ADD CONSTRAINT ${pkName} PRIMARY KEY (txn_id)`);
        }
      }

      this.io?.emit('index-contention-status', { message: `Index setup complete: ${indexType}`, indexChanged: true });
    } catch (err) {
      console.error('Error setting up index:', err);
      this.io?.emit('index-contention-status', { message: `Index setup error: ${err.message}` });
    }
  }

  async changeIndex(db, newIndexType) {
    this.config.indexType = newIndexType;
    await this.setupIndex(db);
  }

  async changeSequenceCache(db, newCache) {
    // newCache: integer (0 == NOCACHE)
    this.config.sequenceCache = newCache;
    const p = this.schemaPrefix ? `${this.schemaPrefix}_` : '';

    this.io?.emit('index-contention-status', { message: `Changing sequence cache to ${newCache}` });

    try {
      for (let i = 1; i <= this.config.tableCount; i++) {
        const suffix = i > 1 ? `_${i}` : '';
        const seqName = `${p}seq_txn_history${suffix}`;
        const sql = (newCache && newCache > 0)
          ? `ALTER SEQUENCE ${seqName} CACHE ${newCache}`
          : `ALTER SEQUENCE ${seqName} NOCACHE`;
        await db.execute(sql);
        console.log(`Sequence ${seqName} altered: ${newCache && newCache > 0 ? 'CACHE ' + newCache : 'NOCACHE'}`);
      }

      this.io?.emit('index-contention-status', { message: `Sequence cache changed to ${newCache}`, sequenceCache: newCache });
    } catch (err) {
      console.log('Error changing sequence cache:', err.message);
      this.io?.emit('index-contention-status', { message: `Sequence cache change error: ${err.message}` });
      throw err;
    }
  }

  async runSequenceCacheABTest(db, { cacheA = 0, cacheB = 100, duration = 10, warmup = 5 } = {}) {
    // Must be running so we can measure under the active workload
    if (!this.isRunning) {
      throw new Error('Index Contention demo must be running to perform A/B test');
    }

    const originalCache = this.config.sequenceCache || 0;
    const variants = [ { name: 'A', cache: cacheA }, { name: 'B', cache: cacheB } ];
    const results = {};

    try {
      for (const v of variants) {
        this.io?.emit('index-contention-status', { message: `A/B test: variant ${v.name} -> cache=${v.cache}` });

        // Apply cache for this variant
        await this.changeSequenceCache(db, v.cache);
        this.config.sequenceCache = v.cache;

        // Warmup period
        await this.sleep(warmup * 1000);

        // Sample tps and avg response time every second for duration
        const samples = [];
        for (let i = 0; i < duration; i++) {
          await this.sleep(1000);
          const tpsSample = this.stats.tps;
          const avgResponseTimeSample = this.stats.responseTimes.length > 0
            ? this.stats.responseTimes.reduce((a, b) => a + b, 0) / this.stats.responseTimes.length
            : 0;
          samples.push({ tps: tpsSample, avgResponseTime: avgResponseTimeSample });
        }

        const meanTps = samples.reduce((s, x) => s + x.tps, 0) / samples.length;
        const meanAvgResponseTime = samples.reduce((s, x) => s + x.avgResponseTime, 0) / samples.length;

        results[v.cache] = { meanTps, meanAvgResponseTime, samples };
      }
    } catch (err) {
      console.log('A/B test error:', err.message);
      throw err;
    } finally {
      // Restore original cache setting
      try {
        await this.changeSequenceCache(db, originalCache);
        this.config.sequenceCache = originalCache;
      } catch (e) {
        console.log('Failed to restore original sequence cache:', e.message);
      }
      this.io?.emit('index-contention-status', { message: `A/B test complete, restored sequence cache to ${originalCache}` });
    }

    // Emit results over socket and return
    this.io?.emit('index-contention-abtest-result', { cacheA, cacheB, results });
    return { cacheA, cacheB, results };
  }

  async runWorker(workerId) {
    const p = this.schemaPrefix ? `${this.schemaPrefix}_` : '';

    while (this.isRunning) {
      if (!this.pool) {
        await this.sleep(100);
        continue;
      }

      let connection;
      const startTime = Date.now();

      try {
        connection = await this.pool.getConnection();

        // Select a random table
        const tableNum = Math.floor(Math.random() * this.config.tableCount) + 1;
        const suffix = tableNum > 1 ? `_${tableNum}` : '';

        // Get session/instance info
        let sessionId = workerId;
        let instanceNum = 1;
        try {
          const result = await connection.execute(
            `SELECT SYS_CONTEXT('USERENV', 'SID') as sid,
                    SYS_CONTEXT('USERENV', 'INSTANCE') as inst FROM dual`
          );
          sessionId = parseInt(result.rows[0]?.SID) || workerId;
          instanceNum = parseInt(result.rows[0]?.INST) || 1;
        } catch (e) {
          // Use defaults
        }

        // Perform insert
        const txnTypes = ['PURCHASE', 'REFUND', 'TRANSFER', 'ADJUSTMENT'];
        const txnType = txnTypes[Math.floor(Math.random() * txnTypes.length)];
        const txnAmount = parseFloat((Math.random() * 10000).toFixed(2));

        if (this.config.indexType === 'none_no_seq') {
          // Use random ID - no sequence contention
          await connection.execute(
            `INSERT INTO ${p}txn_history${suffix}
               (txn_id, session_id, instance_id, txn_type, txn_amount, txn_data)
             VALUES
               (TRUNC(DBMS_RANDOM.VALUE(1, 999999999999)), :1, :2, :3, :4, :5)`,
            [sessionId, instanceNum, txnType, txnAmount, `Worker ${workerId}`]
          );
        } else {
          // Use sequence
          await connection.execute(
            `INSERT INTO ${p}txn_history${suffix}
               (txn_id, session_id, instance_id, txn_type, txn_amount, txn_data)
             VALUES
               (${p}seq_txn_history${suffix}.NEXTVAL, :1, :2, :3, :4, :5)`,
            [sessionId, instanceNum, txnType, txnAmount, `Worker ${workerId}`]
          );
        }

        await connection.commit();

        // Record success
        const responseTime = Date.now() - startTime;
        this.stats.totalTransactions++;
        this.stats.responseTimes.push(responseTime);

        // Keep only last 1000 response times for average
        if (this.stats.responseTimes.length > 1000) {
          this.stats.responseTimes.shift();
        }

      } catch (err) {
        this.stats.errors++;
        if (!err.message.includes('pool is terminating') &&
            !err.message.includes('NJS-003') &&
            !err.message.includes('NJS-500')) {
          // Log only non-pool errors
          if (this.stats.errors % 100 === 1) {
            console.log(`Index contention worker ${workerId} error:`, err.message);
          }
        }

        if (connection) {
          try { await connection.rollback(); } catch (e) {}
        }
      } finally {
        if (connection) {
          try { await connection.close(); } catch (e) {}
        }
      }

      // Think time
      if (this.isRunning && this.config.thinkTime > 0) {
        await this.sleep(this.config.thinkTime);
      }
    }
  }

  reportStats() {
    const tps = this.stats.totalTransactions - this.previousStats.totalTransactions;
    this.stats.tps = tps;

    const avgResponseTime = this.stats.responseTimes.length > 0
      ? this.stats.responseTimes.reduce((a, b) => a + b, 0) / this.stats.responseTimes.length
      : 0;

    const metrics = {
      tps,
      avgResponseTime,
      totalTransactions: this.stats.totalTransactions,
      errors: this.stats.errors
    };

    this.previousStats.totalTransactions = this.stats.totalTransactions;

    if (this.io) {
      this.io.emit('index-contention-metrics', metrics);
    }
  }

  async reportWaitEvents(db) {
    if (!this.isRunning) return;

    const p = this.schemaPrefix ? `${this.schemaPrefix}_` : '';

    try {
      // Query v$session_event for relevant wait events
      // This requires SELECT on V$ views
      const result = await db.execute(`
        SELECT event, SUM(time_waited_micro)/1000000 as time_seconds
        FROM v$session_event
        WHERE event IN (
          'buffer busy waits',
          'enq: TX - index contention',
          'gc buffer busy acquire',
          'gc buffer busy release',
          'cell single block physical read',
          'enq: TX - row lock contention'
        )
        GROUP BY event
      `);

      const waitEvents = {
        'buffer busy waits': 0,
        'enq: TX - index contention': 0,
        'gc buffer busy acquire': 0,
        'gc buffer busy release': 0,
        'cell single block physical read': 0
      };

      for (const row of result.rows) {
        if (waitEvents.hasOwnProperty(row.EVENT)) {
          waitEvents[row.EVENT] = row.TIME_SECONDS || 0;
        }
      }

      if (this.io) {
        this.io.emit('index-contention-metrics', {
          tps: this.stats.tps,
          avgResponseTime: this.stats.responseTimes.length > 0
            ? this.stats.responseTimes.reduce((a, b) => a + b, 0) / this.stats.responseTimes.length
            : 0,
          totalTransactions: this.stats.totalTransactions,
          errors: this.stats.errors,
          waitEvents
        });
      }
    } catch (err) {
      // Might not have access to v$ views
      console.log('Cannot query wait events:', err.message);
    }
  }

  async stop() {
    console.log('Stopping Index Contention Demo...');
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
      this.io.emit('index-contention-stopped', {
        totalTransactions: this.stats.totalTransactions,
        errors: this.stats.errors
      });
    }

    console.log('Index Contention Demo stopped');
    return this.stats;
  }

  sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }
}

module.exports = new IndexContentionEngine();
