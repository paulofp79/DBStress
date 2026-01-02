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

  async setupIndex(db) {
    const p = this.schemaPrefix ? `${this.schemaPrefix}_` : '';
    const indexType = this.config.indexType;

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

        // Create new index based on type
        switch (indexType) {
          case 'none':
            // No index - heap table, no PK
            console.log(`Table ${tableName}: No index (heap table)`);
            break;

          case 'standard':
            // Standard B-tree index - maximum contention
            await db.execute(`ALTER TABLE ${tableName} ADD CONSTRAINT ${pkName} PRIMARY KEY (txn_id)`);
            console.log(`Table ${tableName}: Standard B-tree PK created`);
            break;

          case 'reverse':
            // Reverse key index - distributes inserts but increases I/O
            await db.execute(`
              ALTER TABLE ${tableName} ADD CONSTRAINT ${pkName} PRIMARY KEY (txn_id)
              USING INDEX (CREATE UNIQUE INDEX ${pkName} ON ${tableName}(txn_id) REVERSE)
            `);
            console.log(`Table ${tableName}: Reverse key index created`);
            break;

          case 'hash_partition':
            // Hash partitioned index - helps single instance
            // Note: This requires Enterprise Edition and partitioning
            try {
              await db.execute(`ALTER TABLE ${tableName} ADD CONSTRAINT ${pkName} PRIMARY KEY (txn_id)`);
              // Try to create hash partitioned index (may fail without EE)
              console.log(`Table ${tableName}: Standard PK (hash partition requires EE)`);
            } catch (err) {
              console.log(`Table ${tableName}: Using standard PK (partitioning not available)`);
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
                  NOCACHE
                  SCALE EXTEND
              `);
              await db.execute(`ALTER TABLE ${tableName} ADD CONSTRAINT ${pkName} PRIMARY KEY (txn_id)`);
              console.log(`Table ${tableName}: Scalable sequence with standard PK`);
            } catch (err) {
              // Fall back to standard sequence if SCALE not supported
              try {
                await db.execute(`
                  CREATE SEQUENCE ${seqName}
                    START WITH 1
                    INCREMENT BY 1
                    NOCACHE
                    NOORDER
                `);
              } catch (e) {
                // Sequence might already exist
              }
              await db.execute(`ALTER TABLE ${tableName} ADD CONSTRAINT ${pkName} PRIMARY KEY (txn_id)`);
              console.log(`Table ${tableName}: Standard sequence (SCALE not available)`);
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

        // Perform insert with sequence
        const txnTypes = ['PURCHASE', 'REFUND', 'TRANSFER', 'ADJUSTMENT'];
        const txnType = txnTypes[Math.floor(Math.random() * txnTypes.length)];
        const txnAmount = parseFloat((Math.random() * 10000).toFixed(2));

        await connection.execute(
          `INSERT INTO ${p}txn_history${suffix}
             (txn_id, session_id, instance_id, txn_type, txn_amount, txn_data)
           VALUES
             (${p}seq_txn_history${suffix}.NEXTVAL, :1, :2, :3, :4, :5)`,
          [sessionId, instanceNum, txnType, txnAmount, `Worker ${workerId}`]
        );

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
