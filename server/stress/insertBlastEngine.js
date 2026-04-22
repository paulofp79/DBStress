class InsertBlastEngine {
  constructor() {
    this.isRunning = false;
    this.config = null;
    this.io = null;
    this.pool = null;
    this.tableNames = [];
    this.workers = [];
    this.activeWorkers = 0;
    this.startedAt = 0;
    this.statsInterval = null;
    this.stopTimer = null;
    this.stats = this.initStats();
    this.previousStats = this.initStats();
  }

  initStats() {
    return {
      inserts: 0,
      commits: 0,
      errors: 0,
      byTable: {},
      startedAt: Date.now()
    };
  }

  normalizeConfig(config = {}) {
    return {
      tablePrefix: String(config.tablePrefix || 'IBLAST').trim().toUpperCase().replace(/[^A-Z0-9_$#]/g, ''),
      tableCount: Math.max(1, Math.min(200, Number.parseInt(config.tableCount, 10) || 8)),
      columnsPerTable: Math.max(4, Math.min(200, Number.parseInt(config.columnsPerTable, 10) || 24)),
      sessions: Math.max(1, Math.min(256, Number.parseInt(config.sessions, 10) || 8)),
      durationSeconds: Math.max(1, Math.min(86400, Number.parseInt(config.durationSeconds, 10) || 60)),
      commitEvery: Math.max(1, Math.min(1000, Number.parseInt(config.commitEvery, 10) || 50))
    };
  }

  getTableNames() {
    return Array.from({ length: this.config.tableCount }, (_, index) => (
      `${this.config.tablePrefix}_T${String(index + 1).padStart(3, '0')}`
    ));
  }

  buildInsertSql(tableName) {
    const columns = [];
    const values = [];
    const binds = {};

    for (let index = 1; index <= this.config.columnsPerTable; index += 1) {
      const suffix = String(index).padStart(3, '0');
      const bindName = `b${suffix}`;

      if (index % 3 === 1) {
        columns.push(`vc_${suffix}`);
        values.push(`:${bindName}`);
      } else if (index % 3 === 2) {
        columns.push(`num_${suffix}`);
        values.push(`:${bindName}`);
      } else {
        columns.push(`dt_${suffix}`);
        values.push(`:${bindName}`);
      }
    }

    return `INSERT INTO ${tableName} (${columns.join(', ')}) VALUES (${values.join(', ')})`;
  }

  buildBindValues(workerId, sequence) {
    const binds = {};

    for (let index = 1; index <= this.config.columnsPerTable; index += 1) {
      const suffix = String(index).padStart(3, '0');
      const bindName = `b${suffix}`;

      if (index % 3 === 1) {
        binds[bindName] = `W${workerId}_R${sequence}_C${suffix}`;
      } else if (index % 3 === 2) {
        binds[bindName] = (workerId * 1000000) + sequence + index;
      } else {
        binds[bindName] = new Date(Date.now() - ((sequence + index) % 86400000));
      }
    }

    return binds;
  }

  async runWorker(workerId) {
    this.activeWorkers += 1;
    let sequence = 0;

    try {
      while (this.isRunning) {
        let connection;
        try {
          connection = await this.pool.getConnection();
          let pending = 0;

          while (this.isRunning) {
            const tableName = this.tableNames[Math.floor(Math.random() * this.tableNames.length)];
            const sql = this.buildInsertSql(tableName);
            sequence += 1;
            await connection.execute(sql, this.buildBindValues(workerId, sequence), { autoCommit: false });
            pending += 1;
            this.stats.inserts += 1;
            this.stats.byTable[tableName] = (this.stats.byTable[tableName] || 0) + 1;

            if (pending >= this.config.commitEvery) {
              await connection.commit();
              this.stats.commits += 1;
              pending = 0;
            }

            if ((Date.now() - this.startedAt) / 1000 >= this.config.durationSeconds) {
              this.isRunning = false;
              break;
            }
          }

          if (pending > 0) {
            await connection.commit();
            this.stats.commits += 1;
          }
        } catch (error) {
          this.stats.errors += 1;
          if (connection) {
            try {
              await connection.rollback();
            } catch (rollbackError) {
              // ignore
            }
          }
          if (!String(error.message || '').includes('pool is closing')) {
            console.log(`Insert blast worker ${workerId} error: ${error.message}`);
          }
        } finally {
          if (connection) {
            try {
              await connection.close();
            } catch (closeError) {
              // ignore
            }
          }
        }
      }
    } finally {
      this.activeWorkers -= 1;
    }
  }

  emitMetrics() {
    const elapsed = Math.max(1, (Date.now() - this.previousStats.startedAt) / 1000);
    const deltaInserts = this.stats.inserts - this.previousStats.inserts;
    const deltaErrors = this.stats.errors - this.previousStats.errors;
    const payload = {
      isRunning: this.isRunning,
      uptime: Math.floor((Date.now() - this.startedAt) / 1000),
      activeWorkers: this.activeWorkers,
      total: {
        inserts: this.stats.inserts,
        commits: this.stats.commits,
        errors: this.stats.errors
      },
      perSecond: {
        inserts: Number((deltaInserts / elapsed).toFixed(2)),
        errors: Number((deltaErrors / elapsed).toFixed(2))
      },
      byTable: this.stats.byTable,
      config: this.config
    };

    if (this.io) {
      this.io.emit('insert-blast-metrics', payload);
      this.io.emit('insert-blast-status', {
        isRunning: this.isRunning,
        config: this.config,
        uptime: payload.uptime,
        activeWorkers: this.activeWorkers
      });
    }

    this.previousStats = {
      inserts: this.stats.inserts,
      commits: this.stats.commits,
      errors: this.stats.errors,
      byTable: { ...this.stats.byTable },
      startedAt: Date.now()
    };
  }

  async start(oracleDb, config = {}, io) {
    if (this.isRunning) {
      throw new Error('Insert blast workload is already running.');
    }

    this.config = this.normalizeConfig(config);
    this.io = io;
    this.isRunning = true;
    this.startedAt = Date.now();
    this.stats = this.initStats();
    this.previousStats = this.initStats();
    this.tableNames = this.getTableNames();
    this.pool = await oracleDb.createStressPool(this.config.sessions);

    this.workers = Array.from({ length: this.config.sessions }, (_, index) => this.runWorker(index + 1));
    this.statsInterval = setInterval(() => this.emitMetrics(), 1000);
    this.stopTimer = setTimeout(() => {
      this.stop().catch(() => {});
    }, this.config.durationSeconds * 1000);

    return this.getStatus();
  }

  getStatus() {
    return {
      isRunning: this.isRunning,
      config: this.config,
      uptime: this.isRunning ? Math.floor((Date.now() - this.startedAt) / 1000) : 0,
      activeWorkers: this.activeWorkers,
      stats: this.stats
    };
  }

  async stop() {
    if (!this.isRunning) {
      return this.getStatus();
    }

    this.isRunning = false;

    if (this.stopTimer) {
      clearTimeout(this.stopTimer);
      this.stopTimer = null;
    }
    if (this.statsInterval) {
      clearInterval(this.statsInterval);
      this.statsInterval = null;
    }
    if (this.pool) {
      try {
        await this.pool.close(10);
      } catch (error) {
        console.log('Insert blast pool close error:', error.message);
      }
      this.pool = null;
    }

    this.emitMetrics();
    return this.getStatus();
  }
}

module.exports = new InsertBlastEngine();
