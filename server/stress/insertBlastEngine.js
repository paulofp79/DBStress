const { v4: uuidv4 } = require('uuid');

class InsertBlastEngine {
  constructor() {
    this.isRunning = false;
    this.config = null;
    this.io = null;
    this.db = null;
    this.tableNames = [];
    this.insertSqlCache = {};
    this.workers = [];
    this.runtimeWorkloads = [];
    this.activeWorkers = 0;
    this.startedAt = 0;
    this.statsInterval = null;
    this.stopTimer = null;
    this.stats = this.initStats();
    this.previousStats = this.initStats();
    this.runId = null;
  }

  initStats(config = null) {
    const workloads = {};
    (config?.workloads || []).forEach((workload) => {
      workloads[workload.id] = {
        id: workload.id,
        name: workload.name,
        tableCount: workload.tableCount,
        sessions: workload.sessions,
        durationSeconds: workload.durationSeconds,
        commitEvery: workload.commitEvery,
        sessionMode: workload.sessionMode,
        inserts: 0,
        commits: 0,
        errors: 0,
        activeWorkers: 0
      };
    });

    return {
      inserts: 0,
      commits: 0,
      errors: 0,
      extentAllocations: 0,
      byTable: {},
      workloads,
      startedAt: Date.now()
    };
  }

  normalizeWorkloads(config = {}) {
    const totalTableCount = Math.max(1, Math.min(5000, Number.parseInt(config.tableCount, 10) || 8));
    const rawWorkloads = Array.isArray(config.workloads) && config.workloads.length > 0
      ? config.workloads
      : [{
        name: config.workloadName || 'Workload 1',
        tableCount: totalTableCount,
        sessions: config.sessions,
        durationSeconds: config.durationSeconds,
        commitEvery: config.commitEvery,
        sessionMode: config.sessionMode
      }];

    return rawWorkloads.slice(0, 20).map((workload, index) => {
      const id = String(workload.id || `workload_${index + 1}`)
        .trim()
        .toLowerCase()
        .replace(/[^a-z0-9_]/g, '_') || `workload_${index + 1}`;

      return {
        id,
        name: String(workload.name || `Workload ${index + 1}`).trim() || `Workload ${index + 1}`,
        tableCount: Math.max(1, Math.min(totalTableCount, Number.parseInt(workload.tableCount, 10) || totalTableCount)),
        sessions: Math.max(1, Number.parseInt(workload.sessions, 10) || 8),
        durationSeconds: Math.max(1, Math.min(86400, Number.parseInt(workload.durationSeconds, 10) || 60)),
        commitEvery: Math.max(1, Number.parseInt(workload.commitEvery, 10) || 50),
        sessionMode: String(workload.sessionMode || 'reuse').trim().toLowerCase() === 'reconnect' ? 'reconnect' : 'reuse'
      };
    });
  }

  normalizeConfig(config = {}) {
    const workloads = this.normalizeWorkloads(config);
    const maxDurationSeconds = workloads.reduce(
      (maxDuration, workload) => Math.max(maxDuration, workload.durationSeconds),
      0
    );
    const totalSessions = workloads.reduce((sum, workload) => sum + workload.sessions, 0);
    const hwMitigation = {
      enabled: config.hwMitigation?.enabled === true || config.hwMitigationEnabled === true,
      preallocateOnStart: config.hwMitigation?.preallocateOnStart !== false && config.preallocateOnStart !== false,
      extentSizeMb: Math.max(8, Math.min(1024, Number.parseInt(config.hwMitigation?.extentSizeMb ?? config.extentSizeMb, 10) || 128)),
      allocateEveryInserts: Math.max(1000, Math.min(10000000, Number.parseInt(config.hwMitigation?.allocateEveryInserts ?? config.allocateEveryInserts, 10) || 100000))
    };

    return {
      tablePrefix: String(config.tablePrefix || 'IBLAST').trim().toUpperCase().replace(/[^A-Z0-9_$#]/g, ''),
      tableCount: Math.max(1, Math.min(5000, Number.parseInt(config.tableCount, 10) || 8)),
      columnsPerTable: Math.max(4, Math.min(200, Number.parseInt(config.columnsPerTable, 10) || 24)),
      workloads,
      totalSessions,
      maxDurationSeconds,
      hwMitigation
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

    for (let index = 1; index <= this.config.columnsPerTable; index += 1) {
      const suffix = String(index).padStart(3, '0');
      const bindName = `b${suffix}`;

      if (index % 3 === 1) {
        columns.push(`vc_${suffix}`);
      } else if (index % 3 === 2) {
        columns.push(`num_${suffix}`);
      } else {
        columns.push(`dt_${suffix}`);
      }

      values.push(`:${bindName}`);
    }

    return `INSERT INTO ${tableName} (${columns.join(', ')}) VALUES (${values.join(', ')})`;
  }

  buildBindValues(workloadId, workerId, sequence) {
    const binds = {};

    for (let index = 1; index <= this.config.columnsPerTable; index += 1) {
      const suffix = String(index).padStart(3, '0');
      const bindName = `b${suffix}`;

      if (index % 3 === 1) {
        binds[bindName] = `${workloadId}_W${workerId}_R${sequence}_C${suffix}`;
      } else if (index % 3 === 2) {
        binds[bindName] = (workerId * 1000000) + sequence + index;
      } else {
        binds[bindName] = new Date(Date.now() - ((sequence + index) % 86400000));
      }
    }

    return binds;
  }

  buildAllocateExtentSql(tableName) {
    const extentSizeMb = this.config?.hwMitigation?.extentSizeMb || 128;
    return `ALTER TABLE ${tableName} ALLOCATE EXTENT (SIZE ${extentSizeMb}M)`;
  }

  async allocateExtent(tableName) {
    if (!this.db) {
      return false;
    }

    const connection = await this.db.createDirectConnection();
    try {
      await connection.execute(this.buildAllocateExtentSql(tableName));
      this.stats.extentAllocations += 1;
      return true;
    } finally {
      await connection.close();
    }
  }

  async preallocateExtentsBeforeStart() {
    if (!this.config?.hwMitigation?.enabled || !this.config.hwMitigation.preallocateOnStart) {
      return;
    }

    let completed = 0;
    const totalTables = this.tableNames.length;
    this.io?.emit('insert-blast-progress', {
      step: `Pre-allocating ${this.config.hwMitigation.extentSizeMb} MB extent(s) for ${totalTables} tables...`,
      progress: 0
    });

    for (const tableName of this.tableNames) {
      try {
        await this.allocateExtent(tableName);
      } catch (error) {
        console.log(`Insert blast extent pre-allocation failed for ${tableName}: ${error.message}`);
      }

      completed += 1;
      if (completed === totalTables || completed % 25 === 0) {
        this.io?.emit('insert-blast-progress', {
          step: `Pre-allocated ${completed}/${totalTables} tables with ${this.config.hwMitigation.extentSizeMb} MB extents...`,
          progress: Math.round((completed / totalTables) * 100)
        });
      }
    }
  }

  maybeScheduleExtentAllocation(tableName, tableInsertCount) {
    const hwMitigation = this.config?.hwMitigation;
    if (!hwMitigation?.enabled || !tableName || !Number.isFinite(tableInsertCount)) {
      return;
    }

    const threshold = Math.max(1, hwMitigation.allocateEveryInserts);
    const nextThreshold = this.nextExtentAllocationAt?.[tableName] || threshold;

    if (tableInsertCount < nextThreshold) {
      return;
    }

    if (this.extentAllocationInFlight?.has(tableName)) {
      return;
    }

    this.extentAllocationInFlight.add(tableName);

    this.allocateExtent(tableName)
      .then((allocated) => {
        if (allocated) {
          this.nextExtentAllocationAt[tableName] = nextThreshold + threshold;
        }
      })
      .catch((error) => {
        console.log(`Insert blast extent allocation failed for ${tableName}: ${error.message}`);
        this.nextExtentAllocationAt[tableName] = nextThreshold + threshold;
      })
      .finally(() => {
        this.extentAllocationInFlight.delete(tableName);
      });
  }

  isWorkloadActive(workload) {
    if (!this.isRunning || !workload?.startedAt) {
      return false;
    }

    return ((Date.now() - workload.startedAt) / 1000) < workload.durationSeconds;
  }

  async tagSession(connection, workload, workerId) {
    const moduleName = 'DBSTRESS_INSERT_BLAST';
    const actionName = this.runId || 'INSERT_BLAST';
    const clientId = `IBLAST:${actionName}:${workload.id}:W${workerId}`;

    try {
      connection.module = moduleName;
      connection.action = actionName;
      connection.clientId = clientId;
    } catch (error) {
      try {
        await connection.execute(
          `
            BEGIN
              DBMS_APPLICATION_INFO.SET_MODULE(:moduleName, :actionName);
              DBMS_SESSION.SET_IDENTIFIER(:clientId);
            END;
          `,
          { moduleName, actionName, clientId },
          { autoCommit: false }
        );
      } catch (innerError) {
        // Best effort only.
      }
    }
  }

  async killTrackedSessions() {
    if (!this.db || !this.runId) {
      return { killed: 0, attempted: 0, failures: [] };
    }

    const actionName = this.runId;
    const failures = [];
    let rows = [];

    try {
      const result = await this.db.execute(
        `
          SELECT inst_id, sid, serial#
          FROM gv$session
          WHERE module = 'DBSTRESS_INSERT_BLAST'
            AND action = :actionName
        `,
        { actionName }
      );
      rows = result.rows || [];
    } catch (error) {
      try {
        const fallback = await this.db.execute(
          `
            SELECT 1 AS inst_id, sid, serial#
            FROM v$session
            WHERE module = 'DBSTRESS_INSERT_BLAST'
              AND action = :actionName
          `,
          { actionName }
        );
        rows = fallback.rows || [];
      } catch (fallbackError) {
        failures.push(`Session lookup failed: ${fallbackError.message}`);
        return { killed: 0, attempted: 0, failures };
      }
    }

    let killed = 0;
    for (const row of rows) {
      const sid = row.SID;
      const serial = row['SERIAL#'];
      const instId = row.INST_ID || 1;
      const killSql = instId
        ? `ALTER SYSTEM KILL SESSION '${sid},${serial},@${instId}' IMMEDIATE`
        : `ALTER SYSTEM KILL SESSION '${sid},${serial}' IMMEDIATE`;

      try {
        await this.db.execute(killSql);
        killed += 1;
      } catch (error) {
        failures.push(`sid=${sid}, serial#=${serial}, inst_id=${instId}: ${error.message}`);
      }
    }

    return { killed, attempted: rows.length, failures };
  }

  async runWorker(workload, workerId) {
    this.activeWorkers += 1;
    this.stats.workloads[workload.id].activeWorkers += 1;

    let sequence = 0;
    let pending = 0;
    let connection = null;
    const reuseSession = workload.sessionMode === 'reuse';

    try {
      if (reuseSession) {
        connection = await workload.pool.getConnection();
        await this.tagSession(connection, workload, workerId);
      }

      while (this.isWorkloadActive(workload)) {
        try {
          if (!reuseSession) {
            connection = await this.db.createDirectConnection();
            await this.tagSession(connection, workload, workerId);
            pending = 0;
          }

          while (this.isWorkloadActive(workload)) {
            const workloadTableNames = workload.tableNames?.length ? workload.tableNames : this.tableNames;
            const tableName = workloadTableNames[Math.floor(Math.random() * workloadTableNames.length)];
            const sql = this.insertSqlCache[tableName] || this.buildInsertSql(tableName);
            sequence += 1;

            await connection.execute(sql, this.buildBindValues(workload.id, workerId, sequence), { autoCommit: false });
            pending += 1;
            this.stats.inserts += 1;
            this.stats.byTable[tableName] = (this.stats.byTable[tableName] || 0) + 1;
            this.stats.workloads[workload.id].inserts += 1;
            this.maybeScheduleExtentAllocation(tableName, this.stats.byTable[tableName]);

            if (pending >= workload.commitEvery) {
              await connection.commit();
              pending = 0;
              this.stats.commits += 1;
              this.stats.workloads[workload.id].commits += 1;
            }

            if (!reuseSession) {
              if (pending > 0) {
                await connection.commit();
                pending = 0;
                this.stats.commits += 1;
                this.stats.workloads[workload.id].commits += 1;
              }
              break;
            }
          }

          if (pending > 0) {
            await connection.commit();
            pending = 0;
            this.stats.commits += 1;
            this.stats.workloads[workload.id].commits += 1;
          }
        } catch (error) {
          this.stats.errors += 1;
          this.stats.workloads[workload.id].errors += 1;

          if (connection) {
            try {
              await connection.rollback();
            } catch (rollbackError) {
              // ignore
            }
          }

          if (!String(error.message || '').includes('pool is closing')) {
            console.log(`Insert blast worker ${workload.name}/${workerId} error: ${error.message}`);
          }
        } finally {
          if (!reuseSession && connection) {
            try {
              await connection.close();
            } catch (closeError) {
              // ignore
            }
            connection = null;
          }
        }
      }
    } finally {
      if (reuseSession && connection) {
        try {
          if (pending > 0) {
            await connection.commit();
            this.stats.commits += 1;
            this.stats.workloads[workload.id].commits += 1;
          }
          await connection.close();
        } catch (closeError) {
          // ignore
        }
      }

      this.activeWorkers -= 1;
      this.stats.workloads[workload.id].activeWorkers = Math.max(
        0,
        this.stats.workloads[workload.id].activeWorkers - 1
      );
    }
  }

  emitMetrics() {
    const elapsed = Math.max(1, (Date.now() - this.previousStats.startedAt) / 1000);
    const deltaInserts = this.stats.inserts - this.previousStats.inserts;
    const deltaErrors = this.stats.errors - this.previousStats.errors;
    const workloads = {};

    Object.values(this.stats.workloads || {}).forEach((workloadStats) => {
      const previousWorkload = this.previousStats.workloads?.[workloadStats.id] || {};
      const workloadDeltaInserts = workloadStats.inserts - Number(previousWorkload.inserts || 0);
      const workloadDeltaErrors = workloadStats.errors - Number(previousWorkload.errors || 0);

      workloads[workloadStats.id] = {
        ...workloadStats,
        perSecond: {
          inserts: Number((workloadDeltaInserts / elapsed).toFixed(2)),
          errors: Number((workloadDeltaErrors / elapsed).toFixed(2))
        }
      };
    });

    const payload = {
      isRunning: this.isRunning,
      uptime: this.startedAt ? Math.floor((Date.now() - this.startedAt) / 1000) : 0,
      activeWorkers: this.activeWorkers,
      total: {
        inserts: this.stats.inserts,
        commits: this.stats.commits,
        errors: this.stats.errors,
        extentAllocations: this.stats.extentAllocations
      },
      perSecond: {
        inserts: Number((deltaInserts / elapsed).toFixed(2)),
        errors: Number((deltaErrors / elapsed).toFixed(2))
      },
      byTable: this.stats.byTable,
      workloads,
      config: this.config
    };

    if (this.io) {
      this.io.emit('insert-blast-metrics', payload);
      this.io.emit('insert-blast-status', this.getStatus());
    }

    this.previousStats = {
      inserts: this.stats.inserts,
      commits: this.stats.commits,
      errors: this.stats.errors,
      byTable: { ...this.stats.byTable },
      workloads: Object.fromEntries(
        Object.entries(this.stats.workloads || {}).map(([id, workloadStats]) => [id, { ...workloadStats }])
      ),
      extentAllocations: this.stats.extentAllocations,
      startedAt: Date.now()
    };
  }

  async start(oracleDb, config = {}, io) {
    if (this.isRunning) {
      throw new Error('Insert blast workload is already running.');
    }

    this.config = this.normalizeConfig(config);
    this.io = io;
    this.db = oracleDb;
    this.runId = uuidv4();
    this.isRunning = true;
    this.startedAt = Date.now();
    this.stats = this.initStats(this.config);
    this.previousStats = this.initStats(this.config);
    this.tableNames = this.getTableNames();
    this.nextExtentAllocationAt = Object.fromEntries(
      this.tableNames.map((tableName) => [tableName, this.config.hwMitigation.allocateEveryInserts])
    );
    this.extentAllocationInFlight = new Set();
    this.insertSqlCache = Object.fromEntries(
      this.tableNames.map((tableName) => [tableName, this.buildInsertSql(tableName)])
    );

    await this.preallocateExtentsBeforeStart();

    this.runtimeWorkloads = this.config.workloads.map((workload) => ({
      ...workload,
      startedAt: Date.now(),
      pool: null,
      tableNames: this.tableNames.slice(0, workload.tableCount)
    }));

    for (const workload of this.runtimeWorkloads) {
      if (workload.sessionMode === 'reuse') {
        workload.pool = await oracleDb.createStressPool(workload.sessions);
      }
    }

    this.workers = this.runtimeWorkloads.flatMap((workload) => (
      Array.from({ length: workload.sessions }, (_, index) => this.runWorker(workload, index + 1))
    ));

    this.statsInterval = setInterval(() => {
      this.emitMetrics();

      if (this.isRunning && this.runtimeWorkloads.every((workload) => !this.isWorkloadActive(workload)) && this.activeWorkers === 0) {
        this.stop().catch(() => {});
      }
    }, 1000);

    this.stopTimer = setTimeout(() => {
      this.stop().catch(() => {});
    }, (this.config.maxDurationSeconds + 1) * 1000);

    return this.getStatus();
  }

  getStatus() {
    const workloadStatuses = (this.config?.workloads || []).map((workload) => {
      const runtimeWorkload = this.runtimeWorkloads.find((item) => item.id === workload.id);
      const workloadStats = this.stats.workloads?.[workload.id] || {};
      const uptime = runtimeWorkload?.startedAt
        ? Math.floor((Date.now() - runtimeWorkload.startedAt) / 1000)
        : 0;

      return {
        ...workload,
        isRunning: Boolean(runtimeWorkload && this.isWorkloadActive(runtimeWorkload)),
        uptime,
        activeWorkers: workloadStats.activeWorkers || 0,
        inserts: workloadStats.inserts || 0,
        commits: workloadStats.commits || 0,
        errors: workloadStats.errors || 0
      };
    });

    return {
      isRunning: this.isRunning,
      config: this.config,
      uptime: this.isRunning && this.startedAt ? Math.floor((Date.now() - this.startedAt) / 1000) : 0,
      activeWorkers: this.activeWorkers,
      workloads: workloadStatuses,
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

    await Promise.race([
      Promise.allSettled(this.workers || []),
      new Promise((resolve) => setTimeout(resolve, 3000))
    ]);

    for (const workload of this.runtimeWorkloads) {
      if (workload.pool) {
        try {
          await workload.pool.close(10);
        } catch (error) {
          console.log(`Insert blast pool close error for ${workload.name}:`, error.message);
        }
        workload.pool = null;
      }
    }

    const killSummary = await this.killTrackedSessions();
    if (this.io && (killSummary.killed > 0 || killSummary.failures.length > 0)) {
      this.io.emit('insert-blast-progress', {
        step: killSummary.failures.length > 0
          ? `Stopped workload. Killed ${killSummary.killed}/${killSummary.attempted} remaining sessions.`
          : `Stopped workload. Killed ${killSummary.killed} remaining sessions.`,
        progress: 100
      });
    }

    this.emitMetrics();

    this.db = null;
    this.insertSqlCache = {};
    this.nextExtentAllocationAt = {};
    this.extentAllocationInFlight = new Set();
    this.workers = [];
    this.runtimeWorkloads = [];
    this.activeWorkers = 0;
    this.runId = null;

    return this.getStatus();
  }
}

module.exports = new InsertBlastEngine();
