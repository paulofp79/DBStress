// GC Congestion Demo Engine
// Creates many hot tables, loads scaled data, runs a RAC-heavy workload,
// and streams filtered GC wait deltas in real time.

const DEFAULT_WAIT_FILTERS = [
  'gc current block congested',
  'gc cr block congested',
  'gc current block busy',
  'gc cr block busy'
];

const DROP_RETRYABLE_ERRORS = ['ORA-00054', 'ORA-04021'];

const clamp = (value, min, max, fallback) => {
  const num = Number(value);
  if (!Number.isFinite(num)) return fallback;
  return Math.min(max, Math.max(min, Math.floor(num)));
};

const clampFloat = (value, min, max, fallback) => {
  const num = Number(value);
  if (!Number.isFinite(num)) return fallback;
  return Math.min(max, Math.max(min, num));
};

class GcCongestionEngine {
  constructor() {
    this.isPreparing = false;
    this.isRunning = false;

    this.db = null;
    this.io = null;
    this.pool = null;

    this.workers = [];
    this.tableNames = [];

    this.config = null;
    this.preparedConfig = null;
    this.sessionModule = null;
    this.stopPromise = null;

    this.stats = {
      totalTransactions: 0,
      errors: 0,
      tps: 0,
      startTime: null
    };
    this.previousStats = {
      totalTransactions: 0,
      lastTick: Date.now()
    };

    this.waitSnapshot = new Map();
    this.statsInterval = null;
    this.waitInterval = null;
  }

  normalizePrefix(prefix = 'GCDEMO') {
    const cleaned = String(prefix).toUpperCase().replace(/[^A-Z0-9_]/g, '').slice(0, 10);
    return cleaned || 'GCDEMO';
  }

  normalizeWaitFilters(filters) {
    if (!Array.isArray(filters) || filters.length === 0) {
      return [...DEFAULT_WAIT_FILTERS];
    }

    return Array.from(
      new Set(
        filters
          .map(v => String(v || '').trim())
          .filter(Boolean)
      )
    ).slice(0, 25);
  }

  normalizeConfig(config = {}) {
    const prefix = this.normalizePrefix(config.schemaPrefix);
    const tableCount = clamp(config.tableCount, 1, 200, 100);
    const scaleFactor = clamp(config.scaleFactor, 1, 1000, 1);
    const baseRowsPerTable = clamp(config.baseRowsPerTable, 100, 2000000, 10000);
    const rowsPerTable = baseRowsPerTable * scaleFactor;

    const threads = clamp(config.threads, 1, 500, 120);
    const thinkTime = clamp(config.thinkTime, 0, 2000, 0);
    const hotRows = clamp(config.hotRows, 10, 100000, 200);
    const updatesPerTxn = clamp(config.updatesPerTxn, 1, 20, 4);
    const hotTableSpan = clamp(config.hotTableSpan, 1, tableCount, Math.min(tableCount, 10));
    const readRatio = clampFloat(config.readRatio, 0, 0.95, 0.15);
    const payloadSize = clamp(config.payloadSize, 20, 1000, 120);
    const indexPartitioning = String(config.indexPartitioning || 'none').toLowerCase() === 'hash'
      ? 'hash'
      : 'none';
    const indexHashPartitions = clamp(config.indexHashPartitions, 2, 512, 16);

    return {
      schemaPrefix: prefix,
      tableCount,
      scaleFactor,
      baseRowsPerTable,
      rowsPerTable,
      threads,
      thinkTime,
      hotRows,
      updatesPerTxn,
      hotTableSpan,
      readRatio,
      payloadSize,
      indexPartitioning,
      indexHashPartitions,
      waitFilters: this.normalizeWaitFilters(config.waitFilters),
      dropExisting: config.dropExisting !== false
    };
  }

  normalizeDropConfig(dropConfig = {}) {
    return {
      schemaPrefix: this.normalizePrefix(dropConfig.schemaPrefix || this.preparedConfig?.schemaPrefix || 'GCDEMO'),
      waitForLogoutSec: clamp(dropConfig.waitForLogoutSec, 0, 600, 30),
      forceLogout: dropConfig.forceLogout === true || dropConfig.forceLogout === 'true',
      dropRetries: clamp(dropConfig.dropRetries, 1, 20, 6),
      retryDelayMs: clamp(dropConfig.retryDelayMs, 100, 10000, 1000)
    };
  }

  buildSessionModule(prefix) {
    return `DBSTRESS_GC_${this.normalizePrefix(prefix)}`.slice(0, 48);
  }

  isRetryableDropError(err) {
    const message = String(err?.message || '');
    return DROP_RETRYABLE_ERRORS.some(code => message.includes(code));
  }

  buildTableName(prefix, index) {
    return `${prefix}_GCT${index}`;
  }

  buildPrimaryKeyName(tableName) {
    return `${tableName}_PK`.slice(0, 30);
  }

  buildIndexName(tableName) {
    return `${tableName}_HGIX`.slice(0, 30);
  }

  emitStatus(message, progress = null, extra = {}) {
    if (!this.io) return;
    this.io.emit('gc-congestion-status', {
      message,
      progress,
      isPreparing: this.isPreparing,
      isRunning: this.isRunning,
      prepared: !!this.preparedConfig,
      ...extra
    });
  }

  async listAvailableWaitEvents(db = null) {
    const dbRef = db || this.db;
    if (!dbRef) {
      return [];
    }

    try {
      const result = await dbRef.execute(`
        SELECT event
        FROM gv$system_event
        WHERE event LIKE 'gc %'
        GROUP BY event
        ORDER BY event
      `);
      return (result.rows || []).map(r => r.EVENT).filter(Boolean);
    } catch (err) {
      try {
        const result = await dbRef.execute(`
          SELECT event
          FROM v$system_event
          WHERE event LIKE 'gc %'
          ORDER BY event
        `);
        return (result.rows || []).map(r => r.EVENT).filter(Boolean);
      } catch (fallbackErr) {
        return [];
      }
    }
  }

  async prepare(db, incomingConfig = {}, io = null) {
    if (this.isRunning) {
      throw new Error('Stop GC workload before preparing tables');
    }
    if (this.isPreparing) {
      throw new Error('GC demo preparation is already in progress');
    }

    this.db = db;
    this.io = io;
    this.isPreparing = true;

    const config = this.normalizeConfig(incomingConfig);
    const tableNames = [];
    for (let i = 1; i <= config.tableCount; i++) {
      tableNames.push(this.buildTableName(config.schemaPrefix, i));
    }

    try {
      this.emitStatus(
        `Preparing ${config.tableCount} GC demo tables (${config.rowsPerTable.toLocaleString()} rows/table)...`,
        1
      );

      for (let i = 0; i < tableNames.length; i++) {
        const tableName = tableNames[i];
        const pkName = this.buildPrimaryKeyName(tableName);
        const idxName = this.buildIndexName(tableName);
        const pct = Math.floor(((i + 1) / tableNames.length) * 100);

        if (config.dropExisting) {
          try {
            await this.db.execute(`DROP TABLE ${tableName} PURGE`);
          } catch (err) {
            if (!String(err.message).includes('ORA-00942')) {
              throw err;
            }
          }
        }

        await this.db.execute(`
          CREATE TABLE ${tableName} (
            id NUMBER NOT NULL,
            hot_group NUMBER NOT NULL,
            counter NUMBER DEFAULT 0,
            payload VARCHAR2(${config.payloadSize}),
            updated_at TIMESTAMP DEFAULT SYSTIMESTAMP,
            CONSTRAINT ${pkName} PRIMARY KEY (id)
          ) INITRANS 100 MAXTRANS 255 PCTFREE 1 LOGGING
        `);

        try {
          const indexDdl = config.indexPartitioning === 'hash'
            ? `CREATE INDEX ${idxName} ON ${tableName}(hot_group) GLOBAL PARTITION BY HASH (hot_group) PARTITIONS ${config.indexHashPartitions} INITRANS 100 PCTFREE 1`
            : `CREATE INDEX ${idxName} ON ${tableName}(hot_group) INITRANS 100 PCTFREE 1`;
          await this.db.execute(indexDdl);
        } catch (err) {
          if (!String(err.message).includes('ORA-00955')) {
            throw err;
          }
        }

        await this.db.execute(
          `
            INSERT /*+ APPEND */ INTO ${tableName} (id, hot_group, counter, payload)
            SELECT
              LEVEL AS id,
              MOD(LEVEL, 16) + 1 AS hot_group,
              0 AS counter,
              RPAD('X', :payloadSize, 'X') AS payload
            FROM dual
            CONNECT BY LEVEL <= :rowCount
          `,
          { payloadSize: config.payloadSize, rowCount: config.rowsPerTable }
        );

        this.emitStatus(`Prepared table ${i + 1}/${tableNames.length}: ${tableName}`, pct);
      }

      this.tableNames = tableNames;
      this.preparedConfig = {
        schemaPrefix: config.schemaPrefix,
        tableCount: config.tableCount,
        rowsPerTable: config.rowsPerTable,
        scaleFactor: config.scaleFactor,
        indexPartitioning: config.indexPartitioning,
        indexHashPartitions: config.indexHashPartitions,
        preparedAt: new Date().toISOString()
      };

      this.emitStatus('GC demo tables prepared successfully', 100, {
        prepared: true,
        preparedConfig: this.preparedConfig
      });

      return {
        success: true,
        ...this.preparedConfig
      };
    } finally {
      this.isPreparing = false;
    }
  }

  async queryModuleSessions(dbRef, moduleName) {
    const binds = { moduleName };
    const parseRows = (rows = []) => rows.map(row => ({
      sid: row.SID,
      serial: row['SERIAL#'],
      instId: row.INST_ID || null
    }));

    try {
      const result = await dbRef.execute(
        `
          SELECT sid, serial#, inst_id
          FROM gv$session
          WHERE username = SYS_CONTEXT('USERENV', 'SESSION_USER')
            AND module = :moduleName
            AND type = 'USER'
            AND sid <> TO_NUMBER(SYS_CONTEXT('USERENV', 'SID'))
        `,
        binds
      );
      return parseRows(result.rows);
    } catch (err) {
      try {
        const fallback = await dbRef.execute(
          `
            SELECT sid, serial#
            FROM v$session
            WHERE username = SYS_CONTEXT('USERENV', 'SESSION_USER')
              AND module = :moduleName
              AND type = 'USER'
              AND sid <> TO_NUMBER(SYS_CONTEXT('USERENV', 'SID'))
          `,
          binds
        );
        return parseRows(fallback.rows);
      } catch (fallbackErr) {
        return [];
      }
    }
  }

  async waitForSessionsToDrain(dbRef, moduleName, timeoutSec = 30) {
    const start = Date.now();
    const timeoutMs = Math.max(0, timeoutSec) * 1000;
    let sessions = await this.queryModuleSessions(dbRef, moduleName);

    while (sessions.length > 0 && (Date.now() - start) < timeoutMs) {
      await this.sleep(1000);
      sessions = await this.queryModuleSessions(dbRef, moduleName);
    }

    return {
      remaining: sessions.length,
      waitedSec: Math.round((Date.now() - start) / 1000)
    };
  }

  async forceKillSessions(dbRef, moduleName) {
    const sessions = await this.queryModuleSessions(dbRef, moduleName);
    let killed = 0;
    let failures = 0;
    let lastError = null;

    for (const session of sessions) {
      const sid = Number(session.sid);
      const serial = Number(session.serial);
      const instId = session.instId ? Number(session.instId) : null;

      if (!Number.isFinite(sid) || !Number.isFinite(serial)) {
        continue;
      }

      const sessionId = instId
        ? `${sid},${serial},@${instId}`
        : `${sid},${serial}`;

      try {
        await dbRef.execute(`ALTER SYSTEM KILL SESSION '${sessionId}' IMMEDIATE`);
        killed += 1;
      } catch (err) {
        const message = String(err.message || '');
        if (
          !message.includes('ORA-00026')
          && !message.includes('ORA-00030')
          && !message.includes('ORA-00031')
        ) {
          failures += 1;
          lastError = message;
        }
      }
    }

    return { attempted: sessions.length, killed, failures, lastError };
  }

  async dropTableWithRetry(dbRef, tableName, retries = 6, retryDelayMs = 1000) {
    for (let attempt = 1; attempt <= retries; attempt++) {
      try {
        await dbRef.execute(`DROP TABLE ${tableName} PURGE`);
        return true;
      } catch (err) {
        const message = String(err.message || '');
        if (message.includes('ORA-00942')) {
          return false;
        }
        if (this.isRetryableDropError(err) && attempt < retries) {
          await this.sleep(retryDelayMs);
          continue;
        }
        throw err;
      }
    }

    return false;
  }

  async dropTables(dropConfig = {}, db = null) {
    const dbRef = db || this.db;
    if (!dbRef) {
      throw new Error('Database not connected');
    }

    const options = this.normalizeDropConfig(dropConfig);
    const prefix = options.schemaPrefix;
    const moduleName = this.buildSessionModule(prefix);

    if (this.isRunning || this.pool) {
      this.emitStatus('Stopping GC workload before dropping tables...', null);
      await this.stop({ drainTimeoutSec: options.waitForLogoutSec });
    }

    let drained = await this.waitForSessionsToDrain(dbRef, moduleName, options.waitForLogoutSec);
    let killStats = { attempted: 0, killed: 0, failures: 0, lastError: null };
    if (drained.remaining > 0 && options.forceLogout) {
      this.emitStatus(`Force-killing ${drained.remaining} remaining GC sessions before drop...`, null);
      killStats = await this.forceKillSessions(dbRef, moduleName);
      drained = await this.waitForSessionsToDrain(dbRef, moduleName, Math.min(20, options.waitForLogoutSec));
    }

    if (drained.remaining > 0) {
      this.emitStatus(`Continuing drop while ${drained.remaining} session(s) are still terminating...`, null);
    }

    const result = await dbRef.execute(
      `
        SELECT table_name
        FROM user_tables
        WHERE table_name LIKE :pattern
        ORDER BY table_name DESC
      `,
      { pattern: `${prefix}_GCT%` }
    );

    const tables = (result.rows || []).map(r => r.TABLE_NAME);
    let dropped = 0;

    for (const tableName of tables) {
      const wasDropped = await this.dropTableWithRetry(
        dbRef,
        tableName,
        options.dropRetries,
        options.retryDelayMs
      );
      if (wasDropped) {
        dropped += 1;
      }
    }

    if (this.preparedConfig && this.preparedConfig.schemaPrefix === prefix) {
      this.preparedConfig = null;
      this.tableNames = [];
      this.sessionModule = null;
    }

    this.emitStatus(`Dropped ${dropped} GC demo tables for prefix ${prefix}`, 100, {
      dropped,
      prepared: !!this.preparedConfig,
      remainingSessions: drained.remaining,
      killedSessions: killStats.killed,
      killFailures: killStats.failures
    });

    return {
      dropped,
      prefix,
      remainingSessions: drained.remaining,
      waitedForLogoutSec: drained.waitedSec,
      forcedLogout: options.forceLogout,
      killedSessions: killStats.killed,
      killAttempts: killStats.attempted,
      killFailures: killStats.failures,
      killError: killStats.lastError
    };
  }

  async start(db, incomingConfig = {}, io = null) {
    if (this.isPreparing) {
      throw new Error('Wait for preparation to complete before starting workload');
    }
    if (this.isRunning) {
      throw new Error('GC congestion workload already running');
    }

    this.db = db;
    this.io = io;

    const config = this.normalizeConfig(incomingConfig);
    const preparedPrefix = this.preparedConfig?.schemaPrefix;
    const preparedCount = this.preparedConfig?.tableCount || 0;

    if (!this.preparedConfig || !this.tableNames.length) {
      throw new Error('Prepare GC demo tables first');
    }
    if (preparedPrefix !== config.schemaPrefix) {
      throw new Error(`Prepared prefix is '${preparedPrefix}'. Prepare tables for '${config.schemaPrefix}' first`);
    }
    if (preparedCount < config.tableCount) {
      throw new Error(
        `Prepared ${preparedCount} tables but workload requested ${config.tableCount}. Re-run prepare with a larger table count`
      );
    }
    const preparedIndexPartitioning = this.preparedConfig.indexPartitioning || 'none';
    if (preparedIndexPartitioning !== config.indexPartitioning) {
      throw new Error(
        `Prepared index layout is '${preparedIndexPartitioning}'. Re-run prepare for '${config.indexPartitioning}' to compare workloads`
      );
    }
    if (
      config.indexPartitioning === 'hash'
      && Number(this.preparedConfig.indexHashPartitions || 0) !== Number(config.indexHashPartitions)
    ) {
      throw new Error(
        `Prepared hash partitions are ${this.preparedConfig.indexHashPartitions}. Re-run prepare with ${config.indexHashPartitions} partitions`
      );
    }

    this.config = config;
    this.sessionModule = this.buildSessionModule(config.schemaPrefix);
    this.isRunning = true;
    this.workers = [];

    this.stats = {
      totalTransactions: 0,
      errors: 0,
      tps: 0,
      startTime: Date.now()
    };
    this.previousStats = {
      totalTransactions: 0,
      lastTick: Date.now()
    };
    this.waitSnapshot = new Map();

    this.emitStatus(
      `Starting GC congestion workload with ${config.threads} threads (${config.tableCount} tables, hot span ${config.hotTableSpan}, index ${config.indexPartitioning}${config.indexPartitioning === 'hash' ? `/${config.indexHashPartitions}` : ''})...`,
      null
    );

    this.pool = await this.db.createStressPool(config.threads + 10);

    await this.captureWaitSnapshot();

    for (let i = 0; i < config.threads; i++) {
      this.workers.push(this.runWorker(i));
    }

    this.statsInterval = setInterval(() => this.reportStats(), 1000);
    this.waitInterval = setInterval(() => this.reportWaits(), 2000);

    this.emitStatus('GC congestion workload running', null);
  }

  async runWorker(workerId) {
    while (this.isRunning) {
      let connection;
      try {
        connection = await this.pool.getConnection();
        try {
          await connection.execute(
            `BEGIN DBMS_APPLICATION_INFO.SET_MODULE(:moduleName, :actionName); END;`,
            {
              moduleName: this.sessionModule || this.buildSessionModule(this.config?.schemaPrefix || 'GCDEMO'),
              actionName: `worker-${workerId}`.slice(0, 32)
            }
          );
        } catch (moduleErr) {
          // Keep workload running even if module tagging fails.
        }

        const hotTableSpan = Math.min(this.config.hotTableSpan, this.config.tableCount, this.tableNames.length);
        const hotRows = Math.max(10, Math.min(this.config.hotRows, this.preparedConfig.rowsPerTable));

        const tableName = this.tableNames[Math.floor(Math.random() * hotTableSpan)];
        const idxName = this.buildIndexName(tableName);
        for (let i = 0; i < this.config.updatesPerTxn; i++) {
          const id = 1 + Math.floor(Math.random() * hotRows);
          const hotGroup = (id % 16) + 1;
          await connection.execute(
            `
              UPDATE /*+ INDEX(t ${idxName}) */ ${tableName} t
              SET counter = counter + 1,
                  updated_at = SYSTIMESTAMP
              WHERE t.hot_group = :hotGroup
                AND t.id = :id
            `,
            { id, hotGroup }
          );
        }

        if (Math.random() < this.config.readRatio) {
          const hotGroup = 1 + Math.floor(Math.random() * 4);
          await connection.execute(
            `
              SELECT /*+ INDEX(t ${idxName}) */ SUM(t.counter)
              FROM ${tableName} t
              WHERE t.hot_group = :hotGroup
                AND ROWNUM <= 64
            `,
            { hotGroup }
          );
        }

        await connection.commit();
        this.stats.totalTransactions += 1;
      } catch (err) {
        this.stats.errors += 1;
        if (connection) {
          try {
            await connection.rollback();
          } catch (rollbackErr) {
            // ignore rollback errors
          }
        }
      } finally {
        if (connection) {
          try {
            await connection.close();
          } catch (closeErr) {
            // ignore close errors
          }
        }
      }

      if (this.isRunning && this.config.thinkTime > 0) {
        await this.sleep(this.config.thinkTime);
      }
    }
  }

  async queryWaitSnapshot() {
    const filters = this.config?.waitFilters || [];
    const binds = {};
    let filterClause = '';

    if (filters.length > 0) {
      const placeholders = filters.map((_, i) => `:w${i}`);
      filters.forEach((eventName, i) => {
        binds[`w${i}`] = eventName;
      });
      filterClause = ` AND event IN (${placeholders.join(', ')})`;
    }

    const query = `
      SELECT
        event,
        SUM(total_waits) AS total_waits,
        SUM(total_timeouts) AS total_timeouts,
        SUM(time_waited_micro) / 1000 AS time_waited_ms
      FROM gv$system_event
      WHERE event LIKE 'gc %'${filterClause}
      GROUP BY event
      ORDER BY event
    `;

    try {
      const result = await this.db.execute(query, binds);
      return (result.rows || []).map(row => ({
        event: row.EVENT,
        totalWaits: row.TOTAL_WAITS || 0,
        totalTimeouts: row.TOTAL_TIMEOUTS || 0,
        timeWaitedMs: row.TIME_WAITED_MS || 0
      }));
    } catch (err) {
      const fallbackQuery = `
        SELECT
          event,
          total_waits AS total_waits,
          total_timeouts AS total_timeouts,
          time_waited_micro / 1000 AS time_waited_ms
        FROM v$system_event
        WHERE event LIKE 'gc %'${filterClause}
        ORDER BY event
      `;
      const fallback = await this.db.execute(fallbackQuery, binds);
      return (fallback.rows || []).map(row => ({
        event: row.EVENT,
        totalWaits: row.TOTAL_WAITS || 0,
        totalTimeouts: row.TOTAL_TIMEOUTS || 0,
        timeWaitedMs: row.TIME_WAITED_MS || 0
      }));
    }
  }

  async captureWaitSnapshot() {
    const rows = await this.queryWaitSnapshot();
    this.waitSnapshot = new Map();
    rows.forEach(row => this.waitSnapshot.set(row.event, row));
  }

  async reportWaits() {
    if (!this.isRunning || !this.io) return;

    try {
      const rows = await this.queryWaitSnapshot();
      const current = new Map(rows.map(r => [r.event, r]));

      const eventNames = this.config.waitFilters.length > 0
        ? this.config.waitFilters
        : Array.from(current.keys());

      const events = eventNames.map(eventName => {
        const now = current.get(eventName) || {
          event: eventName,
          totalWaits: 0,
          totalTimeouts: 0,
          timeWaitedMs: 0
        };
        const prev = this.waitSnapshot.get(eventName) || {
          totalWaits: 0,
          totalTimeouts: 0,
          timeWaitedMs: 0
        };

        const deltaWaits = Math.max(0, now.totalWaits - prev.totalWaits);
        const deltaTimeouts = Math.max(0, now.totalTimeouts - prev.totalTimeouts);
        const deltaTimeMs = Math.max(0, now.timeWaitedMs - prev.timeWaitedMs);
        const avgWaitMs = deltaWaits > 0 ? deltaTimeMs / deltaWaits : 0;

        return {
          event: eventName,
          totalWaits: now.totalWaits,
          totalTimeouts: now.totalTimeouts,
          timeWaitedMs: parseFloat(now.timeWaitedMs.toFixed(2)),
          deltaWaits,
          deltaTimeouts,
          deltaTimeMs: parseFloat(deltaTimeMs.toFixed(2)),
          avgWaitMs: parseFloat(avgWaitMs.toFixed(3))
        };
      });

      this.waitSnapshot = current;

      this.io.emit('gc-congestion-waits', {
        timestamp: Date.now(),
        events,
        selectedFilters: this.config.waitFilters
      });
    } catch (err) {
      // Keep the workload running even if wait polling fails intermittently.
    }
  }

  reportStats() {
    if (!this.isRunning || !this.io) return;

    const now = Date.now();
    const elapsedSeconds = (now - this.previousStats.lastTick) / 1000;
    const txnDelta = this.stats.totalTransactions - this.previousStats.totalTransactions;
    const tps = elapsedSeconds > 0 ? Math.round(txnDelta / elapsedSeconds) : 0;

    this.stats.tps = tps;
    this.previousStats.totalTransactions = this.stats.totalTransactions;
    this.previousStats.lastTick = now;

    this.io.emit('gc-congestion-metrics', {
      tps,
      totalTransactions: this.stats.totalTransactions,
      errors: this.stats.errors,
      uptime: this.stats.startTime ? Math.floor((now - this.stats.startTime) / 1000) : 0,
      config: {
        threads: this.config.threads,
        tableCount: this.config.tableCount,
        hotRows: this.config.hotRows,
        hotTableSpan: this.config.hotTableSpan
      }
    });
  }

  getStatus() {
    const now = Date.now();
    return {
      isPreparing: this.isPreparing,
      isRunning: this.isRunning,
      prepared: !!this.preparedConfig,
      preparedConfig: this.preparedConfig,
      workloadConfig: this.config,
      stats: {
        ...this.stats,
        uptime: this.stats.startTime ? Math.floor((now - this.stats.startTime) / 1000) : 0
      }
    };
  }

  async stop(options = {}) {
    if (this.stopPromise) {
      return this.stopPromise;
    }

    this.stopPromise = (async () => {
      this.isRunning = false;

      if (this.statsInterval) {
        clearInterval(this.statsInterval);
        this.statsInterval = null;
      }
      if (this.waitInterval) {
        clearInterval(this.waitInterval);
        this.waitInterval = null;
      }

      const drainTimeoutSec = clamp(options.drainTimeoutSec, 0, 600, 15);
      const activeWorkers = [...this.workers];

      if (activeWorkers.length > 0) {
        const waitForWorkers = Promise.allSettled(activeWorkers);
        if (drainTimeoutSec > 0) {
          await Promise.race([
            waitForWorkers,
            this.sleep(drainTimeoutSec * 1000)
          ]);
        } else {
          await waitForWorkers;
        }
      }

      if (this.pool) {
        try {
          await this.pool.close(drainTimeoutSec);
        } catch (err) {
          try {
            await this.pool.close(0);
          } catch (closeErr) {
            // ignore pool close warnings
          }
        }
        this.pool = null;
      }

      this.workers = [];

      this.emitStatus('GC congestion workload stopped', null, { isRunning: false });

      return {
        totalTransactions: this.stats.totalTransactions,
        errors: this.stats.errors
      };
    })();

    try {
      return await this.stopPromise;
    } finally {
      this.stopPromise = null;
    }
  }

  sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }
}

module.exports = new GcCongestionEngine();
