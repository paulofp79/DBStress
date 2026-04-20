const KEY_WAIT_EVENTS = [
  'library cache: mutex X',
  'library cache: mutex S',
  'library cache lock',
  'library cache pin',
  'library cache load lock',
  'cursor: mutex X',
  'cursor: mutex S',
  'cursor: pin S wait on X',
  'latch: ges resource hash list',
  'latch: library cache',
  'latch: shared pool',
  'gc current block congested',
  'gc cr block congested',
  'gc current block busy',
  'gc cr failure',
  'gc buffer busy acquire',
  'gc buffer busy release',
  'row cache lock'
];

const clampInt = (value, min, max, fallback) => {
  const num = Number.parseInt(value, 10);
  if (!Number.isFinite(num)) return fallback;
  return Math.min(max, Math.max(min, num));
};

const toPlainProcedureName = (value, fallback) => {
  const cleaned = String(value || fallback || '')
    .trim()
    .replace(/^"+|"+$/g, '');
  return cleaned || fallback;
};

class LibraryCacheLockEngine {
  constructor() {
    this.isRunning = false;
    this.config = null;
    this.pool = null;
    this.io = null;
    this.db = null;
    this.workers = [];

    this.stats = this.createEmptyStats();
    this.previousStats = {
      totalCalls: 0,
      lastTick: Date.now()
    };

    this.statsInterval = null;
    this.waitEventsInterval = null;
    this.runStartSnapshot = null;
    this.lastSampleSnapshot = null;
    this.latestSample = null;
    this.lastRunSummary = null;
    this.runId = null;
  }

  createEmptyStats() {
    return {
      totalCalls: 0,
      errors: 0,
      responseTimes: [],
      callsPerSecond: 0,
      startTime: null,
      lastError: null
    };
  }

  normalizeConfig(config = {}) {
    const procedureName = toPlainProcedureName(config.procedureName, 'GRAV_SESSION_MFES_ONLINE');
    const procedureOwner = toPlainProcedureName(config.procedureOwner, '');
    const modulePrefix = String(config.modulePrefix || 'MFES')
      .replace(/\s+/g, '_')
      .slice(0, 18) || 'MFES';

    return {
      threads: clampInt(config.threads, 1, 500, 64),
      loopDelay: clampInt(config.loopDelay, 0, 5000, 0),
      moduleLength: clampInt(config.moduleLength, 30, 96, 42),
      procedureName,
      procedureOwner,
      modulePrefix,
      runLabel: String(config.runLabel || '').trim() || procedureName,
      waitSampleSeconds: clampInt(config.waitSampleSeconds, 2, 30, 5)
    };
  }

  qualifyProcedure() {
    const { procedureOwner, procedureName } = this.config;
    return procedureOwner ? `${procedureOwner}.${procedureName}` : procedureName;
  }

  buildAnonymousBlock() {
    return `BEGIN ${this.qualifyProcedure()}(:moduleName); END;`;
  }

  buildModuleName(workerId, iteration) {
    const base = `${this.config.modulePrefix}_${workerId.toString(36).toUpperCase()}_${iteration.toString(36).toUpperCase()}_${Date.now().toString(36).toUpperCase()}`;
    const padded = `${base}_LIBCACHELOCK_SIMULATION_PAYLOAD`;
    return padded.slice(0, this.config.moduleLength).padEnd(this.config.moduleLength, 'X');
  }

  emitStatus(message, extra = {}) {
    this.io?.emit('library-cache-lock-status', {
      running: this.isRunning,
      message,
      runId: this.runId,
      ...extra
    });
  }

  async start(db, incomingConfig, io) {
    if (this.isRunning) {
      throw new Error('Library Cache Lock workload is already running');
    }

    this.db = db;
    this.io = io;
    this.config = this.normalizeConfig(incomingConfig);
    this.runId = `lcl-${Date.now()}`;
    this.lastRunSummary = null;

    await this.validateProcedureExists();

    this.stats = this.createEmptyStats();
    this.stats.startTime = Date.now();
    this.previousStats = {
      totalCalls: 0,
      lastTick: Date.now()
    };
    this.latestSample = null;

    this.emitStatus(`Capturing baseline for ${this.qualifyProcedure()}...`);

    this.runStartSnapshot = await this.captureSystemSnapshot();
    this.lastSampleSnapshot = this.runStartSnapshot;

    this.pool = await db.createStressPool(this.config.threads + 8);
    this.workers = [];
    this.isRunning = true;

    const block = this.buildAnonymousBlock();
    for (let i = 0; i < this.config.threads; i++) {
      this.workers.push(this.runWorker(i, block));
    }

    this.statsInterval = setInterval(() => this.reportStats(), 1000);
    this.waitEventsInterval = setInterval(
      () => this.captureAndEmitSample(),
      this.config.waitSampleSeconds * 1000
    );

    this.emitStatus(`Running ${this.qualifyProcedure()} with ${this.config.threads} sessions`, {
      config: this.config
    });
  }

  async validateProcedureExists() {
    const owner = this.config.procedureOwner.toUpperCase();
    const name = this.config.procedureName.toUpperCase();

    const sql = owner
      ? `
        SELECT COUNT(*) AS total
        FROM all_objects
        WHERE owner = :owner
          AND object_name = :name
          AND object_type = 'PROCEDURE'
      `
      : `
        SELECT COUNT(*) AS total
        FROM user_objects
        WHERE object_name = :name
          AND object_type = 'PROCEDURE'
      `;

    const binds = owner ? { owner, name } : { name };
    const result = await this.db.execute(sql, binds);
    const total = result.rows?.[0]?.TOTAL || 0;

    if (!total) {
      throw new Error(`Procedure ${this.qualifyProcedure()} was not found. Compile it first or change the target name.`);
    }
  }

  async runWorker(workerId, block) {
    let iteration = 0;

    while (this.isRunning) {
      let connection;

      try {
        connection = await this.pool.getConnection();

        while (this.isRunning) {
          iteration += 1;
          const start = process.hrtime.bigint();

          try {
            await connection.execute(block, {
              moduleName: this.buildModuleName(workerId, iteration)
            }, {
              autoCommit: false
            });

            const elapsedMs = Number(process.hrtime.bigint() - start) / 1e6;
            this.stats.totalCalls += 1;
            this.stats.responseTimes.push(elapsedMs);
            if (this.stats.responseTimes.length > 2000) {
              this.stats.responseTimes.shift();
            }
          } catch (err) {
            this.stats.errors += 1;
            this.stats.lastError = err.message;
            if (this.stats.errors <= 5 || this.stats.errors % 100 === 0) {
              console.log(`Library Cache Lock worker ${workerId} error:`, err.message);
            }
          }

          if (this.isRunning && this.config.loopDelay > 0) {
            await this.sleep(this.config.loopDelay);
          }
        }
      } catch (err) {
        this.stats.errors += 1;
        this.stats.lastError = err.message;
        if (!String(err.message).includes('pool is terminating')) {
          console.log(`Library Cache Lock connection error (worker ${workerId}):`, err.message);
        }
      } finally {
        if (connection) {
          try {
            await connection.close();
          } catch (closeErr) {
            // Ignore close errors during shutdown.
          }
        }
      }

      if (this.isRunning) {
        await this.sleep(100);
      }
    }
  }

  reportStats() {
    const now = Date.now();
    const elapsedSeconds = Math.max(0.001, (now - this.previousStats.lastTick) / 1000);
    const totalCalls = this.stats.totalCalls;
    const callsDelta = totalCalls - this.previousStats.totalCalls;
    const callsPerSecond = callsDelta / elapsedSeconds;

    this.previousStats = {
      totalCalls,
      lastTick: now
    };
    this.stats.callsPerSecond = callsPerSecond;

    const responseTimes = this.stats.responseTimes;
    const avgLatencyMs = responseTimes.length
      ? responseTimes.reduce((sum, value) => sum + value, 0) / responseTimes.length
      : 0;

    this.io?.emit('library-cache-lock-metrics', {
      runId: this.runId,
      totalCalls,
      errors: this.stats.errors,
      callsPerSecond: Number(callsPerSecond.toFixed(2)),
      avgLatencyMs: Number(avgLatencyMs.toFixed(2)),
      durationSeconds: this.stats.startTime
        ? Math.floor((Date.now() - this.stats.startTime) / 1000)
        : 0,
      latestSample: this.latestSample,
      lastError: this.stats.lastError
    });
  }

  async captureAndEmitSample() {
    if (!this.isRunning) return;

    try {
      const currentSnapshot = await this.captureSystemSnapshot();
      const sample = this.computeRunDelta(this.lastSampleSnapshot, currentSnapshot);
      sample.capturedAt = currentSnapshot.capturedAt;
      this.lastSampleSnapshot = currentSnapshot;
      this.latestSample = sample;

      this.io?.emit('library-cache-lock-wait-events', {
        runId: this.runId,
        sample
      });
    } catch (err) {
      console.log('Library Cache Lock sample error:', err.message);
    }
  }

  async stop() {
    if (!this.isRunning && !this.pool) {
      return {
        summary: this.lastRunSummary,
        stats: {
          totalCalls: this.stats.totalCalls,
          errors: this.stats.errors
        }
      };
    }

    this.isRunning = false;

    if (this.statsInterval) {
      clearInterval(this.statsInterval);
      this.statsInterval = null;
    }

    if (this.waitEventsInterval) {
      clearInterval(this.waitEventsInterval);
      this.waitEventsInterval = null;
    }

    await this.sleep(250);

    let finalSnapshot = null;
    try {
      finalSnapshot = await this.captureSystemSnapshot();
    } catch (err) {
      console.log('Unable to capture final Library Cache Lock snapshot:', err.message);
    }

    if (this.pool) {
      try {
        await this.pool.close(2);
      } catch (err) {
        console.log('Library Cache Lock pool close warning:', err.message);
      }
      this.pool = null;
    }

    this.workers = [];

    const summary = finalSnapshot && this.runStartSnapshot
      ? this.buildRunSummary(finalSnapshot)
      : this.buildFallbackSummary();

    this.lastRunSummary = summary;

    const payload = {
      runId: this.runId,
      summary,
      stats: {
        totalCalls: this.stats.totalCalls,
        errors: this.stats.errors,
        lastError: this.stats.lastError
      }
    };

    this.io?.emit('library-cache-lock-stopped', payload);
    this.emitStatus('Stopped', { summary });

    this.runStartSnapshot = null;
    this.lastSampleSnapshot = null;
    this.latestSample = null;

    return payload;
  }

  buildFallbackSummary() {
    const durationSeconds = this.stats.startTime
      ? Math.max(0.001, (Date.now() - this.stats.startTime) / 1000)
      : 0;
    const avgLatencyMs = this.stats.responseTimes.length
      ? this.stats.responseTimes.reduce((sum, value) => sum + value, 0) / this.stats.responseTimes.length
      : 0;

    return {
      runId: this.runId,
      runLabel: this.config?.runLabel || this.config?.procedureName || 'Library Cache Lock',
      qualifiedProcedure: this.qualifyProcedure(),
      startedAt: this.stats.startTime ? new Date(this.stats.startTime).toISOString() : null,
      completedAt: new Date().toISOString(),
      durationSeconds: Number(durationSeconds.toFixed(2)),
      totalCalls: this.stats.totalCalls,
      errors: this.stats.errors,
      avgLatencyMs: Number(avgLatencyMs.toFixed(2)),
      callsPerSecond: durationSeconds > 0
        ? Number((this.stats.totalCalls / durationSeconds).toFixed(2))
        : 0,
      dbCpuSharePct: 0,
      averageActiveSessions: 0,
      commitRatePerSecond: 0,
      parseHardPerSecond: 0,
      userCallsPerSecond: 0,
      executeCountPerSecond: 0,
      keyWaits: [],
      topWaitEvents: [],
      matchedSql: []
    };
  }

  buildRunSummary(finalSnapshot) {
    const summary = this.computeRunDelta(this.runStartSnapshot, finalSnapshot);
    return {
      runId: this.runId,
      runLabel: this.config.runLabel,
      qualifiedProcedure: this.qualifyProcedure(),
      startedAt: new Date(this.stats.startTime).toISOString(),
      completedAt: new Date().toISOString(),
      totalCalls: this.stats.totalCalls,
      errors: this.stats.errors,
      avgLatencyMs: this.stats.responseTimes.length
        ? Number((this.stats.responseTimes.reduce((sum, value) => sum + value, 0) / this.stats.responseTimes.length).toFixed(2))
        : 0,
      callsPerSecond: summary.durationSeconds > 0
        ? Number((this.stats.totalCalls / summary.durationSeconds).toFixed(2))
        : 0,
      ...summary
    };
  }

  computeRunDelta(startSnapshot, endSnapshot) {
    const durationSeconds = Math.max(0.001, (endSnapshot.capturedAt - startSnapshot.capturedAt) / 1000);
    const waitAgg = new Map();

    for (const [key, current] of endSnapshot.waitEvents.entries()) {
      const previous = startSnapshot.waitEvents.get(key) || { totalWaits: 0, timeWaitedMicro: 0 };
      const event = current.event;
      const agg = waitAgg.get(event) || {
        event,
        totalWaits: 0,
        timeWaitedMicro: 0
      };

      agg.totalWaits += Math.max(0, current.totalWaits - previous.totalWaits);
      agg.timeWaitedMicro += Math.max(0, current.timeWaitedMicro - previous.timeWaitedMicro);
      waitAgg.set(event, agg);
    }

    const waitEvents = Array.from(waitAgg.values())
      .map((event) => ({
        event: event.event,
        totalWaits: event.totalWaits,
        timeWaitedSeconds: Number((event.timeWaitedMicro / 1e6).toFixed(3)),
        avgWaitMs: event.totalWaits > 0
          ? Number((event.timeWaitedMicro / event.totalWaits / 1000).toFixed(3))
          : 0
      }))
      .filter((event) => event.totalWaits > 0 || event.timeWaitedSeconds > 0)
      .sort((a, b) => b.timeWaitedSeconds - a.timeWaitedSeconds);

    const topWaitEvents = waitEvents.slice(0, 10);
    const keyWaits = KEY_WAIT_EVENTS.map((eventName) => (
      waitEvents.find((event) => event.event === eventName) || {
        event: eventName,
        totalWaits: 0,
        timeWaitedSeconds: 0,
        avgWaitMs: 0
      }
    ));

    const dbTimeSeconds = this.computeStatDelta(startSnapshot.timeModel, endSnapshot.timeModel, 'DB time') / 1e6;
    const dbCpuSeconds = this.computeStatDelta(startSnapshot.timeModel, endSnapshot.timeModel, 'DB CPU') / 1e6;
    const commits = this.computeStatDelta(startSnapshot.sysstat, endSnapshot.sysstat, 'user commits');
    const parseHard = this.computeStatDelta(startSnapshot.sysstat, endSnapshot.sysstat, 'parse count (hard)');
    const userCalls = this.computeStatDelta(startSnapshot.sysstat, endSnapshot.sysstat, 'user calls');
    const executeCount = this.computeStatDelta(startSnapshot.sysstat, endSnapshot.sysstat, 'execute count');

    const matchedSql = this.computeSqlDelta(startSnapshot.targetSql, endSnapshot.targetSql);

    return {
      durationSeconds: Number(durationSeconds.toFixed(2)),
      dbTimeSeconds: Number(dbTimeSeconds.toFixed(3)),
      dbCpuSeconds: Number(dbCpuSeconds.toFixed(3)),
      dbCpuSharePct: dbTimeSeconds > 0
        ? Number(((dbCpuSeconds / dbTimeSeconds) * 100).toFixed(2))
        : 0,
      averageActiveSessions: Number((dbTimeSeconds / durationSeconds).toFixed(2)),
      commitRatePerSecond: Number((commits / durationSeconds).toFixed(2)),
      parseHardPerSecond: Number((parseHard / durationSeconds).toFixed(2)),
      userCallsPerSecond: Number((userCalls / durationSeconds).toFixed(2)),
      executeCountPerSecond: Number((executeCount / durationSeconds).toFixed(2)),
      keyWaits,
      topWaitEvents,
      matchedSql
    };
  }

  computeStatDelta(startMap, endMap, statName) {
    let total = 0;
    for (const [key, value] of endMap.entries()) {
      const [, name] = key.split(':');
      if (name !== statName) continue;
      total += Math.max(0, value - (startMap.get(key) || 0));
    }
    return total;
  }

  computeSqlDelta(startMap, endMap) {
    const sqlRows = [];

    for (const [sqlId, current] of endMap.entries()) {
      const previous = startMap.get(sqlId) || {
        executions: 0,
        elapsedTime: 0,
        cpuTime: 0
      };

      const executions = Math.max(0, current.executions - previous.executions);
      const elapsedSeconds = Math.max(0, current.elapsedTime - previous.elapsedTime) / 1e6;
      const cpuSeconds = Math.max(0, current.cpuTime - previous.cpuTime) / 1e6;

      if (executions <= 0 && elapsedSeconds <= 0 && cpuSeconds <= 0) {
        continue;
      }

      sqlRows.push({
        sqlId,
        executions,
        elapsedSeconds: Number(elapsedSeconds.toFixed(3)),
        cpuSeconds: Number(cpuSeconds.toFixed(3)),
        sqlText: current.sqlText
      });
    }

    return sqlRows
      .sort((a, b) => b.elapsedSeconds - a.elapsedSeconds)
      .slice(0, 10);
  }

  async captureSystemSnapshot() {
    const [waitEventsResult, timeModelResult, sysstatResult, sqlResult] = await Promise.all([
      this.queryWaitEvents(),
      this.queryTimeModel(),
      this.querySysstat(),
      this.queryTargetSql()
    ]);

    const snapshot = {
      capturedAt: Date.now(),
      waitEvents: new Map(),
      timeModel: new Map(),
      sysstat: new Map(),
      targetSql: new Map()
    };

    for (const row of waitEventsResult.rows || []) {
      snapshot.waitEvents.set(`${row.INST_ID}:${row.EVENT}`, {
        event: row.EVENT,
        totalWaits: Number(row.TOTAL_WAITS || 0),
        timeWaitedMicro: Number(row.TIME_WAITED_MICRO || 0)
      });
    }

    for (const row of timeModelResult.rows || []) {
      snapshot.timeModel.set(`${row.INST_ID}:${row.STAT_NAME}`, Number(row.VALUE || 0));
    }

    for (const row of sysstatResult.rows || []) {
      snapshot.sysstat.set(`${row.INST_ID}:${row.NAME}`, Number(row.VALUE || 0));
    }

    for (const row of sqlResult.rows || []) {
      const current = snapshot.targetSql.get(row.SQL_ID) || {
        executions: 0,
        elapsedTime: 0,
        cpuTime: 0,
        sqlText: row.SQL_TEXT
      };
      current.executions += Number(row.EXECUTIONS || 0);
      current.elapsedTime += Number(row.ELAPSED_TIME || 0);
      current.cpuTime += Number(row.CPU_TIME || 0);
      current.sqlText = current.sqlText || row.SQL_TEXT;
      snapshot.targetSql.set(row.SQL_ID, current);
    }

    return snapshot;
  }

  async queryWithFallback(primarySql, fallbackSql, binds = {}) {
    try {
      return await this.db.execute(primarySql, binds);
    } catch (primaryError) {
      if (!fallbackSql) {
        throw primaryError;
      }
      return await this.db.execute(fallbackSql, binds);
    }
  }

  async queryWaitEvents() {
    return this.queryWithFallback(
      `
        SELECT
          inst_id,
          event,
          total_waits,
          time_waited_micro
        FROM gv$system_event
        WHERE wait_class <> 'Idle'
          AND total_waits > 0
      `,
      `
        SELECT
          1 AS inst_id,
          event,
          total_waits,
          time_waited_micro
        FROM v$system_event
        WHERE wait_class <> 'Idle'
          AND total_waits > 0
      `
    );
  }

  async queryTimeModel() {
    return this.queryWithFallback(
      `
        SELECT
          inst_id,
          stat_name,
          value
        FROM gv$sys_time_model
        WHERE stat_name IN ('DB time', 'DB CPU')
      `,
      `
        SELECT
          1 AS inst_id,
          stat_name,
          value
        FROM v$sys_time_model
        WHERE stat_name IN ('DB time', 'DB CPU')
      `
    );
  }

  async querySysstat() {
    return this.queryWithFallback(
      `
        SELECT
          inst_id,
          name,
          value
        FROM gv$sysstat
        WHERE name IN (
          'user commits',
          'parse count (hard)',
          'parse count (total)',
          'execute count',
          'user calls'
        )
      `,
      `
        SELECT
          1 AS inst_id,
          name,
          value
        FROM v$sysstat
        WHERE name IN (
          'user commits',
          'parse count (hard)',
          'parse count (total)',
          'execute count',
          'user calls'
        )
      `
    );
  }

  async queryTargetSql() {
    const likeExpr = `%${this.config.procedureName.toUpperCase()}%`;

    return this.queryWithFallback(
      `
        SELECT * FROM (
          SELECT
            inst_id,
            sql_id,
            executions,
            elapsed_time,
            cpu_time,
            SUBSTR(sql_text, 1, 200) AS sql_text,
            last_active_time
          FROM gv$sqlstats
          WHERE UPPER(sql_text) LIKE :likeExpr
          ORDER BY last_active_time DESC
        )
        WHERE ROWNUM <= 20
      `,
      `
        SELECT * FROM (
          SELECT
            1 AS inst_id,
            sql_id,
            executions,
            elapsed_time,
            cpu_time,
            SUBSTR(sql_text, 1, 200) AS sql_text,
            last_active_time
          FROM v$sqlstats
          WHERE UPPER(sql_text) LIKE :likeExpr
          ORDER BY last_active_time DESC
        )
        WHERE ROWNUM <= 20
      `,
      { likeExpr }
    );
  }

  sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}

module.exports = new LibraryCacheLockEngine();
