const oracledb = require('oracledb');

const LAB_TABLE = 'GC_AR_ROWS';
const MODULE_NAME = 'DBSTRESS_GC_AR';
const EVENTS = [
  'gc buffer busy acquire',
  'gc buffer busy release',
  'gc current block busy',
  'gc cr block busy',
  'buffer busy waits',
  'enq: TX - allocate ITL entry',
  'enq: TX - row lock contention'
];

const clamp = (value, min, max, fallback) => {
  const num = Number(value);
  if (!Number.isFinite(num)) return fallback;
  return Math.min(max, Math.max(min, Math.floor(num)));
};

const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

class GcAcquireReleaseEngine {
  constructor() {
    this.db = null;
    this.io = null;
    this.isRunning = false;
    this.isSettingUp = false;
    this.mode = null;
    this.config = null;
    this.workers = [];
    this.pools = [];
    this.monitorInterval = null;
    this.statsInterval = null;
    this.lastMonitor = null;
    this.logs = [];
    this.stats = {
      startedAt: null,
      completedLoops: 0,
      commits: 0,
      errors: 0
    };
    this.nextInsertId = 1000000;
  }

  addLog(message, level = 'info') {
    const entry = {
      ts: new Date().toISOString(),
      level,
      message: String(message || '').replace(/password=[^\s]+/ig, 'password=***')
    };
    this.logs = [entry, ...this.logs].slice(0, 200);
    if (this.io) {
      this.io.emit('gc-ar-log', entry);
    }
  }

  emitStatus(extra = {}) {
    if (!this.io) return;
    this.io.emit('gc-ar-status', this.getStatus(extra));
  }

  normalizeWorkloadConfig(incoming = {}) {
    const mode = incoming.mode === 'two-instance' ? 'two-instance' : 'one-instance';
    const rowCount = clamp(incoming.rowCount, 1, 1000, 128);
    const hotRowMin = clamp(incoming.hotRowMin, 1, rowCount, 1);
    const hotRowMax = clamp(incoming.hotRowMax, hotRowMin, rowCount, Math.min(rowCount, 128));
    const workloadShape = incoming.workloadShape === 'update-hot-block'
      ? 'update-hot-block'
      : 'insert-hot-index';
    const base = {
      mode,
      loopsPerWorker: clamp(incoming.loopsPerWorker, 1, 1000000, 20000),
      commitEvery: clamp(incoming.commitEvery, 1, 10000, 1),
      rowCount,
      hotRowMin,
      hotRowMax,
      rowTargetMode: incoming.rowTargetMode === 'random' ? 'random' : 'spread',
      workloadShape,
      monitorRefreshMs: clamp(incoming.monitorRefreshMs, 1000, 30000, 2000),
      killExistingSessions: incoming.killExistingSessions !== false
    };

    if (mode === 'two-instance') {
      return {
        ...base,
        instance1ConnectionString: String(incoming.instance1ConnectionString || '').trim(),
        instance2ConnectionString: String(incoming.instance2ConnectionString || '').trim(),
        workersInstance1: clamp(incoming.workersInstance1, 0, 1000, 25),
        workersInstance2: clamp(incoming.workersInstance2, 0, 1000, 25)
      };
    }

    return {
      ...base,
      instance2ConnectionString: String(incoming.instance2ConnectionString || '').trim(),
      workers: clamp(incoming.workers, 1, 1000, 50)
    };
  }

  async validateConnection(db) {
    const checks = [
      { name: 'GV$SESSION', sql: 'SELECT COUNT(*) AS CNT FROM gv$session WHERE ROWNUM <= 1' },
      { name: 'GV$INSTANCE', sql: 'SELECT COUNT(*) AS CNT FROM gv$instance WHERE ROWNUM <= 1' },
      { name: 'GV$ACTIVE_SESSION_HISTORY', sql: 'SELECT COUNT(*) AS CNT FROM gv$active_session_history WHERE ROWNUM <= 1' },
      { name: 'GV$SYSTEM_EVENT', sql: 'SELECT COUNT(*) AS CNT FROM gv$system_event WHERE ROWNUM <= 1' },
      { name: 'DBA_OBJECTS', sql: 'SELECT COUNT(*) AS CNT FROM dba_objects WHERE ROWNUM <= 1' }
    ];

    const infoResult = await db.execute(`
      SELECT
        SYS_CONTEXT('USERENV', 'DB_NAME') AS database_name,
        SYS_CONTEXT('USERENV', 'CON_NAME') AS pdb_name,
        SYS_CONTEXT('USERENV', 'SESSION_USER') AS username,
        i.instance_name,
        i.host_name
      FROM gv$instance i
      WHERE i.inst_id = SYS_CONTEXT('USERENV', 'INSTANCE')
    `);

    const permissions = [];
    for (const check of checks) {
      try {
        await db.execute(check.sql);
        permissions.push({ object: check.name, ok: true });
      } catch (err) {
        permissions.push({ object: check.name, ok: false, error: err.message });
      }
    }

    return {
      info: infoResult.rows?.[0] || {},
      permissions,
      ready: permissions.every(item => item.ok)
    };
  }

  async setupLab(db, io = null, options = {}) {
    this.db = db;
    this.io = io;
    if (this.isRunning) throw new Error('Stop the acquire/release workload before setup');
    if (this.isSettingUp) throw new Error('Setup is already running');
    this.isSettingUp = true;
    const rowCount = clamp(options.rowCount, 1, 1000, 128);

    try {
      this.addLog(`Setting up ${LAB_TABLE} with ${rowCount} hot rows`);
      try {
        await db.execute(`DROP TABLE ${LAB_TABLE} PURGE`);
      } catch (err) {
        if (!String(err.message).includes('ORA-00942')) throw err;
      }

      await db.execute(`
        CREATE TABLE ${LAB_TABLE} (
          id NUMBER NOT NULL,
          pad VARCHAR2(100),
          counter NUMBER DEFAULT 0,
          session_bucket NUMBER,
          request_id NUMBER,
          updated_at TIMESTAMP DEFAULT SYSTIMESTAMP,
          CONSTRAINT GC_AR_ROWS_PK PRIMARY KEY (id)
        ) INITRANS 1 PCTFREE 0
      `);

      await db.execute(
        `
          INSERT INTO ${LAB_TABLE} (id, pad, counter, session_bucket, request_id)
          SELECT LEVEL, RPAD('X', 100, 'X'), 0, MOD(LEVEL, 100), LEVEL
          FROM dual
          CONNECT BY LEVEL <= :rowCount
        `,
        { rowCount }
      );

      await db.execute(`
        CREATE INDEX GC_AR_REQ_IX ON ${LAB_TABLE}(request_id)
        INITRANS 1 PCTFREE 0
      `);

      await db.execute(`
        CREATE INDEX GC_AR_BUCKET_IX ON ${LAB_TABLE}(session_bucket)
        INITRANS 1 PCTFREE 0
      `);

      await db.execute(`BEGIN DBMS_STATS.GATHER_TABLE_STATS(USER, '${LAB_TABLE}'); END;`);
      const distribution = await this.getBlockDistribution(db);
      this.addLog(`Setup complete; found ${distribution.length} file/block group(s)`);
      return { tableName: LAB_TABLE, rowCount, distribution };
    } finally {
      this.isSettingUp = false;
      this.emitStatus();
    }
  }

  async getBlockDistribution(dbRef = null) {
    const db = dbRef || this.db;
    const result = await db.execute(`
      SELECT
        DBMS_ROWID.ROWID_RELATIVE_FNO(rowid) AS file_no,
        DBMS_ROWID.ROWID_BLOCK_NUMBER(rowid) AS block_no,
        COUNT(*) AS rows_in_block,
        MIN(id) AS min_id,
        MAX(id) AS max_id
      FROM ${LAB_TABLE}
      GROUP BY
        DBMS_ROWID.ROWID_RELATIVE_FNO(rowid),
        DBMS_ROWID.ROWID_BLOCK_NUMBER(rowid)
      ORDER BY rows_in_block DESC, file_no, block_no
    `);

    return (result.rows || []).map(row => ({
      fileNo: row.FILE_NO,
      blockNo: row.BLOCK_NO,
      rowsInBlock: row.ROWS_IN_BLOCK,
      minId: row.MIN_ID,
      maxId: row.MAX_ID
    }));
  }

  async createWorkerPool(size, connectionString) {
    const overrides = {
      connectionString: connectionString || undefined,
      poolMin: Math.min(size, 10),
      poolIncrement: Math.min(size, 5)
    };
    const pool = await this.db.createStressPool(size, overrides);
    this.pools.push(pool);
    return pool;
  }

  async start(db, incoming = {}, io = null) {
    this.db = db;
    this.io = io;
    const config = this.normalizeWorkloadConfig(incoming);
    const totalWorkers = config.mode === 'two-instance'
      ? config.workersInstance1 + config.workersInstance2
      : config.workers;
    if (totalWorkers < 1) throw new Error('At least one worker is required');

    await db.execute(`SELECT COUNT(*) AS CNT FROM ${LAB_TABLE}`);

    if (this.isRunning) {
      if (!config.killExistingSessions) {
        throw new Error('Acquire/release workload already running. Stop it first or enable existing-session cleanup.');
      }
      this.addLog('Stopping existing in-memory acquire/release workload before starting a new one', 'warn');
      await this.stop({ kill: true, drainSeconds: 5 });
    }

    const existingSessions = await this.queryToolSessions();
    if (existingSessions.length > 0) {
      if (!config.killExistingSessions) {
        throw new Error(`Found ${existingSessions.length} existing DBSTRESS_GC_AR session(s). Stop them first or enable existing-session cleanup.`);
      }
      this.addLog(`Killing ${existingSessions.length} existing DBSTRESS_GC_AR session(s) before starting`, 'warn');
      const killResult = await this.killToolSessions(existingSessions);
      const drainResult = await this.waitForToolSessionsToDrain(10);
      this.addLog(`Existing session cleanup complete; killed ${killResult.killed}/${killResult.attempted}, remaining ${drainResult.remaining}`);
    }

    this.config = config;
    this.mode = config.mode;
    this.isRunning = true;
    this.workers = [];
    this.pools = [];
    this.stats = {
      startedAt: Date.now(),
      completedLoops: 0,
      commits: 0,
      errors: 0
    };
    this.nextInsertId = Math.max(
      config.rowCount + 1,
      Math.floor(Date.now() / 1000) * 1000000
    );

    try {
      this.addLog(`Starting ${config.mode} ${config.workloadShape} acquire/release workload with ${totalWorkers} worker(s)`);

      if (config.mode === 'two-instance') {
        const pool1 = await this.createWorkerPool(Math.max(config.workersInstance1, 1), config.instance1ConnectionString);
        const pool2 = await this.createWorkerPool(Math.max(config.workersInstance2, 1), config.instance2ConnectionString);
        for (let i = 0; i < config.workersInstance1; i++) {
          this.workers.push(this.runWorker(pool1, `inst1-worker-${i + 1}`, 'instance-1', i));
        }
        for (let i = 0; i < config.workersInstance2; i++) {
          this.workers.push(this.runWorker(pool2, `inst2-worker-${i + 1}`, 'instance-2', config.workersInstance1 + i));
        }
      } else {
        const pool = await this.createWorkerPool(config.workers, config.instance2ConnectionString);
        for (let i = 0; i < config.workers; i++) {
          this.workers.push(this.runWorker(pool, `one-inst-worker-${i + 1}`, 'instance-2', i));
        }
      }

      this.monitorInterval = setInterval(() => this.safeRefreshMonitor(), config.monitorRefreshMs);
      this.statsInterval = setInterval(() => this.emitStatus(), 1000);
      await this.safeRefreshMonitor();
      this.emitStatus();
      return this.getStatus();
    } catch (err) {
      this.addLog(`Startup failed: ${err.message}`, 'error');
      await this.stop({ kill: true, drainSeconds: 2 });
      throw err;
    }
  }

  async safeRefreshMonitor() {
    try {
      return await this.refreshMonitor();
    } catch (err) {
      this.addLog(`Monitor refresh failed: ${err.message}`, 'warn');
      return this.lastMonitor;
    }
  }

  async queryToolSessions() {
    if (!this.db) return [];
    const result = await this.db.execute(
      `
        SELECT inst_id, sid, serial#
        FROM gv$session
        WHERE module = :moduleName
          AND type = 'USER'
      `,
      { moduleName: MODULE_NAME }
    );

    return result.rows || [];
  }

  async waitForToolSessionsToDrain(timeoutSeconds = 10) {
    const timeoutMs = Math.max(0, Number(timeoutSeconds) || 0) * 1000;
    const startedAt = Date.now();
    let sessions = await this.queryToolSessions();

    while (sessions.length > 0 && Date.now() - startedAt < timeoutMs) {
      await sleep(1000);
      sessions = await this.queryToolSessions();
    }

    return {
      remaining: sessions.length,
      waitedSeconds: Math.round((Date.now() - startedAt) / 1000)
    };
  }

  async killToolSessions(sessions = null) {
    const targetSessions = sessions || await this.queryToolSessions();
    let killed = 0;

    for (const row of targetSessions) {
      const sessionId = `${row.SID},${row['SERIAL#']},@${row.INST_ID}`;
      try {
        await this.db.execute(`ALTER SYSTEM KILL SESSION '${sessionId}' IMMEDIATE`);
        killed += 1;
      } catch (err) {
        const message = String(err.message || '');
        if (!message.includes('ORA-00030') && !message.includes('ORA-00031')) {
          this.addLog(`Could not kill session ${sessionId}: ${err.message}`, 'warn');
        }
      }
    }

    return { attempted: targetSessions.length, killed };
  }

  getTargetId(workerIndex, completed) {
    const range = Math.max(1, this.config.hotRowMax - this.config.hotRowMin + 1);
    if (this.config.rowTargetMode === 'random') {
      return this.config.hotRowMin + Math.floor(Math.random() * range);
    }
    return this.config.hotRowMin + ((workerIndex + completed) % range);
  }

  getNextInsertId() {
    this.nextInsertId += 1;
    return this.nextInsertId;
  }

  async executeInsertHotIndex(connection, workerIndex, completed) {
    const nextId = this.getNextInsertId();

    await connection.execute(
      `
        INSERT INTO ${LAB_TABLE} (id, pad, counter, session_bucket, request_id, updated_at)
        VALUES (
          :nextId,
          RPAD('X', 100, 'X'),
          :counterValue,
          :sessionBucket,
          :nextId,
          SYSTIMESTAMP
        )
      `,
      {
        nextId,
        counterValue: completed,
        sessionBucket: workerIndex % 100
      },
      { autoCommit: false }
    );
  }

  async executeUpdateHotBlock(connection, workerIndex, completed) {
    const targetId = this.getTargetId(workerIndex, completed);
    await connection.execute(
      `
        UPDATE ${LAB_TABLE}
        SET counter = counter + 1,
            updated_at = SYSTIMESTAMP
        WHERE id = :targetId
      `,
      { targetId },
      { autoCommit: false }
    );
  }

  async runWorker(pool, action, serviceLabel, workerIndex = 0) {
    let completed = 0;
    let connection;

    try {
      connection = await pool.getConnection();
      await connection.execute(
        `BEGIN DBMS_APPLICATION_INFO.SET_MODULE(:moduleName, :actionName); END;`,
        { moduleName: MODULE_NAME, actionName: action }
      );

      while (this.isRunning && completed < this.config.loopsPerWorker) {
        try {
          if (this.config.workloadShape === 'insert-hot-index') {
            await this.executeInsertHotIndex(connection, workerIndex, completed);
          } else {
            await this.executeUpdateHotBlock(connection, workerIndex, completed);
          }

          completed += 1;
          this.stats.completedLoops += 1;
          if (completed % this.config.commitEvery === 0) {
            await connection.commit();
            this.stats.commits += 1;
          }
        } catch (err) {
          this.stats.errors += 1;
          this.addLog(`${action} on ${serviceLabel} failed: ${err.message}`, 'error');
          try {
            await connection.rollback();
          } catch (rollbackErr) {
            // Ignore rollback failures while stopping.
          }
          await sleep(250);
        }
      }

      if (completed % this.config.commitEvery !== 0) {
        await connection.commit();
        this.stats.commits += 1;
      }
    } catch (err) {
      this.stats.errors += 1;
      this.addLog(`${action} on ${serviceLabel} failed: ${err.message}`, 'error');
    } finally {
      if (connection) {
        try {
          await connection.execute(
            `BEGIN DBMS_APPLICATION_INFO.SET_MODULE(NULL, NULL); END;`
          );
          await connection.close();
        } catch (closeErr) {
          // Ignore close failures while stopping.
        }
      }
    }

    this.addLog(`${action} finished after ${completed} loop(s)`);
  }

  async refreshMonitor() {
    if (!this.db) return null;
    const moduleBind = { moduleName: MODULE_NAME };

    const activeResult = await this.db.execute(
      `
        SELECT
          inst_id,
          sid,
          serial#,
          username,
          event,
          state,
          wait_time_micro / 1000000 AS wait_seconds,
          p1 AS file_no,
          p2 AS block_no,
          p3 AS class_no,
          module,
          action,
          sql_id
        FROM gv$session
        WHERE module = :moduleName
          AND type = 'USER'
        ORDER BY inst_id, sid
      `,
      moduleBind
    );

    const waitResult = await this.db.execute(
      `
        SELECT
          inst_id,
          sid,
          username,
          event,
          state,
          wait_time_micro / 1000000 AS wait_seconds,
          p1 AS file_no,
          p2 AS block_no,
          p3 AS class_no,
          module,
          action,
          sql_id
        FROM gv$session
        WHERE module = :moduleName
          AND event IN (${EVENTS.map((_, i) => `:event${i}`).join(', ')})
        ORDER BY inst_id, event, sid
      `,
      EVENTS.reduce((acc, eventName, i) => ({ ...acc, [`event${i}`]: eventName, moduleName: MODULE_NAME }), {})
    );

    const ashResult = await this.db.execute(
      `
        SELECT
          inst_id,
          event,
          sql_id,
          module,
          action,
          p1 AS file_no,
          p2 AS block_no,
          p3 AS class_no,
          COUNT(*) AS sample_count,
          COUNT(DISTINCT session_id || ',' || session_serial#) AS session_count
        FROM gv$active_session_history
        WHERE sample_time >= SYSTIMESTAMP - NUMTODSINTERVAL(:ashSeconds, 'SECOND')
          AND module = :moduleName
          AND event IN (${EVENTS.map((_, i) => `:event${i}`).join(', ')})
        GROUP BY inst_id, event, sql_id, module, action, p1, p2, p3
        ORDER BY sample_count DESC
        FETCH FIRST 50 ROWS ONLY
      `,
      EVENTS.reduce((acc, eventName, i) => ({ ...acc, [`event${i}`]: eventName, moduleName: MODULE_NAME, ashSeconds: 30 }), {})
    );

    const mapSession = row => ({
      instId: row.INST_ID,
      sid: row.SID,
      serial: row['SERIAL#'],
      username: row.USERNAME,
      event: row.EVENT,
      state: row.STATE,
      waitSeconds: Number(row.WAIT_SECONDS || 0),
      fileNo: row.FILE_NO,
      blockNo: row.BLOCK_NO,
      classNo: row.CLASS_NO,
      pText: this.formatPText(row.FILE_NO, row.BLOCK_NO, row.CLASS_NO),
      module: row.MODULE,
      action: row.ACTION,
      sqlId: row.SQL_ID
    });

    this.lastMonitor = {
      timestamp: Date.now(),
      activeSessions: (activeResult.rows || []).map(mapSession),
      waitRows: (waitResult.rows || []).map(mapSession),
      ashRows: (ashResult.rows || []).map(row => ({
        instId: row.INST_ID,
        event: row.EVENT,
        sqlId: row.SQL_ID,
        module: row.MODULE,
        action: row.ACTION,
        fileNo: row.FILE_NO,
        blockNo: row.BLOCK_NO,
        classNo: row.CLASS_NO,
        pText: this.formatPText(row.FILE_NO, row.BLOCK_NO, row.CLASS_NO),
        sampleCount: Number(row.SAMPLE_COUNT || 0),
        sessionCount: Number(row.SESSION_COUNT || 0)
      }))
    };

    if (this.io) {
      this.io.emit('gc-ar-monitor', this.lastMonitor);
    }
    return this.lastMonitor;
  }

  formatPText(fileNo, blockNo, classNo) {
    if (fileNo === null || fileNo === undefined) return '';
    return `file# ${fileNo}-block# ${blockNo}-class# ${classNo}`;
  }

  async stop(options = {}) {
    this.isRunning = false;
    if (this.monitorInterval) {
      clearInterval(this.monitorInterval);
      this.monitorInterval = null;
    }
    if (this.statsInterval) {
      clearInterval(this.statsInterval);
      this.statsInterval = null;
    }

    const workers = [...this.workers];
    const drainMs = clamp(options.drainSeconds, 0, 120, 10) * 1000;
    if (workers.length > 0) {
      await Promise.race([
        Promise.allSettled(workers),
        sleep(drainMs)
      ]);
    }

    for (const pool of this.pools) {
      try {
        await pool.close(0);
      } catch (err) {
        // Ignore pool close failures during stop.
      }
    }
    this.pools = [];
    this.workers = [];

    const killResult = options.kill === false ? null : await this.killToolSessions();
    this.addLog(`Workload stopped${killResult ? `; killed ${killResult.killed}/${killResult.attempted} remaining session(s)` : ''}`);
    this.emitStatus();
    return { ...this.stats, killResult };
  }

  async cleanup(db, io = null) {
    this.db = db;
    this.io = io;
    if (this.isRunning) {
      await this.stop({ kill: true, drainSeconds: 5 });
    }
    try {
      await db.execute(`DROP TABLE ${LAB_TABLE} PURGE`);
      this.addLog(`Dropped ${LAB_TABLE}`);
      return { dropped: true };
    } catch (err) {
      if (String(err.message).includes('ORA-00942')) {
        return { dropped: false, message: `${LAB_TABLE} did not exist` };
      }
      throw err;
    } finally {
      this.emitStatus();
    }
  }

  getStatus(extra = {}) {
    return {
      isSettingUp: this.isSettingUp,
      isRunning: this.isRunning,
      mode: this.mode,
      config: this.config,
      workerCount: this.workers.length,
      stats: {
        ...this.stats,
        uptimeSeconds: this.stats.startedAt ? Math.floor((Date.now() - this.stats.startedAt) / 1000) : 0
      },
      logs: this.logs,
      monitor: this.lastMonitor,
      ...extra
    };
  }
}

module.exports = new GcAcquireReleaseEngine();
