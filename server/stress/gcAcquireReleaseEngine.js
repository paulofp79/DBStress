const oracledb = require('oracledb');

const LAB_TABLE = 'GC_AR_ROWS';
const CUSTOMER_TABLE = '"file_@443"';
const NOTE_REPRO_TABLE = 'T_TEST';
const MODULE_NAME = 'DBSTRESS_GC_AR';
const EVENTS = [
  'gc buffer busy acquire',
  'gc buffer busy release',
  'gc current request',
  'log file sync',
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

const sanitizeTablespace = (value = '') => String(value || '')
  .trim()
  .toUpperCase()
  .replace(/[^A-Z0-9_$#]/g, '')
  .slice(0, 128);

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
    const workloadShape = ['update-hot-block', 'manual-lgnn-hang'].includes(incoming.workloadShape)
      ? incoming.workloadShape
      : 'insert-hot-index';
    const objectProfile = incoming.objectProfile === 'customer-file'
      ? 'customer-file'
      : 'standard';
    const base = {
      mode,
      loopsPerWorker: clamp(incoming.loopsPerWorker, 1, 1000000, 20000),
      commitEvery: clamp(incoming.commitEvery, 1, 10000, 1),
      rowCount,
      hotRowMin,
      hotRowMax,
      rowTargetMode: incoming.rowTargetMode === 'random' ? 'random' : 'spread',
      workloadShape,
      objectProfile,
      tablespaceName: sanitizeTablespace(incoming.tablespaceName),
      remotePrimerEnabled: incoming.remotePrimerEnabled === true,
      remotePrimerConnectionString: String(incoming.remotePrimerConnectionString || '').trim(),
      remotePrimerSessions: clamp(incoming.remotePrimerSessions, 0, 100, 4),
      remotePrimerThinkMs: clamp(incoming.remotePrimerThinkMs, 0, 5000, 10),
      manualStepDelayMs: clamp(incoming.manualStepDelayMs, 500, 30000, 3000),
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
    const isNoteRepro = options.workloadShape === 'manual-lgnn-hang';
    const objectProfile = isNoteRepro
      ? 'note-repro'
      : options.objectProfile === 'customer-file' ? 'customer-file' : 'standard';

    try {
      this.addLog(`Setting up ${this.getTableNameForProfile(objectProfile)} with ${isNoteRepro ? 300 : rowCount} seed rows`);

      if (objectProfile === 'note-repro') {
        await this.setupNoteReproTable(db);
      } else if (objectProfile === 'customer-file') {
        await this.setupCustomerFileTable(db, rowCount, options);
      } else {
        await this.setupStandardTable(db, rowCount);
      }

      const distribution = await this.getBlockDistribution(db, objectProfile);
      this.addLog(`Setup complete; found ${distribution.length} file/block group(s)`);
      return {
        tableName: this.getTableNameForProfile(objectProfile),
        objectProfile,
        rowCount: isNoteRepro ? 300 : rowCount,
        distribution
      };
    } finally {
      this.isSettingUp = false;
      this.emitStatus();
    }
  }

  async setupStandardTable(db, rowCount) {
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
  }

  async setupCustomerFileTable(db, rowCount, options = {}) {
    const tablespace = sanitizeTablespace(options.tablespaceName);
    const tableTablespace = tablespace ? ` TABLESPACE "${tablespace}"` : '';
    const lobTablespace = tablespace ? ` TABLESPACE "${tablespace}"` : '';

    try {
      await db.execute(`DROP TABLE ${CUSTOMER_TABLE} PURGE`);
    } catch (err) {
      if (!String(err.message).includes('ORA-00942')) throw err;
    }

    await db.execute(`
      CREATE TABLE ${CUSTOMER_TABLE}
      (
        "Key" RAW(52),
        "Record" BLOB,
        "RecordLength" NUMBER(10,0),
        "key1" RAW(260) GENERATED ALWAYS AS
          (CAST("SYS"."DBMS_LOB"."SUBSTR"("Record",260,53) AS RAW(260))) VIRTUAL
      )
      SEGMENT CREATION IMMEDIATE
      PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255
      NOCOMPRESS LOGGING${tableTablespace}
      STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
        PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
        BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
      LOB ("Record") STORE AS SECUREFILE (
        ${lobTablespace} ENABLE STORAGE IN ROW CHUNK 8192
        NOCACHE LOGGING NOCOMPRESS KEEP_DUPLICATES
        STORAGE(INITIAL 106496 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
          PCTINCREASE 0
          BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
      )
    `);

    await db.execute(`
      CREATE UNIQUE INDEX "SYS_C0010790" ON ${CUSTOMER_TABLE} ("Key")
      PCTFREE 10 INITRANS 2 MAXTRANS 255${tableTablespace}
      STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
        PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
        BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
    `);

    await db.execute(`
      CREATE INDEX "file_@443_key1_Key_IDX" ON ${CUSTOMER_TABLE} ("key1", "Key")
      PCTFREE 10 INITRANS 2 MAXTRANS 255${tableTablespace}
      STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
        PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
        BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
    `);

    await db.execute(`
      CREATE INDEX "file_@443_key1_IDX" ON ${CUSTOMER_TABLE} ("key1")
      PCTFREE 10 INITRANS 2 MAXTRANS 255${tableTablespace}
      STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
        PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
        BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
    `);

    await db.execute(`
      ALTER TABLE ${CUSTOMER_TABLE}
      ADD PRIMARY KEY ("Key")
      USING INDEX "SYS_C0010790"
      ENABLE
    `);

    const rows = [];
    for (let i = 1; i <= rowCount; i++) {
      rows.push(this.buildCustomerBind(i, i % 100, 512));
    }

    await db.executeMany(
      `
        INSERT INTO ${CUSTOMER_TABLE} ("Key", "Record", "RecordLength")
        VALUES (:keyValue, :recordValue, :recordLength)
      `,
      rows,
      {
        bindDefs: {
          keyValue: { type: oracledb.BUFFER, maxSize: 52 },
          recordValue: { type: oracledb.BUFFER, maxSize: 512 },
          recordLength: { type: oracledb.NUMBER }
        }
      }
    );

    await db.execute(`BEGIN DBMS_STATS.GATHER_TABLE_STATS(USER, 'file_@443'); END;`);
  }

  async setupNoteReproTable(db) {
    try {
      await db.execute(`DROP TABLE ${NOTE_REPRO_TABLE} PURGE`);
    } catch (err) {
      if (!String(err.message).includes('ORA-00942')) throw err;
    }

    await db.execute(`CREATE TABLE ${NOTE_REPRO_TABLE} (n1 NUMBER, v1 VARCHAR2(100))`);
    await db.execute(`
      INSERT INTO ${NOTE_REPRO_TABLE}
      SELECT n1, LPAD(n1, 100, 'AAAAAAA')
      FROM (SELECT LEVEL n1 FROM dual CONNECT BY LEVEL <= 300)
    `);
    await db.execute(`CREATE INDEX t_test_n1 ON ${NOTE_REPRO_TABLE} (n1)`);
    await db.execute(`BEGIN DBMS_STATS.GATHER_TABLE_STATS(USER, '${NOTE_REPRO_TABLE}', CASCADE => TRUE); END;`);
    await db.commit();
  }

  getTableNameForProfile(objectProfile) {
    if (objectProfile === 'customer-file') return CUSTOMER_TABLE;
    if (objectProfile === 'note-repro') return NOTE_REPRO_TABLE;
    return LAB_TABLE;
  }

  async getBlockDistribution(dbRef = null, objectProfile = this.config?.objectProfile || 'standard') {
    const db = dbRef || this.db;
    if (objectProfile === 'note-repro') {
      const result = await db.execute(`
        SELECT
          n1,
          DBMS_ROWID.ROWID_RELATIVE_FNO(rowid) AS file_no,
          DBMS_ROWID.ROWID_BLOCK_NUMBER(rowid) AS block_no,
          DBMS_ROWID.ROWID_OBJECT(rowid) AS object_no,
          LENGTH(v1) AS v1_length
        FROM ${NOTE_REPRO_TABLE}
        WHERE n1 IN (96, 97, 98, 99, 100)
        ORDER BY n1
      `);

      return (result.rows || []).map(row => ({
        fileNo: row.FILE_NO,
        blockNo: row.BLOCK_NO,
        objectNo: row.OBJECT_NO,
        rowsInBlock: 1,
        minId: row.N1,
        maxId: row.N1,
        n1: row.N1,
        v1Length: row.V1_LENGTH,
        resourceName: `[0x${Number(row.BLOCK_NO).toString(16)}][0x${Number(row.FILE_NO).toString(16)}],[BL]`
      }));
    }

    if (objectProfile === 'customer-file') {
      const result = await db.execute(`
        SELECT
          DBMS_ROWID.ROWID_RELATIVE_FNO(rowid) AS file_no,
          DBMS_ROWID.ROWID_BLOCK_NUMBER(rowid) AS block_no,
          COUNT(*) AS rows_in_block
        FROM ${CUSTOMER_TABLE}
        GROUP BY
          DBMS_ROWID.ROWID_RELATIVE_FNO(rowid),
          DBMS_ROWID.ROWID_BLOCK_NUMBER(rowid)
        ORDER BY rows_in_block DESC, file_no, block_no
      `);

      return (result.rows || []).map(row => ({
        fileNo: row.FILE_NO,
        blockNo: row.BLOCK_NO,
        rowsInBlock: row.ROWS_IN_BLOCK,
        minId: null,
        maxId: null
      }));
    }

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
    if (totalWorkers < 1 && config.workloadShape !== 'manual-lgnn-hang') {
      throw new Error('At least one worker is required');
    }

    const targetTable = config.workloadShape === 'manual-lgnn-hang'
      ? NOTE_REPRO_TABLE
      : this.getTableNameForProfile(config.objectProfile);
    await db.execute(`SELECT COUNT(*) AS CNT FROM ${targetTable}`);

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
      const runLabel = config.workloadShape === 'manual-lgnn-hang'
        ? '4 staged note repro session(s)'
        : `${totalWorkers} worker(s)`;
      this.addLog(`Starting ${config.mode} ${config.workloadShape} acquire/release workload with ${runLabel}`);

      if (config.workloadShape === 'manual-lgnn-hang') {
        await this.startManualLgnnRepro(config);
      } else if (config.mode === 'two-instance') {
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

        if (config.remotePrimerEnabled && config.remotePrimerConnectionString && config.remotePrimerSessions > 0) {
          const primerPool = await this.createWorkerPool(config.remotePrimerSessions, config.remotePrimerConnectionString);
          for (let i = 0; i < config.remotePrimerSessions; i++) {
            this.workers.push(this.runRemotePrimer(primerPool, `remote-primer-${i + 1}`, i));
          }
          this.addLog(`Started ${config.remotePrimerSessions} remote GC primer session(s)`);
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

  buildCustomerBind(id, bucket = 0, recordLength = 512) {
    const keyValue = Buffer.alloc(52);
    keyValue.writeBigUInt64BE(BigInt(id), 44);

    const recordValue = Buffer.alloc(recordLength, 0);
    const key1 = Buffer.alloc(260, bucket % 256);
    key1.copy(recordValue, 52);
    recordValue.writeUInt32BE(id >>> 0, 0);

    return {
      keyValue,
      recordValue,
      recordLength
    };
  }

  async startManualLgnnRepro(config) {
    if (!config.instance1ConnectionString || !config.instance2ConnectionString) {
      throw new Error('Manual LGNN hang repro requires instance 1/master and instance 2/non-master connection strings.');
    }

    const masterPool = await this.createWorkerPool(3, config.instance1ConnectionString);
    const nonMasterPool = await this.createWorkerPool(2, config.instance2ConnectionString);
    const delay = config.manualStepDelayMs;

    this.addLog('Manual repro assumes instance-1 is the master/owner node and LGNN is already stopped there', 'warn');
    this.workers.push(this.runManualNoteSession(masterPool, 'note-holder-log-file-sync-n100', 100, true));

    this.workers.push((async () => {
      await sleep(delay);
      return this.runManualNoteSession(nonMasterPool, 'note-nonmaster-gc-current-n99', 99, false);
    })());

    this.workers.push((async () => {
      await sleep(delay * 2);
      return this.runManualNoteSession(masterPool, 'note-master-gc-buffer-busy-acquire-n98', 98, false);
    })());

    this.workers.push((async () => {
      await sleep(delay * 3);
      return this.runManualNoteSession(nonMasterPool, 'note-nonmaster-gc-buffer-busy-release-n97', 97, false);
    })());
  }

  async runManualNoteSession(pool, action, n1, commitAfterUpdate) {
    let connection;

    try {
      connection = await pool.getConnection();
      await connection.execute(
        `BEGIN DBMS_APPLICATION_INFO.SET_MODULE(:moduleName, :actionName); END;`,
        { moduleName: MODULE_NAME, actionName: action }
      );
      this.addLog(`${action}: updating T_TEST n1=${n1}`);

      await connection.execute(
        `UPDATE ${NOTE_REPRO_TABLE} SET v1 = :value WHERE n1 = :n1`,
        { value: `test_n1_${n1}`, n1 },
        { autoCommit: false }
      );
      this.stats.completedLoops += 1;

      if (commitAfterUpdate) {
        this.addLog(`${action}: update complete; committing. If LGNN is stopped this session should wait on log file sync.`);
        await connection.commit();
        this.stats.commits += 1;
      } else {
        this.addLog(`${action}: update returned; holding transaction open for monitor visibility.`);
        while (this.isRunning) {
          await sleep(1000);
        }
      }
    } catch (err) {
      this.stats.errors += 1;
      this.addLog(`${action} failed: ${err.message}`, 'error');
    } finally {
      if (connection) {
        try {
          await connection.rollback();
          await connection.execute(`BEGIN DBMS_APPLICATION_INFO.SET_MODULE(NULL, NULL); END;`);
          await connection.close();
        } catch (closeErr) {
          // Stop may kill hung sessions before the client can close them cleanly.
        }
      }
    }
  }

  async executeInsertHotIndex(connection, workerIndex, completed) {
    const nextId = this.getNextInsertId();
    if (this.config.objectProfile === 'customer-file') {
      const bind = this.buildCustomerBind(nextId, workerIndex % 8, 512);
      await connection.execute(
        `
          INSERT INTO ${CUSTOMER_TABLE} ("Key", "Record", "RecordLength")
          VALUES (:keyValue, :recordValue, :recordLength)
        `,
        bind,
        { autoCommit: false }
      );
      return;
    }

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
    if (this.config.objectProfile === 'customer-file') {
      const targetId = this.getTargetId(workerIndex, completed);
      const bind = this.buildCustomerBind(targetId, workerIndex % 8, 512);
      await connection.execute(
        `
          UPDATE ${CUSTOMER_TABLE}
          SET "Record" = :recordValue,
              "RecordLength" = :recordLength
          WHERE "Key" = :keyValue
        `,
        bind,
        { autoCommit: false }
      );
      return;
    }

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

  async runRemotePrimer(pool, action, workerIndex = 0) {
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
          await connection.commit();
          completed += 1;

          if (this.config.remotePrimerThinkMs > 0) {
            await sleep(this.config.remotePrimerThinkMs);
          }
        } catch (err) {
          this.stats.errors += 1;
          this.addLog(`${action} failed: ${err.message}`, 'warn');
          try {
            await connection.rollback();
          } catch (rollbackErr) {
            // Ignore rollback failures while stopping.
          }
          await sleep(250);
        }
      }
    } catch (err) {
      this.stats.errors += 1;
      this.addLog(`${action} failed: ${err.message}`, 'warn');
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

    this.addLog(`${action} finished after ${completed} primer loop(s)`);
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
    const tables = [NOTE_REPRO_TABLE, CUSTOMER_TABLE, LAB_TABLE];
    let dropped = false;
    try {
      for (const tableName of tables) {
        try {
          await db.execute(`DROP TABLE ${tableName} PURGE`);
          this.addLog(`Dropped ${tableName}`);
          dropped = true;
        } catch (err) {
          if (!String(err.message).includes('ORA-00942')) throw err;
        }
      }
      return { dropped };
    } catch (err) {
      throw err;
    } finally {
      if (!dropped) {
        this.addLog(`${NOTE_REPRO_TABLE}, ${CUSTOMER_TABLE}, and ${LAB_TABLE} were not present`);
      }
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
