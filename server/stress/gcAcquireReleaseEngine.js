const oracledb = require('oracledb');

const LAB_TABLE = 'GC_AR_ROWS';
const CUSTOMER_TABLE = '"file_@443"';
const NOTE_REPRO_TABLE = 'T_TEST';
const MODULE_NAME = 'DBSTRESS_GC_AR';
const FILE443_WORKLOAD = 'file443-paced-insert';
const FILE443_MODULE_NAME = 'DBSTRESS_FILE443_INSERT';
const FILE443_DEFAULT_TABLE = 'file_@443';
const FILE443_DEFAULT_REVERSE_TABLE = 'file_@443_RK';
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

const sanitizeObjectStem = (value = 'OBJ') => {
  const sanitized = String(value || 'OBJ')
    .trim()
    .toUpperCase()
    .replace(/[^A-Z0-9_$#]/g, '');
  const withLeadingLetter = /^[A-Z]/.test(sanitized) ? sanitized : `T${sanitized}`;
  return (withLeadingLetter || 'OBJ').slice(0, 18);
};

const quoteIdentifier = (value) => {
  const trimmed = String(value || '').trim();
  if (!trimmed) throw new Error('Oracle object name is required');
  return `"${trimmed.replace(/"/g, '""')}"`;
};

const clampNumber = (value, min, max, fallback) => {
  const num = Number(value);
  if (!Number.isFinite(num)) return fallback;
  return Math.min(max, Math.max(min, num));
};

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
    this.file443RunId = null;
    this.file443NextSlotAt = 0;
    this.file443StopTimer = null;
    this.file443WaitBaseline = new Map();
    this.lastFile443Runs = {
      normal: null,
      reverse: null
    };
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

  getConnectedUsername() {
    return String(this.db?.getStatus?.()?.config?.user || '').trim().toUpperCase();
  }

  normalizeFile443Config(incoming = {}) {
    const targetOwner = String(incoming.file443TargetOwner || incoming.targetOwner || '').trim();
    const targetTable = String(incoming.file443TargetTable || incoming.targetTable || FILE443_DEFAULT_TABLE).trim();
    const reverseTable = String(incoming.file443ReverseTable || incoming.reverseTable || FILE443_DEFAULT_REVERSE_TABLE).trim();
    const variant = incoming.file443Variant === 'reverse-key' ? 'reverse-key' : 'normal';
    const targetInsertsPerSec = clampNumber(incoming.file443TargetInsertsPerSec, 1, 100000, 50);
    const durationSeconds = clamp(incoming.file443DurationSeconds, 0, 86400, 0);
    const workers = clamp(incoming.file443Workers ?? incoming.workers, 1, 1000, 8);

    return {
      mode: 'one-instance',
      loopsPerWorker: 1000000,
      commitEvery: 1,
      rowCount: 0,
      hotRowMin: 1,
      hotRowMax: 1,
      rowTargetMode: 'spread',
      workloadShape: FILE443_WORKLOAD,
      objectProfile: 'customer-file',
      tablespaceName: sanitizeTablespace(incoming.tablespaceName),
      remotePrimerEnabled: false,
      remotePrimerConnectionString: '',
      remotePrimerSessions: 0,
      remotePrimerThinkMs: 0,
      manualStepDelayMs: 3000,
      monitorRefreshMs: clamp(incoming.monitorRefreshMs, 1000, 30000, 2000),
      killExistingSessions: incoming.killExistingSessions !== false,
      instance2ConnectionString: String(incoming.file443ServiceConnectionString || incoming.instance2ConnectionString || '').trim(),
      workers,
      file443TargetOwner: targetOwner,
      file443TargetTable: targetTable || FILE443_DEFAULT_TABLE,
      file443ReverseTable: reverseTable || FILE443_DEFAULT_REVERSE_TABLE,
      file443Variant: variant,
      file443Workers: workers,
      file443TargetInsertsPerSec: targetInsertsPerSec,
      file443DurationSeconds: durationSeconds,
      file443ServiceConnectionString: String(incoming.file443ServiceConnectionString || incoming.instance2ConnectionString || '').trim()
    };
  }

  normalizeWorkloadConfig(incoming = {}) {
    if (incoming.workloadShape === FILE443_WORKLOAD) {
      return this.normalizeFile443Config(incoming);
    }

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
    if (options.workloadShape === FILE443_WORKLOAD) {
      try {
        return await this.setupFile443PacedInsert(db, options);
      } finally {
        this.isSettingUp = false;
        this.emitStatus();
      }
    }

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

  quoteQualifiedName(owner, name) {
    return owner
      ? `${quoteIdentifier(owner)}.${quoteIdentifier(name)}`
      : quoteIdentifier(name);
  }

  resolveFile443Config(config = {}) {
    const owner = String(config.file443TargetOwner || this.getConnectedUsername()).trim();
    if (!owner) {
      throw new Error('Target owner is required for file_@443 paced inserts');
    }

    return {
      ...config,
      file443TargetOwner: owner,
      file443TargetTable: String(config.file443TargetTable || FILE443_DEFAULT_TABLE).trim(),
      file443ReverseTable: String(config.file443ReverseTable || FILE443_DEFAULT_REVERSE_TABLE).trim()
    };
  }

  getFile443RunTableName(config = this.config) {
    if (config?.file443Variant === 'reverse-key') {
      return config.file443ReverseTable || FILE443_DEFAULT_REVERSE_TABLE;
    }
    return config?.file443TargetTable || FILE443_DEFAULT_TABLE;
  }

  getFile443RunTableRef(config = this.config) {
    const resolved = this.resolveFile443Config(config);
    return this.quoteQualifiedName(resolved.file443TargetOwner, this.getFile443RunTableName(resolved));
  }

  getFile443TargetTableRef(config = this.config) {
    const resolved = this.resolveFile443Config(config);
    return this.quoteQualifiedName(resolved.file443TargetOwner, resolved.file443TargetTable);
  }

  getFile443ReverseTableRef(config = this.config) {
    const resolved = this.resolveFile443Config(config);
    return this.quoteQualifiedName(resolved.file443TargetOwner, resolved.file443ReverseTable);
  }

  buildFile443ObjectName(tableName, suffix) {
    const stem = sanitizeObjectStem(tableName);
    return `${stem}_${suffix}`.slice(0, 30);
  }

  async validateFile443Table(db, config = this.config) {
    const tableRef = this.getFile443RunTableRef(config);
    await db.execute(`SELECT "Key", "Record", "RecordLength", "key1" FROM ${tableRef} WHERE 1 = 0`);
    return {
      tableName: this.getFile443RunTableName(config),
      tableRef,
      owner: this.resolveFile443Config(config).file443TargetOwner
    };
  }

  async setupFile443PacedInsert(db, options = {}) {
    const config = this.resolveFile443Config(this.normalizeFile443Config(options));
    const targetRef = this.getFile443TargetTableRef(config);
    this.addLog(`Validating existing file_@443 target ${targetRef}`);
    await db.execute(`SELECT "Key", "Record", "RecordLength", "key1" FROM ${targetRef} WHERE 1 = 0`);

    let createdReverseClone = false;
    if (config.file443Variant === 'reverse-key') {
      await this.setupFile443ReverseClone(db, config);
      createdReverseClone = true;
    }

    const validated = await this.validateFile443Table(db, config);
    this.addLog(`${createdReverseClone ? 'Reverse-key clone created' : 'Existing target validated'} for file_@443 paced insert test`);
    return {
      tableName: validated.tableName,
      owner: validated.owner,
      tableRef: validated.tableRef,
      objectProfile: FILE443_WORKLOAD,
      workloadShape: FILE443_WORKLOAD,
      variant: config.file443Variant,
      reverseCloneCreated: createdReverseClone,
      distribution: []
    };
  }

  async setupFile443ReverseClone(db, config = this.config) {
    const resolved = this.resolveFile443Config(config);
    const cloneRef = this.getFile443ReverseTableRef(resolved);
    const pkIndexName = this.buildFile443ObjectName(resolved.file443ReverseTable, 'PK_RK');
    const key1KeyIndexName = this.buildFile443ObjectName(resolved.file443ReverseTable, 'K1K_RK');
    const key1IndexName = this.buildFile443ObjectName(resolved.file443ReverseTable, 'K1_RK');
    const pkConstraintName = this.buildFile443ObjectName(resolved.file443ReverseTable, 'PK');
    const pkIndexRef = this.quoteQualifiedName(resolved.file443TargetOwner, pkIndexName);
    const key1KeyIndexRef = this.quoteQualifiedName(resolved.file443TargetOwner, key1KeyIndexName);
    const key1IndexRef = this.quoteQualifiedName(resolved.file443TargetOwner, key1IndexName);

    try {
      await db.execute(`DROP TABLE ${cloneRef} PURGE`);
      this.addLog(`Dropped existing reverse-key clone ${cloneRef}`);
    } catch (err) {
      if (!String(err.message).includes('ORA-00942')) throw err;
    }

    await db.execute(`
      CREATE TABLE ${cloneRef}
      (
        "Key" RAW(52),
        "Record" BLOB,
        "RecordLength" NUMBER(10,0),
        "key1" RAW(260) GENERATED ALWAYS AS
          (CAST("SYS"."DBMS_LOB"."SUBSTR"("Record",260,53) AS RAW(260))) VIRTUAL
      )
      SEGMENT CREATION IMMEDIATE
      PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255
      NOCOMPRESS LOGGING
      STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
        PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
        BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
      LOB ("Record") STORE AS SECUREFILE (
        ENABLE STORAGE IN ROW CHUNK 8192
        NOCACHE LOGGING NOCOMPRESS KEEP_DUPLICATES
        STORAGE(INITIAL 106496 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
          PCTINCREASE 0
          BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
      )
    `);

    await db.execute(`
      CREATE UNIQUE INDEX ${pkIndexRef} ON ${cloneRef} ("Key")
      PCTFREE 10 INITRANS 2 MAXTRANS 255
      STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
        PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
        BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
      REVERSE
    `);

    await db.execute(`
      CREATE INDEX ${key1KeyIndexRef} ON ${cloneRef} ("key1", "Key")
      PCTFREE 10 INITRANS 2 MAXTRANS 255
      STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
        PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
        BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
      REVERSE
    `);

    await db.execute(`
      CREATE INDEX ${key1IndexRef} ON ${cloneRef} ("key1")
      PCTFREE 10 INITRANS 2 MAXTRANS 255
      STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
        PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
        BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
      REVERSE
    `);

    await db.execute(`
      ALTER TABLE ${cloneRef}
      ADD CONSTRAINT ${quoteIdentifier(pkConstraintName)}
      PRIMARY KEY ("Key")
      USING INDEX ${pkIndexRef}
      ENABLE
    `);

    await db.execute(`BEGIN DBMS_STATS.GATHER_TABLE_STATS(:ownerName, :tableName, CASCADE => TRUE); END;`, {
      ownerName: resolved.file443TargetOwner,
      tableName: resolved.file443ReverseTable
    });
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

  getModuleNameForWorkload(workloadShape = this.config?.workloadShape) {
    return workloadShape === FILE443_WORKLOAD ? FILE443_MODULE_NAME : MODULE_NAME;
  }

  async validateFile443Service(config = this.config) {
    const connectionString = String(config?.file443ServiceConnectionString || config?.instance2ConnectionString || '').trim();
    if (!connectionString) {
      throw new Error('Service connect string is required for file_@443 paced inserts');
    }

    let connection;
    try {
      connection = await this.db.createDirectConnection({ connectionString });
      await connection.execute(`SELECT 1 FROM dual`);
    } finally {
      if (connection) {
        await connection.close();
      }
    }
  }

  async queryWaitEventTotals() {
    const binds = EVENTS.reduce((acc, eventName, index) => {
      acc[`event${index}`] = eventName;
      return acc;
    }, {});
    const eventList = EVENTS.map((_, index) => `:event${index}`).join(', ');

    try {
      const result = await this.db.execute(
        `
          SELECT
            event,
            SUM(total_waits) AS total_waits,
            SUM(total_timeouts) AS total_timeouts,
            SUM(time_waited_micro) / 1000 AS time_waited_ms
          FROM gv$system_event
          WHERE event IN (${eventList})
          GROUP BY event
        `,
        binds
      );
      return result.rows || [];
    } catch (err) {
      const result = await this.db.execute(
        `
          SELECT
            event,
            total_waits,
            total_timeouts,
            time_waited_micro / 1000 AS time_waited_ms
          FROM v$system_event
          WHERE event IN (${eventList})
        `,
        binds
      );
      return result.rows || [];
    }
  }

  async captureFile443WaitBaseline() {
    const rows = await this.queryWaitEventTotals();
    this.file443WaitBaseline = new Map((rows || []).map(row => [row.EVENT, {
      totalWaits: Number(row.TOTAL_WAITS || 0),
      totalTimeouts: Number(row.TOTAL_TIMEOUTS || 0),
      timeWaitedMs: Number(row.TIME_WAITED_MS || 0)
    }]));
  }

  async buildFile443WaitDeltas() {
    const rows = await this.queryWaitEventTotals();
    return (rows || []).map(row => {
      const baseline = this.file443WaitBaseline.get(row.EVENT) || {
        totalWaits: 0,
        totalTimeouts: 0,
        timeWaitedMs: 0
      };
      const totalWaits = Number(row.TOTAL_WAITS || 0);
      const totalTimeouts = Number(row.TOTAL_TIMEOUTS || 0);
      const timeWaitedMs = Number(row.TIME_WAITED_MS || 0);
      const deltaWaits = Math.max(0, totalWaits - baseline.totalWaits);
      const deltaTimeMs = Math.max(0, timeWaitedMs - baseline.timeWaitedMs);

      return {
        event: row.EVENT,
        totalWaits,
        totalTimeouts,
        timeWaitedMs: Number(timeWaitedMs.toFixed(2)),
        deltaWaits,
        deltaTimeouts: Math.max(0, totalTimeouts - baseline.totalTimeouts),
        deltaTimeMs: Number(deltaTimeMs.toFixed(2)),
        avgWaitMs: deltaWaits > 0 ? Number((deltaTimeMs / deltaWaits).toFixed(3)) : 0
      };
    });
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

    const sessionModuleName = this.getModuleNameForWorkload(config.workloadShape);
    if (config.workloadShape === FILE443_WORKLOAD) {
      Object.assign(config, this.resolveFile443Config(config));
      await this.validateFile443Service(config);
      await this.validateFile443Table(db, config);
    } else {
      const targetTable = config.workloadShape === 'manual-lgnn-hang'
      ? NOTE_REPRO_TABLE
      : this.getTableNameForProfile(config.objectProfile);
      await db.execute(`SELECT COUNT(*) AS CNT FROM ${targetTable}`);
    }

    if (this.isRunning) {
      if (!config.killExistingSessions) {
        throw new Error('Acquire/release workload already running. Stop it first or enable existing-session cleanup.');
      }
      this.addLog('Stopping existing in-memory acquire/release workload before starting a new one', 'warn');
      await this.stop({ kill: true, drainSeconds: 5 });
    }

    const existingSessions = await this.queryToolSessions(sessionModuleName);
    if (existingSessions.length > 0) {
      if (!config.killExistingSessions) {
        throw new Error(`Found ${existingSessions.length} existing ${sessionModuleName} session(s). Stop them first or enable existing-session cleanup.`);
      }
      this.addLog(`Killing ${existingSessions.length} existing ${sessionModuleName} session(s) before starting`, 'warn');
      const killResult = await this.killToolSessions(existingSessions);
      const drainResult = await this.waitForToolSessionsToDrain(10, sessionModuleName);
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
      errors: 0,
      targetInsertsPerSec: config.workloadShape === FILE443_WORKLOAD ? config.file443TargetInsertsPerSec : null,
      achievedInsertsPerSec: 0
    };
    this.nextInsertId = Math.max(
      config.rowCount + 1,
      (Date.now() * 1000) + Math.floor(Math.random() * 1000)
    );
    this.file443RunId = config.workloadShape === FILE443_WORKLOAD
      ? `F443_${Date.now()}_${Math.random().toString(36).slice(2, 7)}`
      : null;
    this.file443NextSlotAt = Date.now();

    try {
      const runLabel = config.workloadShape === 'manual-lgnn-hang'
        ? '4 staged note repro session(s)'
        : `${totalWorkers} worker(s)`;
      this.addLog(`Starting ${config.mode} ${config.workloadShape} acquire/release workload with ${runLabel}`);

      if (config.workloadShape === FILE443_WORKLOAD) {
        await this.startFile443PacedInsert(config);
      } else if (config.workloadShape === 'manual-lgnn-hang') {
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

  async queryToolSessions(moduleName = this.getModuleNameForWorkload()) {
    if (!this.db) return [];
    const result = await this.db.execute(
      `
        SELECT inst_id, sid, serial#
        FROM gv$session
        WHERE module = :moduleName
          AND type = 'USER'
      `,
      { moduleName }
    );

    return result.rows || [];
  }

  async waitForToolSessionsToDrain(timeoutSeconds = 10, moduleName = this.getModuleNameForWorkload()) {
    const timeoutMs = Math.max(0, Number(timeoutSeconds) || 0) * 1000;
    const startedAt = Date.now();
    let sessions = await this.queryToolSessions(moduleName);

    while (sessions.length > 0 && Date.now() - startedAt < timeoutMs) {
      await sleep(1000);
      sessions = await this.queryToolSessions(moduleName);
    }

    return {
      remaining: sessions.length,
      waitedSeconds: Math.round((Date.now() - startedAt) / 1000)
    };
  }

  async killToolSessions(sessions = null, moduleName = this.getModuleNameForWorkload()) {
    const targetSessions = sessions || await this.queryToolSessions(moduleName);
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

  async startFile443PacedInsert(config) {
    await this.captureFile443WaitBaseline();
    const tableRef = this.getFile443RunTableRef(config);
    this.stats.tableName = this.getFile443RunTableName(config);
    this.stats.tableOwner = config.file443TargetOwner;
    this.stats.tableRef = tableRef;
    this.stats.variant = config.file443Variant;
    this.stats.durationSeconds = config.file443DurationSeconds;
    this.stats.targetInsertsPerSec = config.file443TargetInsertsPerSec;
    this.stats.workerSessions = config.file443Workers;
    this.stats.totalInserts = 0;

    const pool = await this.createWorkerPool(config.file443Workers, config.file443ServiceConnectionString);
    for (let i = 0; i < config.file443Workers; i++) {
      this.workers.push(this.runFile443Worker(pool, `file443-${config.file443Variant}-worker-${i + 1}`, i));
    }

    if (config.file443DurationSeconds > 0) {
      this.file443StopTimer = setTimeout(() => {
        this.addLog(`file_@443 paced insert duration reached ${config.file443DurationSeconds}s; stopping workload`);
        this.stop({ kill: true, drainSeconds: 10 }).catch(err => {
          this.addLog(`Duration stop failed: ${err.message}`, 'error');
        });
      }, config.file443DurationSeconds * 1000);
    }

    this.addLog(`file_@443 paced insert target ${config.file443TargetInsertsPerSec}/sec across ${config.file443Workers} session(s) on ${tableRef}`);
  }

  reserveFile443Slot() {
    const now = Date.now();
    const intervalMs = 1000 / Math.max(1, Number(this.config?.file443TargetInsertsPerSec || 50));
    const scheduledAt = Math.max(now, this.file443NextSlotAt || now);
    this.file443NextSlotAt = scheduledAt + intervalMs;
    return scheduledAt;
  }

  isFile443WorkloadActive() {
    if (!this.isRunning || this.config?.workloadShape !== FILE443_WORKLOAD) {
      return false;
    }
    const durationSeconds = Number(this.config.file443DurationSeconds || 0);
    if (durationSeconds <= 0) {
      return true;
    }
    return Date.now() - this.stats.startedAt < durationSeconds * 1000;
  }

  async executeFile443Insert(connection, workerIndex = 0) {
    const nextId = this.getNextInsertId();
    const bind = this.buildCustomerBind(nextId, workerIndex % 32, 512);
    await connection.execute(
      `
        INSERT INTO ${this.getFile443RunTableRef(this.config)} ("Key", "Record", "RecordLength")
        VALUES (:keyValue, :recordValue, :recordLength)
      `,
      bind,
      { autoCommit: false }
    );
  }

  async runFile443Worker(pool, action, workerIndex = 0) {
    let completed = 0;
    let connection;

    try {
      connection = await pool.getConnection();
      await connection.execute(
        `
          BEGIN
            DBMS_APPLICATION_INFO.SET_MODULE(:moduleName, :actionName);
            DBMS_SESSION.SET_IDENTIFIER(:clientId);
          END;
        `,
        {
          moduleName: FILE443_MODULE_NAME,
          actionName: action.slice(0, 32),
          clientId: `${this.file443RunId || 'FILE443'}:W${workerIndex + 1}`.slice(0, 64)
        }
      );

      while (this.isFile443WorkloadActive()) {
        const scheduledAt = this.reserveFile443Slot();
        const waitMs = scheduledAt - Date.now();
        if (waitMs > 0) {
          await sleep(waitMs);
        }
        if (!this.isFile443WorkloadActive()) {
          break;
        }

        try {
          await this.executeFile443Insert(connection, workerIndex);
          await connection.commit();
          completed += 1;
          this.stats.completedLoops += 1;
          this.stats.totalInserts = this.stats.completedLoops;
          this.stats.commits += 1;
        } catch (err) {
          this.stats.errors += 1;
          this.addLog(`${action} insert failed: ${err.message}`, 'error');
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
      this.addLog(`${action} failed: ${err.message}`, 'error');
    } finally {
      if (connection) {
        try {
          await connection.execute(
            `
              BEGIN
                DBMS_APPLICATION_INFO.SET_MODULE(NULL, NULL);
                DBMS_SESSION.CLEAR_IDENTIFIER;
              END;
            `
          );
          await connection.close();
        } catch (closeErr) {
          // Stop may kill sessions before the client can close them cleanly.
        }
      }
    }

    this.addLog(`${action} finished after ${completed} insert(s)`);
  }

  async startManualLgnnRepro(config) {
    if (!config.instance1ConnectionString || !config.instance2ConnectionString) {
      throw new Error('Manual LGNN hang repro requires instance 1/master and instance 2/non-master connection strings.');
    }

    const masterWaiters = clamp(config.workersInstance1, 0, 100, 1);
    const nonMasterWaiters = clamp(config.workersInstance2, 0, 100, 1);
    const targetRows = await this.getNoteReproSameBlockRows(100, 2 + masterWaiters + nonMasterWaiters);
    const holderN1 = 100;
    const currentN1 = targetRows.find(n1 => n1 !== holderN1) || 99;
    const remainingRows = targetRows.filter(n1 => ![holderN1, currentN1].includes(n1));
    const masterRows = remainingRows.slice(0, masterWaiters);
    const nonMasterRows = remainingRows.slice(masterWaiters, masterWaiters + nonMasterWaiters);
    const neededRows = 2 + masterWaiters + nonMasterWaiters;

    if (targetRows.length < neededRows) {
      this.addLog(`Manual repro found ${targetRows.length} same-block T_TEST rows for ${neededRows} requested sessions; reduce worker counts if row-lock waits appear`, 'warn');
    }

    const masterPool = await this.createWorkerPool(Math.max(1 + masterRows.length, 1), config.instance1ConnectionString);
    const nonMasterPool = await this.createWorkerPool(Math.max(1 + nonMasterRows.length, 1), config.instance2ConnectionString);
    const delay = config.manualStepDelayMs;

    this.addLog('Manual repro assumes instance-1 is the master/owner node and LGNN is already stopped there', 'warn');
    this.addLog(`Manual repro starting holder n1=${holderN1}, current-request n1=${currentN1}, master waiters=${masterRows.length}, non-master waiters=${nonMasterRows.length}`);
    this.workers.push(this.runManualNoteSession(masterPool, `note-holder-log-file-sync-n${holderN1}`, holderN1, true));

    this.workers.push((async () => {
      await sleep(delay);
      return this.runManualNoteSession(nonMasterPool, `note-nonmaster-gc-current-n${currentN1}`, currentN1, false);
    })());

    masterRows.forEach((n1, index) => {
      this.workers.push((async () => {
        await sleep(delay * (2 + index));
        return this.runManualNoteSession(masterPool, `note-master-gc-buffer-busy-acquire-n${n1}`, n1, false);
      })());
    });

    nonMasterRows.forEach((n1, index) => {
      this.workers.push((async () => {
        await sleep(delay * (2 + masterRows.length + index));
        return this.runManualNoteSession(nonMasterPool, `note-nonmaster-gc-buffer-busy-release-n${n1}`, n1, false);
      })());
    });
  }

  async getNoteReproSameBlockRows(anchorN1 = 100, limit = 20) {
    const result = await this.db.execute(
      `
        WITH anchor AS (
          SELECT
            DBMS_ROWID.ROWID_RELATIVE_FNO(rowid) AS file_no,
            DBMS_ROWID.ROWID_BLOCK_NUMBER(rowid) AS block_no
          FROM ${NOTE_REPRO_TABLE}
          WHERE n1 = :anchorN1
        )
        SELECT n1
        FROM (
          SELECT t.n1
          FROM ${NOTE_REPRO_TABLE} t
          CROSS JOIN anchor a
          WHERE DBMS_ROWID.ROWID_RELATIVE_FNO(t.rowid) = a.file_no
            AND DBMS_ROWID.ROWID_BLOCK_NUMBER(t.rowid) = a.block_no
          ORDER BY
            CASE t.n1
              WHEN 100 THEN 0
              WHEN 99 THEN 1
              WHEN 98 THEN 2
              WHEN 97 THEN 3
              WHEN 96 THEN 4
              ELSE 5
            END,
            t.n1 DESC
        )
        WHERE ROWNUM <= :rowLimit
      `,
      { anchorN1, rowLimit: limit }
    );

    return (result.rows || []).map(row => row.N1);
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
    const moduleName = this.getModuleNameForWorkload();
    const moduleBind = { moduleName };
    const eventBinds = EVENTS.reduce((acc, eventName, i) => ({ ...acc, [`event${i}`]: eventName, moduleName }), {});

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
          AND event IN (${EVENTS.map((_, i) => `:event${i}`).join(', ')})
        ORDER BY inst_id, event, sid
      `,
      eventBinds
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
      { ...eventBinds, ashSeconds: 30 }
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

    const activeSessions = (activeResult.rows || []).map(mapSession);
    const waitRows = (waitResult.rows || []).map(mapSession);
    let file443 = null;
    if (this.config?.workloadShape === FILE443_WORKLOAD) {
      const elapsedSeconds = this.stats.startedAt ? Math.max(0, (Date.now() - this.stats.startedAt) / 1000) : 0;
      const achievedInsertsPerSec = elapsedSeconds > 0
        ? Number((Number(this.stats.completedLoops || 0) / elapsedSeconds).toFixed(2))
        : 0;
      const targetWaitRows = activeSessions.filter(row => row.event === 'gc buffer busy acquire');
      const activeSessionCount = activeSessions.length;
      const targetWaitPercent = activeSessionCount > 0
        ? Number(((targetWaitRows.length / activeSessionCount) * 100).toFixed(2))
        : 0;

      this.stats.achievedInsertsPerSec = achievedInsertsPerSec;
      file443 = {
        moduleName: FILE443_MODULE_NAME,
        runId: this.file443RunId,
        variant: this.config.file443Variant,
        tableOwner: this.config.file443TargetOwner,
        tableName: this.getFile443RunTableName(this.config),
        targetInsertsPerSec: this.config.file443TargetInsertsPerSec,
        achievedInsertsPerSec,
        activeSessionCount,
        targetWaitEvent: 'gc buffer busy acquire',
        targetWaitSessionCount: targetWaitRows.length,
        targetWaitPercent,
        waitDeltas: await this.buildFile443WaitDeltas(),
        lastRuns: this.lastFile443Runs
      };
    }

    this.lastMonitor = {
      timestamp: Date.now(),
      activeSessions,
      waitRows,
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
      })),
      file443
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

  async buildFile443RunSummary(killResult = null) {
    if (this.config?.workloadShape !== FILE443_WORKLOAD) {
      return null;
    }

    let waitDeltas = this.lastMonitor?.file443?.waitDeltas || [];
    if (waitDeltas.length === 0 && this.db) {
      try {
        waitDeltas = await this.buildFile443WaitDeltas();
      } catch (err) {
        waitDeltas = [];
      }
    }

    const elapsedSeconds = this.stats.startedAt
      ? Math.max(0, (Date.now() - this.stats.startedAt) / 1000)
      : 0;
    const achievedInsertsPerSec = elapsedSeconds > 0
      ? Number((Number(this.stats.completedLoops || 0) / elapsedSeconds).toFixed(2))
      : 0;
    const gcBufferBusyAcquire = waitDeltas.find(row => row.event === 'gc buffer busy acquire') || {
      deltaWaits: 0,
      deltaTimeMs: 0,
      avgWaitMs: 0
    };

    return {
      completedAt: Date.now(),
      variant: this.config.file443Variant,
      tableOwner: this.config.file443TargetOwner,
      tableName: this.getFile443RunTableName(this.config),
      targetInsertsPerSec: this.config.file443TargetInsertsPerSec,
      achievedInsertsPerSec,
      totalInserts: Number(this.stats.completedLoops || 0),
      commits: Number(this.stats.commits || 0),
      errors: Number(this.stats.errors || 0),
      elapsedSeconds: Number(elapsedSeconds.toFixed(1)),
      activeWaitPercent: Number(this.lastMonitor?.file443?.targetWaitPercent || 0),
      gcBufferBusyAcquire,
      killResult
    };
  }

  async stop(options = {}) {
    const moduleName = this.getModuleNameForWorkload();
    this.isRunning = false;
    if (this.file443StopTimer) {
      clearTimeout(this.file443StopTimer);
      this.file443StopTimer = null;
    }
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

    const killResult = options.kill === false ? null : await this.killToolSessions(null, moduleName);
    const file443Summary = await this.buildFile443RunSummary(killResult);
    if (file443Summary) {
      const key = file443Summary.variant === 'reverse-key' ? 'reverse' : 'normal';
      this.lastFile443Runs = {
        ...this.lastFile443Runs,
        [key]: file443Summary
      };
      if (this.lastMonitor?.file443) {
        this.lastMonitor.file443.lastRuns = this.lastFile443Runs;
      }
    }
    this.addLog(`Workload stopped${killResult ? `; killed ${killResult.killed}/${killResult.attempted} remaining session(s)` : ''}`);
    this.emitStatus();
    return { ...this.stats, killResult, file443Summary, lastFile443Runs: this.lastFile443Runs };
  }

  async cleanupFile443Clone(db, options = {}) {
    const config = this.resolveFile443Config(this.normalizeFile443Config({
      ...this.config,
      ...(options || {}),
      workloadShape: FILE443_WORKLOAD,
      file443Variant: 'reverse-key'
    }));
    const cloneRef = this.getFile443ReverseTableRef(config);
    let dropped = false;

    try {
      await db.execute(`DROP TABLE ${cloneRef} PURGE`);
      this.addLog(`Dropped reverse-key clone ${cloneRef}`);
      dropped = true;
    } catch (err) {
      if (!String(err.message).includes('ORA-00942')) throw err;
      this.addLog(`Reverse-key clone ${cloneRef} was not present`);
    }

    return { dropped, tableName: config.file443ReverseTable, owner: config.file443TargetOwner };
  }

  async cleanup(db, io = null, options = {}) {
    this.db = db;
    this.io = io;
    if (this.isRunning) {
      await this.stop({ kill: true, drainSeconds: 5 });
    }

    if (options.workloadShape === FILE443_WORKLOAD || this.config?.workloadShape === FILE443_WORKLOAD) {
      try {
        return await this.cleanupFile443Clone(db, options);
      } finally {
        this.emitStatus();
      }
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
    const elapsedSeconds = this.stats.startedAt ? Math.max(0, (Date.now() - this.stats.startedAt) / 1000) : 0;
    const achievedInsertsPerSec = this.config?.workloadShape === FILE443_WORKLOAD && elapsedSeconds > 0
      ? Number((Number(this.stats.completedLoops || 0) / elapsedSeconds).toFixed(2))
      : this.stats.achievedInsertsPerSec;

    return {
      isSettingUp: this.isSettingUp,
      isRunning: this.isRunning,
      mode: this.mode,
      config: this.config,
      workerCount: this.workers.length,
      stats: {
        ...this.stats,
        achievedInsertsPerSec,
        uptimeSeconds: this.stats.startedAt ? Math.floor((Date.now() - this.stats.startedAt) / 1000) : 0
      },
      logs: this.logs,
      monitor: this.lastMonitor,
      lastFile443Runs: this.lastFile443Runs,
      ...extra
    };
  }
}

module.exports = new GcAcquireReleaseEngine();
