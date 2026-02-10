const oracledb = require('oracledb');

// Transparent Data Encryption (TDE) Comparison Engine
// Creates identical workloads on encrypted and non-encrypted tables
// and compares performance metrics such as buffer gets, CPU time, and elapsed time.

class TdeComparisonEngine {
  constructor() {
    this.isRunning = false;
    this.db = null;
    this.io = null;
    this.stopRequested = false;

    this.config = {
      schemaPrefix: 'TDE_DEMO',
      tableName: 'TDE_TEST_DATA',
      rowCount: 200000,
      batchSize: 5000,
      gatherStats: true,
      runSelect: true,
      runInsert: true,
      runUpdate: true
    };

    this.results = {
      encrypted: null,
      unencrypted: null,
      comparison: null
    };

    this.currentPhase = '';
    this.progress = 0;
  }

  emitStatus(message, progress) {
    this.currentPhase = message;
    this.progress = progress;

    if (this.stopRequested && progress >= 0) {
      message = `${message} (stop requested)`;
    }

    if (this.io) {
      this.io.emit('tde-comparison-status', {
        message,
        progress,
        results: this.results
      });
    }
  }

  async start(db, config, io) {
    if (this.isRunning) {
      throw new Error('TDE comparison test already running');
    }

    this.isRunning = true;
    this.stopRequested = false;
    this.db = db;
    this.io = io;
    this.config = {
      ...this.config,
      ...config
    };

    this.results = {
      encrypted: null,
      unencrypted: null,
      comparison: null
    };

    try {
      this.emitStatus('Preparing environment...', 5);
      await this.guardedStep('Creating non-encrypted test table...', 15, () => this.createTable(false));

      await this.guardedStep('Populating non-encrypted table...', 25, () => this.populateTable(false, 25, 35));

      const plainMetrics = await this.guardedStep('Running workload on non-encrypted table...', 35, () => this.runWorkload(false, 35, 45));
      this.results.unencrypted = plainMetrics;

      await this.guardedStep('Cleaning up non-encrypted table...', 45, () => this.cleanupTable(false));

      await this.guardedStep('Creating encrypted test table...', 55, () => this.createTable(true));

      await this.guardedStep('Populating encrypted table...', 65, () => this.populateTable(true, 65, 75));

      const encMetrics = await this.guardedStep('Running workload on encrypted table...', 75, () => this.runWorkload(true, 75, 90));
      this.results.encrypted = encMetrics;

      this.emitStatus('Calculating comparison metrics...', 90);
      this.results.comparison = this.calculateComparison();

      if (this.config.cleanup !== false) {
        await this.guardedStep('Cleaning up encrypted table...', 95, () => this.cleanupTable(true));
      }

      this.emitStatus('TDE comparison test complete!', 100);
    } catch (err) {
      if (err.isUserCancelled || this.stopRequested) {
        console.warn('TDE comparison stopped by user');
        this.emitStatus('TDE comparison stopped by user.', -1);
        return this.results;
      }

      console.error('TDE comparison error:', err);
      this.emitStatus(`Error: ${err.message}`, -1);
      throw err;
    } finally {
      this.isRunning = false;
      this.stopRequested = false;
    }

    return this.results;
  }

  async guardedStep(message, progress, stepFn = null) {
    this.ensureRunning();
    this.emitStatus(message, progress);
    if (!stepFn) {
      this.ensureRunning();
      return null;
    }

    const result = await stepFn();
    this.ensureRunning();
    return result;
  }

  ensureRunning() {
    if (this.stopRequested) {
      const err = new Error('TDE comparison stopped');
      err.isUserCancelled = true;
      throw err;
    }
  }

  async createTable(isEncrypted) {
    const suffix = isEncrypted ? 'ENC' : 'PLAIN';
    const tableName = `${this.config.schemaPrefix}_${suffix}`;

    const columnDefs = isEncrypted
      ? `
        ID NUMBER PRIMARY KEY,
        CUSTOMER_ID NUMBER ENCRYPT USING 'AES256',
        ORDER_DATE DATE ENCRYPT USING 'AES256',
        STATUS VARCHAR2(20) ENCRYPT USING 'AES256',
        AMOUNT NUMBER(12,2) ENCRYPT USING 'AES256',
        PADDING VARCHAR2(200)
      `
      : `
        ID NUMBER PRIMARY KEY,
        CUSTOMER_ID NUMBER,
        ORDER_DATE DATE,
        STATUS VARCHAR2(20),
        AMOUNT NUMBER(12,2),
        PADDING VARCHAR2(200)
      `;

    await this.db.execute(`BEGIN EXECUTE IMMEDIATE 'DROP TABLE ${tableName} PURGE'; EXCEPTION WHEN OTHERS THEN NULL; END;`);

    await this.db.execute(`
      CREATE TABLE ${tableName} (
        ${columnDefs}
      )
    `);

    await this.db.execute(`
      BEGIN
        EXECUTE IMMEDIATE 'CREATE INDEX ${tableName}_STATUS_IDX ON ${tableName}(STATUS)';
      EXCEPTION
        WHEN OTHERS THEN NULL;
      END;
    `);
  }

  async populateTable(isEncrypted, progressStart = null, progressEnd = null) {
    const suffix = isEncrypted ? 'ENC' : 'PLAIN';
    const tableName = `${this.config.schemaPrefix}_${suffix}`;
    const batchSize = this.config.batchSize;
    let inserted = 0;
    const totalRows = this.config.rowCount;
    const hasProgressRange = typeof progressStart === 'number' && typeof progressEnd === 'number';

    while (inserted < this.config.rowCount) {
      this.ensureRunning();
      const currentBatch = Math.min(batchSize, this.config.rowCount - inserted);

      const plsql = `
        DECLARE
          TYPE t_num IS TABLE OF NUMBER INDEX BY PLS_INTEGER;
          TYPE t_date IS TABLE OF DATE INDEX BY PLS_INTEGER;
          TYPE t_varchar IS TABLE OF VARCHAR2(200) INDEX BY PLS_INTEGER;
          v_id t_num;
          v_customer t_num;
          v_date t_date;
          v_status t_varchar;
          v_amount t_num;
          v_padding t_varchar;
        BEGIN
          FOR i IN 1..${currentBatch} LOOP
            v_id(i) := ${inserted} + i;
            v_customer(i) := MOD(${inserted} + i, 10000);
            v_date(i) := TRUNC(SYSDATE) - DBMS_RANDOM.VALUE(0, 365);
            v_status(i) := CASE MOD(${inserted} + i, 5)
                             WHEN 0 THEN 'NEW'
                             WHEN 1 THEN 'PENDING'
                             WHEN 2 THEN 'SHIPPED'
                             WHEN 3 THEN 'COMPLETE'
                             ELSE 'CANCELLED'
                           END;
            v_amount(i) := ROUND(DBMS_RANDOM.VALUE(10, 1000), 2);
            v_padding(i) := DBMS_RANDOM.STRING('A', 200);
          END LOOP;

          FORALL i IN 1..${currentBatch}
            INSERT INTO ${tableName} (ID, CUSTOMER_ID, ORDER_DATE, STATUS, AMOUNT, PADDING)
            VALUES (v_id(i), v_customer(i), v_date(i), v_status(i), v_amount(i), v_padding(i));
        END;
      `;

      await this.db.execute(plsql);
      inserted += currentBatch;
      if (hasProgressRange) {
        const fraction = Math.min(inserted / totalRows, 1);
        const progress = progressStart + fraction * (progressEnd - progressStart);
        this.emitStatus(`Populating ${tableName}... (${inserted}/${totalRows})`, Math.round(progress));
      } else if (typeof progressStart === 'number') {
        this.emitStatus(`Populating ${tableName}... (${inserted}/${totalRows})`, progressStart);
      }
    }

    if (this.config.gatherStats !== false) {
      const statsProgress = hasProgressRange ? progressEnd : progressStart;
      if (typeof statsProgress === 'number') {
        this.emitStatus(`Gathering statistics for ${tableName}...`, Math.round(statsProgress));
      }
      await this.db.execute(`
        BEGIN
          DBMS_STATS.GATHER_TABLE_STATS(USER, '${tableName}', cascade => TRUE);
        END;
      `);
    }
  }

  async runWorkload(isEncrypted, progressStart = null, progressEnd = null) {
    const suffix = isEncrypted ? 'ENC' : 'PLAIN';
    const tableName = `${this.config.schemaPrefix}_${suffix}`;

    const moduleName = `TDE_${suffix}`;
    await this.db.execute(`BEGIN DBMS_STATS.FLUSH_DATABASE_MONITORING_INFO; END;`);
    const metrics = {
      suffix,
      rowCount: this.config.rowCount,
      operations: [],
      sessionStats: []
    };

    const connection = await this.db.getConnection();
    const phases = [];
    if (this.config.runSelect) phases.push('SELECT');
    if (this.config.runInsert) phases.push('INSERT');
    if (this.config.runUpdate) phases.push('UPDATE');
    const hasProgressRange = typeof progressStart === 'number' && typeof progressEnd === 'number' && phases.length > 0;
    let completedPhases = 0;
    let sessionSnapshotBefore = null;

    const updateProgress = (label) => {
      if (hasProgressRange) {
        const progress = progressStart + ((completedPhases + 1) / phases.length) * (progressEnd - progressStart);
        this.emitStatus(`Running workload on ${tableName} (${label})`, Math.round(progress));
      } else if (typeof progressStart === 'number') {
        this.emitStatus(`Running workload on ${tableName} (${label})`, progressStart);
      }
      completedPhases += 1;
    };

    try {
      await connection.execute(`BEGIN DBMS_APPLICATION_INFO.SET_MODULE(:m, NULL); END;`, { m: moduleName });
      sessionSnapshotBefore = await this.captureSessionSnapshot(connection);

      if (this.config.runSelect) {
        this.ensureRunning();
        metrics.operations.push(await this.measureSql(connection, `
          SELECT /*+ MONITOR */ COUNT(*)
          FROM ${tableName}
          WHERE STATUS = 'COMPLETE'
        `, 'SELECT COUNT'));
        updateProgress('SELECT');
      }

      if (this.config.runInsert) {
        this.ensureRunning();
        metrics.operations.push(await this.measurePlsql(connection, 'INSERT BATCH', `
          DECLARE
            v_start_id NUMBER;
          BEGIN
            SELECT NVL(MAX(ID), 0)
            INTO v_start_id
            FROM ${tableName};

            FOR i IN 1..1000 LOOP
              INSERT INTO ${tableName} (ID, CUSTOMER_ID, ORDER_DATE, STATUS, AMOUNT, PADDING)
              VALUES (v_start_id + i,
                      MOD(i, 10000),
                      SYSDATE,
                      'NEW',
                      DBMS_RANDOM.VALUE(10,1000),
                      DBMS_RANDOM.STRING('A', 200));
            END LOOP;
            COMMIT;
          END;
        `));
        updateProgress('INSERT');
      }

      if (this.config.runUpdate) {
        this.ensureRunning();
        metrics.operations.push(await this.measureSql(connection, `
          UPDATE /*+ MONITOR */ ${tableName}
          SET STATUS = 'COMPLETE'
          WHERE STATUS = 'PENDING'
          AND ROWNUM <= 1000
        `, 'UPDATE STATUS'));
        await connection.commit();
        updateProgress('UPDATE');
      }

      const sessionSnapshotAfter = await this.captureSessionSnapshot(connection);
      if (sessionSnapshotBefore && sessionSnapshotAfter) {
        metrics.sessionStats = this.computeSessionStats(connection, moduleName, sessionSnapshotBefore, sessionSnapshotAfter);
      }
    } finally {
      await connection.close();
    }

    if (!metrics.sessionStats || metrics.sessionStats.length === 0) {
      metrics.sessionStats = await this.fetchSessionStats(moduleName);
    }

    return metrics;
  }

  async measureSql(connection, sql, label) {
    this.ensureRunning();
    const start = Date.now();
    await connection.execute(sql, [], { autoCommit: false });
    const elapsed = (Date.now() - start) / 1000;

    return {
      label,
      type: 'SQL',
      elapsedSeconds: elapsed
    };
  }

  async measurePlsql(connection, label, plsql) {
    this.ensureRunning();
    const start = Date.now();
    await connection.execute(plsql, [], { autoCommit: false });
    const elapsed = (Date.now() - start) / 1000;

    return {
      label,
      type: 'PLSQL',
      elapsedSeconds: elapsed
    };
  }


  async captureSessionSnapshot(connection) {
    try {
      const result = await connection.execute(`
        SELECT
          n.stat_name,
          s.value
        FROM v$sesstat s
        JOIN v$statname n ON s.statistic# = n.statistic#
        WHERE s.sid = SYS_CONTEXT('USERENV', 'SID')
          AND n.stat_name IN ('CPU used by this session', 'DB time', 'session logical reads', 'physical reads')
      `);
      const stats = {};
      for (const row of result.rows || []) {
        stats[row.STAT_NAME] = row.VALUE;
      }
      return stats;
    } catch (err) {
      console.warn('Unable to capture session snapshot:', err.message);
      return null;
    }
  }

  async computeSessionStats(connection, moduleName, snapshotBefore, snapshotAfter) {
    return this.fetchSessionStats(moduleName);
  }


  async fetchSessionStats(moduleName) {
    const queries = [
      {
        name: 'V$SQL_MONITOR',
        sql: `
          SELECT
            s.sql_id,
            s.plan_hash_value,
            m.module,
            m.action,
            m.cpu_time,
            m.buffer_gets,
            m.disk_reads,
            m.elapsed_time,
            m.executions
          FROM v$sql_monitor m
          JOIN v$sql s ON s.sql_id = m.sql_id
          WHERE m.module = :moduleName
          ORDER BY m.sql_exec_start DESC
        `
      },
      {
        name: 'V$SQLSTATS',
        sql: `
          SELECT
            sql_id,
            plan_hash_value,
            module,
            action,
            cpu_time,
            buffer_gets,
            disk_reads,
            elapsed_time,
            executions
          FROM v$sqlstats
          WHERE module = :moduleName
          ORDER BY last_active_time DESC
        `
      },
      {
        name: 'V$SQL',
        sql: `
          SELECT
            sql_id,
            plan_hash_value,
            module,
            action,
            cpu_time,
            buffer_gets,
            disk_reads,
            elapsed_time,
            executions
          FROM v$sql
          WHERE module = :moduleName
          ORDER BY last_active_time DESC
        `
      }
    ];

    for (const { name, sql } of queries) {
      try {
        const result = await this.db.execute(sql, { moduleName });
        if (result.rows && result.rows.length > 0) {
          if (name !== 'V$SQL_MONITOR') {
            console.info(`Using ${name} for TDE comparison metrics`);
          }
          return result.rows;
        }
      } catch (err) {
        console.warn(`Unable to query ${name}:`, err.message);
      }
    }

    return [];
  }

  calculateComparison() {
    if (!this.results.encrypted || !this.results.unencrypted) {
      return null;
    }

    const metrics = ['cpu_time', 'buffer_gets', 'elapsed_time', 'disk_reads'];

    const aggregate = (sessionStats, metric) => {
      return sessionStats.reduce((sum, row) => sum + (row[metric.toUpperCase()] || 0), 0);
    };

    const comparison = {};
    metrics.forEach(metric => {
      comparison[metric] = {
        encrypted: aggregate(this.results.encrypted.sessionStats || [], metric),
        unencrypted: aggregate(this.results.unencrypted.sessionStats || [], metric)
      };
      comparison[metric].delta = comparison[metric].encrypted - comparison[metric].unencrypted;
      comparison[metric].deltaPercent = comparison[metric].unencrypted
        ? (comparison[metric].delta / comparison[metric].unencrypted) * 100
        : null;
    });

    return comparison;
  }

  async cleanupTable(isEncrypted) {
    const suffix = isEncrypted ? 'ENC' : 'PLAIN';
    const tableName = `${this.config.schemaPrefix}_${suffix}`;
    await this.db.execute(`BEGIN EXECUTE IMMEDIATE 'DROP TABLE ${tableName} PURGE'; EXCEPTION WHEN OTHERS THEN NULL; END;`);
  }

  getStatus() {
    return {
      isRunning: this.isRunning,
      config: this.config,
      results: this.results,
      currentPhase: this.currentPhase,
      progress: this.progress
    };
  }

  stop() {
    if (!this.isRunning) {
      return { success: false, message: 'TDE comparison is not running' };
    }

    this.stopRequested = true;
    this.emitStatus('Stop requested. Cleaning up...', this.progress);
    return { success: true, message: 'Stop requested' };
  }
}

module.exports = new TdeComparisonEngine();
