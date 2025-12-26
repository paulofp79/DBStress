// Metrics Collector for Oracle Database Performance Monitoring

class MetricsCollector {
  constructor() {
    this.isRunning = false;
    this.db = null;
    this.io = null;
    this.collectInterval = null;
    this.collectionFrequency = 2000; // 2 seconds
  }

  start(db, io) {
    if (this.isRunning) {
      return;
    }

    this.db = db;
    this.io = io;
    this.isRunning = true;

    console.log('Starting metrics collection...');

    // Start collecting metrics
    this.collectInterval = setInterval(() => this.collectMetrics(), this.collectionFrequency);

    // Collect immediately
    this.collectMetrics();
  }

  async collectMetrics() {
    if (!this.isRunning || !this.db) return;

    try {
      const [waitEvents, systemStats, sessionStats, sqlStats, gcWaitEvents] = await Promise.all([
        this.getTopWaitEvents(),
        this.getSystemStats(),
        this.getSessionStats(),
        this.getTopSQL(),
        this.getGCWaitEvents()
      ]);

      const metrics = {
        timestamp: Date.now(),
        waitEvents,
        systemStats,
        sessionStats,
        sqlStats,
        gcWaitEvents
      };

      if (this.io) {
        this.io.emit('db-metrics', metrics);
      }
    } catch (err) {
      console.log('Metrics collection error:', err.message);
    }
  }

  async getTopWaitEvents() {
    try {
      const result = await this.db.execute(`
        SELECT
          event,
          total_waits,
          time_waited_micro / 1000000 as time_waited_seconds,
          average_wait * 10 as average_wait_ms,
          wait_class
        FROM (
          SELECT
            event,
            total_waits,
            time_waited_micro,
            CASE WHEN total_waits > 0 THEN time_waited_micro / total_waits / 1000 ELSE 0 END as average_wait,
            wait_class
          FROM v$system_event
          WHERE wait_class != 'Idle'
            AND total_waits > 0
          ORDER BY time_waited_micro DESC
        )
        WHERE ROWNUM <= 10
      `);

      return result.rows.map(row => ({
        event: row.EVENT,
        totalWaits: row.TOTAL_WAITS,
        timeWaitedSeconds: parseFloat(row.TIME_WAITED_SECONDS?.toFixed(2) || 0),
        averageWaitMs: parseFloat(row.AVERAGE_WAIT_MS?.toFixed(2) || 0),
        waitClass: row.WAIT_CLASS
      }));
    } catch (err) {
      // May not have access to V$ views
      console.log('Wait events query error:', err.message);
      return [];
    }
  }

  async getSystemStats() {
    try {
      const result = await this.db.execute(`
        SELECT
          (SELECT value FROM v$sysstat WHERE name = 'user commits') as commits,
          (SELECT value FROM v$sysstat WHERE name = 'user rollbacks') as rollbacks,
          (SELECT value FROM v$sysstat WHERE name = 'execute count') as executions,
          (SELECT value FROM v$sysstat WHERE name = 'parse count (total)') as parses,
          (SELECT value FROM v$sysstat WHERE name = 'physical reads') as physical_reads,
          (SELECT value FROM v$sysstat WHERE name = 'physical writes') as physical_writes,
          (SELECT value FROM v$sysstat WHERE name = 'redo size') as redo_size,
          (SELECT value FROM v$sysstat WHERE name = 'db block gets') as db_block_gets,
          (SELECT value FROM v$sysstat WHERE name = 'consistent gets') as consistent_gets
        FROM dual
      `);

      if (result.rows.length > 0) {
        const row = result.rows[0];
        return {
          commits: row.COMMITS || 0,
          rollbacks: row.ROLLBACKS || 0,
          executions: row.EXECUTIONS || 0,
          parses: row.PARSES || 0,
          physicalReads: row.PHYSICAL_READS || 0,
          physicalWrites: row.PHYSICAL_WRITES || 0,
          redoSize: row.REDO_SIZE || 0,
          dbBlockGets: row.DB_BLOCK_GETS || 0,
          consistentGets: row.CONSISTENT_GETS || 0
        };
      }
      return {};
    } catch (err) {
      console.log('System stats query error:', err.message);
      return {};
    }
  }

  async getSessionStats() {
    try {
      const result = await this.db.execute(`
        SELECT
          COUNT(*) as total_sessions,
          SUM(CASE WHEN status = 'ACTIVE' THEN 1 ELSE 0 END) as active_sessions,
          SUM(CASE WHEN status = 'INACTIVE' THEN 1 ELSE 0 END) as inactive_sessions,
          SUM(CASE WHEN blocking_session IS NOT NULL THEN 1 ELSE 0 END) as blocked_sessions
        FROM v$session
        WHERE type = 'USER'
      `);

      if (result.rows.length > 0) {
        const row = result.rows[0];
        return {
          totalSessions: row.TOTAL_SESSIONS || 0,
          activeSessions: row.ACTIVE_SESSIONS || 0,
          inactiveSessions: row.INACTIVE_SESSIONS || 0,
          blockedSessions: row.BLOCKED_SESSIONS || 0
        };
      }
      return {};
    } catch (err) {
      console.log('Session stats query error:', err.message);
      return {};
    }
  }

  async getTopSQL() {
    try {
      const result = await this.db.execute(`
        SELECT
          sql_id,
          SUBSTR(sql_text, 1, 100) as sql_text,
          executions,
          elapsed_time / 1000000 as elapsed_seconds,
          cpu_time / 1000000 as cpu_seconds,
          buffer_gets,
          disk_reads,
          rows_processed
        FROM (
          SELECT
            sql_id,
            sql_text,
            executions,
            elapsed_time,
            cpu_time,
            buffer_gets,
            disk_reads,
            rows_processed
          FROM v$sql
          WHERE executions > 0
            AND parsing_schema_name NOT IN ('SYS', 'SYSTEM')
          ORDER BY elapsed_time DESC
        )
        WHERE ROWNUM <= 5
      `);

      return result.rows.map(row => ({
        sqlId: row.SQL_ID,
        sqlText: row.SQL_TEXT,
        executions: row.EXECUTIONS || 0,
        elapsedSeconds: parseFloat(row.ELAPSED_SECONDS?.toFixed(3) || 0),
        cpuSeconds: parseFloat(row.CPU_SECONDS?.toFixed(3) || 0),
        bufferGets: row.BUFFER_GETS || 0,
        diskReads: row.DISK_READS || 0,
        rowsProcessed: row.ROWS_PROCESSED || 0
      }));
    } catch (err) {
      console.log('Top SQL query error:', err.message);
      return [];
    }
  }

  // GC (Global Cache) Wait Events for RAC monitoring
  async getGCWaitEvents() {
    try {
      // Query GC-related wait events from gv$system_event (RAC) or v$system_event
      const result = await this.db.execute(`
        SELECT
          inst_id,
          event,
          total_waits,
          total_timeouts,
          time_waited_micro / 1000 as time_waited_ms,
          CASE WHEN total_waits > 0
               THEN time_waited_micro / total_waits / 1000
               ELSE 0 END as avg_wait_ms
        FROM gv$system_event
        WHERE event LIKE 'gc %'
          AND total_waits > 0
        ORDER BY time_waited_micro DESC
        FETCH FIRST 20 ROWS ONLY
      `);

      return result.rows.map(row => ({
        instId: row.INST_ID || 1,
        event: row.EVENT,
        totalWaits: row.TOTAL_WAITS || 0,
        totalTimeouts: row.TOTAL_TIMEOUTS || 0,
        timeWaitedMs: parseFloat(row.TIME_WAITED_MS?.toFixed(2) || 0),
        avgWaitMs: parseFloat(row.AVG_WAIT_MS?.toFixed(3) || 0)
      }));
    } catch (err) {
      // Try single-instance view if gv$ not available
      try {
        const result = await this.db.execute(`
          SELECT
            1 as inst_id,
            event,
            total_waits,
            total_timeouts,
            time_waited_micro / 1000 as time_waited_ms,
            CASE WHEN total_waits > 0
                 THEN time_waited_micro / total_waits / 1000
                 ELSE 0 END as avg_wait_ms
          FROM v$system_event
          WHERE event LIKE 'gc %'
            AND total_waits > 0
          ORDER BY time_waited_micro DESC
          FETCH FIRST 20 ROWS ONLY
        `);

        return result.rows.map(row => ({
          instId: 1,
          event: row.EVENT,
          totalWaits: row.TOTAL_WAITS || 0,
          totalTimeouts: row.TOTAL_TIMEOUTS || 0,
          timeWaitedMs: parseFloat(row.TIME_WAITED_MS?.toFixed(2) || 0),
          avgWaitMs: parseFloat(row.AVG_WAIT_MS?.toFixed(3) || 0)
        }));
      } catch (e) {
        console.log('GC wait events query error:', e.message);
        return [];
      }
    }
  }

  stop() {
    console.log('Stopping metrics collection...');
    this.isRunning = false;

    if (this.collectInterval) {
      clearInterval(this.collectInterval);
      this.collectInterval = null;
    }

    this.db = null;
    this.io = null;
  }
}

module.exports = new MetricsCollector();
