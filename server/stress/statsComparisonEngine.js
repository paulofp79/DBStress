// Statistics Generation Comparison Engine
// Compares performance of DBMS_STATS.GATHER_TABLE_STATS with different histogram options:
// - METHOD_OPT => 'FOR ALL COLUMNS SIZE 254' (fixed histogram buckets)
// - METHOD_OPT => 'FOR ALL COLUMNS SIZE AUTO' (auto-determined histograms)
// Tracks SYS.HISTGRM$ table size and execution time

class StatsComparisonEngine {
  constructor() {
    this.isRunning = false;
    this.config = null;
    this.db = null;
    this.io = null;
    this.schemaPrefix = '';

    // Test results
    this.results = {
      size254: null,
      sizeAuto: null,
      comparison: null
    };

    // Progress tracking
    this.currentPhase = '';
    this.progress = 0;
  }

  async start(db, config, io) {
    if (this.isRunning) {
      throw new Error('Stats comparison test already running');
    }

    this.config = {
      tableCount: config.tableCount || 10,
      rowsPerTable: config.rowsPerTable || 100000,
      columnsPerTable: config.columnsPerTable || 20,
      schemaPrefix: config.schemaPrefix || 'STATS_TEST',
      parallelDegree: config.parallelDegree || 4,
      ...config
    };

    this.schemaPrefix = this.config.schemaPrefix;
    this.db = db;
    this.io = io;
    this.isRunning = true;

    // Reset results
    this.results = {
      size254: null,
      sizeAuto: null,
      comparison: null
    };

    console.log(`Starting Stats Comparison Test with ${this.config.tableCount} tables, ${this.config.rowsPerTable} rows each`);

    this.emitStatus('Starting stats comparison test...', 0);

    try {
      // Run the comparison test
      await this.runComparison();
    } catch (err) {
      console.error('Stats comparison error:', err);
      this.emitStatus(`Error: ${err.message}`, -1);
      this.isRunning = false;
      throw err;
    }

    this.isRunning = false;
    return this.results;
  }

  async runComparison() {
    // Phase 1: Create test tables
    this.emitStatus('Phase 1: Creating test tables...', 5);
    await this.createTestTables();

    // Phase 2: Populate test tables with varied data
    this.emitStatus('Phase 2: Populating test tables with varied data...', 15);
    await this.populateTestTables();

    // Phase 3: Get initial HISTGRM$ size
    this.emitStatus('Phase 3: Getting initial histogram table size...', 25);
    const initialHistSize = await this.getHistogramTableSize();
    console.log(`Initial HISTGRM$ size: ${initialHistSize} rows`);

    // Phase 4: Test with SIZE 254
    this.emitStatus('Phase 4: Gathering stats with SIZE 254...', 30);
    const result254 = await this.gatherStatsWithMethod('SIZE 254');
    this.results.size254 = {
      ...result254,
      histogramRowsBefore: initialHistSize
    };

    // Phase 5: Get HISTGRM$ size after SIZE 254
    this.emitStatus('Phase 5: Measuring histogram size after SIZE 254...', 50);
    const histSizeAfter254 = await this.getHistogramTableSize();
    this.results.size254.histogramRowsAfter = histSizeAfter254;
    this.results.size254.histogramRowsAdded = histSizeAfter254 - initialHistSize;

    // Phase 6: Delete statistics to reset
    this.emitStatus('Phase 6: Deleting statistics for reset...', 55);
    await this.deleteStats();

    // Phase 7: Get HISTGRM$ size after delete (baseline for AUTO)
    const histSizeAfterDelete = await this.getHistogramTableSize();
    console.log(`HISTGRM$ size after delete: ${histSizeAfterDelete} rows`);

    // Phase 8: Test with SIZE AUTO
    this.emitStatus('Phase 8: Gathering stats with SIZE AUTO...', 60);
    const resultAuto = await this.gatherStatsWithMethod('SIZE AUTO');
    this.results.sizeAuto = {
      ...resultAuto,
      histogramRowsBefore: histSizeAfterDelete
    };

    // Phase 9: Get HISTGRM$ size after SIZE AUTO
    this.emitStatus('Phase 9: Measuring histogram size after SIZE AUTO...', 80);
    const histSizeAfterAuto = await this.getHistogramTableSize();
    this.results.sizeAuto.histogramRowsAfter = histSizeAfterAuto;
    this.results.sizeAuto.histogramRowsAdded = histSizeAfterAuto - histSizeAfterDelete;

    // Phase 10: Calculate comparison
    this.emitStatus('Phase 10: Calculating comparison...', 90);
    this.calculateComparison();

    // Phase 11: Cleanup (optional based on config)
    if (this.config.cleanup !== false) {
      this.emitStatus('Phase 11: Cleaning up test tables...', 95);
      await this.cleanupTestTables();
    }

    this.emitStatus('Comparison complete!', 100);
    this.emitResults();
  }

  async createTestTables() {
    const p = this.schemaPrefix ? `${this.schemaPrefix}_` : '';

    for (let i = 1; i <= this.config.tableCount; i++) {
      const tableName = `${p}TEST_TABLE_${i}`;

      // Drop if exists
      try {
        await this.db.execute(`DROP TABLE ${tableName} PURGE`);
      } catch (err) {
        // Table doesn't exist, that's fine
      }

      // Create table with multiple columns of different types
      // This creates varied data patterns for histogram generation
      const columns = [];
      for (let col = 1; col <= this.config.columnsPerTable; col++) {
        if (col <= 5) {
          // Numeric columns with varying cardinality
          columns.push(`COL_NUM_${col} NUMBER`);
        } else if (col <= 10) {
          // Varchar columns with varying lengths
          columns.push(`COL_VARCHAR_${col} VARCHAR2(100)`);
        } else if (col <= 15) {
          // Date columns
          columns.push(`COL_DATE_${col} DATE`);
        } else {
          // More varchar columns with different patterns
          columns.push(`COL_TEXT_${col} VARCHAR2(200)`);
        }
      }

      const createSQL = `
        CREATE TABLE ${tableName} (
          ID NUMBER PRIMARY KEY,
          ${columns.join(',\n          ')}
        )
      `;

      await this.db.execute(createSQL);
      console.log(`Created table: ${tableName}`);

      this.emitStatus(`Creating test tables... (${i}/${this.config.tableCount})`, 5 + (i / this.config.tableCount) * 10);
    }
  }

  async populateTestTables() {
    const p = this.schemaPrefix ? `${this.schemaPrefix}_` : '';
    const batchSize = 10000;

    for (let i = 1; i <= this.config.tableCount; i++) {
      const tableName = `${p}TEST_TABLE_${i}`;
      let inserted = 0;

      while (inserted < this.config.rowsPerTable) {
        const currentBatch = Math.min(batchSize, this.config.rowsPerTable - inserted);

        // Build insert with varied data patterns
        // Using PL/SQL block for efficient bulk insert
        const plsql = `
          DECLARE
            TYPE t_id IS TABLE OF NUMBER INDEX BY PLS_INTEGER;
            TYPE t_num IS TABLE OF NUMBER INDEX BY PLS_INTEGER;
            TYPE t_varchar IS TABLE OF VARCHAR2(100) INDEX BY PLS_INTEGER;
            TYPE t_date IS TABLE OF DATE INDEX BY PLS_INTEGER;
            TYPE t_text IS TABLE OF VARCHAR2(200) INDEX BY PLS_INTEGER;

            v_id t_id;
            v_num1 t_num; v_num2 t_num; v_num3 t_num; v_num4 t_num; v_num5 t_num;
            v_vc1 t_varchar; v_vc2 t_varchar; v_vc3 t_varchar; v_vc4 t_varchar; v_vc5 t_varchar;
            v_dt1 t_date; v_dt2 t_date; v_dt3 t_date; v_dt4 t_date; v_dt5 t_date;
            v_tx1 t_text; v_tx2 t_text; v_tx3 t_text; v_tx4 t_text; v_tx5 t_text;
          BEGIN
            FOR j IN 1..${currentBatch} LOOP
              v_id(j) := ${inserted} + j;

              -- Numeric columns with varying distributions
              v_num1(j) := MOD(${inserted} + j, 10);                    -- 10 distinct values (low cardinality)
              v_num2(j) := MOD(${inserted} + j, 100);                   -- 100 distinct values
              v_num3(j) := MOD(${inserted} + j, 1000);                  -- 1000 distinct values
              v_num4(j) := DBMS_RANDOM.VALUE(1, 10000);                 -- Random high cardinality
              v_num5(j) := CASE WHEN MOD(j, 100) = 0 THEN 999999       -- Skewed distribution
                               ELSE MOD(${inserted} + j, 50) END;

              -- Varchar columns with varying patterns
              v_vc1(j) := 'STATUS_' || LPAD(MOD(${inserted} + j, 5), 2, '0');      -- 5 distinct
              v_vc2(j) := 'CATEGORY_' || LPAD(MOD(${inserted} + j, 20), 3, '0');   -- 20 distinct
              v_vc3(j) := 'TYPE_' || LPAD(MOD(${inserted} + j, 100), 4, '0');      -- 100 distinct
              v_vc4(j) := DBMS_RANDOM.STRING('A', 20);                              -- Random
              v_vc5(j) := CASE MOD(j, 1000)
                            WHEN 0 THEN 'RARE_VALUE'
                            ELSE 'COMMON_' || MOD(j, 10)
                          END;

              -- Date columns
              v_dt1(j) := TRUNC(SYSDATE) - MOD(${inserted} + j, 365);              -- Last year
              v_dt2(j) := TRUNC(SYSDATE) - MOD(${inserted} + j, 30);               -- Last month
              v_dt3(j) := TRUNC(SYSDATE) - DBMS_RANDOM.VALUE(0, 1000);             -- Random
              v_dt4(j) := ADD_MONTHS(TRUNC(SYSDATE, 'MM'), -MOD(${inserted} + j, 24)); -- Monthly buckets
              v_dt5(j) := TRUNC(SYSDATE);                                           -- All same (null-like)

              -- Text columns with varied patterns
              v_tx1(j) := RPAD('DESCRIPTION_' || MOD(${inserted} + j, 50), 100, 'X');
              v_tx2(j) := DBMS_RANDOM.STRING('X', 50);
              v_tx3(j) := 'PREFIX_' || TO_CHAR(MOD(${inserted} + j, 200), 'FM0000') || '_SUFFIX';
              v_tx4(j) := CASE MOD(j, 5)
                            WHEN 0 THEN 'A_COMMON'
                            WHEN 1 THEN 'B_COMMON'
                            WHEN 2 THEN 'C_COMMON'
                            ELSE 'D_' || j
                          END;
              v_tx5(j) := 'RECORD_' || LPAD(${inserted} + j, 10, '0');
            END LOOP;

            FORALL j IN 1..${currentBatch}
              INSERT INTO ${tableName} (
                ID,
                COL_NUM_1, COL_NUM_2, COL_NUM_3, COL_NUM_4, COL_NUM_5,
                COL_VARCHAR_6, COL_VARCHAR_7, COL_VARCHAR_8, COL_VARCHAR_9, COL_VARCHAR_10,
                COL_DATE_11, COL_DATE_12, COL_DATE_13, COL_DATE_14, COL_DATE_15,
                COL_TEXT_16, COL_TEXT_17, COL_TEXT_18, COL_TEXT_19, COL_TEXT_20
              ) VALUES (
                v_id(j),
                v_num1(j), v_num2(j), v_num3(j), v_num4(j), v_num5(j),
                v_vc1(j), v_vc2(j), v_vc3(j), v_vc4(j), v_vc5(j),
                v_dt1(j), v_dt2(j), v_dt3(j), v_dt4(j), v_dt5(j),
                v_tx1(j), v_tx2(j), v_tx3(j), v_tx4(j), v_tx5(j)
              );
            COMMIT;
          END;
        `;

        await this.db.execute(plsql);
        inserted += currentBatch;
      }

      console.log(`Populated table ${tableName} with ${this.config.rowsPerTable} rows`);
      this.emitStatus(
        `Populating test tables... (${i}/${this.config.tableCount})`,
        15 + (i / this.config.tableCount) * 10
      );
    }
  }

  async getHistogramTableSize() {
    // Get size of histogram data for our test tables
    const p = this.schemaPrefix ? `${this.schemaPrefix}_` : '';

    const result = await this.db.execute(`
      SELECT COUNT(*) as HIST_ROWS
      FROM SYS.HISTGRM$ h
      JOIN SYS.OBJ$ o ON h.OBJ# = o.OBJ#
      WHERE o.NAME LIKE '${p}TEST_TABLE_%'
    `);

    return result.rows[0]?.HIST_ROWS || 0;
  }

  async getHistogramTableSizeBytes() {
    // Get actual bytes used in HISTGRM$ for our tables
    const p = this.schemaPrefix ? `${this.schemaPrefix}_` : '';

    try {
      const result = await this.db.execute(`
        SELECT
          COUNT(*) as ROW_COUNT,
          SUM(
            NVL(LENGTH(ENDPOINT_ACTUAL), 0) +
            8 + -- OBJ#
            8 + -- COL#
            8 + -- BUCKET
            8 + -- ENDPOINT
            NVL(LENGTH(EPVALUE), 0) +
            8   -- EP_REPEAT_COUNT
          ) as ESTIMATED_BYTES
        FROM SYS.HISTGRM$ h
        JOIN SYS.OBJ$ o ON h.OBJ# = o.OBJ#
        WHERE o.NAME LIKE '${p}TEST_TABLE_%'
      `);

      return {
        rows: result.rows[0]?.ROW_COUNT || 0,
        bytes: result.rows[0]?.ESTIMATED_BYTES || 0
      };
    } catch (err) {
      console.log('Could not get detailed histogram size:', err.message);
      return { rows: 0, bytes: 0 };
    }
  }

  async gatherStatsWithMethod(methodOpt) {
    const p = this.schemaPrefix ? `${this.schemaPrefix}_` : '';
    const startTime = Date.now();
    const tableTimings = [];

    for (let i = 1; i <= this.config.tableCount; i++) {
      const tableName = `${p}TEST_TABLE_${i}`;
      const tableStart = Date.now();

      await this.db.execute(`
        BEGIN
          DBMS_STATS.GATHER_TABLE_STATS(
            ownname => USER,
            tabname => '${tableName}',
            method_opt => 'FOR ALL COLUMNS ${methodOpt}',
            degree => ${this.config.parallelDegree},
            cascade => TRUE,
            no_invalidate => FALSE
          );
        END;
      `);

      const tableTime = Date.now() - tableStart;
      tableTimings.push({
        table: tableName,
        timeMs: tableTime
      });

      console.log(`Gathered stats for ${tableName} with ${methodOpt}: ${tableTime}ms`);

      // Update progress
      const baseProgress = methodOpt === 'SIZE 254' ? 30 : 60;
      this.emitStatus(
        `Gathering stats with ${methodOpt}... (${i}/${this.config.tableCount})`,
        baseProgress + (i / this.config.tableCount) * 20
      );
    }

    const totalTime = Date.now() - startTime;

    // Get detailed histogram info
    const histogramDetails = await this.getHistogramDetails();

    return {
      method: methodOpt,
      totalTimeMs: totalTime,
      avgTimePerTableMs: totalTime / this.config.tableCount,
      tableTimings,
      histogramDetails
    };
  }

  async getHistogramDetails() {
    const p = this.schemaPrefix ? `${this.schemaPrefix}_` : '';

    const result = await this.db.execute(`
      SELECT
        t.TABLE_NAME,
        c.COLUMN_NAME,
        c.HISTOGRAM,
        c.NUM_BUCKETS,
        c.NUM_DISTINCT
      FROM USER_TAB_COLUMNS c
      JOIN USER_TABLES t ON c.TABLE_NAME = t.TABLE_NAME
      WHERE t.TABLE_NAME LIKE '${p}TEST_TABLE_%'
      ORDER BY t.TABLE_NAME, c.COLUMN_ID
    `);

    const histogramsByType = {
      NONE: 0,
      FREQUENCY: 0,
      HEIGHT_BALANCED: 0,
      HYBRID: 0,
      TOP_FREQUENCY: 0
    };

    const columnDetails = [];
    for (const row of result.rows) {
      const histType = row.HISTOGRAM || 'NONE';
      histogramsByType[histType] = (histogramsByType[histType] || 0) + 1;

      columnDetails.push({
        table: row.TABLE_NAME,
        column: row.COLUMN_NAME,
        histogram: histType,
        buckets: row.NUM_BUCKETS || 0,
        distinctValues: row.NUM_DISTINCT || 0
      });
    }

    return {
      histogramsByType,
      columnDetails,
      totalColumns: columnDetails.length,
      columnsWithHistograms: columnDetails.filter(c => c.histogram !== 'NONE').length
    };
  }

  async deleteStats() {
    const p = this.schemaPrefix ? `${this.schemaPrefix}_` : '';

    for (let i = 1; i <= this.config.tableCount; i++) {
      const tableName = `${p}TEST_TABLE_${i}`;

      await this.db.execute(`
        BEGIN
          DBMS_STATS.DELETE_TABLE_STATS(
            ownname => USER,
            tabname => '${tableName}'
          );
        END;
      `);
    }

    console.log('Deleted statistics for all test tables');
  }

  calculateComparison() {
    const s254 = this.results.size254;
    const sAuto = this.results.sizeAuto;

    this.results.comparison = {
      // Time comparison
      timeDifferenceMs: s254.totalTimeMs - sAuto.totalTimeMs,
      timeRatio: s254.totalTimeMs / sAuto.totalTimeMs,
      size254FasterBy: sAuto.totalTimeMs - s254.totalTimeMs,

      // Histogram size comparison
      histogramRowsDifference: s254.histogramRowsAdded - sAuto.histogramRowsAdded,
      histogramRatio: s254.histogramRowsAdded / (sAuto.histogramRowsAdded || 1),

      // Summary
      summary: {
        size254: {
          time: `${(s254.totalTimeMs / 1000).toFixed(2)} seconds`,
          histogramRows: s254.histogramRowsAdded,
          columnsWithHistograms: s254.histogramDetails?.columnsWithHistograms || 0
        },
        sizeAuto: {
          time: `${(sAuto.totalTimeMs / 1000).toFixed(2)} seconds`,
          histogramRows: sAuto.histogramRowsAdded,
          columnsWithHistograms: sAuto.histogramDetails?.columnsWithHistograms || 0
        },
        winner: {
          speed: s254.totalTimeMs < sAuto.totalTimeMs ? 'SIZE 254' : 'SIZE AUTO',
          storage: s254.histogramRowsAdded < sAuto.histogramRowsAdded ? 'SIZE 254' : 'SIZE AUTO'
        },
        recommendation: this.generateRecommendation(s254, sAuto)
      }
    };

    console.log('Comparison results:', this.results.comparison.summary);
  }

  generateRecommendation(s254, sAuto) {
    const timeDiffPercent = Math.abs(s254.totalTimeMs - sAuto.totalTimeMs) / Math.max(s254.totalTimeMs, sAuto.totalTimeMs) * 100;
    const storageDiffPercent = Math.abs(s254.histogramRowsAdded - sAuto.histogramRowsAdded) / Math.max(s254.histogramRowsAdded, sAuto.histogramRowsAdded || 1) * 100;

    let recommendation = '';

    if (s254.histogramRowsAdded > sAuto.histogramRowsAdded * 2) {
      recommendation = 'SIZE AUTO is recommended. SIZE 254 creates significantly more histogram data, which increases storage and maintenance overhead without necessarily improving query optimization.';
    } else if (sAuto.totalTimeMs > s254.totalTimeMs * 1.5) {
      recommendation = 'SIZE 254 may be better for your workload. While it creates more histogram data, the stats gathering is faster.';
    } else if (timeDiffPercent < 10 && storageDiffPercent > 50) {
      recommendation = 'SIZE AUTO is recommended. Both methods have similar gathering times, but SIZE AUTO creates fewer histograms, reducing storage and dictionary contention.';
    } else {
      recommendation = 'SIZE AUTO is generally recommended by Oracle. It creates histograms only where needed based on data distribution analysis.';
    }

    return recommendation;
  }

  async cleanupTestTables() {
    const p = this.schemaPrefix ? `${this.schemaPrefix}_` : '';

    for (let i = 1; i <= this.config.tableCount; i++) {
      const tableName = `${p}TEST_TABLE_${i}`;

      try {
        await this.db.execute(`DROP TABLE ${tableName} PURGE`);
        console.log(`Dropped table: ${tableName}`);
      } catch (err) {
        console.log(`Could not drop ${tableName}: ${err.message}`);
      }
    }
  }

  emitStatus(message, progress) {
    this.currentPhase = message;
    this.progress = progress;

    if (this.io) {
      this.io.emit('stats-comparison-status', {
        running: this.isRunning,
        message,
        progress,
        config: this.config
      });
    }
  }

  emitResults() {
    if (this.io) {
      this.io.emit('stats-comparison-results', this.results);
    }
  }

  getStatus() {
    return {
      isRunning: this.isRunning,
      currentPhase: this.currentPhase,
      progress: this.progress,
      config: this.config,
      results: this.results
    };
  }

  async stop() {
    console.log('Stopping Stats Comparison Test...');
    this.isRunning = false;

    // Cleanup if requested
    if (this.config?.cleanup !== false && this.db) {
      try {
        await this.cleanupTestTables();
      } catch (err) {
        console.log('Cleanup warning:', err.message);
      }
    }

    this.emitStatus('Stopped', 0);
    return this.results;
  }
}

module.exports = new StatsComparisonEngine();
