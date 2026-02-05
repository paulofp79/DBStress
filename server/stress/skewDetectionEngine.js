// Skew Detection Demo Engine
// Creates test tables with pre-defined skew patterns and analyzes columns
// to show skew metrics and DBMS_STATS recommendations
//
// Based on Oracle Skew Data Detection documentation:
// - Skew Ratio: MAX(freq) / AVG(freq) - >2 indicates skew
// - Max Value %: MAX(freq) / SUM(freq) * 100 - >30% indicates skew
// - CV%: STDDEV(freq) / AVG(freq) * 100 - >50% indicates variation

class SkewDetectionEngine {
  constructor() {
    this.isRunning = false;
    this.db = null;
    this.io = null;
    this.tablesCreated = false;
    this.schemaPrefix = 'SKEW';

    // Progress tracking
    this.currentPhase = '';
    this.progress = 0;

    // Test table definitions with skew patterns
    this.testTables = [
      {
        name: 'SKEW_TEST_ORDERS',
        rows: 100000,
        columns: [
          {
            name: 'ORDER_ID',
            type: 'NUMBER',
            isPK: true
          },
          {
            name: 'STATUS',
            type: 'VARCHAR2(20)',
            distribution: [
              { value: 'ACTIVE', percent: 95 },
              { value: 'PENDING', percent: 3 },
              { value: 'COMPLETED', percent: 1.5 },
              { value: 'CANCELLED', percent: 0.5 }
            ],
            expectedSkew: 'EXTREME'
          },
          {
            name: 'ORDER_TYPE',
            type: 'VARCHAR2(20)',
            distribution: [
              { value: 'ONLINE', percent: 25 },
              { value: 'STORE', percent: 25 },
              { value: 'PHONE', percent: 25 },
              { value: 'MOBILE', percent: 25 }
            ],
            expectedSkew: 'NONE'
          },
          {
            name: 'REGION',
            type: 'VARCHAR2(20)',
            distribution: [
              { value: 'NORTH', percent: 50 },
              { value: 'SOUTH', percent: 30 },
              { value: 'EAST', percent: 10 },
              { value: 'WEST', percent: 10 }
            ],
            expectedSkew: 'MODERATE'
          },
          {
            name: 'CREATED_DATE',
            type: 'DATE',
            distribution: 'RANDOM'
          }
        ]
      },
      {
        name: 'SKEW_TEST_AUDIT_LOGS',
        rows: 50000,
        columns: [
          {
            name: 'LOG_ID',
            type: 'NUMBER',
            isPK: true
          },
          {
            name: 'EVENT_TYPE',
            type: 'VARCHAR2(20)',
            distribution: [
              { value: 'INFO', percent: 99 },
              { value: 'WARNING', percent: 0.8 },
              { value: 'ERROR', percent: 0.2 }
            ],
            expectedSkew: 'EXTREME'
          },
          {
            name: 'SEVERITY',
            type: 'VARCHAR2(10)',
            distribution: [
              { value: 'LOW', percent: 99 },
              { value: 'MEDIUM', percent: 0.7 },
              { value: 'HIGH', percent: 0.2 },
              { value: 'CRITICAL', percent: 0.1 }
            ],
            expectedSkew: 'EXTREME'
          },
          {
            name: 'LOG_DATE',
            type: 'DATE',
            distribution: 'RANDOM'
          }
        ]
      },
      {
        name: 'SKEW_TEST_PRODUCTS',
        rows: 20000,
        columns: [
          {
            name: 'PRODUCT_ID',
            type: 'NUMBER',
            isPK: true
          },
          {
            name: 'CATEGORY',
            type: 'VARCHAR2(30)',
            distribution: [
              { value: 'ELECTRONICS', percent: 20 },
              { value: 'CLOTHING', percent: 20 },
              { value: 'HOME', percent: 20 },
              { value: 'SPORTS', percent: 20 },
              { value: 'BOOKS', percent: 20 }
            ],
            expectedSkew: 'NONE'
          },
          {
            name: 'BRAND',
            type: 'VARCHAR2(30)',
            distribution: [
              { value: 'BRAND_A', percent: 10 },
              { value: 'BRAND_B', percent: 10 },
              { value: 'BRAND_C', percent: 10 },
              { value: 'BRAND_D', percent: 10 },
              { value: 'BRAND_E', percent: 10 },
              { value: 'BRAND_F', percent: 10 },
              { value: 'BRAND_G', percent: 10 },
              { value: 'BRAND_H', percent: 10 },
              { value: 'BRAND_I', percent: 10 },
              { value: 'BRAND_J', percent: 10 }
            ],
            expectedSkew: 'NONE'
          },
          {
            name: 'PRICE',
            type: 'NUMBER(10,2)',
            distribution: 'RANDOM_NUMERIC',
            min: 10,
            max: 1000
          }
        ]
      },
      {
        name: 'SKEW_TEST_TRANSACTIONS',
        rows: 80000,
        columns: [
          {
            name: 'TXN_ID',
            type: 'NUMBER',
            isPK: true
          },
          {
            name: 'CURRENCY',
            type: 'VARCHAR2(3)',
            distribution: [
              { value: 'USD', percent: 90 },
              { value: 'EUR', percent: 5 },
              { value: 'GBP', percent: 3 },
              { value: 'JPY', percent: 2 }
            ],
            expectedSkew: 'HIGH'
          },
          {
            name: 'COUNTRY',
            type: 'VARCHAR2(30)',
            distribution: [
              { value: 'USA', percent: 85 },
              { value: 'UK', percent: 5 },
              { value: 'GERMANY', percent: 4 },
              { value: 'FRANCE', percent: 3 },
              { value: 'JAPAN', percent: 3 }
            ],
            expectedSkew: 'HIGH'
          },
          {
            name: 'CHANNEL',
            type: 'VARCHAR2(20)',
            distribution: [
              { value: 'WEB', percent: 33.33 },
              { value: 'MOBILE', percent: 33.33 },
              { value: 'API', percent: 33.34 }
            ],
            expectedSkew: 'NONE'
          },
          {
            name: 'AMOUNT',
            type: 'NUMBER(15,2)',
            distribution: 'RANDOM_NUMERIC',
            min: 1,
            max: 10000
          },
          {
            name: 'TXN_DATE',
            type: 'DATE',
            distribution: 'RANDOM'
          }
        ]
      }
    ];
  }

  emitStatus(message, progress) {
    this.currentPhase = message;
    this.progress = progress;

    if (this.io) {
      this.io.emit('skew-detection-status', {
        tablesCreated: this.tablesCreated,
        message,
        progress
      });
    }
  }

  async start(db, config, io) {
    if (this.isRunning) {
      throw new Error('Skew Detection demo already running');
    }

    this.db = db;
    this.io = io;
    this.isRunning = true;

    console.log('Starting Skew Detection Demo - Creating test tables...');
    this.emitStatus('Creating test tables...', 0);

    try {
      await this.createTestTables();
      this.tablesCreated = true;
      this.emitStatus('Test tables created successfully!', 100);
    } catch (err) {
      console.error('Error creating test tables:', err);
      this.emitStatus(`Error: ${err.message}`, -1);
      this.isRunning = false;
      throw err;
    }

    this.isRunning = false;
    return { success: true, tablesCreated: true };
  }

  async createTestTables() {
    const totalTables = this.testTables.length;
    let tableIndex = 0;

    for (const tableDef of this.testTables) {
      tableIndex++;
      const baseProgress = ((tableIndex - 1) / totalTables) * 100;

      // Drop table if exists
      this.emitStatus(`Dropping ${tableDef.name} if exists...`, baseProgress);
      try {
        await this.db.execute(`DROP TABLE ${tableDef.name} PURGE`);
      } catch (err) {
        // Table doesn't exist, ignore
      }

      // Build CREATE TABLE statement
      this.emitStatus(`Creating ${tableDef.name}...`, baseProgress + 5);
      const columnDefs = tableDef.columns.map(col => {
        let def = `${col.name} ${col.type}`;
        if (col.isPK) {
          def += ' PRIMARY KEY';
        }
        return def;
      }).join(',\n          ');

      await this.db.execute(`
        CREATE TABLE ${tableDef.name} (
          ${columnDefs}
        )
      `);

      console.log(`Created table: ${tableDef.name}`);

      // Populate data
      this.emitStatus(`Populating ${tableDef.name} (${tableDef.rows.toLocaleString()} rows)...`, baseProgress + 10);
      await this.populateTable(tableDef, (pct) => {
        this.emitStatus(
          `Populating ${tableDef.name}... ${pct}%`,
          baseProgress + 10 + (pct / 100) * 80
        );
      });

      console.log(`Populated table: ${tableDef.name} with ${tableDef.rows} rows`);
    }
  }

  async populateTable(tableDef, progressCallback) {
    const batchSize = 5000;
    let inserted = 0;

    while (inserted < tableDef.rows) {
      const currentBatch = Math.min(batchSize, tableDef.rows - inserted);

      // Build column lists
      const columnNames = tableDef.columns.map(c => c.name).join(', ');

      // Build PL/SQL for bulk insert with skewed distributions
      const plsql = this.buildBulkInsertPLSQL(tableDef, inserted, currentBatch);

      await this.db.execute(plsql);
      inserted += currentBatch;

      const pct = Math.round((inserted / tableDef.rows) * 100);
      if (progressCallback) {
        progressCallback(pct);
      }
    }
  }

  buildBulkInsertPLSQL(tableDef, startId, batchSize) {
    const typeDeclarations = [];
    const varDeclarations = [];
    const loopAssignments = [];
    const columnList = [];
    const valueList = [];

    for (const col of tableDef.columns) {
      const varName = `v_${col.name.toLowerCase()}`;
      columnList.push(col.name);

      if (col.isPK) {
        // Primary key - sequential ID
        typeDeclarations.push(`TYPE t_${col.name.toLowerCase()} IS TABLE OF NUMBER INDEX BY PLS_INTEGER;`);
        varDeclarations.push(`${varName} t_${col.name.toLowerCase()};`);
        loopAssignments.push(`${varName}(j) := ${startId} + j;`);
        valueList.push(`${varName}(j)`);
      } else if (col.type.startsWith('DATE')) {
        // Date column - random dates
        typeDeclarations.push(`TYPE t_${col.name.toLowerCase()} IS TABLE OF DATE INDEX BY PLS_INTEGER;`);
        varDeclarations.push(`${varName} t_${col.name.toLowerCase()};`);
        loopAssignments.push(`${varName}(j) := TRUNC(SYSDATE) - DBMS_RANDOM.VALUE(0, 365);`);
        valueList.push(`${varName}(j)`);
      } else if (col.distribution === 'RANDOM_NUMERIC') {
        // Random numeric
        typeDeclarations.push(`TYPE t_${col.name.toLowerCase()} IS TABLE OF NUMBER INDEX BY PLS_INTEGER;`);
        varDeclarations.push(`${varName} t_${col.name.toLowerCase()};`);
        loopAssignments.push(`${varName}(j) := ROUND(DBMS_RANDOM.VALUE(${col.min || 1}, ${col.max || 1000}), 2);`);
        valueList.push(`${varName}(j)`);
      } else if (Array.isArray(col.distribution)) {
        // Skewed distribution
        const isVarchar = col.type.startsWith('VARCHAR2');
        const typeSpec = isVarchar ? col.type : 'NUMBER';

        typeDeclarations.push(`TYPE t_${col.name.toLowerCase()} IS TABLE OF ${typeSpec} INDEX BY PLS_INTEGER;`);
        varDeclarations.push(`${varName} t_${col.name.toLowerCase()};`);

        // Build CASE statement for skewed distribution
        const caseStatements = this.buildSkewedCaseStatement(col.distribution);
        loopAssignments.push(`${varName}(j) := ${caseStatements};`);
        valueList.push(`${varName}(j)`);
      } else {
        // Fallback - random string
        typeDeclarations.push(`TYPE t_${col.name.toLowerCase()} IS TABLE OF VARCHAR2(100) INDEX BY PLS_INTEGER;`);
        varDeclarations.push(`${varName} t_${col.name.toLowerCase()};`);
        loopAssignments.push(`${varName}(j) := DBMS_RANDOM.STRING('A', 20);`);
        valueList.push(`${varName}(j)`);
      }
    }

    return `
      DECLARE
        ${typeDeclarations.join('\n        ')}
        ${varDeclarations.join('\n        ')}
        v_rand NUMBER;
      BEGIN
        FOR j IN 1..${batchSize} LOOP
          v_rand := DBMS_RANDOM.VALUE(0, 100);
          ${loopAssignments.join('\n          ')}
        END LOOP;

        FORALL j IN 1..${batchSize}
          INSERT INTO ${tableDef.name} (${columnList.join(', ')})
          VALUES (${valueList.join(', ')});

        COMMIT;
      END;
    `;
  }

  buildSkewedCaseStatement(distribution) {
    // Build cumulative percentage ranges
    let cumulative = 0;
    const cases = [];

    for (let i = 0; i < distribution.length; i++) {
      const item = distribution[i];
      const prevCumulative = cumulative;
      cumulative += item.percent;

      const value = typeof item.value === 'string' ? `'${item.value}'` : item.value;

      if (i === distribution.length - 1) {
        // Last item - use ELSE
        cases.push(`ELSE ${value}`);
      } else {
        cases.push(`WHEN v_rand < ${cumulative} THEN ${value}`);
      }
    }

    return `CASE ${cases.join(' ')} END`;
  }

  async analyzeSkew() {
    if (!this.db) {
      throw new Error('Not connected to database');
    }

    console.log('Analyzing skew in test tables...');
    this.emitStatus('Analyzing skew patterns...', 0);

    const analysisResults = [];

    for (let i = 0; i < this.testTables.length; i++) {
      const tableDef = this.testTables[i];
      const progress = Math.round((i / this.testTables.length) * 100);
      this.emitStatus(`Analyzing ${tableDef.name}...`, progress);

      // Analyze each column with a distribution
      for (const col of tableDef.columns) {
        if (col.isPK || col.distribution === 'RANDOM' || col.distribution === 'RANDOM_NUMERIC') {
          continue; // Skip PK and random columns
        }

        try {
          const skewMetrics = await this.analyzeColumnSkew(tableDef.name, col.name);
          const classification = this.classifySkew(skewMetrics);
          const recommendation = this.getRecommendation(classification, skewMetrics);

          analysisResults.push({
            tableName: tableDef.name,
            columnName: col.name,
            expectedSkew: col.expectedSkew || 'UNKNOWN',
            ...skewMetrics,
            classification,
            recommendation
          });
        } catch (err) {
          console.error(`Error analyzing ${tableDef.name}.${col.name}:`, err.message);
        }
      }
    }

    this.emitStatus('Analysis complete!', 100);

    // Emit results
    if (this.io) {
      this.io.emit('skew-detection-analysis-results', analysisResults);
    }

    return analysisResults;
  }

  async analyzeColumnSkew(tableName, columnName) {
    // Query to get frequency distribution
    const result = await this.db.execute(`
      SELECT
        ${columnName} as VAL,
        COUNT(*) as FREQ
      FROM ${tableName}
      GROUP BY ${columnName}
    `);

    const frequencies = result.rows.map(r => r.FREQ);
    const values = result.rows.map(r => r.VAL);
    const distinctCount = frequencies.length;

    if (distinctCount === 0) {
      return {
        distinctValues: 0,
        skewRatio: 0,
        maxValuePercent: 0,
        cvPercent: 0,
        maxValue: null,
        maxFrequency: 0,
        totalRows: 0
      };
    }

    const totalRows = frequencies.reduce((a, b) => a + b, 0);
    const maxFrequency = Math.max(...frequencies);
    const maxValueIndex = frequencies.indexOf(maxFrequency);
    const maxValue = values[maxValueIndex];
    const avgFrequency = totalRows / distinctCount;

    // Calculate skew metrics
    const skewRatio = maxFrequency / avgFrequency;
    const maxValuePercent = (maxFrequency / totalRows) * 100;

    // Calculate coefficient of variation (CV%)
    const sumSquaredDiff = frequencies.reduce((sum, f) => sum + Math.pow(f - avgFrequency, 2), 0);
    const stdDev = Math.sqrt(sumSquaredDiff / distinctCount);
    const cvPercent = (stdDev / avgFrequency) * 100;

    return {
      distinctValues: distinctCount,
      skewRatio: Math.round(skewRatio * 100) / 100,
      maxValuePercent: Math.round(maxValuePercent * 100) / 100,
      cvPercent: Math.round(cvPercent * 100) / 100,
      maxValue,
      maxFrequency,
      totalRows
    };
  }

  classifySkew(metrics) {
    const { skewRatio, maxValuePercent } = metrics;

    if (skewRatio > 10 || maxValuePercent > 80) {
      return 'EXTREME';
    } else if (skewRatio > 5 || maxValuePercent > 50) {
      return 'HIGH';
    } else if (skewRatio > 2 || maxValuePercent > 30) {
      return 'MODERATE';
    } else {
      return 'LOW';
    }
  }

  getRecommendation(classification, metrics) {
    switch (classification) {
      case 'EXTREME':
        return {
          methodOpt: 'SIZE 254',
          reason: 'Extreme skew detected. Use SIZE 254 for frequency histograms to capture all distinct values.',
          histogramType: 'FREQUENCY'
        };
      case 'HIGH':
        return {
          methodOpt: 'SIZE 254',
          reason: 'High skew detected. SIZE 254 recommended to ensure optimizer has accurate cardinality estimates.',
          histogramType: 'FREQUENCY or HYBRID'
        };
      case 'MODERATE':
        return {
          methodOpt: `SIZE ${Math.min(254, metrics.distinctValues)}`,
          reason: 'Moderate skew detected. Use SIZE equal to distinct values for optimal histogram.',
          histogramType: 'FREQUENCY'
        };
      default:
        return {
          methodOpt: 'SIZE AUTO or SIZE 1',
          reason: 'Low or no skew detected. Histograms may not be necessary. SIZE AUTO will determine automatically.',
          histogramType: 'NONE or AUTO'
        };
    }
  }

  async gatherStats(tableName, methodOpt) {
    if (!this.db) {
      throw new Error('Not connected to database');
    }

    const methodOptStr = methodOpt.startsWith('FOR ALL')
      ? methodOpt
      : `FOR ALL COLUMNS ${methodOpt}`;

    console.log(`Gathering stats for ${tableName} with METHOD_OPT => '${methodOptStr}'`);
    this.emitStatus(`Gathering stats for ${tableName}...`, 50);

    const startTime = Date.now();

    await this.db.execute(`
      BEGIN
        DBMS_STATS.GATHER_TABLE_STATS(
          ownname => USER,
          tabname => :tabName,
          method_opt => :methodOpt,
          cascade => TRUE,
          degree => 4
        );
      END;
    `, {
      tabName: tableName,
      methodOpt: methodOptStr
    });

    const elapsed = ((Date.now() - startTime) / 1000).toFixed(2);
    console.log(`Stats gathered in ${elapsed}s`);
    this.emitStatus(`Stats gathered for ${tableName} in ${elapsed}s`, 100);

    return { success: true, elapsed: parseFloat(elapsed), tableName, methodOpt: methodOptStr };
  }

  async getHistogramInfo(tableName) {
    if (!this.db) {
      throw new Error('Not connected to database');
    }

    // Get histogram info from USER_TAB_COL_STATISTICS
    const histResult = await this.db.execute(`
      SELECT
        column_name,
        num_distinct,
        num_nulls,
        num_buckets,
        histogram,
        density,
        low_value,
        high_value
      FROM user_tab_col_statistics
      WHERE table_name = :tabName
      ORDER BY column_name
    `, { tabName: tableName });

    const histogramInfo = histResult.rows.map(row => ({
      columnName: row.COLUMN_NAME,
      numDistinct: row.NUM_DISTINCT || 0,
      numNulls: row.NUM_NULLS || 0,
      numBuckets: row.NUM_BUCKETS || 0,
      histogramType: row.HISTOGRAM || 'NONE',
      density: row.DENSITY || 0
    }));

    // Get table stats
    const tableResult = await this.db.execute(`
      SELECT
        num_rows,
        blocks,
        avg_row_len,
        last_analyzed
      FROM user_tables
      WHERE table_name = :tabName
    `, { tabName: tableName });

    const tableStats = tableResult.rows.length > 0 ? {
      numRows: tableResult.rows[0].NUM_ROWS || 0,
      blocks: tableResult.rows[0].BLOCKS || 0,
      avgRowLen: tableResult.rows[0].AVG_ROW_LEN || 0,
      lastAnalyzed: tableResult.rows[0].LAST_ANALYZED
    } : {};

    // Emit results
    if (this.io) {
      this.io.emit('skew-detection-histogram-info', {
        tableName,
        histogramInfo,
        tableStats
      });
    }

    return { histogramInfo, tableStats };
  }

  async getAllHistogramInfo() {
    const allInfo = [];

    for (const tableDef of this.testTables) {
      try {
        const info = await this.getHistogramInfo(tableDef.name);
        allInfo.push({
          tableName: tableDef.name,
          ...info
        });
      } catch (err) {
        console.log(`Could not get histogram info for ${tableDef.name}:`, err.message);
      }
    }

    return allInfo;
  }

  async stop() {
    console.log('Stopping Skew Detection Demo...');
    this.isRunning = false;
    this.emitStatus('Stopped', 0);
    return { success: true };
  }

  async dropTables() {
    if (!this.db) {
      throw new Error('Not connected to database');
    }

    console.log('Dropping skew detection test tables...');
    this.emitStatus('Dropping test tables...', 0);

    let dropped = 0;
    for (const tableDef of this.testTables) {
      try {
        await this.db.execute(`DROP TABLE ${tableDef.name} PURGE`);
        console.log(`Dropped table: ${tableDef.name}`);
        dropped++;
      } catch (err) {
        console.log(`Could not drop ${tableDef.name}:`, err.message);
      }

      this.emitStatus(`Dropping tables... ${dropped}/${this.testTables.length}`,
        Math.round((dropped / this.testTables.length) * 100));
    }

    this.tablesCreated = false;
    this.emitStatus('Test tables dropped', 100);
    return { success: true, droppedCount: dropped };
  }

  getStatus() {
    return {
      tablesCreated: this.tablesCreated,
      currentPhase: this.currentPhase,
      progress: this.progress,
      testTables: this.testTables.map(t => ({
        name: t.name,
        rows: t.rows,
        columnCount: t.columns.length
      }))
    };
  }

  async checkTablesExist() {
    if (!this.db) {
      return false;
    }

    try {
      const result = await this.db.execute(`
        SELECT COUNT(*) as CNT
        FROM user_tables
        WHERE table_name IN (${this.testTables.map(t => `'${t.name}'`).join(', ')})
      `);

      const count = result.rows[0]?.CNT || 0;
      this.tablesCreated = count === this.testTables.length;
      return this.tablesCreated;
    } catch (err) {
      return false;
    }
  }
}

module.exports = new SkewDetectionEngine();
