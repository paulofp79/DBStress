const oracledb = require('oracledb');

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

const normalizeUpper = (value, fallback = '') =>
  String(value || fallback).trim().replace(/^"+|"+$/g, '').toUpperCase();

const normalizeFreeText = (value, fallback = '') =>
  String(value || fallback).trim() || fallback;

const average = (values = []) => (
  values.length ? values.reduce((sum, value) => sum + value, 0) / values.length : 0
);

const AUTH_ERROR_CODES = [
  'ORA-01017',
  'ORA-28000',
  'ORA-28001',
  'ORA-28002',
  'ORA-28003',
  'ORA-28040'
];

const SCHEMA_ERROR_CODES = [
  'ORA-00942',
  'ORA-04043'
];

const REQUIRED_WORKLOAD_TABLES = [
  'ORDERS',
  'CUSTOMERS',
  'PRODUCTS',
  'INVENTORY',
  'PRODUCT_REVIEWS',
  'ORDER_HISTORY',
  'ORDER_ITEMS',
  'WAREHOUSES'
];

class LibraryCacheLockEngine {
  constructor() {
    this.isRunning = false;
    this.config = null;
    this.db = null;
    this.io = null;
    this.workers = [];
    this.routePools = new Map();
    this.routes = [];

    this.stats = this.createEmptyStats();
    this.previousStats = { totalTransactions: 0, lastTick: Date.now() };

    this.statsInterval = null;
    this.waitEventsInterval = null;
    this.expansionInterval = null;
    this.autoStopTimer = null;
    this.runStartSnapshot = null;
    this.lastSampleSnapshot = null;
    this.latestSample = null;
    this.lastRunSummary = null;
    this.runId = null;
    this.stopPromise = null;
    this.nextWorkerId = 0;
    this.workerStates = new Map();
    this.awrState = this.createEmptyAwrState();
  }

  createEmptyStats() {
    return {
      totalTransactions: 0,
      totalErrors: 0,
      totalLogons: 0,
      totalLogoffs: 0,
      currentSessions: 0,
      peakSessions: 0,
      responseTimes: [],
      startTime: null,
      lastError: null,
      transactionsPerSecond: 0
    };
  }

  createRouteStats() {
    return {
      totalTransactions: 0,
      totalErrors: 0,
      totalLogons: 0,
      totalLogoffs: 0,
      currentSessions: 0,
      peakSessions: 0,
      initialSessions: 0,
      maxSessions: 0,
      responseTimes: [],
      previousTransactions: 0,
      tps: 0
    };
  }

  createEmptyAwrState() {
    return {
      begin: null,
      end: null,
      warnings: [],
      routeName: null,
      connectionString: null
    };
  }

  emitStatus(message, extra = {}) {
    this.io?.emit('library-cache-lock-status', {
      running: this.isRunning,
      message,
      runId: this.runId,
      ...extra
    });
  }

  getAwrConnectionTarget() {
    const route = this.routes?.[0] || null;
    return {
      routeName: route?.name || 'Primary Service',
      connectionString: route?.connectionString || this.db.getCredentials().connectionString
    };
  }

  buildAwrSummaryFields() {
    const begin = this.awrState?.begin || null;
    const end = this.awrState?.end || null;
    const instanceNumbers = end?.instanceNumbers?.length
      ? end.instanceNumbers
      : begin?.instanceNumbers?.length
        ? begin.instanceNumbers
        : [];

    return {
      awrBeginSnapId: begin?.snapId ?? null,
      awrEndSnapId: end?.snapId ?? null,
      awrDbid: begin?.dbid ?? end?.dbid ?? null,
      awrInstanceNumbers: instanceNumbers,
      awrBeginCapturedAt: begin?.capturedAt ?? null,
      awrEndCapturedAt: end?.capturedAt ?? null,
      awrRouteName: this.awrState?.routeName || null,
      awrWarnings: Array.isArray(this.awrState?.warnings) ? [...this.awrState.warnings] : []
    };
  }

  normalizeRoute(route = {}, index, defaultConnectionString, defaultProcedureName, defaultOwner) {
    const name = normalizeFreeText(route.name, `Service ${index + 1}`);
    const connectionString = normalizeFreeText(route.connectionString, defaultConnectionString);
    const procedureName = normalizeUpper(route.procedureName, defaultProcedureName);
    const procedureOwner = normalizeUpper(route.procedureOwner, defaultOwner);

    return {
      id: `route-${index + 1}`,
      name,
      connectionString,
      procedureName,
      procedureOwner,
      instanceName: null
    };
  }

  normalizeConfig(config = {}) {
    const credentials = this.db.getCredentials();
    const defaultConnectionString = credentials.connectionString;
    const defaultProcedureName = normalizeUpper(config.procedureName, 'GRAV_SESSION_MFES_ONLINE');
    const defaultOwner = normalizeUpper(config.procedureOwner, '');
    const scenario = config.scenario === 'split-services'
      ? 'split-services'
      : config.scenario === 'no-alter-session'
        ? 'no-alter-session'
        : 'single-service';

    let routes;
    if (scenario === 'split-services') {
      const incomingRoutes = Array.isArray(config.services) ? config.services : [];
      routes = incomingRoutes
        .map((route, index) => this.normalizeRoute(route, index, defaultConnectionString, defaultProcedureName, defaultOwner))
        .filter((route) => route.connectionString && route.procedureName);

      if (routes.length === 0) {
        routes = [
          this.normalizeRoute({ name: 'Service 1' }, 0, defaultConnectionString, `${defaultProcedureName}_1`, defaultOwner),
          this.normalizeRoute({ name: 'Service 2' }, 1, defaultConnectionString, `${defaultProcedureName}_2`, defaultOwner),
          this.normalizeRoute({ name: 'Service 3' }, 2, defaultConnectionString, `${defaultProcedureName}_3`, defaultOwner),
          this.normalizeRoute({ name: 'Service 4' }, 3, defaultConnectionString, `${defaultProcedureName}_4`, defaultOwner)
        ];
      }
    } else {
      routes = [
        this.normalizeRoute({
          name: normalizeFreeText(config.singleServiceName, 'Primary Service'),
          connectionString: normalizeFreeText(config.singleServiceConnectionString, defaultConnectionString),
          procedureName: defaultProcedureName,
          procedureOwner: defaultOwner
        }, 0, defaultConnectionString, defaultProcedureName, defaultOwner)
      ];
    }

    const maxSessions = clampInt(
      config.maxSessions ?? config.threads,
      1,
      5000,
      500
    );
    const initialSessions = clampInt(
      config.initialSessions,
      1,
      maxSessions,
      Math.min(100, maxSessions)
    );
    const schemaPrefix = normalizeUpper(config.schemaPrefix, '');
    const tableOwner = normalizeUpper(config.tableOwner, '');
    const modulePrefix = normalizeFreeText(config.modulePrefix, 'MFES').replace(/\s+/g, '_').slice(0, 18) || 'MFES';

    return {
      scenario,
      loginMode: 'persistent',
      initialSessions,
      maxSessions,
      loopDelay: clampInt(config.loopDelay, 0, 5000, 0),
      moduleLength: clampInt(config.moduleLength, 30, 96, 42),
      modulePrefix,
      schemaPrefix,
      tableOwner,
      waitSampleSeconds: clampInt(config.waitSampleSeconds, 2, 30, 5),
      durationMinutes: clampInt(config.durationMinutes, 0, 1440, 0),
      selectsPerTxn: clampInt(config.selectsPerTxn, 1, 10, 2),
      insertsPerTxn: clampInt(config.insertsPerTxn, 0, 10, 1),
      updatesPerTxn: clampInt(config.updatesPerTxn, 0, 10, 1),
      deletesPerTxn: clampInt(config.deletesPerTxn, 0, 10, 1),
      runLabel: normalizeFreeText(
        config.runLabel,
        scenario === 'split-services'
          ? 'Split Services'
          : scenario === 'no-alter-session'
            ? 'No ALTER SESSION'
            : 'Single Service'
      ),
      routes
    };
  }

  qualifyProcedure(route) {
    return route.procedureOwner ? `${route.procedureOwner}.${route.procedureName}` : route.procedureName;
  }

  buildAnonymousBlock(route) {
    return `BEGIN ${this.qualifyProcedure(route)}(:moduleName); END;`;
  }

  buildModuleName(route, workerId, iteration) {
    const base = `${this.config.modulePrefix}_${route.name.replace(/\s+/g, '').slice(0, 10)}_${workerId.toString(36).toUpperCase()}_${iteration.toString(36).toUpperCase()}_${Date.now().toString(36).toUpperCase()}`;
    const padded = `${base}_LIBCACHELOCK_WORKLOAD`;
    return padded.slice(0, this.config.moduleLength).padEnd(this.config.moduleLength, 'X');
  }

  getTablePrefix() {
    return this.config.schemaPrefix ? `${this.config.schemaPrefix}_` : '';
  }

  qualifyTable(tableName) {
    return this.config.tableOwner ? `${this.config.tableOwner}.${tableName}` : tableName;
  }

  getQualifiedTableName(baseTableName) {
    return this.qualifyTable(`${this.getTablePrefix()}${baseTableName}`);
  }

  isAuthError(err) {
    const message = String(err?.message || '');
    return AUTH_ERROR_CODES.some((code) => message.includes(code));
  }

  isSchemaError(err) {
    const message = String(err?.message || '');
    return SCHEMA_ERROR_CODES.some((code) => message.includes(code));
  }

  buildRequiredTableNames() {
    const prefix = this.getTablePrefix();
    return REQUIRED_WORKLOAD_TABLES.map((tableName) => `${prefix}${tableName}`);
  }

  async validateWorkloadTables(connection, route) {
    const requiredTableNames = this.buildRequiredTableNames();
    const binds = {};
    const placeholders = requiredTableNames.map((tableName, index) => {
      const bindName = `tableName${index}`;
      binds[bindName] = tableName;
      return `:${bindName}`;
    });

    const result = await connection.execute(
      this.config.tableOwner
        ? `
          SELECT table_name
          FROM all_tables
          WHERE owner = :tableOwner
            AND table_name IN (${placeholders.join(', ')})
        `
        : `
          SELECT table_name
          FROM user_tables
          WHERE table_name IN (${placeholders.join(', ')})
        `,
      this.config.tableOwner
        ? { ...binds, tableOwner: this.config.tableOwner }
        : binds,
      { outFormat: oracledb.OUT_FORMAT_OBJECT }
    );

    const existing = new Set((result.rows || []).map((row) => row.TABLE_NAME));
    const missing = requiredTableNames.filter((tableName) => !existing.has(tableName));

    if (missing.length > 0) {
      throw new Error(
        `Route ${route.name} is missing workload tables: ${missing.join(', ')}. ` +
        `Check the service/PDB, schema prefix${this.config.tableOwner ? `, and table owner ${this.config.tableOwner}` : ''} before starting the run.`
      );
    }
  }

  async stopForReason(reason, options = {}) {
    const { fatal = false } = options;
    if (this.stopPromise) {
      return this.stopPromise;
    }

    if (fatal) {
      this.stats.lastError = reason;
    }
    this.emitStatus(`${fatal ? 'Fatal error' : 'Stopping'}: ${reason}`);
    this.stopPromise = this.stop();
    try {
      await this.stopPromise;
    } finally {
      this.stopPromise = null;
    }
  }

  async start(db, incomingConfig, io) {
    if (this.isRunning) {
      throw new Error('Library Cache Lock workload is already running');
    }

    this.db = db;
    this.io = io;
    this.config = this.normalizeConfig(incomingConfig);
    this.routes = this.config.routes.map((route) => ({ ...route }));
    this.runId = `lcl-${Date.now()}`;
    this.lastRunSummary = null;
    this.latestSample = null;
    this.nextWorkerId = 0;
    this.workerStates = new Map();
    this.awrState = this.createEmptyAwrState();

    await this.validateRoutes();

    const awrTarget = this.getAwrConnectionTarget();
    this.awrState.routeName = awrTarget.routeName;
    this.awrState.connectionString = awrTarget.connectionString;

    this.planRouteSessionTargets();

    this.stats = this.createEmptyStats();
    this.stats.startTime = Date.now();
    this.previousStats = {
      totalTransactions: 0,
      lastTick: Date.now()
    };

    try {
      this.awrState.begin = await this.captureAwrSnapshot();
      this.emitStatus(
        `Captured AWR begin snapshot ${this.awrState.begin.snapId} on ${this.awrState.routeName}`,
        { awr: this.buildAwrSummaryFields() }
      );
    } catch (err) {
      const warning = `AWR begin snapshot failed: ${err.message}`;
      this.awrState.warnings.push(warning);
      console.log(warning);
      this.emitStatus(`${warning}. Continuing workload start.`);
    }

    this.emitStatus('Capturing baseline snapshot...');
    this.runStartSnapshot = await this.captureSystemSnapshot();
    this.lastSampleSnapshot = this.runStartSnapshot;

    await this.createRoutePools();

    this.isRunning = true;
    this.workers = [];

    this.spawnWorkers(this.config.initialSessions);

    this.statsInterval = setInterval(() => this.reportStats(), 1000);
    this.waitEventsInterval = setInterval(
      () => this.captureAndEmitSample(),
      this.config.waitSampleSeconds * 1000
    );
    this.expansionInterval = setInterval(() => this.evaluatePressureAndExpand(), 3000);

    if (this.config.durationMinutes > 0) {
      this.autoStopTimer = setTimeout(() => {
        this.stopForReason(`Configured runtime of ${this.config.durationMinutes} minute(s) reached`).catch((err) => {
          console.log('Auto-stop error:', err.message);
        });
      }, this.config.durationMinutes * 60 * 1000);
    }

    this.emitStatus(
      `Running ${this.config.initialSessions.toLocaleString()} initial session(s), auto-expanding up to ${this.config.maxSessions.toLocaleString()} across ${this.routes.length} route(s)`,
      {
      config: this.config,
      routes: this.describeRoutes(),
      awr: this.buildAwrSummaryFields()
      }
    );
  }

  async validateRoutes() {
    const seen = new Set();

    for (const route of this.routes) {
      const dedupeKey = `${route.connectionString}::${this.qualifyProcedure(route)}`;
      if (seen.has(dedupeKey)) {
        continue;
      }
      seen.add(dedupeKey);

      let connection;
      try {
        connection = await this.db.createDirectConnection({ connectionString: route.connectionString });
        const objectResult = await connection.execute(
          route.procedureOwner
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
            `,
          route.procedureOwner
            ? { owner: route.procedureOwner, name: route.procedureName }
            : { name: route.procedureName },
          { outFormat: oracledb.OUT_FORMAT_OBJECT }
        );

        if (!(objectResult.rows?.[0]?.TOTAL > 0)) {
          throw new Error(`Procedure ${this.qualifyProcedure(route)} not found on ${route.connectionString}`);
        }

        await this.validateWorkloadTables(connection, route);

        try {
          const instanceResult = await connection.execute(
            `SELECT instance_name FROM v$instance`,
            [],
            { outFormat: oracledb.OUT_FORMAT_OBJECT }
          );
          route.instanceName = instanceResult.rows?.[0]?.INSTANCE_NAME || null;
        } catch (err) {
          route.instanceName = null;
        }
      } finally {
        if (connection) {
          try {
            await connection.close();
          } catch (closeErr) {
            // Ignore close failures during validation.
          }
        }
      }
    }
  }

  planRouteSessionTargets() {
    this.routes.forEach((route) => {
      route.stats = this.createRouteStats();
    });

    for (let workerId = 0; workerId < this.config.maxSessions; workerId++) {
      const route = this.routes[workerId % this.routes.length];
      route.stats.maxSessions += 1;
      if (workerId < this.config.initialSessions) {
        route.stats.initialSessions += 1;
      }
    }
  }

  async createRoutePools() {
    for (const route of this.routes) {
      const poolSize = Math.max(2, route.stats.maxSessions + 2);
      try {
        const pool = await this.db.createStressPool(poolSize, { connectionString: route.connectionString });
        this.routePools.set(route.id, pool);
      } catch (err) {
        if (this.isAuthError(err)) {
          throw new Error(`Authentication failed for ${route.name}: ${err.message}`);
        }
        throw err;
      }
    }
  }

  describeRoutes() {
    return this.routes.map((route) => ({
      id: route.id,
      name: route.name,
      connectionString: route.connectionString,
      procedure: this.qualifyProcedure(route),
      assignedWorkers: route.stats?.currentSessions || 0,
      initialSessions: route.stats?.initialSessions || 0,
      maxSessions: route.stats?.maxSessions || 0,
      instanceName: route.instanceName
    }));
  }

  spawnWorkers(count) {
    for (let i = 0; i < count; i++) {
      if (this.nextWorkerId >= this.config.maxSessions) {
        return;
      }

      const workerId = this.nextWorkerId;
      const route = this.routes[workerId % this.routes.length];
      this.nextWorkerId += 1;

      route.stats.currentSessions += 1;
      route.stats.peakSessions = Math.max(route.stats.peakSessions, route.stats.currentSessions);
      this.stats.currentSessions += 1;
      this.stats.peakSessions = Math.max(this.stats.peakSessions, this.stats.currentSessions);

      this.workerStates.set(workerId, {
        workerId,
        routeId: route.id,
        completedTransactions: 0,
        lastSeenCompletedTransactions: 0
      });

      this.workers.push(this.runWorker(workerId));
    }
  }

  evaluatePressureAndExpand() {
    if (!this.isRunning || this.stats.currentSessions >= this.config.maxSessions) {
      return;
    }

    let activeWorkers = 0;
    let busyWorkers = 0;

    for (const workerState of this.workerStates.values()) {
      activeWorkers += 1;
      if (workerState.completedTransactions > workerState.lastSeenCompletedTransactions) {
        busyWorkers += 1;
      }
      workerState.lastSeenCompletedTransactions = workerState.completedTransactions;
    }

    if (activeWorkers === 0) {
      return;
    }

    const utilization = busyWorkers / activeWorkers;
    if (utilization < 0.85) {
      return;
    }

    const remaining = this.config.maxSessions - this.stats.currentSessions;
    const expansionStep = Math.min(remaining, Math.max(1, Math.ceil(activeWorkers * 0.1)));
    this.spawnWorkers(expansionStep);
    this.emitStatus(
      `Pressure detected (${Math.round(utilization * 100)}% busy). Expanded to ${this.stats.currentSessions.toLocaleString()} session(s).`,
      {
        routes: this.describeRoutes()
      }
    );
  }

  async runWorker(workerId) {
    const route = this.routes[workerId % this.routes.length];
    const workerState = this.workerStates.get(workerId);
    let iteration = 0;

    while (this.isRunning) {
      let connection;
      try {
        connection = await this.acquireConnection(route);
        route.stats.totalLogons += 1;
        this.stats.totalLogons += 1;

        while (this.isRunning) {
          iteration += 1;
          const start = process.hrtime.bigint();

          try {
            await this.executeBusinessTransaction(connection, route, workerId, iteration);
            await connection.commit();

            const elapsedMs = Number(process.hrtime.bigint() - start) / 1e6;
            route.stats.totalTransactions += 1;
            route.stats.responseTimes.push(elapsedMs);
            if (route.stats.responseTimes.length > 2000) {
              route.stats.responseTimes.shift();
            }

            this.stats.totalTransactions += 1;
            this.stats.responseTimes.push(elapsedMs);
            if (this.stats.responseTimes.length > 4000) {
              this.stats.responseTimes.shift();
            }
            if (workerState) {
              workerState.completedTransactions += 1;
            }
          } catch (err) {
            route.stats.totalErrors += 1;
            this.stats.totalErrors += 1;
            this.stats.lastError = err.message;

            try {
              await connection.rollback();
            } catch (rollbackErr) {
              // Ignore rollback failures during error handling.
            }

            if (this.isAuthError(err)) {
              await this.stopForReason(`Authentication/account error on ${route.name}: ${err.message}`, { fatal: true });
              return;
            }

            if (this.isSchemaError(err)) {
              await this.stopForReason(`Missing object on ${route.name}: ${err.message}`, { fatal: true });
              return;
            }

            if (this.stats.totalErrors <= 5 || this.stats.totalErrors % 100 === 0) {
              console.log(`Library Cache Lock route ${route.name} worker ${workerId} error:`, err.message);
            }

            if (
              String(err.message).includes('DPI-1010') ||
              String(err.message).includes('NJS-500') ||
              String(err.message).includes('ORA-03113') ||
              String(err.message).includes('ORA-03114') ||
              String(err.message).includes('ORA-12537')
            ) {
              break;
            }
          }

          if (this.isRunning && this.config.loopDelay > 0) {
            await this.sleep(this.config.loopDelay);
          }
        }
      } catch (err) {
        route.stats.totalErrors += 1;
        this.stats.totalErrors += 1;
        this.stats.lastError = err.message;
        if (this.isAuthError(err)) {
          await this.stopForReason(`Authentication/account error on ${route.name}: ${err.message}`, { fatal: true });
          return;
        }
        if (!String(err.message).includes('pool is terminating')) {
          console.log(`Library Cache Lock connection error on ${route.name}:`, err.message);
        }
      } finally {
        if (connection) {
          try {
            await connection.close();
            route.stats.totalLogoffs += 1;
            this.stats.totalLogoffs += 1;
          } catch (closeErr) {
            this.stats.lastError = closeErr.message;
          }
        }
      }

      if (this.isRunning) {
        await this.sleep(100);
      }
    }

    route.stats.currentSessions = Math.max(0, route.stats.currentSessions - 1);
    this.stats.currentSessions = Math.max(0, this.stats.currentSessions - 1);
    this.workerStates.delete(workerId);
  }

  async acquireConnection(route) {
    const pool = this.routePools.get(route.id);
    if (!pool) {
      throw new Error(`No pool available for ${route.name}`);
    }
    try {
      return await pool.getConnection();
    } catch (err) {
      if (this.isAuthError(err)) {
        throw new Error(`Authentication failed on route ${route.name}: ${err.message}`);
      }
      throw err;
    }
  }

  async executeBusinessTransaction(connection, route, workerId, iteration) {
    await connection.execute(
      this.buildAnonymousBlock(route),
      { moduleName: this.buildModuleName(route, workerId, iteration) },
      { autoCommit: false }
    );

    for (let i = 0; i < this.config.selectsPerTxn; i++) {
      await this.performSelect(connection, workerId, i);
    }
    for (let i = 0; i < this.config.insertsPerTxn; i++) {
      await this.performInsert(connection, workerId, iteration, i);
    }
    for (let i = 0; i < this.config.updatesPerTxn; i++) {
      await this.performUpdate(connection, workerId, i);
    }
    for (let i = 0; i < this.config.deletesPerTxn; i++) {
      await this.performDelete(connection, workerId, i);
    }
  }

  async performSelect(connection, workerId = 0, selectIndex = 0) {
    const type = (workerId + selectIndex + Date.now()) % 4;
    const ordersTable = this.getQualifiedTableName('orders');
    const customersTable = this.getQualifiedTableName('customers');
    const inventoryTable = this.getQualifiedTableName('inventory');
    const productsTable = this.getQualifiedTableName('products');
    const warehousesTable = this.getQualifiedTableName('warehouses');
    const orderItemsTable = this.getQualifiedTableName('order_items');

    if (type === 0) {
      await connection.execute(
        `SELECT o.order_id, o.status, o.total_amount, c.customer_id, c.first_name, c.last_name
         FROM ${ordersTable} o
         JOIN ${customersTable} c ON c.customer_id = o.customer_id
         WHERE ROWNUM <= 25`,
        [],
        { outFormat: oracledb.OUT_FORMAT_OBJECT, autoCommit: false }
      );
      return;
    }

    if (type === 1) {
      const customerId = await this.getRandomId(connection, customersTable, 'customer_id');
      if (!customerId) return;

      await connection.execute(
        `SELECT o.order_id, o.order_date, o.status, o.total_amount
         FROM ${ordersTable} o
         WHERE o.customer_id = :customerId
         ORDER BY o.order_date DESC
         FETCH FIRST 20 ROWS ONLY`,
        { customerId },
        { outFormat: oracledb.OUT_FORMAT_OBJECT, autoCommit: false }
      );
      return;
    }

    if (type === 2) {
      await connection.execute(
        `SELECT p.product_id, p.product_name, i.quantity_on_hand, w.warehouse_name
         FROM ${inventoryTable} i
         JOIN ${productsTable} p ON p.product_id = i.product_id
         JOIN ${warehousesTable} w ON w.warehouse_id = i.warehouse_id
         WHERE ROWNUM <= 25`,
        [],
        { outFormat: oracledb.OUT_FORMAT_OBJECT, autoCommit: false }
      );
      return;
    }

    const orderId = await this.getRandomId(connection, ordersTable, 'order_id');
    if (!orderId) return;

    await connection.execute(
      `SELECT oi.order_id, oi.product_id, oi.quantity, oi.unit_price, p.product_name
       FROM ${orderItemsTable} oi
       JOIN ${productsTable} p ON p.product_id = oi.product_id
       WHERE oi.order_id = :orderId`,
      { orderId },
      { outFormat: oracledb.OUT_FORMAT_OBJECT, autoCommit: false }
    );
  }

  async performInsert(connection, workerId = 0, iteration = 0, insertIndex = 0) {
    const type = (workerId + iteration + insertIndex) % 2;
    const ordersTable = this.getQualifiedTableName('orders');
    const orderHistoryTable = this.getQualifiedTableName('order_history');
    const productsTable = this.getQualifiedTableName('products');
    const customersTable = this.getQualifiedTableName('customers');
    const productReviewsTable = this.getQualifiedTableName('product_reviews');

    if (type === 0) {
      const orderId = await this.getWorkerScopedId(connection, ordersTable, 'order_id', workerId, insertIndex);
      if (!orderId) return;

      await connection.execute(
        `INSERT INTO ${orderHistoryTable} (
           order_id,
           old_status,
           new_status,
           changed_by,
           change_reason
         ) VALUES (
           :orderId,
           'PENDING',
           'PROCESSING',
           :changedBy,
           :reason
         )`,
        {
          orderId,
          changedBy: `LCL_${workerId}`,
          reason: `Iteration ${iteration}`
        },
        { autoCommit: false }
      );
      return;
    }

    const productId = await this.getWorkerScopedId(connection, productsTable, 'product_id', workerId, insertIndex);
    const customerId = await this.getWorkerScopedId(connection, customersTable, 'customer_id', workerId, insertIndex + 17);
    if (!productId || !customerId) return;

    await connection.execute(
      `INSERT INTO ${productReviewsTable} (
         product_id,
         customer_id,
         rating,
         review_title,
         review_text,
         is_verified_purchase
       ) VALUES (
         :productId,
         :customerId,
         :rating,
         :title,
         :reviewText,
         1
       )`,
      {
        productId,
        customerId,
        rating: ((workerId + insertIndex) % 5) + 1,
        title: `LCL-${workerId}-${iteration}-${insertIndex}`,
        reviewText: `Library Cache Lock run ${this.runId}`
      },
      { autoCommit: false }
    );
  }

  async performUpdate(connection, workerId = 0, updateIndex = 0) {
    const type = (workerId + updateIndex + Date.now()) % 3;
    const ordersTable = this.getQualifiedTableName('orders');
    const customersTable = this.getQualifiedTableName('customers');
    const inventoryTable = this.getQualifiedTableName('inventory');

    if (type === 0) {
      const orderId = await this.getWorkerScopedId(connection, ordersTable, 'order_id', workerId, updateIndex);
      if (!orderId) return;

      const statuses = ['PENDING', 'PROCESSING', 'SHIPPED', 'DELIVERED', 'CANCELLED'];
      const status = statuses[(workerId + updateIndex) % statuses.length];
      await connection.execute(
        `UPDATE ${ordersTable}
         SET status = :status,
             updated_at = CURRENT_TIMESTAMP
         WHERE order_id = :orderId`,
        { status, orderId },
        { autoCommit: false }
      );
      return;
    }

    if (type === 1) {
      const customerId = await this.getWorkerScopedId(connection, customersTable, 'customer_id', workerId, updateIndex + 31);
      if (!customerId) return;

      await connection.execute(
        `UPDATE ${customersTable}
         SET balance = balance + :delta,
             updated_at = CURRENT_TIMESTAMP
         WHERE customer_id = :customerId`,
        {
          delta: Number((((workerId % 7) - 3) * 5.25).toFixed(2)),
          customerId
        },
        { autoCommit: false }
      );
      return;
    }

    const inventoryId = await this.getWorkerScopedId(connection, inventoryTable, 'inventory_id', workerId, updateIndex + 53);
    if (!inventoryId) return;

    await connection.execute(
      `UPDATE ${inventoryTable}
       SET quantity_on_hand = GREATEST(0, quantity_on_hand + :delta),
           updated_at = CURRENT_TIMESTAMP
       WHERE inventory_id = :inventoryId`,
      {
        delta: ((workerId + updateIndex) % 9) - 4,
        inventoryId
      },
      { autoCommit: false }
    );
  }

  async performDelete(connection, workerId = 0, deleteIndex = 0) {
    const type = (workerId + deleteIndex) % 2;
    const productReviewsTable = this.getQualifiedTableName('product_reviews');
    const orderHistoryTable = this.getQualifiedTableName('order_history');

    if (type === 0) {
      const reviewId = await this.getWorkerScopedId(connection, productReviewsTable, 'review_id', workerId, deleteIndex);
      if (!reviewId) return;

      await connection.execute(
        `DELETE FROM ${productReviewsTable} WHERE review_id = :reviewId`,
        { reviewId },
        { autoCommit: false }
      );
      return;
    }

    const historyId = await this.getWorkerScopedId(connection, orderHistoryTable, 'history_id', workerId, deleteIndex + 19);
    if (!historyId) return;

    await connection.execute(
      `DELETE FROM ${orderHistoryTable} WHERE history_id = :historyId`,
      { historyId },
      { autoCommit: false }
    );
  }

  async getRandomId(connection, tableName, idColumn) {
    try {
      const sampleResult = await connection.execute(
        `SELECT ${idColumn} FROM ${tableName} SAMPLE(1) WHERE ROWNUM = 1`,
        [],
        { outFormat: oracledb.OUT_FORMAT_OBJECT, autoCommit: false }
      );
      const fromSample = sampleResult.rows?.[0]?.[idColumn.toUpperCase()];
      if (fromSample !== undefined && fromSample !== null) {
        return fromSample;
      }

      const fallbackResult = await connection.execute(
        `SELECT MIN(${idColumn}) AS id_value FROM ${tableName}`,
        [],
        { outFormat: oracledb.OUT_FORMAT_OBJECT, autoCommit: false }
      );
      return fallbackResult.rows?.[0]?.ID_VALUE || null;
    } catch (err) {
      return null;
    }
  }

  async getWorkerScopedId(connection, tableName, idColumn, workerId = 0, salt = 0) {
    const shardCounts = [1024, 256, 64, 16, 4];

    for (const shardCount of shardCounts) {
      const bucket = Math.abs(workerId + salt) % shardCount;
      try {
        const scopedResult = await connection.execute(
          `SELECT MIN(${idColumn}) AS id_value
           FROM ${tableName}
           WHERE MOD(${idColumn}, :shardCount) = :bucket`,
          { shardCount, bucket },
          { outFormat: oracledb.OUT_FORMAT_OBJECT, autoCommit: false }
        );

        const scopedId = scopedResult.rows?.[0]?.ID_VALUE;
        if (scopedId !== undefined && scopedId !== null) {
          return scopedId;
        }
      } catch (err) {
        // Fall back to a broader selection if the modulo path is not suitable.
      }
    }

    return this.getRandomId(connection, tableName, idColumn);
  }

  reportStats() {
    const now = Date.now();
    const elapsedSeconds = Math.max(0.001, (now - this.previousStats.lastTick) / 1000);
    const txDelta = this.stats.totalTransactions - this.previousStats.totalTransactions;
    const transactionsPerSecond = txDelta / elapsedSeconds;
    this.stats.transactionsPerSecond = transactionsPerSecond;

    this.previousStats = {
      totalTransactions: this.stats.totalTransactions,
      lastTick: now
    };

    const routeMetrics = this.routes.map((route) => {
      const delta = route.stats.totalTransactions - route.stats.previousTransactions;
      route.stats.previousTransactions = route.stats.totalTransactions;
      route.stats.tps = delta / elapsedSeconds;

      return {
        routeId: route.id,
        name: route.name,
        instanceName: route.instanceName,
        procedure: this.qualifyProcedure(route),
        connectionString: route.connectionString,
        assignedWorkers: route.stats.currentSessions,
        currentSessions: route.stats.currentSessions,
        peakSessions: route.stats.peakSessions,
        initialSessions: route.stats.initialSessions,
        maxSessions: route.stats.maxSessions,
        totalTransactions: route.stats.totalTransactions,
        totalErrors: route.stats.totalErrors,
        transactionsPerSecond: Number(route.stats.tps.toFixed(2)),
        avgTransactionMs: Number(average(route.stats.responseTimes).toFixed(2)),
        totalLogons: route.stats.totalLogons,
        totalLogoffs: route.stats.totalLogoffs
      };
    });

    this.io?.emit('library-cache-lock-metrics', {
      runId: this.runId,
      totalTransactions: this.stats.totalTransactions,
      totalErrors: this.stats.totalErrors,
      totalLogons: this.stats.totalLogons,
      totalLogoffs: this.stats.totalLogoffs,
      currentSessions: this.stats.currentSessions,
      peakSessions: this.stats.peakSessions,
      initialSessions: this.config.initialSessions,
      maxSessions: this.config.maxSessions,
      transactionsPerSecond: Number(transactionsPerSecond.toFixed(2)),
      avgTransactionMs: Number(average(this.stats.responseTimes).toFixed(2)),
      durationSeconds: this.stats.startTime
        ? Math.floor((Date.now() - this.stats.startTime) / 1000)
        : 0,
      loginMode: this.config.loginMode,
      scenario: this.config.scenario,
      routeMetrics,
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
    this.isRunning = false;

    if (this.statsInterval) {
      clearInterval(this.statsInterval);
      this.statsInterval = null;
    }

    if (this.waitEventsInterval) {
      clearInterval(this.waitEventsInterval);
      this.waitEventsInterval = null;
    }

    if (this.expansionInterval) {
      clearInterval(this.expansionInterval);
      this.expansionInterval = null;
    }

    if (this.autoStopTimer) {
      clearTimeout(this.autoStopTimer);
      this.autoStopTimer = null;
    }

    await this.sleep(250);

    let finalSnapshot = null;
    try {
      finalSnapshot = await this.captureSystemSnapshot();
    } catch (err) {
      console.log('Unable to capture final Library Cache Lock snapshot:', err.message);
    }

    try {
      this.awrState.end = await this.captureAwrSnapshot();
    } catch (err) {
      const warning = `AWR end snapshot failed: ${err.message}`;
      this.awrState.warnings.push(warning);
      console.log(warning);
    }

    for (const pool of this.routePools.values()) {
      try {
        await pool.close(2);
      } catch (err) {
        console.log('Library Cache Lock pool close warning:', err.message);
      }
    }
    this.routePools.clear();

    const summary = finalSnapshot && this.runStartSnapshot
      ? this.buildRunSummary(finalSnapshot)
      : this.buildFallbackSummary();

    this.lastRunSummary = summary;

    const payload = {
      runId: this.runId,
      summary,
      stats: {
        totalTransactions: this.stats.totalTransactions,
        totalErrors: this.stats.totalErrors,
        totalLogons: this.stats.totalLogons,
        totalLogoffs: this.stats.totalLogoffs,
        lastError: this.stats.lastError,
        awr: this.buildAwrSummaryFields()
      }
    };

    this.io?.emit('library-cache-lock-stopped', payload);
    this.emitStatus('Stopped', { summary });

    this.runStartSnapshot = null;
    this.lastSampleSnapshot = null;
    this.latestSample = null;
    this.workers = [];
    this.workerStates.clear();
    this.nextWorkerId = 0;

    return payload;
  }

  buildRouteSummaries(durationSeconds) {
    return this.routes.map((route) => ({
      routeId: route.id,
      name: route.name,
      instanceName: route.instanceName,
      connectionString: route.connectionString,
      procedure: this.qualifyProcedure(route),
      assignedWorkers: route.stats.currentSessions,
      currentSessions: route.stats.currentSessions,
      peakSessions: route.stats.peakSessions,
      initialSessions: route.stats.initialSessions,
      maxSessions: route.stats.maxSessions,
      totalTransactions: route.stats.totalTransactions,
      totalErrors: route.stats.totalErrors,
      avgTransactionMs: Number(average(route.stats.responseTimes).toFixed(2)),
      transactionsPerSecond: durationSeconds > 0
        ? Number((route.stats.totalTransactions / durationSeconds).toFixed(2))
        : 0,
      totalLogons: route.stats.totalLogons,
      totalLogoffs: route.stats.totalLogoffs
    }));
  }

  buildFallbackSummary() {
    const durationSeconds = this.stats.startTime
      ? Math.max(0.001, (Date.now() - this.stats.startTime) / 1000)
      : 0;

    return {
      runId: this.runId,
      runLabel: this.config?.runLabel || 'Library Cache Lock',
      scenario: this.config?.scenario || 'single-service',
      loginMode: this.config?.loginMode || 'persistent',
      schemaPrefix: this.config?.schemaPrefix || '',
      tableOwner: this.config?.tableOwner || '',
      durationMinutes: this.config?.durationMinutes || 0,
      initialSessions: this.config?.initialSessions || 0,
      maxSessions: this.config?.maxSessions || 0,
      peakSessions: this.stats.peakSessions,
      startedAt: this.stats.startTime ? new Date(this.stats.startTime).toISOString() : null,
      completedAt: new Date().toISOString(),
      durationSeconds: Number(durationSeconds.toFixed(2)),
      totalTransactions: this.stats.totalTransactions,
      transactionsPerSecond: durationSeconds > 0
        ? Number((this.stats.totalTransactions / durationSeconds).toFixed(2))
        : 0,
      avgTransactionMs: Number(average(this.stats.responseTimes).toFixed(2)),
      totalErrors: this.stats.totalErrors,
      totalLogons: this.stats.totalLogons,
      totalLogoffs: this.stats.totalLogoffs,
      dbCpuSharePct: 0,
      averageActiveSessions: 0,
      commitRatePerSecond: 0,
      parseHardPerSecond: 0,
      userCallsPerSecond: 0,
      executeCountPerSecond: 0,
      keyWaits: [],
      topWaitEvents: [],
      matchedSql: [],
      routes: this.buildRouteSummaries(durationSeconds),
      ...this.buildAwrSummaryFields()
    };
  }

  buildRunSummary(finalSnapshot) {
    const summary = this.computeRunDelta(this.runStartSnapshot, finalSnapshot);
    return {
      runId: this.runId,
      runLabel: this.config.runLabel,
      scenario: this.config.scenario,
      loginMode: this.config.loginMode,
      schemaPrefix: this.config.schemaPrefix,
      tableOwner: this.config.tableOwner,
      durationMinutes: this.config.durationMinutes,
      initialSessions: this.config.initialSessions,
      maxSessions: this.config.maxSessions,
      peakSessions: this.stats.peakSessions,
      startedAt: new Date(this.stats.startTime).toISOString(),
      completedAt: new Date().toISOString(),
      totalTransactions: this.stats.totalTransactions,
      transactionsPerSecond: summary.durationSeconds > 0
        ? Number((this.stats.totalTransactions / summary.durationSeconds).toFixed(2))
        : 0,
      avgTransactionMs: Number(average(this.stats.responseTimes).toFixed(2)),
      totalErrors: this.stats.totalErrors,
      totalLogons: this.stats.totalLogons,
      totalLogoffs: this.stats.totalLogoffs,
      routes: this.buildRouteSummaries(summary.durationSeconds),
      ...this.buildAwrSummaryFields(),
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
      matchedSql: this.computeSqlDelta(startSnapshot.targetSql, endSnapshot.targetSql, durationSeconds)
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

  computeSqlDelta(startMap, endMap, durationSeconds = 0.001) {
    const sqlRows = [];

    for (const [sqlId, current] of endMap.entries()) {
      const previous = startMap.get(sqlId) || {
        executions: 0,
        elapsedTime: 0,
        cpuTime: 0,
        bufferGets: 0,
        diskReads: 0,
        rowsProcessed: 0
      };

      const executions = Math.max(0, current.executions - previous.executions);
      const elapsedSeconds = Math.max(0, current.elapsedTime - previous.elapsedTime) / 1e6;
      const cpuSeconds = Math.max(0, current.cpuTime - previous.cpuTime) / 1e6;
      const bufferGets = Math.max(0, current.bufferGets - previous.bufferGets);
      const diskReads = Math.max(0, current.diskReads - previous.diskReads);
      const rowsProcessed = Math.max(0, current.rowsProcessed - previous.rowsProcessed);

      if (executions <= 0 && elapsedSeconds <= 0 && cpuSeconds <= 0 && bufferGets <= 0 && diskReads <= 0 && rowsProcessed <= 0) {
        continue;
      }

      sqlRows.push({
        sqlId,
        executions,
        execsPerSecond: Number((executions / Math.max(durationSeconds, 0.001)).toFixed(2)),
        elapsedSeconds: Number(elapsedSeconds.toFixed(3)),
        cpuSeconds: Number(cpuSeconds.toFixed(3)),
        avgElapsedMs: executions > 0
          ? Number(((elapsedSeconds * 1000) / executions).toFixed(3))
          : 0,
        avgCpuMs: executions > 0
          ? Number(((cpuSeconds * 1000) / executions).toFixed(3))
          : 0,
        bufferGets,
        diskReads,
        rowsProcessed,
        instanceCount: current.instIds?.size || 0,
        lastActiveTime: current.lastActiveTimeMs
          ? new Date(current.lastActiveTimeMs).toISOString()
          : null,
        sqlText: current.sqlText
      });
    }

    return sqlRows
      .sort((a, b) => (
        b.executions - a.executions ||
        b.elapsedSeconds - a.elapsedSeconds ||
        b.cpuSeconds - a.cpuSeconds
      ))
      .slice(0, 10);
  }

  async captureAwrSnapshot() {
    const target = this.getAwrConnectionTarget();
    let connection;

    try {
      connection = await this.db.createDirectConnection({ connectionString: target.connectionString });

      const dbidResult = await connection.execute(
        `SELECT dbid FROM v$database`,
        [],
        { outFormat: oracledb.OUT_FORMAT_OBJECT }
      );
      const dbid = Number(dbidResult.rows?.[0]?.DBID || 0);

      const beforeResult = await connection.execute(
        `
          SELECT MAX(snap_id) AS snap_id
          FROM dba_hist_snapshot
          WHERE dbid = :dbid
        `,
        { dbid },
        { outFormat: oracledb.OUT_FORMAT_OBJECT }
      );
      const previousSnapId = Number(beforeResult.rows?.[0]?.SNAP_ID || 0);

      await connection.execute(`BEGIN DBMS_WORKLOAD_REPOSITORY.CREATE_SNAPSHOT(); END;`);

      let rows = [];
      for (let attempt = 0; attempt < 5; attempt++) {
        const snapshotRows = await connection.execute(
          `
            SELECT
              snap_id,
              dbid,
              instance_number,
              begin_interval_time,
              end_interval_time
            FROM dba_hist_snapshot
            WHERE dbid = :dbid
              AND snap_id = (
                SELECT MAX(snap_id)
                FROM dba_hist_snapshot
                WHERE dbid = :dbid
                  AND snap_id > :previousSnapId
              )
            ORDER BY instance_number
          `,
          { dbid, previousSnapId },
          { outFormat: oracledb.OUT_FORMAT_OBJECT }
        );

        rows = snapshotRows.rows || [];
        if (rows.length > 0) {
          break;
        }
        await this.sleep(500);
      }

      if (rows.length === 0) {
        throw new Error('Snapshot created, but snap_id could not be resolved from DBA_HIST_SNAPSHOT');
      }

      return {
        snapId: Number(rows[0].SNAP_ID || 0),
        dbid: Number(rows[0].DBID || dbid || 0),
        instanceNumbers: rows
          .map((row) => Number(row.INSTANCE_NUMBER || 0))
          .filter((value) => Number.isFinite(value) && value > 0),
        beginIntervalTime: rows[0].BEGIN_INTERVAL_TIME
          ? new Date(rows[0].BEGIN_INTERVAL_TIME).toISOString()
          : null,
        endIntervalTime: rows[0].END_INTERVAL_TIME
          ? new Date(rows[0].END_INTERVAL_TIME).toISOString()
          : null,
        capturedAt: new Date().toISOString(),
        routeName: target.routeName
      };
    } finally {
      if (connection) {
        try {
          await connection.close();
        } catch (closeErr) {
          // Ignore AWR helper connection close failures.
        }
      }
    }
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
        bufferGets: 0,
        diskReads: 0,
        rowsProcessed: 0,
        lastActiveTimeMs: 0,
        instIds: new Set(),
        sqlText: row.SQL_TEXT
      };
      current.executions += Number(row.EXECUTIONS || 0);
      current.elapsedTime += Number(row.ELAPSED_TIME || 0);
      current.cpuTime += Number(row.CPU_TIME || 0);
      current.bufferGets += Number(row.BUFFER_GETS || 0);
      current.diskReads += Number(row.DISK_READS || 0);
      current.rowsProcessed += Number(row.ROWS_PROCESSED || 0);
      const lastActiveTimeMs = row.LAST_ACTIVE_TIME ? new Date(row.LAST_ACTIVE_TIME).getTime() : 0;
      if (Number.isFinite(lastActiveTimeMs) && lastActiveTimeMs > current.lastActiveTimeMs) {
        current.lastActiveTimeMs = lastActiveTimeMs;
      }
      current.instIds.add(Number(row.INST_ID || 1));
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
    const procedureNames = Array.from(new Set(this.routes.map((route) => route.procedureName))).filter(Boolean);
    if (procedureNames.length === 0) {
      return { rows: [] };
    }

    const bindNames = {};
    const predicates = procedureNames.map((procedureName, index) => {
      const bindName = `likeExpr${index}`;
      bindNames[bindName] = `%${procedureName.toUpperCase()}%`;
      return `UPPER(sql_text) LIKE :${bindName}`;
    });
    const whereClause = predicates.join(' OR ');

    return this.queryWithFallback(
      `
        SELECT * FROM (
          SELECT
            inst_id,
            sql_id,
            executions,
            elapsed_time,
            cpu_time,
            buffer_gets,
            disk_reads,
            rows_processed,
            SUBSTR(sql_text, 1, 200) AS sql_text,
            last_active_time
          FROM gv$sqlstats
          WHERE ${whereClause}
          ORDER BY last_active_time DESC
        )
        WHERE ROWNUM <= 30
      `,
      `
        SELECT * FROM (
          SELECT
            1 AS inst_id,
            sql_id,
            executions,
            elapsed_time,
            cpu_time,
            buffer_gets,
            disk_reads,
            rows_processed,
            SUBSTR(sql_text, 1, 200) AS sql_text,
            last_active_time
          FROM v$sqlstats
          WHERE ${whereClause}
          ORDER BY last_active_time DESC
        )
        WHERE ROWNUM <= 30
      `,
      bindNames
    );
  }

  sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}

module.exports = new LibraryCacheLockEngine();
