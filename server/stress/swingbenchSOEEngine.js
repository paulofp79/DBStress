const fs = require('fs');
const path = require('path');
const oracledb = require('oracledb');

const PROFILE_PATH = path.join(__dirname, '../../swingbench/SOE_Server_Side_V2.xml');

const FIRST_NAMES = ['james', 'maria', 'liam', 'ava', 'noah', 'mia', 'oliver', 'sofia', 'lucas', 'amelia'];
const LAST_NAMES = ['smith', 'johnson', 'brown', 'silva', 'garcia', 'miller', 'rodrigues', 'martin', 'clark', 'young'];
const TOWNS = ['Sao Paulo', 'Austin', 'Lisbon', 'Miami', 'Porto Alegre', 'Dallas', 'Curitiba', 'Seattle'];
const COUNTIES = ['Travis', 'King', 'Orange', 'Cook', 'Broward', 'Maricopa', 'Fulton', 'Suffolk'];
const COUNTRIES = ['Brazil', 'United States', 'Portugal', 'Mexico', 'Canada'];
const NLS_LANGS = ['AMERICAN', 'BRAZILIAN PORTUGUESE', 'SPANISH', 'FRENCH'];
const NLS_TERRITORIES = ['BRAZIL', 'AMERICA', 'PORTUGAL', 'MEXICO', 'CANADA'];

class SwingbenchSOEEngine {
  constructor() {
    this.isRunning = false;
    this.config = null;
    this.pool = null;
    this.io = null;
    this.workers = [];
    this.activeWorkers = 0;
    this.stats = this.initStats();
    this.previousStats = this.initStats();
    this.statsInterval = null;
    this.stopTimer = null;
    this.profileCache = null;
    this.runtimeMetadata = null;
  }

  initStats() {
    return {
      transactions: 0,
      selects: 0,
      inserts: 0,
      updates: 0,
      deletes: 0,
      commits: 0,
      rollbacks: 0,
      sleepMs: 0,
      errors: 0,
      startTime: Date.now(),
      byTransaction: {}
    };
  }

  getStatus() {
    return {
      isRunning: this.isRunning,
      config: this.config,
      stats: this.stats,
      uptime: this.isRunning ? Math.floor((Date.now() - this.stats.startTime) / 1000) : 0
    };
  }

  getTextBetween(xml, tag) {
    const match = xml.match(new RegExp(`<${tag}>([\\s\\S]*?)</${tag}>`, 'i'));
    return match ? match[1].trim() : '';
  }

  parseBoolean(text, fallback = false) {
    if (!text) return fallback;
    return String(text).trim().toLowerCase() === 'true';
  }

  parseProfileXml() {
    const xml = fs.readFileSync(PROFILE_PATH, 'utf8');
    const transactionMatches = [...xml.matchAll(/<Transaction>([\s\S]*?)<\/Transaction>/gi)];

    const transactions = transactionMatches.map((match) => {
      const block = match[1];
      return {
        id: this.getTextBetween(block, 'Id'),
        shortName: this.getTextBetween(block, 'ShortName'),
        className: this.getTextBetween(block, 'ClassName'),
        weight: Number.parseInt(this.getTextBetween(block, 'Weight'), 10) || 0,
        enabled: this.parseBoolean(this.getTextBetween(block, 'Enabled'), true)
      };
    });

    return {
      name: this.getTextBetween(xml, 'Name').replace(/^"+|"+$/g, ''),
      comment: this.getTextBetween(xml, 'Comment'),
      defaults: {
        username: this.getTextBetween(xml, 'UserName') || 'soe',
        password: 'soe',
        connectString: this.getTextBetween(xml, 'ConnectString') || '',
        users: Number.parseInt(this.getTextBetween(xml, 'NumberOfUsers'), 10) || 16,
        minDelay: Number.parseInt(this.getTextBetween(xml, 'MinDelay'), 10) || 0,
        maxDelay: Number.parseInt(this.getTextBetween(xml, 'MaxDelay'), 10) || 0,
        interMinDelay: Number.parseInt(this.getTextBetween(xml, 'InterMinDelay'), 10) || 0,
        interMaxDelay: Number.parseInt(this.getTextBetween(xml, 'InterMaxDelay'), 10) || 0,
        queryTimeout: Number.parseInt(this.getTextBetween(xml, 'QueryTimeout'), 10) || 120,
        logonDelay: Number.parseInt(this.getTextBetween(xml, 'LogonDelay'), 10) || 20,
        maxTransactions: Number.parseInt(this.getTextBetween(xml, 'MaxTransactions'), 10) || -1,
        runTime: this.getTextBetween(xml, 'RunTime') || '0:0',
        transactions
      }
    };
  }

  getProfile() {
    if (!this.profileCache) {
      this.profileCache = this.parseProfileXml();
    }
    return this.profileCache;
  }

  buildDefaultConfig() {
    const profile = this.getProfile();
    return {
      profileName: profile.name,
      profileComment: profile.comment,
      ...profile.defaults,
      profileConnectString: profile.defaults.connectString,
      connectString: ''
    };
  }

  normalizeConfig(config = {}) {
    const defaults = this.buildDefaultConfig();
    const users = Math.max(1, Math.min(200, Number.parseInt(config.users, 10) || defaults.users));
    const minDelay = Math.max(0, Number.parseInt(config.minDelay, 10) || defaults.minDelay);
    const maxDelay = Math.max(minDelay, Number.parseInt(config.maxDelay, 10) || defaults.maxDelay);
    const interMinDelay = Math.max(0, Number.parseInt(config.interMinDelay, 10) || defaults.interMinDelay);
    const interMaxDelay = Math.max(interMinDelay, Number.parseInt(config.interMaxDelay, 10) || defaults.interMaxDelay);
    const queryTimeout = Math.max(1, Number.parseInt(config.queryTimeout, 10) || defaults.queryTimeout);
    const maxTransactions = Number.parseInt(config.maxTransactions, 10);
    const durationSeconds = Math.max(0, Number.parseInt(config.durationSeconds, 10) || 0);
    const transactions = Array.isArray(config.transactions) && config.transactions.length > 0
      ? config.transactions.map((txn) => ({
          ...txn,
          weight: Math.max(0, Number.parseInt(txn.weight, 10) || 0),
          enabled: txn.enabled !== false
        }))
      : defaults.transactions;

    const enabledTransactions = transactions.filter((txn) => txn.enabled && txn.weight > 0);
    if (enabledTransactions.length === 0) {
      throw new Error('Enable at least one Swingbench transaction with weight greater than zero.');
    }

    return {
      ...defaults,
      ...config,
      username: String(config.username || defaults.username).trim(),
      password: String(config.password || defaults.password).trim(),
      connectString: String(config.connectString || '').trim(),
      users,
      minDelay,
      maxDelay,
      interMinDelay,
      interMaxDelay,
      queryTimeout,
      maxTransactions: Number.isFinite(maxTransactions) ? maxTransactions : defaults.maxTransactions,
      durationSeconds,
      transactions
    };
  }

  async loadRuntimeMetadata(connection) {
    const metadata = {
      minCustomerId: 1,
      maxCustomerId: 1000000,
      minWarehouseId: 1,
      maxWarehouseId: 1000,
      salesRepIds: [145, 146, 147, 148, 149, 150]
    };

    try {
      const metaRows = await connection.execute(
        `SELECT metadata_key, metadata_value FROM orderentry_metadata`,
        [],
        { outFormat: oracledb.OUT_FORMAT_OBJECT }
      );
      for (const row of metaRows.rows || []) {
        if (row.METADATA_KEY === 'SOE_MIN_CUSTOMER_ID') metadata.minCustomerId = Number(row.METADATA_VALUE || 1);
        if (row.METADATA_KEY === 'SOE_MAX_CUSTOMER_ID') metadata.maxCustomerId = Number(row.METADATA_VALUE || 1000000);
      }
    } catch (err) {
      // Fall back to defaults.
    }

    try {
      const warehouseResult = await connection.execute(
        `SELECT MIN(warehouse_id) AS min_id, MAX(warehouse_id) AS max_id FROM warehouses`,
        [],
        { outFormat: oracledb.OUT_FORMAT_OBJECT }
      );
      const row = warehouseResult.rows?.[0];
      if (row?.MIN_ID) metadata.minWarehouseId = Number(row.MIN_ID);
      if (row?.MAX_ID) metadata.maxWarehouseId = Number(row.MAX_ID);
    } catch (err) {
      // Fall back to defaults.
    }

    try {
      const salesRepResult = await connection.execute(
        `SELECT DISTINCT sales_rep_id FROM orders WHERE sales_rep_id IS NOT NULL AND ROWNUM <= 100`,
        [],
        { outFormat: oracledb.OUT_FORMAT_OBJECT }
      );
      const ids = (salesRepResult.rows || []).map((row) => Number(row.SALES_REP_ID)).filter(Boolean);
      if (ids.length > 0) {
        metadata.salesRepIds = ids;
      }
    } catch (err) {
      // Fall back to defaults.
    }

    this.runtimeMetadata = metadata;
  }

  randomInt(min, max) {
    return Math.floor(Math.random() * (max - min + 1)) + min;
  }

  randomItem(list) {
    return list[Math.floor(Math.random() * list.length)];
  }

  async ensureSessionState(connection) {
    await connection.execute(
      `BEGIN ORDERENTRY.setPLSQLCOMMIT('true'); END;`,
      [],
      { autoCommit: false }
    );
  }

  selectTransaction() {
    const enabled = this.config.transactions.filter((txn) => txn.enabled && txn.weight > 0);
    const totalWeight = enabled.reduce((sum, txn) => sum + txn.weight, 0);
    const pick = Math.random() * totalWeight;
    let running = 0;

    for (const txn of enabled) {
      running += txn.weight;
      if (pick <= running) {
        return txn;
      }
    }

    return enabled[enabled.length - 1];
  }

  async getRandomCustomer(connection) {
    const customerId = this.randomInt(this.runtimeMetadata.minCustomerId, this.runtimeMetadata.maxCustomerId);
    const result = await connection.execute(
      `
        SELECT customer_id, cust_first_name, cust_last_name
        FROM customers
        WHERE customer_id = :customerId
      `,
      { customerId },
      { outFormat: oracledb.OUT_FORMAT_OBJECT }
    );

    if (result.rows?.length) {
      return result.rows[0];
    }

    const fallback = await connection.execute(
      `
        SELECT customer_id, cust_first_name, cust_last_name
        FROM (
          SELECT customer_id, cust_first_name, cust_last_name
          FROM customers
          ORDER BY dbms_random.value
        )
        WHERE ROWNUM = 1
      `,
      [],
      { outFormat: oracledb.OUT_FORMAT_OBJECT }
    );

    return fallback.rows?.[0] || null;
  }

  async getRandomWarehouse(connection) {
    const warehouseId = this.randomInt(this.runtimeMetadata.minWarehouseId, this.runtimeMetadata.maxWarehouseId);
    const result = await connection.execute(
      `SELECT warehouse_id FROM warehouses WHERE warehouse_id = :warehouseId`,
      { warehouseId },
      { outFormat: oracledb.OUT_FORMAT_OBJECT }
    );

    if (result.rows?.length) {
      return Number(result.rows[0].WAREHOUSE_ID);
    }

    const fallback = await connection.execute(
      `SELECT warehouse_id FROM (SELECT warehouse_id FROM warehouses ORDER BY dbms_random.value) WHERE ROWNUM = 1`,
      [],
      { outFormat: oracledb.OUT_FORMAT_OBJECT }
    );

    return Number(fallback.rows?.[0]?.WAREHOUSE_ID || this.runtimeMetadata.minWarehouseId);
  }

  async executeTransaction(connection, txn) {
    const minDelay = this.config.minDelay;
    const maxDelay = this.config.maxDelay;
    const outBinds = {
      result: { dir: oracledb.BIND_OUT, type: oracledb.STRING, maxSize: 4000 }
    };

    switch (txn.shortName) {
      case 'BP': {
        const customer = await this.getRandomCustomer(connection);
        return connection.execute(
          `BEGIN :result := ORDERENTRY.browseProducts(:customerId, :minDelay, :maxDelay); END;`,
          { ...outBinds, customerId: customer?.CUSTOMER_ID || this.runtimeMetadata.minCustomerId, minDelay, maxDelay },
          { autoCommit: false }
        );
      }
      case 'OP': {
        const customer = await this.getRandomCustomer(connection);
        return connection.execute(
          `BEGIN :result := ORDERENTRY.newOrder(:customerId, :minDelay, :maxDelay); END;`,
          { ...outBinds, customerId: customer?.CUSTOMER_ID || this.runtimeMetadata.minCustomerId, minDelay, maxDelay },
          { autoCommit: false }
        );
      }
      case 'BO': {
        const customer = await this.getRandomCustomer(connection);
        return connection.execute(
          `BEGIN :result := ORDERENTRY.browseAndUpdateOrders(:customerId, :minDelay, :maxDelay); END;`,
          { ...outBinds, customerId: customer?.CUSTOMER_ID || this.runtimeMetadata.minCustomerId, minDelay, maxDelay },
          { autoCommit: false }
        );
      }
      case 'PO': {
        return connection.execute(
          `BEGIN :result := ORDERENTRY.processOrders(:minDelay, :maxDelay); END;`,
          { ...outBinds, minDelay, maxDelay },
          { autoCommit: false }
        );
      }
      case 'NCR': {
        const firstName = this.randomItem(FIRST_NAMES);
        const lastName = this.randomItem(LAST_NAMES);
        return connection.execute(
          `BEGIN :result := ORDERENTRY.newCustomer(:firstName, :lastName, :nlsLang, :nlsTerritory, :town, :county, :country, :minDelay, :maxDelay); END;`,
          {
            ...outBinds,
            firstName,
            lastName,
            nlsLang: this.randomItem(NLS_LANGS),
            nlsTerritory: this.randomItem(NLS_TERRITORIES),
            town: this.randomItem(TOWNS),
            county: this.randomItem(COUNTIES),
            country: this.randomItem(COUNTRIES),
            minDelay,
            maxDelay
          },
          { autoCommit: false }
        );
      }
      case 'UCD': {
        const customer = await this.getRandomCustomer(connection);
        return connection.execute(
          `BEGIN :result := ORDERENTRY.updateCustomerDetails(:firstName, :lastName, :town, :county, :country, :minDelay, :maxDelay); END;`,
          {
            ...outBinds,
            firstName: customer?.CUST_FIRST_NAME || this.randomItem(FIRST_NAMES),
            lastName: customer?.CUST_LAST_NAME || this.randomItem(LAST_NAMES),
            town: this.randomItem(TOWNS),
            county: this.randomItem(COUNTIES),
            country: this.randomItem(COUNTRIES),
            minDelay,
            maxDelay
          },
          { autoCommit: false }
        );
      }
      case 'SQ': {
        const salesRepId = this.randomItem(this.runtimeMetadata.salesRepIds);
        return connection.execute(
          `BEGIN :result := ORDERENTRY.SalesRepsQuery(:salesRepId, :minDelay, :maxDelay); END;`,
          { ...outBinds, salesRepId, minDelay, maxDelay },
          { autoCommit: false }
        );
      }
      case 'WQ': {
        const warehouseId = await this.getRandomWarehouse(connection);
        return connection.execute(
          `BEGIN :result := ORDERENTRY.WarehouseOrdersQuery(:warehouseId, :minDelay, :maxDelay); END;`,
          { ...outBinds, warehouseId, minDelay, maxDelay },
          { autoCommit: false }
        );
      }
      case 'WA': {
        const warehouseId = await this.getRandomWarehouse(connection);
        return connection.execute(
          `BEGIN :result := ORDERENTRY.WarehouseActivityQuery(:warehouseId, :minDelay, :maxDelay); END;`,
          { ...outBinds, warehouseId, minDelay, maxDelay },
          { autoCommit: false }
        );
      }
      default:
        throw new Error(`Unsupported Swingbench transaction '${txn.shortName}'.`);
    }
  }

  applyDmlResult(txn, resultString) {
    const parts = String(resultString || '').split(',').map((value) => Number.parseInt(value, 10) || 0);
    const [selects, inserts, updates, deletes, commits, rollbacks, sleepMs] = parts;
    this.stats.transactions += 1;
    this.stats.selects += selects;
    this.stats.inserts += inserts;
    this.stats.updates += updates;
    this.stats.deletes += deletes;
    this.stats.commits += commits;
    this.stats.rollbacks += rollbacks;
    this.stats.sleepMs += sleepMs;

    if (!this.stats.byTransaction[txn.shortName]) {
      this.stats.byTransaction[txn.shortName] = { count: 0, name: txn.id };
    }
    this.stats.byTransaction[txn.shortName].count += 1;
  }

  async runWorker(workerId) {
    this.activeWorkers += 1;

    try {
      while (this.isRunning) {
        let connection;
        try {
          connection = await this.pool.getConnection();
          await this.ensureSessionState(connection);

          const txn = this.selectTransaction();
          const result = await this.executeTransaction(connection, txn);
          const resultString = result.outBinds?.result || '';
          this.applyDmlResult(txn, resultString);

          if (this.config.maxTransactions > 0 && this.stats.transactions >= this.config.maxTransactions) {
            this.stop().catch(() => {});
            return;
          }
        } catch (error) {
          this.stats.errors += 1;
          if (!String(error.message || '').includes('pool is closing')) {
            console.log(`Swingbench SOE worker ${workerId} error: ${error.message}`);
          }
        } finally {
          if (connection) {
            try {
              await connection.close();
            } catch (closeError) {
              // Ignore close errors.
            }
          }
        }

        const interDelay = this.randomInt(this.config.interMinDelay, this.config.interMaxDelay);
        if (this.isRunning && interDelay > 0) {
          await new Promise((resolve) => setTimeout(resolve, interDelay));
        }
      }
    } finally {
      this.activeWorkers -= 1;
    }
  }

  reportStats() {
    const elapsedSeconds = Math.max(1, (Date.now() - this.previousStats.startTime) / 1000);
    const deltaTransactions = this.stats.transactions - this.previousStats.transactions;
    const deltaSelects = this.stats.selects - this.previousStats.selects;
    const deltaInserts = this.stats.inserts - this.previousStats.inserts;
    const deltaUpdates = this.stats.updates - this.previousStats.updates;
    const deltaDeletes = this.stats.deletes - this.previousStats.deletes;
    const deltaErrors = this.stats.errors - this.previousStats.errors;

    const payload = {
      tps: Number((deltaTransactions / elapsedSeconds).toFixed(2)),
      total: {
        transactions: this.stats.transactions,
        selects: this.stats.selects,
        inserts: this.stats.inserts,
        updates: this.stats.updates,
        deletes: this.stats.deletes,
        commits: this.stats.commits,
        rollbacks: this.stats.rollbacks,
        errors: this.stats.errors
      },
      perSecond: {
        transactions: Number((deltaTransactions / elapsedSeconds).toFixed(2)),
        selects: Number((deltaSelects / elapsedSeconds).toFixed(2)),
        inserts: Number((deltaInserts / elapsedSeconds).toFixed(2)),
        updates: Number((deltaUpdates / elapsedSeconds).toFixed(2)),
        deletes: Number((deltaDeletes / elapsedSeconds).toFixed(2)),
        errors: Number((deltaErrors / elapsedSeconds).toFixed(2))
      },
      byTransaction: this.stats.byTransaction,
      uptime: Math.floor((Date.now() - this.stats.startTime) / 1000),
      activeWorkers: this.activeWorkers,
      config: this.config
    };

    if (this.io) {
      this.io.emit('swingbench-soe-metrics', payload);
      this.io.emit('swingbench-soe-status', {
        isRunning: this.isRunning,
        config: this.config,
        uptime: payload.uptime,
        activeWorkers: this.activeWorkers
      });
    }

    this.previousStats = {
      ...this.stats,
      startTime: Date.now(),
      byTransaction: { ...this.stats.byTransaction }
    };
  }

  async start(db, config = {}, io) {
    if (this.isRunning) {
      throw new Error('Swingbench SOE workload is already running.');
    }

    this.config = this.normalizeConfig(config);
    const currentConnectionString = db.getCredentials().connectionString;
    const effectiveConnectString = this.config.connectString || currentConnectionString;
    this.config.connectString = effectiveConnectString;
    this.io = io;
    this.stats = this.initStats();
    this.previousStats = { ...this.stats, byTransaction: {} };
    this.isRunning = true;

    const bootstrapConnection = await db.createDirectConnection({
      user: this.config.username,
      password: this.config.password,
      connectionString: effectiveConnectString
    });

    try {
      await this.ensureSessionState(bootstrapConnection);
      await this.loadRuntimeMetadata(bootstrapConnection);
    } finally {
      await bootstrapConnection.close();
    }

    this.pool = await db.createStressPool(this.config.users, {
      user: this.config.username,
      password: this.config.password,
      connectionString: effectiveConnectString
    });

    this.workers = Array.from({ length: this.config.users }, (_, index) => this.runWorker(index));
    this.statsInterval = setInterval(() => this.reportStats(), 1000);

    if (this.config.durationSeconds > 0) {
      this.stopTimer = setTimeout(() => {
        this.stop().catch((error) => console.log('Swingbench SOE auto-stop error:', error.message));
      }, this.config.durationSeconds * 1000);
    }

    return this.getStatus();
  }

  async stop() {
    if (!this.isRunning) {
      return this.getStatus();
    }

    this.isRunning = false;

    if (this.stopTimer) {
      clearTimeout(this.stopTimer);
      this.stopTimer = null;
    }

    if (this.statsInterval) {
      clearInterval(this.statsInterval);
      this.statsInterval = null;
    }

    if (this.pool) {
      try {
        await this.pool.close(10);
      } catch (error) {
        console.log('Swingbench SOE pool close error:', error.message);
      } finally {
        this.pool = null;
      }
    }

    if (this.io) {
      this.io.emit('swingbench-soe-status', {
        isRunning: false,
        config: this.config,
        uptime: Math.floor((Date.now() - this.stats.startTime) / 1000),
        activeWorkers: this.activeWorkers
      });
    }

    return this.getStatus();
  }
}

module.exports = new SwingbenchSOEEngine();
