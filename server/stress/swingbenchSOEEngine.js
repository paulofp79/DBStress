const fs = require('fs');
const path = require('path');
const oracledb = require('oracledb');

const PROFILE_PATH = path.join(__dirname, '../../swingbench/SOE_Server_Side_V2.xml');

const FIRST_NAMES = ['james', 'maria', 'liam', 'ava', 'noah', 'mia', 'oliver', 'sofia', 'lucas', 'amelia'];
const LAST_NAMES = ['smith', 'johnson', 'brown', 'silva', 'garcia', 'miller', 'rodrigues', 'martin', 'clark', 'young'];
const TOWNS = ['Sao Paulo', 'Austin', 'Lisbon', 'Miami', 'Porto Alegre', 'Dallas', 'Curitiba', 'Seattle'];
const COUNTIES = ['Travis', 'King', 'Orange', 'Cook', 'Broward', 'Maricopa', 'Fulton', 'Suffolk'];
const COUNTRIES = ['Brazil', 'United States', 'Portugal', 'Mexico', 'Canada'];
const NLS_LANGS = ['US', 'BR', 'ES', 'FR', 'DE', 'UK'];
const NLS_TERRITORIES = ['BRAZIL', 'AMERICA', 'PORTUGAL', 'MEXICO', 'CANADA'];

const WORKLOAD_DEFINITIONS = [
  {
    id: 'Customer Registration',
    shortName: 'NCR',
    className: 'com.dom.benchmarking.swingbench.benchmarks.orderentryjdbc.NewCustomerProcess',
    weight: 15,
    enabled: true
  },
  {
    id: 'Browse Products',
    shortName: 'BP',
    className: 'com.dom.benchmarking.swingbench.benchmarks.orderentryjdbc.BrowseProducts',
    weight: 50,
    enabled: true
  },
  {
    id: 'Order Products',
    shortName: 'OP',
    className: 'com.dom.benchmarking.swingbench.benchmarks.orderentryjdbc.NewOrderProcess',
    weight: 40,
    enabled: true
  },
  {
    id: 'Process Orders',
    shortName: 'PO',
    className: 'com.dom.benchmarking.swingbench.benchmarks.orderentryjdbc.ProcessOrders',
    weight: 5,
    enabled: true
  },
  {
    id: 'Browse Orders',
    shortName: 'BO',
    className: 'com.dom.benchmarking.swingbench.benchmarks.orderentryjdbc.BrowseAndUpdateOrders',
    weight: 5,
    enabled: true
  },
  {
    id: 'Sales Rep Query',
    shortName: 'SQ',
    className: 'com.dom.benchmarking.swingbench.benchmarks.orderentryjdbc.SalesRepsOrdersQuery',
    weight: 2,
    enabled: true
  },
  {
    id: 'Warehouse Query',
    shortName: 'WQ',
    className: 'com.dom.benchmarking.swingbench.benchmarks.orderentryjdbc.WarehouseOrdersQuery',
    weight: 2,
    enabled: true
  }
];

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
      uptime: this.stats?.startTime ? Math.floor((Date.now() - this.stats.startTime) / 1000) : 0
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
        runTime: this.getTextBetween(xml, 'RunTime') || '0:0'
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
      profileName: `${profile.name} JDBC`,
      profileComment: 'JDBC-style SOE workload built from the bundled Swingbench orderentryjdbc Java sources.',
      ...profile.defaults,
      profileConnectString: profile.defaults.connectString,
      connectString: '',
      transactions: WORKLOAD_DEFINITIONS.map((txn) => ({ ...txn }))
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
      throw new Error('Enable at least one Swingbench JDBC transaction with weight greater than zero.');
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
      minOrderId: 1,
      maxOrderId: 146610,
      minProductId: 1,
      maxProductId: 1000,
      minCategoryId: 1,
      maxCategoryId: 199,
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
        if (row.METADATA_KEY === 'SOE_MIN_ORDER_ID') metadata.minOrderId = Number(row.METADATA_VALUE || 1);
        if (row.METADATA_KEY === 'SOE_MAX_ORDER_ID') metadata.maxOrderId = Number(row.METADATA_VALUE || 146610);
      }
    } catch (err) {
      // Fall back to table probes below.
    }

    try {
      const customerRange = await connection.execute(
        `SELECT MIN(customer_id) AS min_id, MAX(customer_id) AS max_id FROM customers`,
        [],
        { outFormat: oracledb.OUT_FORMAT_OBJECT }
      );
      const row = customerRange.rows?.[0];
      if (row?.MIN_ID) metadata.minCustomerId = Number(row.MIN_ID);
      if (row?.MAX_ID) metadata.maxCustomerId = Number(row.MAX_ID);
    } catch (err) {
      // Fall back to defaults.
    }

    try {
      const orderRange = await connection.execute(
        `SELECT MIN(order_id) AS min_id, MAX(order_id) AS max_id FROM orders`,
        [],
        { outFormat: oracledb.OUT_FORMAT_OBJECT }
      );
      const row = orderRange.rows?.[0];
      if (row?.MIN_ID) metadata.minOrderId = Number(row.MIN_ID);
      if (row?.MAX_ID) metadata.maxOrderId = Number(row.MAX_ID);
    } catch (err) {
      // Fall back to defaults.
    }

    try {
      const warehouseRange = await connection.execute(
        `SELECT MIN(warehouse_id) AS min_id, MAX(warehouse_id) AS max_id FROM warehouses`,
        [],
        { outFormat: oracledb.OUT_FORMAT_OBJECT }
      );
      const row = warehouseRange.rows?.[0];
      if (row?.MIN_ID) metadata.minWarehouseId = Number(row.MIN_ID);
      if (row?.MAX_ID) metadata.maxWarehouseId = Number(row.MAX_ID);
    } catch (err) {
      // Fall back to defaults.
    }

    try {
      const productRange = await connection.execute(
        `SELECT MIN(product_id) AS min_id, MAX(product_id) AS max_id, MIN(category_id) AS min_category_id, MAX(category_id) AS max_category_id FROM products`,
        [],
        { outFormat: oracledb.OUT_FORMAT_OBJECT }
      );
      const row = productRange.rows?.[0];
      if (row?.MIN_ID) metadata.minProductId = Number(row.MIN_ID);
      if (row?.MAX_ID) metadata.maxProductId = Number(row.MAX_ID);
      if (row?.MIN_CATEGORY_ID) metadata.minCategoryId = Number(row.MIN_CATEGORY_ID);
      if (row?.MAX_CATEGORY_ID) metadata.maxCategoryId = Number(row.MAX_CATEGORY_ID);
    } catch (err) {
      // Fall back to defaults.
    }

    try {
      const salesRepResult = await connection.execute(
        `SELECT DISTINCT sales_rep_id FROM orders WHERE sales_rep_id IS NOT NULL AND ROWNUM <= 250`,
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
    const safeMin = Number.isFinite(min) ? min : 0;
    const safeMax = Number.isFinite(max) ? max : safeMin;
    if (safeMax <= safeMin) {
      return safeMin;
    }
    return Math.floor(Math.random() * (safeMax - safeMin + 1)) + safeMin;
  }

  randomItem(list) {
    if (!Array.isArray(list) || list.length === 0) {
      return null;
    }
    return list[Math.floor(Math.random() * list.length)];
  }

  createCounters() {
    return {
      selects: 0,
      inserts: 0,
      updates: 0,
      deletes: 0,
      commits: 0,
      rollbacks: 0,
      sleepMs: 0
    };
  }

  async sleepWithinTransaction(counters) {
    const delayMs = this.randomInt(this.config.minDelay, this.config.maxDelay);
    if (delayMs > 0) {
      await new Promise((resolve) => setTimeout(resolve, delayMs));
      counters.sleepMs += delayMs;
    }
  }

  async sleepBetweenTransactions() {
    const interDelay = this.randomInt(this.config.interMinDelay, this.config.interMaxDelay);
    if (interDelay > 0) {
      await new Promise((resolve) => setTimeout(resolve, interDelay));
    }
  }

  async prepareConnection(connection, workerId) {
    if (typeof connection.callTimeout !== 'undefined') {
      connection.callTimeout = this.config.queryTimeout * 1000;
    }

    try {
      connection.module = 'DBSTRESS_SWINGBENCH_SOE';
      connection.action = `W${workerId}`;
      connection.clientId = `SOE:W${workerId}`;
    } catch (err) {
      // Best effort only.
    }
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

  async fetchOne(connection, sql, binds = {}) {
    const result = await connection.execute(sql, binds, { outFormat: oracledb.OUT_FORMAT_OBJECT, autoCommit: false });
    return result.rows?.[0] || null;
  }

  async fetchAll(connection, sql, binds = {}) {
    const result = await connection.execute(sql, binds, { outFormat: oracledb.OUT_FORMAT_OBJECT, autoCommit: false });
    return result.rows || [];
  }

  async logon(connection, customerId, counters) {
    await connection.execute(
      `INSERT INTO logon (logon_id, customer_id, logon_date) VALUES (logon_seq.nextval, :customerId, TRUNC(SYSDATE))`,
      { customerId },
      { autoCommit: false }
    );
    counters.inserts += 1;
    await connection.commit();
    counters.commits += 1;
  }

  async getRandomCustomer(connection, counters = null) {
    const customerId = this.randomInt(this.runtimeMetadata.minCustomerId, this.runtimeMetadata.maxCustomerId);
    const candidate = await this.fetchOne(
      connection,
      `
        SELECT customer_id, cust_first_name, cust_last_name
        FROM customers
        WHERE customer_id = :customerId
      `,
      { customerId }
    );
    if (counters) counters.selects += 1;

    if (candidate) {
      return candidate;
    }

    const fallback = await this.fetchOne(
      connection,
      `
        SELECT customer_id, cust_first_name, cust_last_name
        FROM (
          SELECT customer_id, cust_first_name, cust_last_name
          FROM customers
          ORDER BY dbms_random.value
        )
        WHERE ROWNUM = 1
      `
    );
    if (counters) counters.selects += 1;
    return fallback;
  }

  async getRandomWarehouse(connection, counters = null) {
    const warehouseId = this.randomInt(this.runtimeMetadata.minWarehouseId, this.runtimeMetadata.maxWarehouseId);
    const candidate = await this.fetchOne(
      connection,
      `SELECT warehouse_id FROM warehouses WHERE warehouse_id = :warehouseId`,
      { warehouseId }
    );
    if (counters) counters.selects += 1;

    if (candidate?.WAREHOUSE_ID) {
      return Number(candidate.WAREHOUSE_ID);
    }

    const fallback = await this.fetchOne(
      connection,
      `SELECT warehouse_id FROM (SELECT warehouse_id FROM warehouses ORDER BY dbms_random.value) WHERE ROWNUM = 1`
    );
    if (counters) counters.selects += 1;
    return Number(fallback?.WAREHOUSE_ID || this.runtimeMetadata.minWarehouseId);
  }

  async getCustomerDetails(connection, customerId, counters) {
    await connection.execute(
      `
        SELECT CUSTOMER_ID, CUST_FIRST_NAME, CUST_LAST_NAME, NLS_LANGUAGE,
               NLS_TERRITORY, CREDIT_LIMIT, CUST_EMAIL, ACCOUNT_MGR_ID, CUSTOMER_SINCE,
               CUSTOMER_CLASS, SUGGESTIONS, DOB, MAILSHOT, PARTNER_MAILSHOT,
               PREFERRED_ADDRESS, PREFERRED_CARD
        FROM customers
        WHERE customer_id = :customerId
          AND ROWNUM < 5
      `,
      { customerId },
      { autoCommit: false }
    );
    counters.selects += 1;
  }

  async getAddressDetails(connection, customerId, counters) {
    await connection.execute(
      `
        SELECT address_id, customer_id, date_created, house_no_or_name, street_name,
               town, county, country, post_code, zip_code
        FROM addresses
        WHERE customer_id = :customerId
          AND ROWNUM < 5
      `,
      { customerId },
      { autoCommit: false }
    );
    counters.selects += 1;
  }

  async getCardDetails(connection, customerId, counters) {
    await connection.execute(
      `
        SELECT card_id, customer_id, card_type, card_number, expiry_date, is_valid, security_code
        FROM card_details
        WHERE customer_id = :customerId
          AND ROWNUM < 5
      `,
      { customerId },
      { autoCommit: false }
    );
    counters.selects += 1;
  }

  async getOrdersByCustomer(connection, customerId, counters) {
    const rows = await this.fetchAll(
      connection,
      `
        SELECT order_id
        FROM orders
        WHERE customer_id = :customerId
          AND ROWNUM < 5
      `,
      { customerId }
    );
    counters.selects += 1;
    return rows.map((row) => Number(row.ORDER_ID)).filter(Boolean);
  }

  async getProductDetailsById(connection, productId, counters) {
    const row = await this.fetchOne(
      connection,
      `
        SELECT products.product_id, products.list_price
        FROM products, inventories
        WHERE inventories.product_id = products.product_id
          AND products.product_id = :productId
          AND ROWNUM < 15
      `,
      { productId }
    );
    counters.selects += 1;
    return Number(row?.LIST_PRICE || 0);
  }

  async getProductDetailsByCategory(connection, categoryId, counters) {
    const warehouseId = await this.getRandomWarehouse(connection, counters);
    const rows = await this.fetchAll(
      connection,
      `
        SELECT products.product_id, products.list_price, inventories.quantity_on_hand
        FROM products, inventories
        WHERE products.category_id = :categoryId
          AND inventories.product_id = products.product_id
          AND inventories.warehouse_id = :warehouseId
          AND ROWNUM < 4
      `,
      { categoryId, warehouseId }
    );
    counters.selects += 1;

    return rows.map((row) => ({
      productId: Number(row.PRODUCT_ID),
      warehouseId,
      quantityAvailable: Number(row.QUANTITY_ON_HAND || 0),
      price: Number(row.LIST_PRICE || 0)
    }));
  }

  applyDmlResult(txn, counters) {
    this.stats.transactions += 1;
    this.stats.selects += counters.selects;
    this.stats.inserts += counters.inserts;
    this.stats.updates += counters.updates;
    this.stats.deletes += counters.deletes;
    this.stats.commits += counters.commits;
    this.stats.rollbacks += counters.rollbacks;
    this.stats.sleepMs += counters.sleepMs;

    if (!this.stats.byTransaction[txn.shortName]) {
      this.stats.byTransaction[txn.shortName] = {
        count: 0,
        name: txn.id,
        className: txn.className
      };
    }

    this.stats.byTransaction[txn.shortName].count += 1;
  }

  async executeNewCustomerProcess(connection) {
    const counters = this.createCounters();
    const firstName = this.randomItem(FIRST_NAMES);
    const lastName = this.randomItem(LAST_NAMES);
    const town = this.randomItem(TOWNS);
    const county = this.randomItem(COUNTIES);
    const country = this.randomItem(COUNTRIES);
    const nlsLang = this.randomItem(NLS_LANGS);
    const nlsTerritory = this.randomItem(NLS_TERRITORIES);

    const seqRow = await this.fetchOne(
      connection,
      `SELECT customer_seq.nextval AS customer_id, address_seq.nextval AS address_id, card_details_seq.nextval AS card_id FROM dual`
    );
    counters.selects += 1;

    const customerId = Number(seqRow.CUSTOMER_ID);
    const addressId = Number(seqRow.ADDRESS_ID);
    const cardId = Number(seqRow.CARD_ID);

    await this.sleepWithinTransaction(counters);

    await connection.execute(
      `
        INSERT INTO customers (
          customer_id, cust_first_name, cust_last_name, nls_language, nls_territory, credit_limit,
          cust_email, account_mgr_id, customer_since, customer_class, suggestions, dob, mailshot,
          partner_mailshot, preferred_address, preferred_card
        ) VALUES (
          :customerId, :firstName, :lastName, :nlsLang, :nlsTerritory, :creditLimit,
          :email, :accountMgrId, :customerSince, 'Ocasional', 'Music', :dob, 'Y', 'N', :addressId, :cardId
        )
      `,
      {
        customerId,
        firstName,
        lastName,
        nlsLang,
        nlsTerritory,
        creditLimit: this.randomInt(100, 5000),
        email: `${firstName}.${lastName}@oracle.com`,
        accountMgrId: this.randomInt(145, 171),
        customerSince: new Date(Date.now() - (this.randomInt(1, 4) * 31556952000)),
        dob: new Date(Date.now() - (this.randomInt(18, 65) * 31556952000)),
        addressId,
        cardId
      },
      { autoCommit: false }
    );
    counters.inserts += 1;

    await connection.execute(
      `
        INSERT INTO addresses (
          address_id, customer_id, date_created, house_no_or_name, street_name, town,
          county, country, post_code, zip_code
        ) VALUES (
          :addressId, :customerId, TRUNC(SYSDATE, 'MI'), :houseNo, 'Street Name', :town,
          :county, :country, 'Postcode', NULL
        )
      `,
      {
        addressId,
        customerId,
        houseNo: this.randomInt(1, 200),
        town,
        county,
        country
      },
      { autoCommit: false }
    );
    counters.inserts += 1;

    await connection.execute(
      `
        INSERT INTO card_details (
          card_id, customer_id, card_type, card_number, expiry_date, is_valid, security_code
        ) VALUES (
          :cardId, :customerId, 'Visa (Debit)', :cardNumber, TRUNC(SYSDATE + :expiryDays), 'Y', :securityCode
        )
      `,
      {
        cardId,
        customerId,
        cardNumber: String(this.randomInt(111111111, 999999999)) + String(this.randomInt(1111, 9999)),
        expiryDays: this.randomInt(365, 1460),
        securityCode: this.randomInt(1111, 9999)
      },
      { autoCommit: false }
    );
    counters.inserts += 1;

    await connection.commit();
    counters.commits += 1;

    await this.sleepWithinTransaction(counters);
    await this.logon(connection, customerId, counters);
    await this.getCustomerDetails(connection, customerId, counters);

    return counters;
  }

  async executeBrowseProducts(connection) {
    const counters = this.createCounters();
    const customer = await this.getRandomCustomer(connection, counters);

    if (customer?.CUSTOMER_ID) {
      await this.getCustomerDetails(connection, Number(customer.CUSTOMER_ID), counters);
      await this.sleepWithinTransaction(counters);
    }

    const browseCount = this.randomInt(1, Math.min(24, this.runtimeMetadata.maxCategoryId));
    for (let index = 0; index < browseCount; index += 1) {
      await this.getProductDetailsById(
        connection,
        this.randomInt(this.runtimeMetadata.minProductId, this.runtimeMetadata.maxProductId),
        counters
      );
      await this.sleepWithinTransaction(counters);
    }

    return counters;
  }

  async executeNewOrderProcess(connection) {
    const counters = this.createCounters();
    const customer = await this.getRandomCustomer(connection, counters);
    if (!customer?.CUSTOMER_ID) {
      return counters;
    }

    const customerId = Number(customer.CUSTOMER_ID);
    await this.logon(connection, customerId, counters);
    await this.getCustomerDetails(connection, customerId, counters);
    await this.getAddressDetails(connection, customerId, counters);
    await this.getCardDetails(connection, customerId, counters);
    await this.sleepWithinTransaction(counters);

    let productOrders = [];
    const browseCount = this.randomInt(1, Math.min(24, this.runtimeMetadata.maxCategoryId));
    for (let index = 0; index < browseCount; index += 1) {
      productOrders = await this.getProductDetailsByCategory(
        connection,
        this.randomInt(this.runtimeMetadata.minCategoryId, this.runtimeMetadata.maxCategoryId),
        counters
      );
      await this.sleepWithinTransaction(counters);
    }

    if (productOrders.length === 0) {
      return counters;
    }

    const seqRow = await this.fetchOne(connection, `SELECT orders_seq.nextval AS order_id FROM dual`);
    counters.selects += 1;
    const orderId = Number(seqRow.ORDER_ID);
    const warehouseId = await this.getRandomWarehouse(connection, counters);

    await connection.execute(
      `
        INSERT INTO orders (
          order_id, order_date, customer_id, warehouse_id, delivery_type, cost_of_delivery, wait_till_all_available
        ) VALUES (
          :orderId, :orderDate, :customerId, :warehouseId, 'Standard', :costOfDelivery, 'ship_asap'
        )
      `,
      {
        orderId,
        orderDate: new Date(),
        customerId,
        warehouseId,
        costOfDelivery: this.randomInt(1, 5)
      },
      { autoCommit: false }
    );
    counters.inserts += 1;
    await this.sleepWithinTransaction(counters);

    const maxItems = Math.max(1, productOrders.length);
    const itemsToBuy = Math.min(maxItems, this.randomInt(1, maxItems));
    let totalOrderCost = 0;

    for (let lineItemId = 0; lineItemId < itemsToBuy; lineItemId += 1) {
      const product = productOrders[lineItemId];
      const price = Number(product.price || 0);

      if (product.quantityAvailable > 0) {
        await connection.execute(
          `
            INSERT INTO order_items (
              order_id, line_item_id, product_id, unit_price, quantity, gift_wrap, condition, estimated_delivery
            ) VALUES (
              :orderId, :lineItemId, :productId, :unitPrice, 1, 'None', 'New', SYSDATE + 3
            )
          `,
          {
            orderId,
            lineItemId,
            productId: product.productId,
            unitPrice: price
          },
          { autoCommit: false }
        );
        counters.inserts += 1;
      }

      totalOrderCost += price;
      await this.sleepWithinTransaction(counters);

      await connection.execute(
        `
          UPDATE inventories
          SET quantity_on_hand = GREATEST(quantity_on_hand - 1, 0)
          WHERE product_id = :productId
            AND warehouse_id = :warehouseId
        `,
        {
          productId: product.productId,
          warehouseId: product.warehouseId
        },
        { autoCommit: false }
      );
      counters.updates += 1;
    }

    await connection.execute(
      `
        UPDATE orders
        SET order_mode = 'online',
            order_status = :orderStatus,
            order_total = :orderTotal
        WHERE order_id = :orderId
      `,
      {
        orderStatus: this.randomInt(0, 4),
        orderTotal: totalOrderCost,
        orderId
      },
      { autoCommit: false }
    );
    counters.updates += 1;

    await connection.commit();
    counters.commits += 1;

    return counters;
  }

  async executeProcessOrders(connection) {
    const counters = this.createCounters();
    const row = await this.fetchOne(
      connection,
      `
        WITH need_to_process AS (
          SELECT order_id, customer_id
          FROM orders
          WHERE order_status <= 4
            AND ROWNUM < 10
        )
        SELECT o.order_id
        FROM orders o,
             need_to_process ntp,
             customers c,
             order_items oi
        WHERE ntp.order_id = o.order_id
          AND c.customer_id = o.customer_id
          AND oi.order_id (+) = o.order_id
          AND ROWNUM = 1
      `
    );
    counters.selects += 1;

    if (!row?.ORDER_ID) {
      return counters;
    }

    await this.sleepWithinTransaction(counters);
    await connection.execute(
      `
        UPDATE orders
        SET order_status = :orderStatus
        WHERE order_id = :orderId
      `,
      {
        orderStatus: this.randomInt(5, 10),
        orderId: Number(row.ORDER_ID)
      },
      { autoCommit: false }
    );
    counters.updates += 1;
    await connection.commit();
    counters.commits += 1;

    return counters;
  }

  async executeSalesRepsOrdersQuery(connection) {
    const counters = this.createCounters();
    const salesRepId = this.randomItem(this.runtimeMetadata.salesRepIds) || this.randomInt(1, 1000);

    await connection.execute(
      `
        SELECT tt.order_total,
               tt.sales_rep_id,
               tt.order_date,
               customers.cust_first_name,
               customers.cust_last_name
        FROM (
          SELECT orders.order_total,
                 orders.sales_rep_id,
                 orders.order_date,
                 orders.customer_id,
                 RANK() OVER (ORDER BY orders.order_total DESC) sal_rank
          FROM orders
          WHERE orders.sales_rep_id = :salesRepId
        ) tt,
        customers
        WHERE tt.sal_rank <= 10
          AND customers.customer_id = tt.customer_id
      `,
      { salesRepId },
      { autoCommit: false }
    );
    counters.selects += 1;

    return counters;
  }

  async executeWarehouseOrdersQuery(connection) {
    const counters = this.createCounters();
    const warehouseId = await this.getRandomWarehouse(connection, counters);

    await connection.execute(
      `
        SELECT order_mode,
               orders.warehouse_id,
               SUM(order_total),
               COUNT(1)
        FROM orders,
             warehouses
        WHERE orders.warehouse_id = warehouses.warehouse_id
          AND warehouses.warehouse_id = :warehouseId
        GROUP BY CUBE(orders.order_mode, orders.warehouse_id)
      `,
      { warehouseId },
      { autoCommit: false }
    );
    counters.selects += 1;

    return counters;
  }

  async executeBrowseAndUpdateOrders(connection) {
    const counters = this.createCounters();
    const customer = await this.getRandomCustomer(connection, counters);
    if (!customer?.CUSTOMER_ID) {
      return counters;
    }

    const customerId = Number(customer.CUSTOMER_ID);
    await this.logon(connection, customerId, counters);
    await this.getCustomerDetails(connection, customerId, counters);
    await this.getAddressDetails(connection, customerId, counters);
    const orders = await this.getOrdersByCustomer(connection, customerId, counters);

    if (orders.length > 0) {
      const selectedOrder = this.randomItem(orders);
      const lineItem = await this.fetchOne(
        connection,
        `
          SELECT order_id, line_item_id, product_id, unit_price
          FROM order_items
          WHERE order_id = :orderId
            AND ROWNUM < 5
        `,
        { orderId: selectedOrder }
      );
      counters.selects += 1;

      if (lineItem?.LINE_ITEM_ID) {
        await connection.execute(
          `
            UPDATE order_items
            SET quantity = quantity + 1
            WHERE order_id = :orderId
              AND line_item_id = :lineItemId
          `,
          {
            orderId: selectedOrder,
            lineItemId: Number(lineItem.LINE_ITEM_ID)
          },
          { autoCommit: false }
        );
        counters.updates += 1;

        await connection.execute(
          `
            UPDATE orders
            SET order_total = order_total + :unitPrice
            WHERE order_id = :orderId
          `,
          {
            unitPrice: Number(lineItem.UNIT_PRICE || 0),
            orderId: selectedOrder
          },
          { autoCommit: false }
        );
        counters.updates += 1;
      }
    }

    await connection.commit();
    counters.commits += 1;
    return counters;
  }

  async executeTransaction(connection, txn) {
    switch (txn.shortName) {
      case 'NCR':
        return this.executeNewCustomerProcess(connection);
      case 'BP':
        return this.executeBrowseProducts(connection);
      case 'OP':
        return this.executeNewOrderProcess(connection);
      case 'PO':
        return this.executeProcessOrders(connection);
      case 'SQ':
        return this.executeSalesRepsOrdersQuery(connection);
      case 'WQ':
        return this.executeWarehouseOrdersQuery(connection);
      case 'BO':
        return this.executeBrowseAndUpdateOrders(connection);
      default:
        throw new Error(`Unsupported Swingbench JDBC transaction '${txn.shortName}'.`);
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
      byTransaction: Object.fromEntries(
        Object.entries(this.stats.byTransaction).map(([key, value]) => [key, { ...value }])
      )
    };
  }

  async runWorker(workerId) {
    this.activeWorkers += 1;
    let connection = null;

    try {
      connection = await this.pool.getConnection();
      await this.prepareConnection(connection, workerId);

      while (this.isRunning) {
        try {
          const txn = this.selectTransaction();
          const counters = await this.executeTransaction(connection, txn);
          this.applyDmlResult(txn, counters);

          if (this.config.maxTransactions > 0 && this.stats.transactions >= this.config.maxTransactions) {
            this.stop().catch(() => {});
            return;
          }

          if (this.isRunning) {
            await this.sleepBetweenTransactions();
          }
        } catch (error) {
          this.stats.errors += 1;

          if (connection) {
            try {
              await connection.rollback();
              this.stats.rollbacks += 1;
            } catch (rollbackError) {
              // Ignore rollback errors.
            }
          }

          if (!String(error.message || '').includes('pool is closing')) {
            console.log(`Swingbench SOE worker ${workerId} error: ${error.message}`);
          }
        }
      }
    } finally {
      if (connection) {
        try {
          await connection.close();
        } catch (closeError) {
          // Ignore close errors.
        }
      }

      this.activeWorkers -= 1;
    }
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
    this.previousStats = this.initStats();
    this.isRunning = true;

    const bootstrapConnection = await db.createDirectConnection({
      user: this.config.username,
      password: this.config.password,
      connectionString: effectiveConnectString
    });

    try {
      await this.prepareConnection(bootstrapConnection, 0);
      await this.loadRuntimeMetadata(bootstrapConnection);
    } finally {
      await bootstrapConnection.close();
    }

    this.pool = await db.createStressPool(this.config.users, {
      user: this.config.username,
      password: this.config.password,
      connectionString: effectiveConnectString
    });

    this.workers = Array.from({ length: this.config.users }, (_, index) => this.runWorker(index + 1));
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

    await Promise.race([
      Promise.allSettled(this.workers || []),
      new Promise((resolve) => setTimeout(resolve, 5000))
    ]);

    if (this.pool) {
      try {
        await this.pool.close(10);
      } catch (error) {
        console.log('Swingbench SOE pool close error:', error.message);
      } finally {
        this.pool = null;
      }
    }

    this.reportStats();

    if (this.io) {
      this.io.emit('swingbench-soe-status', {
        isRunning: false,
        config: this.config,
        uptime: Math.floor((Date.now() - this.stats.startTime) / 1000),
        activeWorkers: this.activeWorkers
      });
    }

    this.workers = [];
    return this.getStatus();
  }
}

module.exports = new SwingbenchSOEEngine();
