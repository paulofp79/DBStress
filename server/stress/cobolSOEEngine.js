const oracledb = require('oracledb');
const { SwingbenchSOEEngine } = require('./swingbenchSOEEngine');

const COBOL_TRANSACTION_DEFINITIONS = [
  {
    id: 'COBOL Customer Inquiry',
    shortName: 'CI',
    className: 'ESORAXA.CBL EXEC SQL CUSTOMER-INQUIRY',
    weight: 45,
    enabled: true,
    mapsTo: 'Browse Products'
  },
  {
    id: 'COBOL New Order',
    shortName: 'CO',
    className: 'ESORAXA.CBL EXEC SQL NEW-ORDER',
    weight: 35,
    enabled: true,
    mapsTo: 'Order Products'
  },
  {
    id: 'COBOL Customer Registration',
    shortName: 'CR',
    className: 'ESORAXA.CBL EXEC SQL CUSTOMER-REGISTRATION',
    weight: 10,
    enabled: true,
    mapsTo: 'Customer Registration'
  },
  {
    id: 'COBOL Order Maintenance',
    shortName: 'CM',
    className: 'ESORAXA.CBL EXEC SQL ORDER-MAINTENANCE',
    weight: 7,
    enabled: true,
    mapsTo: 'Browse Orders'
  },
  {
    id: 'COBOL Fulfillment Batch',
    shortName: 'FB',
    className: 'ESORAXA.CBL EXEC SQL PROCESS-ORDERS',
    weight: 3,
    enabled: true,
    mapsTo: 'Process Orders'
  }
];

class CobolSOEEngine extends SwingbenchSOEEngine {
  constructor() {
    super({
      eventPrefix: 'cobol-soe',
      displayName: 'COBOL XA SOE',
      moduleName: 'DBSTRESS_COBOL_ESORAXA',
      clientIdPrefix: 'ESORAXA'
    });
  }

  buildDefaultConfig() {
    return {
      profileName: 'ESORAXA COBOL XA SOE',
      profileComment: 'COBOL-style Oracle workload modeled after the Micro Focus ESORAXA Oracle XA switch sample.',
      username: 'soe',
      password: 'soe',
      connectString: '',
      users: 8,
      minDelay: 5,
      maxDelay: 25,
      interMinDelay: 0,
      interMaxDelay: 10,
      queryTimeout: 120,
      maxTransactions: -1,
      durationSeconds: 0,
      cobolProgram: 'ESORAXA.CBL',
      xaOpenString: 'Oracle_XA+Acc=P/soe/soe+SesTm=60+Threads=true',
      xaMode: 'LOCALTX',
      transactionModel: 'COBSQL EXEC SQL with Oracle XA open/start/end/commit/rollback semantics',
      transactions: COBOL_TRANSACTION_DEFINITIONS.map((txn) => ({ ...txn }))
    };
  }

  normalizeConfig(config = {}) {
    const normalized = super.normalizeConfig(config);

    return {
      ...normalized,
      cobolProgram: String(config.cobolProgram || normalized.cobolProgram || 'ESORAXA.CBL').trim(),
      xaOpenString: String(config.xaOpenString || normalized.xaOpenString || '').trim(),
      xaMode: String(config.xaMode || normalized.xaMode || 'LOCALTX').trim(),
      transactionModel: normalized.transactionModel
    };
  }

  async prepareConnection(connection, workerId) {
    await super.prepareConnection(connection, workerId);

    try {
      connection.action = `ESXA-W${workerId}-${this.config?.xaMode || 'LOCALTX'}`;
      connection.clientId = `COBOL:${this.config?.cobolProgram || 'ESORAXA'}:W${workerId}`;
    } catch (err) {
      // Best effort only.
    }
  }

  async loadRuntimeMetadata(connection) {
    await super.loadRuntimeMetadata(connection);

    try {
      const productRange = await connection.execute(
        `
          SELECT MIN(product_id) AS min_id,
                 MAX(product_id) AS max_id,
                 MIN(category_id) AS min_category_id,
                 MAX(category_id) AS max_category_id
          FROM product_information
        `,
        [],
        { outFormat: oracledb.OUT_FORMAT_OBJECT }
      );
      const row = productRange.rows?.[0];
      if (row?.MIN_ID) this.runtimeMetadata.minProductId = Number(row.MIN_ID);
      if (row?.MAX_ID) this.runtimeMetadata.maxProductId = Number(row.MAX_ID);
      if (row?.MIN_CATEGORY_ID) this.runtimeMetadata.minCategoryId = Number(row.MIN_CATEGORY_ID);
      if (row?.MAX_CATEGORY_ID) this.runtimeMetadata.maxCategoryId = Number(row.MAX_CATEGORY_ID);
    } catch (err) {
      // Keep the base fallback metadata.
    }
  }

  async getProductDetailsById(connection, productId, counters) {
    const row = await this.fetchOne(
      connection,
      `
        SELECT product_information.product_id, product_information.list_price
        FROM product_information, inventories
        WHERE inventories.product_id = product_information.product_id
          AND product_information.product_id = :productId
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
        SELECT product_information.product_id,
               product_information.list_price,
               inventories.quantity_on_hand
        FROM product_information, inventories
        WHERE product_information.category_id = :categoryId
          AND inventories.product_id = product_information.product_id
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

  async executeTransaction(connection, txn) {
    switch (txn.shortName) {
      case 'CI':
        return this.executeBrowseProducts(connection);
      case 'CO':
        return this.executeNewOrderProcess(connection);
      case 'CR':
        return this.executeNewCustomerProcess(connection);
      case 'CM':
        return this.executeBrowseAndUpdateOrders(connection);
      case 'FB':
        return this.executeProcessOrders(connection);
      default:
        return super.executeTransaction(connection, txn);
    }
  }
}

module.exports = new CobolSOEEngine();
