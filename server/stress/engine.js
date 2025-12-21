// Stress Test Engine for Oracle Database - Multi-Schema Support
const oracledb = require('oracledb');

class StressEngine {
  constructor() {
    this.isRunning = false;
    this.config = null;
    this.workers = [];
    this.pool = null;
    this.io = null;
    this.schemas = [];  // Array of schema prefixes being tested

    // Stats per schema
    this.schemaStats = {};  // keyed by schema prefix
    this.previousSchemaStats = {};

    this.statsInterval = null;
  }

  initSchemaStats(schemaId) {
    return {
      inserts: 0,
      updates: 0,
      deletes: 0,
      selects: 0,
      transactions: 0,
      errors: 0,
      startTime: Date.now()
    };
  }

  async start(db, config, io) {
    if (this.isRunning) {
      throw new Error('Stress test already running');
    }

    // Support for multiple schemas
    this.schemas = config.schemas || [{ prefix: '' }];

    this.config = {
      sessions: config.sessions || 10,
      insertsPerSecond: config.insertsPerSecond || 50,
      updatesPerSecond: config.updatesPerSecond || 30,
      deletesPerSecond: config.deletesPerSecond || 10,
      selectsPerSecond: config.selectsPerSecond || 100,
      thinkTime: config.thinkTime || 100, // ms between operations
      ...config
    };

    this.io = io;
    this.isRunning = true;

    // Initialize stats for each schema
    this.schemaStats = {};
    this.previousSchemaStats = {};
    for (const schema of this.schemas) {
      const schemaId = schema.prefix || 'default';
      this.schemaStats[schemaId] = this.initSchemaStats(schemaId);
      this.previousSchemaStats[schemaId] = { ...this.schemaStats[schemaId] };
    }

    console.log('Starting stress test with schemas:', this.schemas.map(s => s.prefix || 'default'));

    // Create a dedicated connection pool for stress testing
    // Calculate sessions per schema: divide total sessions among schemas
    const totalSessions = this.config.sessions * this.schemas.length;
    this.pool = await db.createStressPool(totalSessions);

    // Start worker sessions for each schema
    for (const schema of this.schemas) {
      const schemaId = schema.prefix || 'default';
      const prefix = schema.prefix || '';

      for (let i = 0; i < this.config.sessions; i++) {
        this.workers.push(this.runWorker(i, prefix, schemaId));
      }
    }

    // Start stats reporting
    this.statsInterval = setInterval(() => this.reportStats(), 1000);

    console.log(`Started ${this.config.sessions} worker sessions per schema (${this.schemas.length} schemas)`);
  }

  async runWorker(workerId, prefix = '', schemaId = 'default') {
    const p = prefix ? `${prefix}_` : '';
    const stats = this.schemaStats[schemaId];

    while (this.isRunning && this.schemaStats[schemaId]) {
      let connection;
      try {
        connection = await this.pool.getConnection();

        // Decide which operation to perform based on configured ratios
        const operation = this.selectOperation();

        switch (operation) {
          case 'INSERT':
            await this.performInsert(connection, p);
            stats.inserts++;
            break;
          case 'UPDATE':
            await this.performUpdate(connection, p);
            stats.updates++;
            break;
          case 'DELETE':
            await this.performDelete(connection, p);
            stats.deletes++;
            break;
          case 'SELECT':
            await this.performSelect(connection, p);
            stats.selects++;
            break;
        }

        stats.transactions++;

        await connection.commit();
      } catch (err) {
        stats.errors++;
        if (connection) {
          try {
            await connection.rollback();
          } catch (e) {
            // Ignore rollback errors
          }
        }
        // Log errors but continue running
        if (!err.message.includes('pool is terminating') &&
            !err.message.includes('NJS-003')) {
          console.log(`Worker ${workerId} [${schemaId}] error:`, err.message);
        }
      } finally {
        if (connection) {
          try {
            await connection.close();
          } catch (e) {
            // Ignore close errors
          }
        }
      }

      // Think time between operations
      if (this.isRunning && this.config.thinkTime > 0) {
        await this.sleep(this.config.thinkTime);
      }
    }
  }

  selectOperation() {
    const total = this.config.insertsPerSecond +
                  this.config.updatesPerSecond +
                  this.config.deletesPerSecond +
                  this.config.selectsPerSecond;

    const rand = Math.random() * total;

    if (rand < this.config.insertsPerSecond) return 'INSERT';
    if (rand < this.config.insertsPerSecond + this.config.updatesPerSecond) return 'UPDATE';
    if (rand < this.config.insertsPerSecond + this.config.updatesPerSecond + this.config.deletesPerSecond) return 'DELETE';
    return 'SELECT';
  }

  async performInsert(connection, p = '') {
    // Insert operations: create new orders, customers, or reviews
    const type = Math.floor(Math.random() * 3);

    if (type === 0) {
      // Insert new order with items
      const customerId = await this.getRandomId(connection, `${p}customers`, 'customer_id');
      const warehouseId = await this.getRandomId(connection, `${p}warehouses`, 'warehouse_id');

      if (!customerId || !warehouseId) return;

      // Insert order
      await connection.execute(
        `INSERT INTO ${p}orders (customer_id, status, warehouse_id, shipping_method, notes)
         VALUES (:1, 'PENDING', :2, 'Standard', 'Stress test order')`,
        [customerId, warehouseId]
      );

      // Get the order ID we just created
      const orderIdResult = await connection.execute(
        `SELECT MAX(order_id) as order_id FROM ${p}orders WHERE customer_id = :1`,
        [customerId]
      );
      const orderId = orderIdResult.rows[0]?.ORDER_ID;

      if (!orderId) return;

      // Add 1-3 order items
      const itemCount = Math.floor(Math.random() * 3) + 1;
      let subtotal = 0;

      for (let i = 0; i < itemCount; i++) {
        const productId = await this.getRandomId(connection, `${p}products`, 'product_id');
        if (!productId) continue;

        const quantity = Math.floor(Math.random() * 5) + 1;
        const unitPrice = parseFloat((Math.random() * 100 + 10).toFixed(2));
        const lineTotal = parseFloat((quantity * unitPrice).toFixed(2));
        subtotal += lineTotal;

        await connection.execute(
          `INSERT INTO ${p}order_items (order_id, product_id, quantity, unit_price, line_total)
           VALUES (:1, :2, :3, :4, :5)`,
          [orderId, productId, quantity, unitPrice, lineTotal]
        );
      }

      // Update order total
      const tax = parseFloat((subtotal * 0.08).toFixed(2));
      const shipping = parseFloat((Math.random() * 15 + 5).toFixed(2));
      const total = parseFloat((subtotal + tax + shipping).toFixed(2));
      await connection.execute(
        `UPDATE ${p}orders SET subtotal = :1, tax_amount = :2, shipping_cost = :3, total_amount = :4
         WHERE order_id = :5`,
        [subtotal, tax, shipping, total, orderId]
      );

    } else if (type === 1) {
      // Insert product review
      const productId = await this.getRandomId(connection, `${p}products`, 'product_id');
      const customerId = await this.getRandomId(connection, `${p}customers`, 'customer_id');

      if (!productId || !customerId) return;

      await connection.execute(
        `INSERT INTO ${p}product_reviews (product_id, customer_id, rating, review_title, review_text, is_verified_purchase)
         VALUES (:1, :2, :3, :4, :5, :6)`,
        [
          productId,
          customerId,
          Math.floor(Math.random() * 5) + 1,
          'Stress test review',
          'This is an automated review generated during stress testing.',
          Math.random() > 0.5 ? 1 : 0
        ]
      );

    } else {
      // Insert order history entry
      const orderId = await this.getRandomId(connection, `${p}orders`, 'order_id');
      if (!orderId) return;

      await connection.execute(
        `INSERT INTO ${p}order_history (order_id, old_status, new_status, changed_by, change_reason)
         VALUES (:1, 'PENDING', 'PROCESSING', 'STRESS_TEST', 'Automated status change')`,
        [orderId]
      );
    }
  }

  async performUpdate(connection, p = '') {
    // Update operations: modify orders, inventory, or customer data
    const type = Math.floor(Math.random() * 4);

    if (type === 0) {
      // Update order status
      const orderId = await this.getRandomId(connection, `${p}orders`, 'order_id');
      if (!orderId) return;

      const statuses = ['PENDING', 'PROCESSING', 'SHIPPED', 'DELIVERED'];
      const newStatus = statuses[Math.floor(Math.random() * statuses.length)];

      await connection.execute(
        `UPDATE ${p}orders SET status = :1, updated_at = CURRENT_TIMESTAMP WHERE order_id = :2`,
        [newStatus, orderId]
      );

    } else if (type === 1) {
      // Update inventory quantity
      const inventoryId = await this.getRandomId(connection, `${p}inventory`, 'inventory_id');
      if (!inventoryId) return;

      const qtyChange = Math.floor(Math.random() * 50) - 25;

      await connection.execute(
        `UPDATE ${p}inventory
         SET quantity_on_hand = GREATEST(0, quantity_on_hand + :1),
             updated_at = CURRENT_TIMESTAMP
         WHERE inventory_id = :2`,
        [qtyChange, inventoryId]
      );

    } else if (type === 2) {
      // Update customer balance
      const customerId = await this.getRandomId(connection, `${p}customers`, 'customer_id');
      if (!customerId) return;

      const balanceChange = parseFloat((Math.random() * 200 - 100).toFixed(2));

      await connection.execute(
        `UPDATE ${p}customers
         SET balance = balance + :1,
             updated_at = CURRENT_TIMESTAMP
         WHERE customer_id = :2`,
        [balanceChange, customerId]
      );

    } else {
      // Update product price
      const productId = await this.getRandomId(connection, `${p}products`, 'product_id');
      if (!productId) return;

      const priceChange = parseFloat((Math.random() * 20 - 10).toFixed(2));

      await connection.execute(
        `UPDATE ${p}products
         SET unit_price = GREATEST(1, unit_price + :1),
             updated_at = CURRENT_TIMESTAMP
         WHERE product_id = :2`,
        [priceChange, productId]
      );
    }
  }

  async performDelete(connection, p = '') {
    // Delete operations: remove old reviews, order history, or cancelled orders
    const type = Math.floor(Math.random() * 3);

    if (type === 0) {
      // Delete a product review
      const reviewId = await this.getRandomId(connection, `${p}product_reviews`, 'review_id');
      if (!reviewId) return;

      await connection.execute(
        `DELETE FROM ${p}product_reviews WHERE review_id = :1`,
        [reviewId]
      );

    } else if (type === 1) {
      // Delete old order history entry
      const historyId = await this.getRandomId(connection, `${p}order_history`, 'history_id');
      if (!historyId) return;

      await connection.execute(
        `DELETE FROM ${p}order_history WHERE history_id = :1`,
        [historyId]
      );

    } else {
      // Delete cancelled order (and its items first)
      try {
        const result = await connection.execute(
          `SELECT order_id FROM ${p}orders WHERE status = 'CANCELLED' AND ROWNUM = 1`
        );

        if (result.rows.length > 0) {
          const orderId = result.rows[0].ORDER_ID;

          // Delete in correct order due to FK constraints
          await connection.execute(`DELETE FROM ${p}payments WHERE order_id = :1`, [orderId]);
          await connection.execute(`DELETE FROM ${p}order_history WHERE order_id = :1`, [orderId]);
          await connection.execute(`DELETE FROM ${p}order_items WHERE order_id = :1`, [orderId]);
          await connection.execute(`DELETE FROM ${p}orders WHERE order_id = :1`, [orderId]);
        }
      } catch (err) {
        // Ignore - might have concurrent deletes
      }
    }
  }

  async performSelect(connection, p = '') {
    // Select operations: various read queries
    const type = Math.floor(Math.random() * 6);

    if (type === 0) {
      // Select orders with customer info
      await connection.execute(
        `SELECT o.order_id, o.status, o.total_amount, c.first_name, c.last_name
         FROM ${p}orders o
         JOIN ${p}customers c ON o.customer_id = c.customer_id
         WHERE ROWNUM <= 100`
      );

    } else if (type === 1) {
      // Select product inventory across warehouses
      await connection.execute(
        `SELECT p.product_name, w.warehouse_name, i.quantity_on_hand
         FROM ${p}inventory i
         JOIN ${p}products p ON i.product_id = p.product_id
         JOIN ${p}warehouses w ON i.warehouse_id = w.warehouse_id
         WHERE ROWNUM <= 100`
      );

    } else if (type === 2) {
      // Select top selling products
      await connection.execute(
        `SELECT p.product_name, SUM(oi.quantity) as total_sold
         FROM ${p}order_items oi
         JOIN ${p}products p ON oi.product_id = p.product_id
         GROUP BY p.product_id, p.product_name
         ORDER BY total_sold DESC
         FETCH FIRST 20 ROWS ONLY`
      );

    } else if (type === 3) {
      // Select customer order history
      const customerId = await this.getRandomId(connection, `${p}customers`, 'customer_id');
      if (!customerId) return;

      await connection.execute(
        `SELECT o.order_id, o.order_date, o.status, o.total_amount
         FROM ${p}orders o
         WHERE o.customer_id = :1
         ORDER BY o.order_date DESC
         FETCH FIRST 50 ROWS ONLY`,
        [customerId]
      );

    } else if (type === 4) {
      // Select order details with items
      const orderId = await this.getRandomId(connection, `${p}orders`, 'order_id');
      if (!orderId) return;

      await connection.execute(
        `SELECT o.order_id, oi.quantity, oi.unit_price, oi.line_total, p.product_name
         FROM ${p}orders o
         JOIN ${p}order_items oi ON o.order_id = oi.order_id
         JOIN ${p}products p ON oi.product_id = p.product_id
         WHERE o.order_id = :1`,
        [orderId]
      );

    } else {
      // Select products by category with reviews
      await connection.execute(
        `SELECT p.product_name, p.unit_price, c.category_name,
                COUNT(r.review_id) as review_count, AVG(r.rating) as avg_rating
         FROM ${p}products p
         JOIN ${p}categories c ON p.category_id = c.category_id
         LEFT JOIN ${p}product_reviews r ON p.product_id = r.product_id
         WHERE ROWNUM <= 50
         GROUP BY p.product_id, p.product_name, p.unit_price, c.category_name`
      );
    }
  }

  async getRandomId(connection, tableName, idColumn) {
    try {
      const result = await connection.execute(
        `SELECT ${idColumn} FROM ${tableName} SAMPLE(1) WHERE ROWNUM = 1`
      );
      return result.rows[0]?.[idColumn.toUpperCase()];
    } catch (err) {
      return null;
    }
  }

  reportStats() {
    const now = Date.now();

    // Calculate stats for each schema
    const schemaMetrics = {};
    let totalTps = 0;
    let totalTransactions = 0;

    for (const schemaId of Object.keys(this.schemaStats)) {
      const stats = this.schemaStats[schemaId];
      const prevStats = this.previousSchemaStats[schemaId] || this.initSchemaStats(schemaId);
      const elapsed = (now - stats.startTime) / 1000;

      const schemaData = {
        schemaId,
        timestamp: now,
        elapsed: Math.floor(elapsed),
        total: {
          inserts: stats.inserts,
          updates: stats.updates,
          deletes: stats.deletes,
          selects: stats.selects,
          transactions: stats.transactions,
          errors: stats.errors
        },
        perSecond: {
          inserts: stats.inserts - prevStats.inserts,
          updates: stats.updates - prevStats.updates,
          deletes: stats.deletes - prevStats.deletes,
          selects: stats.selects - prevStats.selects,
          transactions: stats.transactions - prevStats.transactions,
          errors: stats.errors - prevStats.errors
        },
        averagePerSecond: {
          transactions: elapsed > 0 ? parseFloat((stats.transactions / elapsed).toFixed(2)) : 0
        }
      };

      schemaData.tps = schemaData.perSecond.transactions;
      schemaMetrics[schemaId] = schemaData;
      totalTps += schemaData.tps;
      totalTransactions += stats.transactions;

      // Store previous stats for this schema
      this.previousSchemaStats[schemaId] = { ...stats };
    }

    // Emit per-schema metrics and combined metrics
    if (this.io) {
      // Emit individual schema metrics
      this.io.emit('stress-metrics-by-schema', schemaMetrics);

      // Emit combined metrics (backward compatible with single-schema clients)
      const firstSchemaId = Object.keys(schemaMetrics)[0];
      if (firstSchemaId) {
        const combinedStats = { ...schemaMetrics[firstSchemaId], schemas: schemaMetrics };
        this.io.emit('stress-metrics', combinedStats);
      }
    }
  }

  updateConfig(newConfig) {
    this.config = { ...this.config, ...newConfig };
    console.log('Updated stress config:', this.config);
  }

  // Stop a specific schema's stress test
  stopSchema(schemaId) {
    console.log(`Stopping stress test for schema: ${schemaId}`);
    delete this.schemaStats[schemaId];
    delete this.previousSchemaStats[schemaId];

    // Check if all schemas are stopped
    if (Object.keys(this.schemaStats).length === 0) {
      this.stop();
    }
  }

  async stop() {
    console.log('Stopping all stress tests...');
    this.isRunning = false;

    // Clear stats interval
    if (this.statsInterval) {
      clearInterval(this.statsInterval);
      this.statsInterval = null;
    }

    // Wait a moment for workers to finish current operations
    await this.sleep(500);

    // Close the stress pool
    if (this.pool) {
      try {
        await this.pool.close(2);
      } catch (err) {
        console.log('Pool close warning:', err.message);
      }
      this.pool = null;
    }

    // Clear workers
    this.workers = [];

    // Report final stats per schema
    const finalStats = {};
    for (const schemaId of Object.keys(this.schemaStats)) {
      const stats = this.schemaStats[schemaId];
      finalStats[schemaId] = {
        ...stats,
        duration: stats.startTime ? (Date.now() - stats.startTime) / 1000 : 0
      };
    }

    console.log('Stress test stopped. Final stats:', finalStats);

    if (this.io) {
      this.io.emit('stress-stopped', { schemas: finalStats, transactions: Object.values(finalStats).reduce((sum, s) => sum + s.transactions, 0) });
    }

    // Clear schema stats
    this.schemaStats = {};
    this.previousSchemaStats = {};

    return finalStats;
  }

  sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }
}

module.exports = new StressEngine();
