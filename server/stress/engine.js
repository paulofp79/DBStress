// Stress Test Engine for Oracle Database
const oracledb = require('oracledb');

class StressEngine {
  constructor() {
    this.isRunning = false;
    this.config = null;
    this.workers = [];
    this.pool = null;
    this.io = null;
    this.stats = {
      inserts: 0,
      updates: 0,
      deletes: 0,
      selects: 0,
      transactions: 0,
      errors: 0,
      startTime: null
    };
    this.statsInterval = null;
    this.previousStats = { ...this.stats };
  }

  async start(db, config, io) {
    if (this.isRunning) {
      throw new Error('Stress test already running');
    }

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
    this.stats = {
      inserts: 0,
      updates: 0,
      deletes: 0,
      selects: 0,
      transactions: 0,
      errors: 0,
      startTime: Date.now()
    };
    this.previousStats = { ...this.stats };

    console.log('Starting stress test with config:', this.config);

    // Create a dedicated connection pool for stress testing
    this.pool = await db.createStressPool(this.config.sessions);

    // Start worker sessions
    for (let i = 0; i < this.config.sessions; i++) {
      this.workers.push(this.runWorker(i));
    }

    // Start stats reporting
    this.statsInterval = setInterval(() => this.reportStats(), 1000);

    console.log(`Started ${this.config.sessions} worker sessions`);
  }

  async runWorker(workerId) {
    while (this.isRunning) {
      let connection;
      try {
        connection = await this.pool.getConnection();

        // Decide which operation to perform based on configured ratios
        const operation = this.selectOperation();

        switch (operation) {
          case 'INSERT':
            await this.performInsert(connection);
            this.stats.inserts++;
            break;
          case 'UPDATE':
            await this.performUpdate(connection);
            this.stats.updates++;
            break;
          case 'DELETE':
            await this.performDelete(connection);
            this.stats.deletes++;
            break;
          case 'SELECT':
            await this.performSelect(connection);
            this.stats.selects++;
            break;
        }

        this.stats.transactions++;

        await connection.commit();
      } catch (err) {
        this.stats.errors++;
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
          console.log(`Worker ${workerId} error:`, err.message);
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

  async performInsert(connection) {
    // Insert operations: create new orders, customers, or reviews
    const type = Math.floor(Math.random() * 3);

    if (type === 0) {
      // Insert new order with items
      const customerId = await this.getRandomId(connection, 'customers', 'customer_id');
      const warehouseId = await this.getRandomId(connection, 'warehouses', 'warehouse_id');

      if (!customerId || !warehouseId) return;

      const result = await connection.execute(
        `INSERT INTO orders (customer_id, status, warehouse_id, shipping_method, notes)
         VALUES (:custId, 'PENDING', :whId, 'Standard', 'Stress test order')
         RETURNING order_id INTO :id`,
        {
          custId: customerId,
          whId: warehouseId,
          id: { type: oracledb.NUMBER, dir: oracledb.BIND_OUT }
        }
      );

      const orderId = result.outBinds.id[0];

      // Add 1-3 order items
      const itemCount = Math.floor(Math.random() * 3) + 1;
      let subtotal = 0;

      for (let i = 0; i < itemCount; i++) {
        const productId = await this.getRandomId(connection, 'products', 'product_id');
        if (!productId) continue;

        const quantity = Math.floor(Math.random() * 5) + 1;
        const unitPrice = parseFloat((Math.random() * 100 + 10).toFixed(2));
        const lineTotal = quantity * unitPrice;
        subtotal += lineTotal;

        await connection.execute(
          `INSERT INTO order_items (order_id, product_id, quantity, unit_price, line_total)
           VALUES (:orderId, :prodId, :qty, :price, :total)`,
          { orderId, prodId: productId, qty: quantity, price: unitPrice, total: lineTotal }
        );
      }

      // Update order total
      const tax = subtotal * 0.08;
      const shipping = Math.random() * 15 + 5;
      await connection.execute(
        `UPDATE orders SET subtotal = :sub, tax_amount = :tax, shipping_cost = :ship, total_amount = :total
         WHERE order_id = :id`,
        { sub: subtotal, tax, ship: shipping, total: subtotal + tax + shipping, id: orderId }
      );

    } else if (type === 1) {
      // Insert product review
      const productId = await this.getRandomId(connection, 'products', 'product_id');
      const customerId = await this.getRandomId(connection, 'customers', 'customer_id');

      if (!productId || !customerId) return;

      await connection.execute(
        `INSERT INTO product_reviews (product_id, customer_id, rating, review_title, review_text, is_verified_purchase)
         VALUES (:prodId, :custId, :rating, :title, :text, :verified)`,
        {
          prodId: productId,
          custId: customerId,
          rating: Math.floor(Math.random() * 5) + 1,
          title: 'Stress test review',
          text: 'This is an automated review generated during stress testing.',
          verified: Math.random() > 0.5 ? 1 : 0
        }
      );

    } else {
      // Insert order history entry
      const orderId = await this.getRandomId(connection, 'orders', 'order_id');
      if (!orderId) return;

      await connection.execute(
        `INSERT INTO order_history (order_id, old_status, new_status, changed_by, change_reason)
         VALUES (:orderId, 'PENDING', 'PROCESSING', 'STRESS_TEST', 'Automated status change')`,
        { orderId }
      );
    }
  }

  async performUpdate(connection) {
    // Update operations: modify orders, inventory, or customer data
    const type = Math.floor(Math.random() * 4);

    if (type === 0) {
      // Update order status
      const orderId = await this.getRandomId(connection, 'orders', 'order_id');
      if (!orderId) return;

      const statuses = ['PENDING', 'PROCESSING', 'SHIPPED', 'DELIVERED'];
      const newStatus = statuses[Math.floor(Math.random() * statuses.length)];

      await connection.execute(
        `UPDATE orders SET status = :status, updated_at = CURRENT_TIMESTAMP WHERE order_id = :id`,
        { status: newStatus, id: orderId }
      );

    } else if (type === 1) {
      // Update inventory quantity
      const inventoryId = await this.getRandomId(connection, 'inventory', 'inventory_id');
      if (!inventoryId) return;

      const qtyChange = Math.floor(Math.random() * 50) - 25;

      await connection.execute(
        `UPDATE inventory
         SET quantity_on_hand = GREATEST(0, quantity_on_hand + :change),
             updated_at = CURRENT_TIMESTAMP
         WHERE inventory_id = :id`,
        { change: qtyChange, id: inventoryId }
      );

    } else if (type === 2) {
      // Update customer balance
      const customerId = await this.getRandomId(connection, 'customers', 'customer_id');
      if (!customerId) return;

      const balanceChange = parseFloat((Math.random() * 200 - 100).toFixed(2));

      await connection.execute(
        `UPDATE customers
         SET balance = balance + :change,
             updated_at = CURRENT_TIMESTAMP
         WHERE customer_id = :id`,
        { change: balanceChange, id: customerId }
      );

    } else {
      // Update product price
      const productId = await this.getRandomId(connection, 'products', 'product_id');
      if (!productId) return;

      const priceChange = parseFloat((Math.random() * 20 - 10).toFixed(2));

      await connection.execute(
        `UPDATE products
         SET unit_price = GREATEST(1, unit_price + :change),
             updated_at = CURRENT_TIMESTAMP
         WHERE product_id = :id`,
        { change: priceChange, id: productId }
      );
    }
  }

  async performDelete(connection) {
    // Delete operations: remove old reviews, order history, or cancelled orders
    const type = Math.floor(Math.random() * 3);

    if (type === 0) {
      // Delete a product review
      const reviewId = await this.getRandomId(connection, 'product_reviews', 'review_id');
      if (!reviewId) return;

      await connection.execute(
        `DELETE FROM product_reviews WHERE review_id = :id`,
        { id: reviewId }
      );

    } else if (type === 1) {
      // Delete old order history entry
      const historyId = await this.getRandomId(connection, 'order_history', 'history_id');
      if (!historyId) return;

      await connection.execute(
        `DELETE FROM order_history WHERE history_id = :id`,
        { id: historyId }
      );

    } else {
      // Delete cancelled order (and its items first)
      try {
        const result = await connection.execute(
          `SELECT order_id FROM orders WHERE status = 'CANCELLED' AND ROWNUM = 1`
        );

        if (result.rows.length > 0) {
          const orderId = result.rows[0].ORDER_ID;

          // Delete in correct order due to FK constraints
          await connection.execute(`DELETE FROM payments WHERE order_id = :id`, { id: orderId });
          await connection.execute(`DELETE FROM order_history WHERE order_id = :id`, { id: orderId });
          await connection.execute(`DELETE FROM order_items WHERE order_id = :id`, { id: orderId });
          await connection.execute(`DELETE FROM orders WHERE order_id = :id`, { id: orderId });
        }
      } catch (err) {
        // Ignore - might have concurrent deletes
      }
    }
  }

  async performSelect(connection) {
    // Select operations: various read queries
    const type = Math.floor(Math.random() * 6);

    if (type === 0) {
      // Select orders with customer info
      await connection.execute(
        `SELECT o.order_id, o.status, o.total_amount, c.first_name, c.last_name
         FROM orders o
         JOIN customers c ON o.customer_id = c.customer_id
         WHERE ROWNUM <= 100`
      );

    } else if (type === 1) {
      // Select product inventory across warehouses
      await connection.execute(
        `SELECT p.product_name, w.warehouse_name, i.quantity_on_hand
         FROM inventory i
         JOIN products p ON i.product_id = p.product_id
         JOIN warehouses w ON i.warehouse_id = w.warehouse_id
         WHERE ROWNUM <= 100`
      );

    } else if (type === 2) {
      // Select top selling products
      await connection.execute(
        `SELECT p.product_name, SUM(oi.quantity) as total_sold
         FROM order_items oi
         JOIN products p ON oi.product_id = p.product_id
         GROUP BY p.product_id, p.product_name
         ORDER BY total_sold DESC
         FETCH FIRST 20 ROWS ONLY`
      );

    } else if (type === 3) {
      // Select customer order history
      const customerId = await this.getRandomId(connection, 'customers', 'customer_id');
      if (!customerId) return;

      await connection.execute(
        `SELECT o.order_id, o.order_date, o.status, o.total_amount
         FROM orders o
         WHERE o.customer_id = :custId
         ORDER BY o.order_date DESC
         FETCH FIRST 50 ROWS ONLY`,
        { custId: customerId }
      );

    } else if (type === 4) {
      // Select order details with items
      const orderId = await this.getRandomId(connection, 'orders', 'order_id');
      if (!orderId) return;

      await connection.execute(
        `SELECT o.order_id, oi.quantity, oi.unit_price, oi.line_total, p.product_name
         FROM orders o
         JOIN order_items oi ON o.order_id = oi.order_id
         JOIN products p ON oi.product_id = p.product_id
         WHERE o.order_id = :orderId`,
        { orderId }
      );

    } else {
      // Select products by category with reviews
      await connection.execute(
        `SELECT p.product_name, p.unit_price, c.category_name,
                COUNT(r.review_id) as review_count, AVG(r.rating) as avg_rating
         FROM products p
         JOIN categories c ON p.category_id = c.category_id
         LEFT JOIN product_reviews r ON p.product_id = r.product_id
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
    const elapsed = (now - this.stats.startTime) / 1000;

    // Calculate per-second rates
    const currentStats = {
      timestamp: now,
      elapsed: Math.floor(elapsed),
      total: {
        inserts: this.stats.inserts,
        updates: this.stats.updates,
        deletes: this.stats.deletes,
        selects: this.stats.selects,
        transactions: this.stats.transactions,
        errors: this.stats.errors
      },
      perSecond: {
        inserts: this.stats.inserts - this.previousStats.inserts,
        updates: this.stats.updates - this.previousStats.updates,
        deletes: this.stats.deletes - this.previousStats.deletes,
        selects: this.stats.selects - this.previousStats.selects,
        transactions: this.stats.transactions - this.previousStats.transactions,
        errors: this.stats.errors - this.previousStats.errors
      },
      averagePerSecond: {
        transactions: elapsed > 0 ? (this.stats.transactions / elapsed).toFixed(2) : 0
      }
    };

    // Calculate TPS
    currentStats.tps = currentStats.perSecond.transactions;

    this.previousStats = { ...this.stats };

    if (this.io) {
      this.io.emit('stress-metrics', currentStats);
    }
  }

  updateConfig(newConfig) {
    this.config = { ...this.config, ...newConfig };
    console.log('Updated stress config:', this.config);
  }

  async stop() {
    console.log('Stopping stress test...');
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

    // Report final stats
    const finalStats = {
      ...this.stats,
      duration: this.stats.startTime ? (Date.now() - this.stats.startTime) / 1000 : 0
    };

    console.log('Stress test stopped. Final stats:', finalStats);

    if (this.io) {
      this.io.emit('stress-stopped', finalStats);
    }

    return finalStats;
  }

  sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }
}

module.exports = new StressEngine();
