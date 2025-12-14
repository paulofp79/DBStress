// Schema Manager for Online Sales Database

const TABLES = {
  REGIONS: `
    CREATE TABLE regions (
      region_id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
      region_name VARCHAR2(100) NOT NULL,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )`,

  COUNTRIES: `
    CREATE TABLE countries (
      country_id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
      country_name VARCHAR2(100) NOT NULL,
      country_code VARCHAR2(3) NOT NULL,
      region_id NUMBER REFERENCES regions(region_id),
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )`,

  WAREHOUSES: `
    CREATE TABLE warehouses (
      warehouse_id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
      warehouse_name VARCHAR2(100) NOT NULL,
      location VARCHAR2(200),
      country_id NUMBER REFERENCES countries(country_id),
      capacity NUMBER DEFAULT 10000,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )`,

  CATEGORIES: `
    CREATE TABLE categories (
      category_id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
      category_name VARCHAR2(100) NOT NULL,
      parent_category_id NUMBER REFERENCES categories(category_id),
      description VARCHAR2(500),
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )`,

  PRODUCTS: `
    CREATE TABLE products (
      product_id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
      product_name VARCHAR2(200) NOT NULL,
      description VARCHAR2(2000),
      category_id NUMBER REFERENCES categories(category_id),
      unit_price NUMBER(10,2) NOT NULL,
      unit_cost NUMBER(10,2),
      weight NUMBER(10,2),
      status VARCHAR2(20) DEFAULT 'ACTIVE',
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )`,

  INVENTORY: `
    CREATE TABLE inventory (
      inventory_id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
      product_id NUMBER NOT NULL REFERENCES products(product_id),
      warehouse_id NUMBER NOT NULL REFERENCES warehouses(warehouse_id),
      quantity_on_hand NUMBER DEFAULT 0,
      quantity_reserved NUMBER DEFAULT 0,
      reorder_level NUMBER DEFAULT 10,
      last_restock_date TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      CONSTRAINT uk_inventory UNIQUE (product_id, warehouse_id)
    )`,

  CUSTOMERS: `
    CREATE TABLE customers (
      customer_id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
      first_name VARCHAR2(100) NOT NULL,
      last_name VARCHAR2(100) NOT NULL,
      email VARCHAR2(200) UNIQUE NOT NULL,
      phone VARCHAR2(20),
      address_line1 VARCHAR2(200),
      address_line2 VARCHAR2(200),
      city VARCHAR2(100),
      state_province VARCHAR2(100),
      postal_code VARCHAR2(20),
      country_id NUMBER REFERENCES countries(country_id),
      customer_type VARCHAR2(20) DEFAULT 'REGULAR',
      credit_limit NUMBER(10,2) DEFAULT 1000,
      balance NUMBER(10,2) DEFAULT 0,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )`,

  ORDERS: `
    CREATE TABLE orders (
      order_id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
      customer_id NUMBER NOT NULL REFERENCES customers(customer_id),
      order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      status VARCHAR2(20) DEFAULT 'PENDING',
      shipping_address VARCHAR2(500),
      shipping_method VARCHAR2(50),
      subtotal NUMBER(12,2) DEFAULT 0,
      tax_amount NUMBER(12,2) DEFAULT 0,
      shipping_cost NUMBER(10,2) DEFAULT 0,
      total_amount NUMBER(12,2) DEFAULT 0,
      notes VARCHAR2(1000),
      warehouse_id NUMBER REFERENCES warehouses(warehouse_id),
      shipped_date TIMESTAMP,
      delivered_date TIMESTAMP,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )`,

  ORDER_ITEMS: `
    CREATE TABLE order_items (
      order_item_id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
      order_id NUMBER NOT NULL REFERENCES orders(order_id),
      product_id NUMBER NOT NULL REFERENCES products(product_id),
      quantity NUMBER NOT NULL,
      unit_price NUMBER(10,2) NOT NULL,
      discount_percent NUMBER(5,2) DEFAULT 0,
      line_total NUMBER(12,2),
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )`,

  PAYMENTS: `
    CREATE TABLE payments (
      payment_id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
      order_id NUMBER NOT NULL REFERENCES orders(order_id),
      payment_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      amount NUMBER(12,2) NOT NULL,
      payment_method VARCHAR2(50) NOT NULL,
      transaction_ref VARCHAR2(100),
      status VARCHAR2(20) DEFAULT 'COMPLETED',
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )`,

  ORDER_HISTORY: `
    CREATE TABLE order_history (
      history_id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
      order_id NUMBER NOT NULL REFERENCES orders(order_id),
      old_status VARCHAR2(20),
      new_status VARCHAR2(20),
      changed_by VARCHAR2(100),
      change_reason VARCHAR2(500),
      changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )`,

  PRODUCT_REVIEWS: `
    CREATE TABLE product_reviews (
      review_id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
      product_id NUMBER NOT NULL REFERENCES products(product_id),
      customer_id NUMBER NOT NULL REFERENCES customers(customer_id),
      rating NUMBER(1) CHECK (rating BETWEEN 1 AND 5),
      review_title VARCHAR2(200),
      review_text VARCHAR2(4000),
      is_verified_purchase NUMBER(1) DEFAULT 0,
      helpful_votes NUMBER DEFAULT 0,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )`
};

const INDEXES = [
  'CREATE INDEX idx_products_category ON products(category_id)',
  'CREATE INDEX idx_products_status ON products(status)',
  'CREATE INDEX idx_inventory_product ON inventory(product_id)',
  'CREATE INDEX idx_inventory_warehouse ON inventory(warehouse_id)',
  'CREATE INDEX idx_customers_email ON customers(email)',
  'CREATE INDEX idx_customers_country ON customers(country_id)',
  'CREATE INDEX idx_orders_customer ON orders(customer_id)',
  'CREATE INDEX idx_orders_status ON orders(status)',
  'CREATE INDEX idx_orders_date ON orders(order_date)',
  'CREATE INDEX idx_orders_warehouse ON orders(warehouse_id)',
  'CREATE INDEX idx_order_items_order ON order_items(order_id)',
  'CREATE INDEX idx_order_items_product ON order_items(product_id)',
  'CREATE INDEX idx_payments_order ON payments(order_id)',
  'CREATE INDEX idx_order_history_order ON order_history(order_id)',
  'CREATE INDEX idx_reviews_product ON product_reviews(product_id)',
  'CREATE INDEX idx_reviews_customer ON product_reviews(customer_id)'
];

const SEQUENCES = [
  'CREATE SEQUENCE order_seq START WITH 100000 INCREMENT BY 1 CACHE 1000',
  'CREATE SEQUENCE customer_seq START WITH 100000 INCREMENT BY 1 CACHE 1000'
];

// Sample data generators
const REGIONS_DATA = [
  'North America', 'South America', 'Europe', 'Asia Pacific', 'Middle East', 'Africa'
];

const COUNTRIES_DATA = [
  { name: 'United States', code: 'USA', region: 'North America' },
  { name: 'Canada', code: 'CAN', region: 'North America' },
  { name: 'Mexico', code: 'MEX', region: 'North America' },
  { name: 'Brazil', code: 'BRA', region: 'South America' },
  { name: 'Argentina', code: 'ARG', region: 'South America' },
  { name: 'United Kingdom', code: 'GBR', region: 'Europe' },
  { name: 'Germany', code: 'DEU', region: 'Europe' },
  { name: 'France', code: 'FRA', region: 'Europe' },
  { name: 'Spain', code: 'ESP', region: 'Europe' },
  { name: 'Italy', code: 'ITA', region: 'Europe' },
  { name: 'Japan', code: 'JPN', region: 'Asia Pacific' },
  { name: 'China', code: 'CHN', region: 'Asia Pacific' },
  { name: 'Australia', code: 'AUS', region: 'Asia Pacific' },
  { name: 'India', code: 'IND', region: 'Asia Pacific' },
  { name: 'UAE', code: 'ARE', region: 'Middle East' },
  { name: 'South Africa', code: 'ZAF', region: 'Africa' }
];

const CATEGORIES_DATA = [
  { name: 'Electronics', parent: null },
  { name: 'Computers', parent: 'Electronics' },
  { name: 'Smartphones', parent: 'Electronics' },
  { name: 'Audio', parent: 'Electronics' },
  { name: 'Clothing', parent: null },
  { name: 'Men\'s Clothing', parent: 'Clothing' },
  { name: 'Women\'s Clothing', parent: 'Clothing' },
  { name: 'Home & Garden', parent: null },
  { name: 'Furniture', parent: 'Home & Garden' },
  { name: 'Kitchen', parent: 'Home & Garden' },
  { name: 'Sports & Outdoors', parent: null },
  { name: 'Fitness', parent: 'Sports & Outdoors' },
  { name: 'Camping', parent: 'Sports & Outdoors' },
  { name: 'Books', parent: null },
  { name: 'Fiction', parent: 'Books' },
  { name: 'Non-Fiction', parent: 'Books' }
];

const FIRST_NAMES = ['James', 'John', 'Robert', 'Michael', 'William', 'David', 'Richard', 'Joseph', 'Thomas', 'Charles', 'Mary', 'Patricia', 'Jennifer', 'Linda', 'Elizabeth', 'Barbara', 'Susan', 'Jessica', 'Sarah', 'Karen', 'Emma', 'Olivia', 'Ava', 'Isabella', 'Sophia', 'Mia', 'Charlotte', 'Amelia', 'Harper', 'Evelyn'];
const LAST_NAMES = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis', 'Rodriguez', 'Martinez', 'Hernandez', 'Lopez', 'Gonzalez', 'Wilson', 'Anderson', 'Thomas', 'Taylor', 'Moore', 'Jackson', 'Martin', 'Lee', 'Perez', 'Thompson', 'White', 'Harris', 'Sanchez', 'Clark', 'Ramirez', 'Lewis', 'Robinson'];
const CITIES = ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix', 'Philadelphia', 'San Antonio', 'San Diego', 'Dallas', 'San Jose', 'Austin', 'Jacksonville', 'Fort Worth', 'Columbus', 'Charlotte', 'Seattle', 'Denver', 'Boston', 'Detroit', 'Portland'];

const PRODUCT_ADJECTIVES = ['Premium', 'Professional', 'Ultra', 'Advanced', 'Classic', 'Modern', 'Deluxe', 'Essential', 'Elite', 'Pro'];
const PRODUCT_NOUNS = ['Laptop', 'Phone', 'Tablet', 'Headphones', 'Speaker', 'Camera', 'Watch', 'Keyboard', 'Mouse', 'Monitor', 'Shirt', 'Pants', 'Jacket', 'Shoes', 'Bag', 'Chair', 'Desk', 'Lamp', 'Sofa', 'Bed'];

class SchemaManager {
  async createSchema(db, progressCallback = () => {}) {
    const tableNames = Object.keys(TABLES);
    const totalSteps = tableNames.length + SEQUENCES.length;
    let currentStep = 0;

    // Create sequences first
    for (const seqSql of SEQUENCES) {
      try {
        await db.execute(seqSql);
      } catch (err) {
        if (!err.message.includes('ORA-00955')) { // Ignore "name already used"
          console.log('Sequence warning:', err.message);
        }
      }
      currentStep++;
      progressCallback({ step: 'Creating sequences...', progress: Math.floor((currentStep / totalSteps) * 30) });
    }

    // Create tables in order (without indexes to avoid overhead during bulk load)
    for (const tableName of tableNames) {
      try {
        await db.execute(TABLES[tableName]);
        console.log(`Created table: ${tableName}`);
      } catch (err) {
        if (!err.message.includes('ORA-00955')) { // Ignore "name already used"
          throw err;
        }
        console.log(`Table ${tableName} already exists`);
      }
      currentStep++;
      progressCallback({ step: `Creating table ${tableName}...`, progress: Math.floor((currentStep / totalSteps) * 30) });
    }
  }

  async createIndexes(db, progressCallback = () => {}) {
    // Create indexes after data population to avoid index build overhead during bulk load
    let currentStep = 0;
    const totalSteps = INDEXES.length;

    for (const indexSql of INDEXES) {
      try {
        await db.execute(indexSql);
      } catch (err) {
        if (!err.message.includes('ORA-00955') && !err.message.includes('ORA-01408')) {
          console.log('Index warning:', err.message);
        }
      }
      currentStep++;
      progressCallback({ step: 'Creating indexes...', progress: 90 + Math.floor((currentStep / totalSteps) * 10) });
    }
  }

  async populateData(db, scaleFactor = 1, progressCallback = () => {}, options = {}) {
    const baseCustomers = 1000 * scaleFactor;
    const baseProducts = 500 * scaleFactor;
    const baseOrders = 5000 * scaleFactor;

    // Options for performance optimization
    const { useDirectPath = false, batchCommitSize = 1000 } = options;

    let progress = 30;

    // Insert regions
    progressCallback({ step: 'Inserting regions...', progress: progress += 2 });
    for (const region of REGIONS_DATA) {
      try {
        await db.execute(
          `INSERT INTO regions (region_name) VALUES (:name)`,
          [region]
        );
      } catch (err) {
        if (!err.message.includes('ORA-00001')) throw err;
      }
    }

    // Get region IDs
    const regionsResult = await db.execute('SELECT region_id, region_name FROM regions');
    const regionMap = {};
    regionsResult.rows.forEach(r => regionMap[r.REGION_NAME] = r.REGION_ID);

    // Insert countries
    progressCallback({ step: 'Inserting countries...', progress: progress += 2 });
    for (const country of COUNTRIES_DATA) {
      try {
        await db.execute(
          `INSERT INTO countries (country_name, country_code, region_id) VALUES (:name, :code, :regionId)`,
          [country.name, country.code, regionMap[country.region]]
        );
      } catch (err) {
        if (!err.message.includes('ORA-00001')) throw err;
      }
    }

    // Get country IDs
    const countriesResult = await db.execute('SELECT country_id, country_name FROM countries');
    const countryMap = {};
    countriesResult.rows.forEach(c => countryMap[c.COUNTRY_NAME] = c.COUNTRY_ID);
    const countryIds = Object.values(countryMap);

    // Insert warehouses
    progressCallback({ step: 'Inserting warehouses...', progress: progress += 2 });
    const warehouseLocations = ['East Coast DC', 'West Coast DC', 'Central DC', 'European DC', 'Asian DC'];
    for (let i = 0; i < warehouseLocations.length; i++) {
      try {
        await db.execute(
          `INSERT INTO warehouses (warehouse_name, location, country_id, capacity) VALUES (:name, :loc, :countryId, :cap)`,
          [warehouseLocations[i], `Warehouse ${i + 1}`, countryIds[i % countryIds.length], 50000 * scaleFactor]
        );
      } catch (err) {
        if (!err.message.includes('ORA-00001')) throw err;
      }
    }

    // Get warehouse IDs
    const warehousesResult = await db.execute('SELECT warehouse_id FROM warehouses');
    const warehouseIds = warehousesResult.rows.map(w => w.WAREHOUSE_ID);

    // Insert categories
    progressCallback({ step: 'Inserting categories...', progress: progress += 2 });
    const categoryMap = {};
    for (const cat of CATEGORIES_DATA) {
      try {
        const parentId = cat.parent ? categoryMap[cat.parent] : null;
        const result = await db.execute(
          `INSERT INTO categories (category_name, parent_category_id, description) VALUES (:name, :parentId, :desc) RETURNING category_id INTO :id`,
          {
            name: cat.name,
            parentId: parentId,
            desc: `${cat.name} category`,
            id: { type: 2002, dir: 3003 } // NUMBER, BIND_OUT
          }
        );
        categoryMap[cat.name] = result.outBinds.id[0];
      } catch (err) {
        if (!err.message.includes('ORA-00001')) {
          // Try to get existing category ID
          const existing = await db.execute('SELECT category_id FROM categories WHERE category_name = :name', [cat.name]);
          if (existing.rows.length > 0) {
            categoryMap[cat.name] = existing.rows[0].CATEGORY_ID;
          }
        }
      }
    }
    const categoryIds = Object.values(categoryMap);

    // Insert products in batches using executeMany
    progressCallback({ step: `Inserting ${baseProducts} products...`, progress: progress = 40 });
    const productBinds = [];
    
    for (let i = 0; i < baseProducts; i++) {
      const adj = PRODUCT_ADJECTIVES[Math.floor(Math.random() * PRODUCT_ADJECTIVES.length)];
      const noun = PRODUCT_NOUNS[Math.floor(Math.random() * PRODUCT_NOUNS.length)];
      const price = (Math.random() * 500 + 10).toFixed(2);
      productBinds.push({
        name: `${adj} ${noun} ${i + 1}`,
        desc: `High quality ${noun.toLowerCase()} with premium features`,
        categoryId: categoryIds[Math.floor(Math.random() * categoryIds.length)],
        price: parseFloat(price),
        cost: parseFloat((price * 0.6).toFixed(2)),
        weight: parseFloat((Math.random() * 10 + 0.5).toFixed(2))
      });
    }

    // Use executeMany for bulk insert with optional direct-path hints
    const productSql = useDirectPath 
      ? `INSERT /*+ APPEND */ INTO products (product_name, description, category_id, unit_price, unit_cost, weight)
         VALUES (:name, :desc, :categoryId, :price, :cost, :weight)`
      : `INSERT INTO products (product_name, description, category_id, unit_price, unit_cost, weight)
         VALUES (:name, :desc, :categoryId, :price, :cost, :weight)`;
    
    try {
      // Auto-commit when batch size allows processing all data in one transaction
      const shouldAutoCommit = batchCommitSize >= baseProducts;
      await db.executeMany(productSql, productBinds, { 
        autoCommit: shouldAutoCommit,
        batchErrors: true 
      });
      // Manual commit if we didn't auto-commit
      if (!shouldAutoCommit) {
        await db.execute('COMMIT');
      }
      progressCallback({ step: `Inserted ${baseProducts} products`, progress: progress = 45 });
    } catch (err) {
      console.log('Product bulk insert warning:', err.message);
    }

    // Get all product IDs via SELECT
    const prodsResult = await db.execute('SELECT product_id FROM products ORDER BY product_id');
    const productIds = prodsResult.rows.map(p => p.PRODUCT_ID);

    // Insert inventory for each product in each warehouse using executeMany
    progressCallback({ step: 'Inserting inventory...', progress: progress = 50 });
    const inventoryBinds = [];
    
    for (const productId of productIds) {
      for (const warehouseId of warehouseIds) {
        inventoryBinds.push({
          prodId: productId,
          whId: warehouseId,
          qty: Math.floor(Math.random() * 1000) + 100,
          reserved: Math.floor(Math.random() * 50),
          reorder: Math.floor(Math.random() * 20) + 10
        });
      }
    }

    const inventorySql = useDirectPath
      ? `INSERT /*+ APPEND */ INTO inventory (product_id, warehouse_id, quantity_on_hand, quantity_reserved, reorder_level)
         VALUES (:prodId, :whId, :qty, :reserved, :reorder)`
      : `INSERT INTO inventory (product_id, warehouse_id, quantity_on_hand, quantity_reserved, reorder_level)
         VALUES (:prodId, :whId, :qty, :reserved, :reorder)`;

    try {
      const shouldAutoCommit = batchCommitSize >= inventoryBinds.length;
      await db.executeMany(inventorySql, inventoryBinds, {
        autoCommit: shouldAutoCommit,
        batchErrors: true
      });
      if (!shouldAutoCommit) {
        await db.execute('COMMIT');
      }
      progressCallback({ step: `Inserted ${inventoryBinds.length} inventory records`, progress: progress = 55 });
    } catch (err) {
      console.log('Inventory bulk insert warning:', err.message);
    }

    // Insert customers in batches using executeMany
    progressCallback({ step: `Inserting ${baseCustomers} customers...`, progress: progress = 60 });
    const customerBinds = [];

    for (let i = 0; i < baseCustomers; i++) {
      const firstName = FIRST_NAMES[Math.floor(Math.random() * FIRST_NAMES.length)];
      const lastName = LAST_NAMES[Math.floor(Math.random() * LAST_NAMES.length)];
      const city = CITIES[Math.floor(Math.random() * CITIES.length)];

      customerBinds.push({
        firstName,
        lastName,
        email: `${firstName.toLowerCase()}.${lastName.toLowerCase()}.${i}@example.com`,
        phone: `+1-${Math.floor(Math.random() * 900 + 100)}-${Math.floor(Math.random() * 900 + 100)}-${Math.floor(Math.random() * 9000 + 1000)}`,
        addr: `${Math.floor(Math.random() * 9999) + 1} Main Street`,
        city,
        state: 'State',
        postal: String(Math.floor(Math.random() * 90000) + 10000),
        countryId: countryIds[Math.floor(Math.random() * countryIds.length)],
        credit: Math.floor(Math.random() * 10000) + 1000
      });
    }

    const customerSql = useDirectPath
      ? `INSERT /*+ APPEND */ INTO customers (first_name, last_name, email, phone, address_line1, city, state_province, postal_code, country_id, credit_limit)
         VALUES (:firstName, :lastName, :email, :phone, :addr, :city, :state, :postal, :countryId, :credit)`
      : `INSERT INTO customers (first_name, last_name, email, phone, address_line1, city, state_province, postal_code, country_id, credit_limit)
         VALUES (:firstName, :lastName, :email, :phone, :addr, :city, :state, :postal, :countryId, :credit)`;

    try {
      const shouldAutoCommit = batchCommitSize >= baseCustomers;
      await db.executeMany(customerSql, customerBinds, {
        autoCommit: shouldAutoCommit,
        batchErrors: true
      });
      if (!shouldAutoCommit) {
        await db.execute('COMMIT');
      }
      progressCallback({ step: `Inserted ${baseCustomers} customers`, progress: progress = 65 });
    } catch (err) {
      console.log('Customer bulk insert warning:', err.message);
    }

    // Get all customer IDs via SELECT
    const custResult = await db.execute('SELECT customer_id FROM customers ORDER BY customer_id');
    const customerIds = custResult.rows.map(c => c.CUSTOMER_ID);

    // Insert orders and order items using executeMany
    progressCallback({ step: `Inserting ${baseOrders} orders...`, progress: progress = 70 });
    const statuses = ['PENDING', 'PROCESSING', 'SHIPPED', 'DELIVERED', 'CANCELLED'];
    const paymentMethods = ['CREDIT_CARD', 'DEBIT_CARD', 'PAYPAL', 'BANK_TRANSFER', 'CRYPTO'];

    // Prepare order data
    const orderBinds = [];
    const orderItemsData = [];
    const paymentsData = [];

    for (let i = 0; i < baseOrders; i++) {
      const customerId = customerIds[Math.floor(Math.random() * customerIds.length)];
      const warehouseId = warehouseIds[Math.floor(Math.random() * warehouseIds.length)];
      const status = statuses[Math.floor(Math.random() * statuses.length)];
      const itemCount = Math.floor(Math.random() * 5) + 1;
      const shippingMethod = ['Standard', 'Express', 'Overnight'][Math.floor(Math.random() * 3)];

      // Calculate order totals
      let subtotal = 0;
      const items = [];
      for (let k = 0; k < itemCount; k++) {
        const productId = productIds[Math.floor(Math.random() * productIds.length)];
        const quantity = Math.floor(Math.random() * 5) + 1;
        const unitPrice = parseFloat((Math.random() * 200 + 10).toFixed(2));
        const lineTotal = quantity * unitPrice;
        subtotal += lineTotal;
        items.push({ productId, quantity, unitPrice, lineTotal });
      }

      const tax = subtotal * 0.08;
      const shipping = Math.random() * 20 + 5;
      const total = subtotal + tax + shipping;

      orderBinds.push({
        custId: customerId,
        status,
        whId: warehouseId,
        ship: shippingMethod,
        notes: `Order ${i + 1}`,
        subtotal,
        tax,
        shippingCost: shipping,
        total
      });

      // Store order items for this order (we'll need the order ID later)
      orderItemsData.push({ orderIndex: i, items });

      // Store payment data for non-pending orders
      if (status !== 'PENDING' && status !== 'CANCELLED') {
        paymentsData.push({
          orderIndex: i,
          amount: total,
          method: paymentMethods[Math.floor(Math.random() * paymentMethods.length)],
          ref: `TXN${Date.now()}${Math.floor(Math.random() * 10000)}`
        });
      }
    }

    // Insert orders using executeMany
    const orderSql = useDirectPath
      ? `INSERT /*+ APPEND */ INTO orders (customer_id, status, warehouse_id, shipping_method, notes, subtotal, tax_amount, shipping_cost, total_amount)
         VALUES (:custId, :status, :whId, :ship, :notes, :subtotal, :tax, :shippingCost, :total)`
      : `INSERT INTO orders (customer_id, status, warehouse_id, shipping_method, notes, subtotal, tax_amount, shipping_cost, total_amount)
         VALUES (:custId, :status, :whId, :ship, :notes, :subtotal, :tax, :shippingCost, :total)`;

    try {
      const shouldAutoCommit = batchCommitSize >= baseOrders;
      await db.executeMany(orderSql, orderBinds, {
        autoCommit: shouldAutoCommit,
        batchErrors: true
      });
      if (!shouldAutoCommit) {
        await db.execute('COMMIT');
      }
      progressCallback({ step: `Inserted ${baseOrders} orders`, progress: progress = 75 });
    } catch (err) {
      console.log('Order bulk insert warning:', err.message);
    }

    // Get all order IDs via SELECT (get the most recent baseOrders orders)
    // Since we're in the schema creation process, we expect minimal concurrent activity
    const ordersResult = await db.execute(
      'SELECT order_id FROM orders ORDER BY created_at DESC, order_id DESC FETCH FIRST :limit ROWS ONLY', 
      [baseOrders]
    );
    const orderIds = ordersResult.rows.map(o => o.ORDER_ID).reverse();
    
    // Validate we got the expected number of orders
    if (orderIds.length !== baseOrders) {
      console.log(`Warning: Expected ${baseOrders} orders but found ${orderIds.length}. Order items and payments may be incomplete.`);
    }

    // Insert order items using executeMany
    progressCallback({ step: 'Inserting order items...', progress: progress = 78 });
    const orderItemBinds = [];
    
    for (let i = 0; i < Math.min(orderItemsData.length, orderIds.length); i++) {
      const orderId = orderIds[i];
      if (!orderId) continue;
      
      for (const item of orderItemsData[i].items) {
        orderItemBinds.push({
          orderId,
          prodId: item.productId,
          qty: item.quantity,
          price: item.unitPrice,
          total: item.lineTotal
        });
      }
    }

    const orderItemSql = useDirectPath
      ? `INSERT /*+ APPEND */ INTO order_items (order_id, product_id, quantity, unit_price, line_total)
         VALUES (:orderId, :prodId, :qty, :price, :total)`
      : `INSERT INTO order_items (order_id, product_id, quantity, unit_price, line_total)
         VALUES (:orderId, :prodId, :qty, :price, :total)`;

    try {
      const shouldAutoCommit = batchCommitSize >= orderItemBinds.length;
      await db.executeMany(orderItemSql, orderItemBinds, {
        autoCommit: shouldAutoCommit,
        batchErrors: true
      });
      if (!shouldAutoCommit) {
        await db.execute('COMMIT');
      }
      progressCallback({ step: `Inserted ${orderItemBinds.length} order items`, progress: progress = 82 });
    } catch (err) {
      console.log('Order items bulk insert warning:', err.message);
    }

    // Insert payments using executeMany
    progressCallback({ step: 'Inserting payments...', progress: progress = 85 });
    const paymentBinds = [];
    
    for (let i = 0; i < Math.min(paymentsData.length, orderIds.length); i++) {
      const payment = paymentsData.find(p => p.orderIndex === i);
      if (!payment) continue;
      
      const orderId = orderIds[i];
      if (!orderId) continue;
      
      paymentBinds.push({
        orderId,
        amount: payment.amount,
        method: payment.method,
        ref: payment.ref
      });
    }

    const paymentSql = useDirectPath
      ? `INSERT /*+ APPEND */ INTO payments (order_id, amount, payment_method, transaction_ref, status)
         VALUES (:orderId, :amount, :method, :ref, 'COMPLETED')`
      : `INSERT INTO payments (order_id, amount, payment_method, transaction_ref, status)
         VALUES (:orderId, :amount, :method, :ref, 'COMPLETED')`;

    if (paymentBinds.length > 0) {
      try {
        const shouldAutoCommit = batchCommitSize >= paymentBinds.length;
        await db.executeMany(paymentSql, paymentBinds, {
          autoCommit: shouldAutoCommit,
          batchErrors: true
        });
        if (!shouldAutoCommit) {
          await db.execute('COMMIT');
        }
        progressCallback({ step: `Inserted ${paymentBinds.length} payments`, progress: progress = 88 });
      } catch (err) {
        console.log('Payment bulk insert warning:', err.message);
      }
    }

    // Add some product reviews
    progressCallback({ step: 'Adding product reviews...', progress: 90 });
    const reviewCount = Math.min(baseCustomers, baseProducts) * 0.5;
    for (let i = 0; i < reviewCount; i++) {
      try {
        await db.execute(
          `INSERT INTO product_reviews (product_id, customer_id, rating, review_title, review_text, is_verified_purchase)
           VALUES (:prodId, :custId, :rating, :title, :text, :verified)`,
          {
            prodId: productIds[Math.floor(Math.random() * productIds.length)],
            custId: customerIds[Math.floor(Math.random() * customerIds.length)],
            rating: Math.floor(Math.random() * 5) + 1,
            title: ['Great product!', 'Good value', 'As expected', 'Could be better', 'Excellent quality'][Math.floor(Math.random() * 5)],
            text: 'This is a sample review for the product.',
            verified: Math.random() > 0.3 ? 1 : 0
          }
        );
      } catch (err) {
        // Ignore duplicate reviews
      }
    }

    progressCallback({ step: 'Data population complete!', progress: 90 });
  }

  async dropSchema(db) {
    const dropOrder = [
      'product_reviews', 'order_history', 'payments', 'order_items', 'orders',
      'customers', 'inventory', 'products', 'categories', 'warehouses',
      'countries', 'regions'
    ];

    for (const table of dropOrder) {
      try {
        await db.execute(`DROP TABLE ${table} CASCADE CONSTRAINTS PURGE`);
        console.log(`Dropped table: ${table}`);
      } catch (err) {
        if (!err.message.includes('ORA-00942')) { // Table doesn't exist
          console.log(`Warning dropping ${table}:`, err.message);
        }
      }
    }

    // Drop sequences
    for (const seq of ['order_seq', 'customer_seq']) {
      try {
        await db.execute(`DROP SEQUENCE ${seq}`);
      } catch (err) {
        // Ignore
      }
    }
  }

  async getSchemaInfo(db) {
    try {
      const tables = await db.execute(`
        SELECT table_name, num_rows, blocks, avg_row_len
        FROM user_tables
        WHERE table_name IN ('REGIONS', 'COUNTRIES', 'WAREHOUSES', 'CATEGORIES', 'PRODUCTS',
                             'INVENTORY', 'CUSTOMERS', 'ORDERS', 'ORDER_ITEMS', 'PAYMENTS',
                             'ORDER_HISTORY', 'PRODUCT_REVIEWS')
        ORDER BY table_name
      `);

      const counts = {};
      for (const table of ['regions', 'countries', 'warehouses', 'categories', 'products',
                           'inventory', 'customers', 'orders', 'order_items', 'payments']) {
        try {
          const result = await db.execute(`SELECT COUNT(*) as cnt FROM ${table}`);
          counts[table] = result.rows[0].CNT;
        } catch (err) {
          counts[table] = 0;
        }
      }

      const totalSize = await db.execute(`
        SELECT SUM(bytes)/1024/1024 as size_mb
        FROM user_segments
        WHERE segment_name IN ('REGIONS', 'COUNTRIES', 'WAREHOUSES', 'CATEGORIES', 'PRODUCTS',
                               'INVENTORY', 'CUSTOMERS', 'ORDERS', 'ORDER_ITEMS', 'PAYMENTS',
                               'ORDER_HISTORY', 'PRODUCT_REVIEWS')
      `);

      return {
        tables: tables.rows,
        counts,
        totalSizeMB: totalSize.rows[0]?.SIZE_MB || 0,
        schemaExists: tables.rows.length > 0
      };
    } catch (err) {
      return {
        tables: [],
        counts: {},
        totalSizeMB: 0,
        schemaExists: false,
        error: err.message
      };
    }
  }
}

module.exports = new SchemaManager();
