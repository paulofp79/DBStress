// Schema Manager for Online Sales Database - Multi-Schema Support
const oracledb = require('oracledb');

// Compression type mappings
const COMPRESSION_TYPES = {
  'none': '',
  'basic': 'ROW STORE COMPRESS BASIC',
  'advanced': 'ROW STORE COMPRESS ADVANCED',
  'query_low': 'COLUMN STORE COMPRESS FOR QUERY LOW',
  'query_high': 'COLUMN STORE COMPRESS FOR QUERY HIGH',
  'archive_low': 'COLUMN STORE COMPRESS FOR ARCHIVE LOW',
  'archive_high': 'COLUMN STORE COMPRESS FOR ARCHIVE HIGH'
};

// Base table definitions (prefix will be added dynamically)
const getTableDDL = (prefix, compressionType = 'none') => {
  const p = prefix ? `${prefix}_` : '';
  const compressClause = COMPRESSION_TYPES[compressionType] ? ` ${COMPRESSION_TYPES[compressionType]}` : '';

  return {
    [`${p}regions`]: `
      CREATE TABLE ${p}regions (
        region_id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        region_name VARCHAR2(100) NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )${compressClause} NOLOGGING`,

    [`${p}countries`]: `
      CREATE TABLE ${p}countries (
        country_id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        country_name VARCHAR2(100) NOT NULL,
        country_code VARCHAR2(3) NOT NULL,
        region_id NUMBER REFERENCES ${p}regions(region_id),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )${compressClause} NOLOGGING`,

    [`${p}warehouses`]: `
      CREATE TABLE ${p}warehouses (
        warehouse_id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        warehouse_name VARCHAR2(100) NOT NULL,
        location VARCHAR2(200),
        country_id NUMBER REFERENCES ${p}countries(country_id),
        capacity NUMBER DEFAULT 10000,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )${compressClause} NOLOGGING`,

    [`${p}categories`]: `
      CREATE TABLE ${p}categories (
        category_id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        category_name VARCHAR2(100) NOT NULL,
        parent_category_id NUMBER REFERENCES ${p}categories(category_id),
        description VARCHAR2(500),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )${compressClause} NOLOGGING`,

    [`${p}products`]: `
      CREATE TABLE ${p}products (
        product_id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        product_name VARCHAR2(200) NOT NULL,
        description VARCHAR2(2000),
        category_id NUMBER REFERENCES ${p}categories(category_id),
        unit_price NUMBER(10,2) NOT NULL,
        unit_cost NUMBER(10,2),
        weight NUMBER(10,2),
        status VARCHAR2(20) DEFAULT 'ACTIVE',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )${compressClause} NOLOGGING`,

    [`${p}inventory`]: `
      CREATE TABLE ${p}inventory (
        inventory_id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        product_id NUMBER NOT NULL REFERENCES ${p}products(product_id),
        warehouse_id NUMBER NOT NULL REFERENCES ${p}warehouses(warehouse_id),
        quantity_on_hand NUMBER DEFAULT 0,
        quantity_reserved NUMBER DEFAULT 0,
        reorder_level NUMBER DEFAULT 10,
        last_restock_date TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        CONSTRAINT ${p}uk_inventory UNIQUE (product_id, warehouse_id)
      )${compressClause} NOLOGGING`,

    [`${p}customers`]: `
      CREATE TABLE ${p}customers (
        customer_id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        first_name VARCHAR2(100) NOT NULL,
        last_name VARCHAR2(100) NOT NULL,
        email VARCHAR2(200) NOT NULL,
        phone VARCHAR2(20),
        address_line1 VARCHAR2(200),
        address_line2 VARCHAR2(200),
        city VARCHAR2(100),
        state_province VARCHAR2(100),
        postal_code VARCHAR2(20),
        country_id NUMBER REFERENCES ${p}countries(country_id),
        customer_type VARCHAR2(20) DEFAULT 'REGULAR',
        credit_limit NUMBER(10,2) DEFAULT 1000,
        balance NUMBER(10,2) DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )${compressClause} NOLOGGING`,

    [`${p}orders`]: `
      CREATE TABLE ${p}orders (
        order_id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        customer_id NUMBER NOT NULL REFERENCES ${p}customers(customer_id),
        order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        status VARCHAR2(20) DEFAULT 'PENDING',
        shipping_address VARCHAR2(500),
        shipping_method VARCHAR2(50),
        subtotal NUMBER(12,2) DEFAULT 0,
        tax_amount NUMBER(12,2) DEFAULT 0,
        shipping_cost NUMBER(10,2) DEFAULT 0,
        total_amount NUMBER(12,2) DEFAULT 0,
        notes VARCHAR2(1000),
        warehouse_id NUMBER REFERENCES ${p}warehouses(warehouse_id),
        shipped_date TIMESTAMP,
        delivered_date TIMESTAMP,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )${compressClause} NOLOGGING`,

    [`${p}order_items`]: `
      CREATE TABLE ${p}order_items (
        order_item_id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        order_id NUMBER NOT NULL REFERENCES ${p}orders(order_id),
        product_id NUMBER NOT NULL REFERENCES ${p}products(product_id),
        quantity NUMBER NOT NULL,
        unit_price NUMBER(10,2) NOT NULL,
        discount_percent NUMBER(5,2) DEFAULT 0,
        line_total NUMBER(12,2),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )${compressClause} NOLOGGING`,

    [`${p}payments`]: `
      CREATE TABLE ${p}payments (
        payment_id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        order_id NUMBER NOT NULL REFERENCES ${p}orders(order_id),
        payment_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        amount NUMBER(12,2) NOT NULL,
        payment_method VARCHAR2(50) NOT NULL,
        transaction_ref VARCHAR2(100),
        status VARCHAR2(20) DEFAULT 'COMPLETED',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )${compressClause} NOLOGGING`,

    [`${p}order_history`]: `
      CREATE TABLE ${p}order_history (
        history_id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        order_id NUMBER NOT NULL REFERENCES ${p}orders(order_id),
        old_status VARCHAR2(20),
        new_status VARCHAR2(20),
        changed_by VARCHAR2(100),
        change_reason VARCHAR2(500),
        changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )${compressClause} NOLOGGING`,

    [`${p}product_reviews`]: `
      CREATE TABLE ${p}product_reviews (
        review_id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        product_id NUMBER NOT NULL REFERENCES ${p}products(product_id),
        customer_id NUMBER NOT NULL REFERENCES ${p}customers(customer_id),
        rating NUMBER(1) CHECK (rating BETWEEN 1 AND 5),
        review_title VARCHAR2(200),
        review_text VARCHAR2(4000),
        is_verified_purchase NUMBER(1) DEFAULT 0,
        helpful_votes NUMBER DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )${compressClause} NOLOGGING`,

    // RAC Contention table - designed for maximum block contention
    // Small PCTFREE, no sequence PK, single block target
    [`${p}rac_hotblock`]: `
      CREATE TABLE ${p}rac_hotblock (
        slot_id NUMBER NOT NULL,
        counter NUMBER DEFAULT 0,
        last_instance NUMBER,
        last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        padding VARCHAR2(500) DEFAULT RPAD('X', 500, 'X'),
        CONSTRAINT ${p}pk_rac_hotblock PRIMARY KEY (slot_id)
      ) PCTFREE 5 INITRANS 1 MAXTRANS 255 LOGGING`,

    // RAC contention with index hot spots
    [`${p}rac_hotindex`]: `
      CREATE TABLE ${p}rac_hotindex (
        id NUMBER NOT NULL,
        bucket NUMBER NOT NULL,
        value NUMBER DEFAULT 0,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        CONSTRAINT ${p}pk_rac_hotindex PRIMARY KEY (id)
      ) PCTFREE 5 LOGGING`
  };
};

const getIndexes = (prefix) => {
  const p = prefix ? `${prefix}_` : '';
  return [
    `CREATE INDEX ${p}idx_products_category ON ${p}products(category_id)`,
    `CREATE INDEX ${p}idx_products_status ON ${p}products(status)`,
    `CREATE INDEX ${p}idx_inventory_product ON ${p}inventory(product_id)`,
    `CREATE INDEX ${p}idx_inventory_warehouse ON ${p}inventory(warehouse_id)`,
    `CREATE INDEX ${p}idx_customers_country ON ${p}customers(country_id)`,
    `CREATE INDEX ${p}idx_orders_customer ON ${p}orders(customer_id)`,
    `CREATE INDEX ${p}idx_orders_status ON ${p}orders(status)`,
    `CREATE INDEX ${p}idx_orders_date ON ${p}orders(order_date)`,
    `CREATE INDEX ${p}idx_orders_warehouse ON ${p}orders(warehouse_id)`,
    `CREATE INDEX ${p}idx_order_items_order ON ${p}order_items(order_id)`,
    `CREATE INDEX ${p}idx_order_items_product ON ${p}order_items(product_id)`,
    `CREATE INDEX ${p}idx_payments_order ON ${p}payments(order_id)`,
    `CREATE INDEX ${p}idx_order_history_order ON ${p}order_history(order_id)`,
    `CREATE INDEX ${p}idx_reviews_product ON ${p}product_reviews(product_id)`,
    `CREATE INDEX ${p}idx_reviews_customer ON ${p}product_reviews(customer_id)`,
    // RAC contention indexes - non-unique index on bucket creates hot index blocks
    `CREATE INDEX ${p}idx_rac_hotindex_bucket ON ${p}rac_hotindex(bucket)`
  ];
};

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
  constructor() {
    this.schemas = new Map(); // Store schema metadata
  }

  getTableNames(prefix) {
    const p = prefix ? `${prefix}_` : '';
    return [
      `${p}regions`, `${p}countries`, `${p}warehouses`, `${p}categories`,
      `${p}products`, `${p}inventory`, `${p}customers`, `${p}orders`,
      `${p}order_items`, `${p}payments`, `${p}order_history`, `${p}product_reviews`,
      `${p}rac_hotblock`, `${p}rac_hotindex`
    ];
  }

  async createSchema(db, options = {}, progressCallback = () => {}) {
    // Support both old boolean 'compress' and new 'compressionType' options
    const { prefix = '', compress = false, compressionType = null } = options;
    const effectiveCompression = compressionType || (compress ? 'advanced' : 'none');
    const tables = getTableDDL(prefix, effectiveCompression);
    const indexes = getIndexes(prefix);
    const tableNames = Object.keys(tables);
    const totalSteps = tableNames.length + indexes.length;
    let currentStep = 0;

    const compressionLabel = COMPRESSION_TYPES[effectiveCompression] || 'no compression';
    progressCallback({ step: `Creating schema${prefix ? ` '${prefix}'` : ''} (${compressionLabel})...`, progress: 0 });

    // Create tables in order
    for (const tableName of tableNames) {
      try {
        await db.execute(tables[tableName]);
        console.log(`Created table: ${tableName}`);
      } catch (err) {
        if (!err.message.includes('ORA-00955')) {
          throw err;
        }
        console.log(`Table ${tableName} already exists`);
      }
      currentStep++;
      progressCallback({ step: `Creating table ${tableName}...`, progress: Math.floor((currentStep / totalSteps) * 50) });
    }

    // Create indexes
    for (const indexSql of indexes) {
      try {
        await db.execute(indexSql);
      } catch (err) {
        if (!err.message.includes('ORA-00955') && !err.message.includes('ORA-01408')) {
          console.log('Index warning:', err.message);
        }
      }
      currentStep++;
      progressCallback({ step: 'Creating indexes...', progress: Math.floor((currentStep / totalSteps) * 50) });
    }

    // Store schema metadata
    this.schemas.set(prefix || 'default', { prefix, compressionType: effectiveCompression, createdAt: new Date() });
  }

  // Parallel batch insert helper - uses executeMany for much better performance
  async parallelInsert(db, sql, dataArray, batchSize = 500, parallelism = 4) {
    if (dataArray.length === 0) return;

    const batches = [];
    for (let i = 0; i < dataArray.length; i += batchSize) {
      batches.push(dataArray.slice(i, i + batchSize));
    }

    console.log(`Inserting ${dataArray.length} rows in ${batches.length} batches (batch size: ${batchSize})`);

    // Process batches sequentially to avoid connection pool exhaustion
    for (let i = 0; i < batches.length; i++) {
      const batch = batches[i];
      let connection;
      try {
        connection = await db.getConnection();

        // Use executeMany for batch insert - much faster than individual inserts
        await connection.executeMany(sql, batch, {
          autoCommit: false,
          batchErrors: true  // Continue on errors
        });

        await connection.commit();
      } catch (err) {
        if (!err.message.includes('ORA-00001')) {
          console.log(`Batch ${i + 1}/${batches.length} warning:`, err.message);
        }
        if (connection) {
          try { await connection.rollback(); } catch (e) {}
        }
      } finally {
        if (connection) {
          try { await connection.close(); } catch (e) {}
        }
      }

      // Progress indication for large inserts
      if ((i + 1) % 10 === 0 || i === batches.length - 1) {
        console.log(`  Batch ${i + 1}/${batches.length} completed`);
      }
    }
  }

  async populateData(db, options = {}, progressCallback = () => {}) {
    const {
      prefix = '',
      scaleFactor = 1,
      parallelism = 10  // Number of parallel insert streams
    } = options;

    const p = prefix ? `${prefix}_` : '';
    const baseCustomers = 1000 * scaleFactor;
    const baseProducts = 500 * scaleFactor;
    const baseOrders = 5000 * scaleFactor;

    let progress = 50;
    progressCallback({ step: 'Starting data population...', progress });

    try {
      // Insert regions
      progressCallback({ step: 'Inserting regions...', progress: progress += 2 });
      for (const region of REGIONS_DATA) {
        try {
          await db.execute(`INSERT /*+ APPEND */ INTO ${p}regions (region_name) VALUES (:1)`, [region]);
        } catch (err) {
          if (!err.message.includes('ORA-00001')) throw err;
        }
      }

      // Get region IDs
      const regionsResult = await db.execute(`SELECT region_id, region_name FROM ${p}regions`);
      const regionMap = {};
      regionsResult.rows.forEach(r => regionMap[r.REGION_NAME] = r.REGION_ID);

      // Insert countries
      progressCallback({ step: 'Inserting countries...', progress: progress += 2 });
      for (const country of COUNTRIES_DATA) {
        try {
          await db.execute(
            `INSERT /*+ APPEND */ INTO ${p}countries (country_name, country_code, region_id) VALUES (:1, :2, :3)`,
            [country.name, country.code, regionMap[country.region]]
          );
        } catch (err) {
          if (!err.message.includes('ORA-00001')) throw err;
        }
      }

      // Get country IDs
      const countriesResult = await db.execute(`SELECT country_id FROM ${p}countries`);
      const countryIds = countriesResult.rows.map(c => c.COUNTRY_ID);

      if (countryIds.length === 0) {
        throw new Error('No countries available.');
      }

      // Insert warehouses
      progressCallback({ step: 'Inserting warehouses...', progress: progress += 2 });
      const warehouseLocations = ['East Coast DC', 'West Coast DC', 'Central DC', 'European DC', 'Asian DC'];
      for (let i = 0; i < warehouseLocations.length; i++) {
        try {
          await db.execute(
            `INSERT /*+ APPEND */ INTO ${p}warehouses (warehouse_name, location, country_id, capacity) VALUES (:1, :2, :3, :4)`,
            [warehouseLocations[i], `Warehouse ${i + 1}`, countryIds[i % countryIds.length], 50000 * scaleFactor]
          );
        } catch (err) {
          if (!err.message.includes('ORA-00001')) throw err;
        }
      }

      // Get warehouse IDs
      const warehousesResult = await db.execute(`SELECT warehouse_id FROM ${p}warehouses`);
      const warehouseIds = warehousesResult.rows.map(w => w.WAREHOUSE_ID);

      // Insert categories
      progressCallback({ step: 'Inserting categories...', progress: progress += 2 });
      for (const cat of CATEGORIES_DATA) {
        if (!cat.parent) {
          try {
            await db.execute(
              `INSERT /*+ APPEND */ INTO ${p}categories (category_name, description) VALUES (:1, :2)`,
              [cat.name, `${cat.name} category`]
            );
          } catch (err) {
            if (!err.message.includes('ORA-00001')) throw err;
          }
        }
      }

      const parentCatResult = await db.execute(`SELECT category_id, category_name FROM ${p}categories`);
      const categoryMap = {};
      parentCatResult.rows.forEach(c => categoryMap[c.CATEGORY_NAME] = c.CATEGORY_ID);

      for (const cat of CATEGORIES_DATA) {
        if (cat.parent && categoryMap[cat.parent]) {
          try {
            await db.execute(
              `INSERT /*+ APPEND */ INTO ${p}categories (category_name, parent_category_id, description) VALUES (:1, :2, :3)`,
              [cat.name, categoryMap[cat.parent], `${cat.name} category`]
            );
          } catch (err) {
            if (!err.message.includes('ORA-00001')) throw err;
          }
        }
      }

      const catResult = await db.execute(`SELECT category_id FROM ${p}categories`);
      const categoryIds = catResult.rows.map(c => c.CATEGORY_ID);

      if (categoryIds.length === 0) {
        throw new Error('No categories available.');
      }

      // PARALLEL INSERT: Products
      progressCallback({ step: `Inserting ${baseProducts} products (parallel)...`, progress: progress += 2 });
      const productData = [];
      for (let i = 0; i < baseProducts; i++) {
        const adj = PRODUCT_ADJECTIVES[Math.floor(Math.random() * PRODUCT_ADJECTIVES.length)];
        const noun = PRODUCT_NOUNS[Math.floor(Math.random() * PRODUCT_NOUNS.length)];
        const price = parseFloat((Math.random() * 500 + 10).toFixed(2));
        productData.push([
          `${adj} ${noun} ${i + 1}`,
          `High quality ${noun.toLowerCase()} with premium features`,
          categoryIds[Math.floor(Math.random() * categoryIds.length)],
          price,
          parseFloat((price * 0.6).toFixed(2)),
          parseFloat((Math.random() * 10 + 0.5).toFixed(2))
        ]);
      }

      await this.parallelInsert(
        db,
        `INSERT INTO ${p}products (product_name, description, category_id, unit_price, unit_cost, weight) VALUES (:1, :2, :3, :4, :5, :6)`,
        productData,
        500
      );
      progressCallback({ step: `Products inserted`, progress: 65 });

      // Get product IDs
      const prodsResult = await db.execute(`SELECT product_id FROM ${p}products`);
      const productIds = prodsResult.rows.map(r => r.PRODUCT_ID);

      // PARALLEL INSERT: Inventory
      progressCallback({ step: 'Inserting inventory (parallel)...', progress: 67 });
      const inventoryData = [];
      for (const productId of productIds) {
        for (const warehouseId of warehouseIds) {
          inventoryData.push([
            productId,
            warehouseId,
            Math.floor(Math.random() * 1000) + 100,
            Math.floor(Math.random() * 50),
            Math.floor(Math.random() * 20) + 10
          ]);
        }
      }

      await this.parallelInsert(
        db,
        `INSERT INTO ${p}inventory (product_id, warehouse_id, quantity_on_hand, quantity_reserved, reorder_level) VALUES (:1, :2, :3, :4, :5)`,
        inventoryData,
        500
      );
      progressCallback({ step: 'Inventory inserted', progress: 70 });

      // PARALLEL INSERT: Customers
      progressCallback({ step: `Inserting ${baseCustomers} customers (parallel)...`, progress: 72 });
      const customerData = [];
      for (let i = 0; i < baseCustomers; i++) {
        const firstName = FIRST_NAMES[Math.floor(Math.random() * FIRST_NAMES.length)];
        const lastName = LAST_NAMES[Math.floor(Math.random() * LAST_NAMES.length)];
        const city = CITIES[Math.floor(Math.random() * CITIES.length)];
        customerData.push([
          firstName,
          lastName,
          `${firstName.toLowerCase()}.${lastName.toLowerCase()}.${prefix || 'def'}.${i}@example.com`,
          `+1-${Math.floor(Math.random() * 900 + 100)}-${Math.floor(Math.random() * 900 + 100)}-${Math.floor(Math.random() * 9000 + 1000)}`,
          `${Math.floor(Math.random() * 9999) + 1} Main Street`,
          city,
          'State',
          String(Math.floor(Math.random() * 90000) + 10000),
          countryIds[Math.floor(Math.random() * countryIds.length)],
          Math.floor(Math.random() * 10000) + 1000
        ]);
      }

      await this.parallelInsert(
        db,
        `INSERT INTO ${p}customers (first_name, last_name, email, phone, address_line1, city, state_province, postal_code, country_id, credit_limit) VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10)`,
        customerData,
        500
      );
      progressCallback({ step: 'Customers inserted', progress: 80 });

      // Get customer IDs
      const custResult = await db.execute(`SELECT customer_id FROM ${p}customers`);
      const customerIds = custResult.rows.map(c => c.CUSTOMER_ID);

      if (customerIds.length === 0 || productIds.length === 0) {
        progressCallback({ step: 'Skipping orders - missing data', progress: 95 });
      } else {
        // PARALLEL INSERT: Orders
        progressCallback({ step: `Inserting ${baseOrders} orders (parallel)...`, progress: 82 });
        const statuses = ['PENDING', 'PROCESSING', 'SHIPPED', 'DELIVERED', 'CANCELLED'];
        const shippingMethods = ['Standard', 'Express', 'Overnight'];

        // Prepare all order data
        const orderData = [];
        for (let i = 0; i < baseOrders; i++) {
          orderData.push([
            customerIds[Math.floor(Math.random() * customerIds.length)],
            statuses[Math.floor(Math.random() * statuses.length)],
            warehouseIds[Math.floor(Math.random() * warehouseIds.length)],
            shippingMethods[Math.floor(Math.random() * 3)],
            `Order ${i + 1}`
          ]);
        }

        await this.parallelInsert(
          db,
          `INSERT INTO ${p}orders (customer_id, status, warehouse_id, shipping_method, notes) VALUES (:1, :2, :3, :4, :5)`,
          orderData,
          1000
        );
        progressCallback({ step: 'Orders inserted', progress: 88 });

        // Get order IDs
        const ordersResult = await db.execute(`SELECT order_id FROM ${p}orders`);
        const orderIds = ordersResult.rows.map(o => o.ORDER_ID);

        // PARALLEL INSERT: Order Items
        progressCallback({ step: 'Inserting order items (parallel)...', progress: 90 });
        const orderItemData = [];
        for (const orderId of orderIds) {
          const itemCount = Math.floor(Math.random() * 3) + 1;
          for (let k = 0; k < itemCount; k++) {
            const quantity = Math.floor(Math.random() * 5) + 1;
            const unitPrice = parseFloat((Math.random() * 200 + 10).toFixed(2));
            const lineTotal = parseFloat((quantity * unitPrice).toFixed(2));
            orderItemData.push([
              orderId,
              productIds[Math.floor(Math.random() * productIds.length)],
              quantity,
              unitPrice,
              lineTotal
            ]);
          }
        }

        await this.parallelInsert(
          db,
          `INSERT INTO ${p}order_items (order_id, product_id, quantity, unit_price, line_total) VALUES (:1, :2, :3, :4, :5)`,
          orderItemData,
          1000  // Larger batches for executeMany
        );
        progressCallback({ step: 'Order items inserted', progress: 95 });
      }

      // Product reviews
      progressCallback({ step: 'Adding product reviews...', progress: 97 });
      if (productIds.length > 0 && customerIds.length > 0) {
        const reviewCount = Math.floor(Math.min(baseCustomers, baseProducts) * 0.3);
        const reviewTitles = ['Great product!', 'Good value', 'As expected', 'Could be better', 'Excellent quality'];
        const reviewData = [];

        for (let i = 0; i < reviewCount; i++) {
          reviewData.push([
            productIds[Math.floor(Math.random() * productIds.length)],
            customerIds[Math.floor(Math.random() * customerIds.length)],
            Math.floor(Math.random() * 5) + 1,
            reviewTitles[Math.floor(Math.random() * reviewTitles.length)],
            'This is a sample review for the product.',
            Math.random() > 0.3 ? 1 : 0
          ]);
        }

        await this.parallelInsert(
          db,
          `INSERT INTO ${p}product_reviews (product_id, customer_id, rating, review_title, review_text, is_verified_purchase) VALUES (:1, :2, :3, :4, :5, :6)`,
          reviewData,
          500
        );
      }

      // RAC Contention tables - populate with hot block rows
      progressCallback({ step: 'Populating RAC contention tables...', progress: 98 });
      console.log('Populating RAC contention tables...');

      // rac_hotblock: 10 slots that will be heavily contended
      // Few rows = all fit in 1-2 blocks = maximum block contention
      try {
        for (let i = 1; i <= 10; i++) {
          await db.execute(
            `INSERT INTO ${p}rac_hotblock (slot_id, counter, last_instance) VALUES (:1, 0, 0)`,
            [i]
          );
        }
        console.log('  rac_hotblock: 10 rows inserted');
      } catch (err) {
        if (!err.message.includes('ORA-00001')) {
          console.log('  rac_hotblock error:', err.message);
        } else {
          console.log('  rac_hotblock: rows already exist');
        }
      }

      // rac_hotindex: 100 rows with only 5 bucket values (hot index leaf blocks)
      try {
        for (let i = 1; i <= 100; i++) {
          await db.execute(
            `INSERT INTO ${p}rac_hotindex (id, bucket, value) VALUES (:1, :2, 0)`,
            [i, (i % 5) + 1]  // bucket 1-5 only
          );
        }
        console.log('  rac_hotindex: 100 rows inserted');
      } catch (err) {
        if (!err.message.includes('ORA-00001')) {
          console.log('  rac_hotindex error:', err.message);
        } else {
          console.log('  rac_hotindex: rows already exist');
        }
      }

      // Set tables back to LOGGING
      progressCallback({ step: 'Setting tables to LOGGING mode...', progress: 99 });
      for (const tableName of this.getTableNames(prefix)) {
        try {
          await db.execute(`ALTER TABLE ${tableName} LOGGING`);
        } catch (err) {
          // Ignore
        }
      }

      progressCallback({ step: 'Data population complete!', progress: 100 });
    } catch (error) {
      console.error('Error during data population:', error);
      throw error;
    }
  }

  async dropSchema(db, prefix = '') {
    const p = prefix ? `${prefix}_` : '';
    const dropOrder = [
      'product_reviews', 'order_history', 'payments', 'order_items', 'orders',
      'customers', 'inventory', 'products', 'categories', 'warehouses',
      'countries', 'regions'
    ];

    for (const table of dropOrder) {
      try {
        await db.execute(`DROP TABLE ${p}${table} CASCADE CONSTRAINTS PURGE`);
        console.log(`Dropped table: ${p}${table}`);
      } catch (err) {
        if (!err.message.includes('ORA-00942')) {
          console.log(`Warning dropping ${p}${table}:`, err.message);
        }
      }
    }

    this.schemas.delete(prefix || 'default');
  }

  async getSchemaInfo(db, prefix = '') {
    const p = prefix ? `${prefix}_` : '';
    try {
      const tableNames = this.getTableNames(prefix);
      const counts = {};

      for (const tableName of tableNames) {
        try {
          const shortName = tableName.replace(p, '');
          const result = await db.execute(`SELECT COUNT(*) as cnt FROM ${tableName}`);
          counts[shortName] = result.rows[0].CNT;
        } catch (err) {
          // Table doesn't exist
        }
      }

      const totalSize = await db.execute(`
        SELECT NVL(SUM(bytes)/1024/1024, 0) as size_mb
        FROM user_segments
        WHERE segment_name LIKE '${p.toUpperCase()}%'
      `);

      const schemaMetadata = this.schemas.get(prefix || 'default');

      return {
        prefix,
        counts,
        totalSizeMB: totalSize.rows[0]?.SIZE_MB || 0,
        schemaExists: Object.keys(counts).length > 0,
        compressionType: schemaMetadata?.compressionType || 'none'
      };
    } catch (err) {
      return {
        prefix,
        counts: {},
        totalSizeMB: 0,
        schemaExists: false,
        error: err.message
      };
    }
  }

  // Get list of all schemas
  async listSchemas(db) {
    try {
      // Find all schema prefixes by looking for tables ending with _regions
      const result = await db.execute(`
        SELECT DISTINCT
          CASE
            WHEN table_name = 'REGIONS' THEN ''
            ELSE SUBSTR(table_name, 1, INSTR(table_name, '_REGIONS') - 1)
          END as prefix
        FROM user_tables
        WHERE table_name LIKE '%REGIONS'
      `);

      const schemas = [];
      for (const row of result.rows) {
        const prefix = row.PREFIX || '';
        const info = await this.getSchemaInfo(db, prefix);
        schemas.push(info);
      }

      return schemas;
    } catch (err) {
      return [];
    }
  }
}

module.exports = new SchemaManager();
