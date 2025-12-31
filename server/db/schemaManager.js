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

  };
};

// Generate RAC hot table DDL dynamically (supports multiple tables)
// Optimized for GC contention (not TX contention):
// - INITRANS 100: Eliminates "enq: TX - allocate ITL entry" waits
// - Small rows (no padding): Packs many rows per block for high concurrency
// - PCTFREE 1: Maximizes rows per block
// - Many rows per block = same block accessed by different instances = gc current block congested
const getRacTableDDL = (prefix, compressionType = 'none', tableNum = 1) => {
  const p = prefix ? `${prefix}_` : '';
  const compressClause = COMPRESSION_TYPES[compressionType] ? ` ${COMPRESSION_TYPES[compressionType]}` : '';
  const suffix = tableNum > 1 ? `_${tableNum}` : '';

  return {
    [`${p}rac_hotblock${suffix}`]: `
      CREATE TABLE ${p}rac_hotblock${suffix} (
        slot_id NUMBER NOT NULL,
        counter NUMBER DEFAULT 0,
        last_instance NUMBER,
        last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        CONSTRAINT ${p}pk_rac_hotblock${suffix} PRIMARY KEY (slot_id)
      )${compressClause} PCTFREE 1 INITRANS 100 MAXTRANS 255 LOGGING`,
    [`${p}rac_hotindex${suffix}`]: `
      CREATE TABLE ${p}rac_hotindex${suffix} (
        id NUMBER NOT NULL,
        bucket NUMBER NOT NULL,
        value NUMBER DEFAULT 0,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        CONSTRAINT ${p}pk_rac_hotindex${suffix} PRIMARY KEY (id)
      )${compressClause} PCTFREE 1 INITRANS 100 MAXTRANS 255 LOGGING`
  };
};

// Generate RAC index DDL for a specific table number
const getRacIndexes = (prefix, tableNum = 1) => {
  const p = prefix ? `${prefix}_` : '';
  const suffix = tableNum > 1 ? `_${tableNum}` : '';
  return [
    `CREATE INDEX ${p}idx_rac_hotindex${suffix}_bucket ON ${p}rac_hotindex${suffix}(bucket)`
  ];
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
    `CREATE INDEX ${p}idx_reviews_customer ON ${p}product_reviews(customer_id)`
    // Note: RAC indexes are created dynamically based on racTableCount
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

  getTableNames(prefix, racTableCount = 1) {
    const p = prefix ? `${prefix}_` : '';
    const baseNames = [
      `${p}regions`, `${p}countries`, `${p}warehouses`, `${p}categories`,
      `${p}products`, `${p}inventory`, `${p}customers`, `${p}orders`,
      `${p}order_items`, `${p}payments`, `${p}order_history`, `${p}product_reviews`
    ];

    // Add RAC tables dynamically
    for (let i = 1; i <= racTableCount; i++) {
      const suffix = i > 1 ? `_${i}` : '';
      baseNames.push(`${p}rac_hotblock${suffix}`);
      baseNames.push(`${p}rac_hotindex${suffix}`);
    }

    return baseNames;
  }

  async createSchema(db, options = {}, progressCallback = () => {}) {
    // Support both old boolean 'compress' and new 'compressionType' options
    const { prefix = '', compress = false, compressionType = null, racTableCount = 1 } = options;
    const effectiveCompression = compressionType || (compress ? 'advanced' : 'none');
    const tables = getTableDDL(prefix, effectiveCompression);
    const indexes = getIndexes(prefix);

    // Add RAC tables and indexes dynamically
    for (let i = 1; i <= racTableCount; i++) {
      const racTables = getRacTableDDL(prefix, effectiveCompression, i);
      Object.assign(tables, racTables);
      indexes.push(...getRacIndexes(prefix, i));
    }

    const tableNames = Object.keys(tables);
    const totalSteps = tableNames.length + indexes.length;
    let currentStep = 0;

    const compressionLabel = COMPRESSION_TYPES[effectiveCompression] || 'no compression';
    const racLabel = racTableCount > 1 ? `, ${racTableCount} RAC tables` : '';
    progressCallback({ step: `Creating schema${prefix ? ` '${prefix}'` : ''} (${compressionLabel}${racLabel})...`, progress: 0 });

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

    // Store schema metadata including racTableCount
    this.schemas.set(prefix || 'default', { prefix, compressionType: effectiveCompression, racTableCount, createdAt: new Date() });
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
      parallelism = 10,  // Number of parallel insert streams
      racTableCount = 1  // Number of RAC hot table pairs
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
      progressCallback({ step: `Populating ${racTableCount} RAC contention table pairs...`, progress: 98 });
      console.log(`Populating ${racTableCount} RAC contention table pairs...`);

      // Populate each RAC table pair
      for (let tableNum = 1; tableNum <= racTableCount; tableNum++) {
        const suffix = tableNum > 1 ? `_${tableNum}` : '';

        // rac_hotblock: 1000 slots spread across ~50-100 blocks
        const hotblockData = [];
        for (let i = 1; i <= 1000; i++) {
          hotblockData.push([i, 0, 0]);
        }
        try {
          await this.parallelInsert(
            db,
            `INSERT INTO ${p}rac_hotblock${suffix} (slot_id, counter, last_instance) VALUES (:1, :2, :3)`,
            hotblockData,
            200
          );
          console.log(`  rac_hotblock${suffix}: 1000 rows inserted`);
        } catch (err) {
          if (!err.message.includes('ORA-00001')) {
            console.log(`  rac_hotblock${suffix} error:`, err.message);
          } else {
            console.log(`  rac_hotblock${suffix}: rows already exist`);
          }
        }

        // rac_hotindex: 5000 rows with 20 bucket values (hot index leaf blocks)
        const hotindexData = [];
        for (let i = 1; i <= 5000; i++) {
          hotindexData.push([i, (i % 20) + 1, 0]);  // bucket 1-20
        }
        try {
          await this.parallelInsert(
            db,
            `INSERT INTO ${p}rac_hotindex${suffix} (id, bucket, value) VALUES (:1, :2, :3)`,
            hotindexData,
            500
          );
          console.log(`  rac_hotindex${suffix}: 5000 rows inserted`);
        } catch (err) {
          if (!err.message.includes('ORA-00001')) {
            console.log(`  rac_hotindex${suffix} error:`, err.message);
          } else {
            console.log(`  rac_hotindex${suffix}: rows already exist`);
          }
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

    // First, find all RAC tables dynamically (could be any number)
    try {
      const racTablesResult = await db.execute(`
        SELECT table_name FROM user_tables
        WHERE table_name LIKE '${p.toUpperCase()}RAC_HOT%'
        ORDER BY table_name DESC
      `);

      // Drop RAC tables first
      for (const row of racTablesResult.rows) {
        try {
          await db.execute(`DROP TABLE ${row.TABLE_NAME} CASCADE CONSTRAINTS PURGE`);
          console.log(`Dropped table: ${row.TABLE_NAME}`);
        } catch (err) {
          if (!err.message.includes('ORA-00942')) {
            console.log(`Warning dropping ${row.TABLE_NAME}:`, err.message);
          }
        }
      }
    } catch (err) {
      console.log('Warning querying RAC tables:', err.message);
    }

    // Then drop the base tables in order
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

  // Generate SQL script for schema creation (can be run from SQL*Plus)
  generateScript(options = {}) {
    const {
      prefix = '',
      compressionType = 'none',
      scaleFactor = 1,
      racTableCount = 1
    } = options;

    const p = prefix ? `${prefix}_` : '';
    const tables = getTableDDL(prefix, compressionType);
    const indexes = getIndexes(prefix);

    // Add RAC tables
    for (let i = 1; i <= racTableCount; i++) {
      const racTables = getRacTableDDL(prefix, compressionType, i);
      Object.assign(tables, racTables);
      indexes.push(...getRacIndexes(prefix, i));
    }

    // Calculate data sizes
    const baseCustomers = 1000 * scaleFactor;
    const baseProducts = 500 * scaleFactor;
    const baseOrders = 5000 * scaleFactor;

    let script = `-- ============================================
-- DBStress Schema Creation Script
-- Generated: ${new Date().toISOString()}
-- Schema Prefix: ${prefix || '(none)'}
-- Compression: ${COMPRESSION_TYPES[compressionType] || 'none'}
-- Scale Factor: ${scaleFactor} (${baseOrders} orders, ${baseCustomers} customers, ${baseProducts} products)
-- RAC Tables: ${racTableCount}
-- ============================================

SET ECHO ON
SET TIMING ON
SET SERVEROUTPUT ON SIZE UNLIMITED
SET DEFINE OFF
WHENEVER SQLERROR CONTINUE

-- ============================================
-- DROP EXISTING TABLES (if any)
-- ============================================
`;

    // Drop tables in reverse order
    const dropOrder = [
      'product_reviews', 'order_history', 'payments', 'order_items', 'orders',
      'customers', 'inventory', 'products', 'categories', 'warehouses',
      'countries', 'regions'
    ];

    // Add RAC tables to drop
    for (let i = racTableCount; i >= 1; i--) {
      const suffix = i > 1 ? `_${i}` : '';
      script += `DROP TABLE ${p}rac_hotindex${suffix} CASCADE CONSTRAINTS PURGE;\n`;
      script += `DROP TABLE ${p}rac_hotblock${suffix} CASCADE CONSTRAINTS PURGE;\n`;
    }

    for (const table of dropOrder) {
      script += `DROP TABLE ${p}${table} CASCADE CONSTRAINTS PURGE;\n`;
    }

    script += `\n-- ============================================
-- CREATE TABLES
-- ============================================\n`;

    // Create tables
    for (const [tableName, ddl] of Object.entries(tables)) {
      script += `\n-- Table: ${tableName}\n`;
      script += ddl.trim() + ';\n';
    }

    script += `\n-- ============================================
-- CREATE INDEXES
-- ============================================\n`;

    // Create indexes
    for (const indexSql of indexes) {
      script += indexSql + ';\n';
    }

    script += `\n-- ============================================
-- POPULATE REFERENCE DATA
-- ============================================\n`;

    // Regions
    script += `\n-- Regions\n`;
    for (const region of REGIONS_DATA) {
      script += `INSERT INTO ${p}regions (region_name) VALUES ('${region}');\n`;
    }
    script += `COMMIT;\n`;

    // Countries
    script += `\n-- Countries\n`;
    for (const country of COUNTRIES_DATA) {
      script += `INSERT INTO ${p}countries (country_name, country_code, region_id)
  SELECT '${country.name}', '${country.code}', region_id FROM ${p}regions WHERE region_name = '${country.region}';\n`;
    }
    script += `COMMIT;\n`;

    // Warehouses
    script += `\n-- Warehouses\n`;
    const warehouseLocations = ['East Coast DC', 'West Coast DC', 'Central DC', 'European DC', 'Asian DC'];
    for (let i = 0; i < warehouseLocations.length; i++) {
      script += `INSERT INTO ${p}warehouses (warehouse_name, location, country_id, capacity)
  SELECT '${warehouseLocations[i]}', 'Warehouse ${i + 1}', country_id, ${50000 * scaleFactor}
  FROM ${p}countries WHERE ROWNUM = 1;\n`;
    }
    script += `COMMIT;\n`;

    // Categories
    script += `\n-- Categories\n`;
    for (const cat of CATEGORIES_DATA) {
      if (!cat.parent) {
        script += `INSERT INTO ${p}categories (category_name, description) VALUES ('${cat.name.replace(/'/g, "''")}', '${cat.name} category');\n`;
      }
    }
    script += `COMMIT;\n`;

    for (const cat of CATEGORIES_DATA) {
      if (cat.parent) {
        script += `INSERT INTO ${p}categories (category_name, parent_category_id, description)
  SELECT '${cat.name.replace(/'/g, "''")}', category_id, '${cat.name} category' FROM ${p}categories WHERE category_name = '${cat.parent}';\n`;
      }
    }
    script += `COMMIT;\n`;

    // Generate bulk data using PL/SQL for performance
    script += `\n-- ============================================
-- POPULATE BULK DATA (using PL/SQL for performance)
-- ============================================

-- Products (${baseProducts} rows)
DECLARE
  TYPE t_varchar_arr IS TABLE OF VARCHAR2(100) INDEX BY PLS_INTEGER;
  TYPE t_number_arr IS TABLE OF NUMBER INDEX BY PLS_INTEGER;
  l_category_ids t_number_arr;
  l_adjectives t_varchar_arr;
  l_nouns t_varchar_arr;
  l_cat_id NUMBER;
  l_adj VARCHAR2(100);
  l_noun VARCHAR2(100);
  l_cat_count NUMBER;
BEGIN
  -- Get category IDs into associative array
  FOR rec IN (SELECT category_id, ROWNUM rn FROM ${p}categories) LOOP
    l_category_ids(rec.rn) := rec.category_id;
  END LOOP;
  l_cat_count := l_category_ids.COUNT;

  -- Adjectives
  l_adjectives(1) := 'Premium'; l_adjectives(2) := 'Professional'; l_adjectives(3) := 'Ultra';
  l_adjectives(4) := 'Advanced'; l_adjectives(5) := 'Classic'; l_adjectives(6) := 'Modern';
  l_adjectives(7) := 'Deluxe'; l_adjectives(8) := 'Essential'; l_adjectives(9) := 'Elite';
  l_adjectives(10) := 'Pro';

  -- Nouns
  l_nouns(1) := 'Laptop'; l_nouns(2) := 'Phone'; l_nouns(3) := 'Tablet';
  l_nouns(4) := 'Headphones'; l_nouns(5) := 'Speaker'; l_nouns(6) := 'Camera';
  l_nouns(7) := 'Watch'; l_nouns(8) := 'Keyboard'; l_nouns(9) := 'Mouse';
  l_nouns(10) := 'Monitor';

  FOR i IN 1..${baseProducts} LOOP
    l_adj := l_adjectives(MOD(i, 10) + 1);
    l_noun := l_nouns(MOD(i, 10) + 1);
    l_cat_id := l_category_ids(MOD(i, l_cat_count) + 1);

    INSERT INTO ${p}products (product_name, description, category_id, unit_price, unit_cost, weight)
    VALUES (
      l_adj || ' ' || l_noun || ' ' || i,
      'High quality product with premium features',
      l_cat_id,
      ROUND(DBMS_RANDOM.VALUE(10, 500), 2),
      ROUND(DBMS_RANDOM.VALUE(5, 300), 2),
      ROUND(DBMS_RANDOM.VALUE(0.5, 10), 2)
    );
    IF MOD(i, 1000) = 0 THEN
      COMMIT;
      DBMS_OUTPUT.PUT_LINE('Products inserted: ' || i);
    END IF;
  END LOOP;
  COMMIT;
  DBMS_OUTPUT.PUT_LINE('Products complete: ${baseProducts} rows');
END;
/

-- Customers (${baseCustomers} rows)
DECLARE
  TYPE t_varchar_arr IS TABLE OF VARCHAR2(100) INDEX BY PLS_INTEGER;
  TYPE t_number_arr IS TABLE OF NUMBER INDEX BY PLS_INTEGER;
  l_first_names t_varchar_arr;
  l_last_names t_varchar_arr;
  l_cities t_varchar_arr;
  l_country_ids t_number_arr;
  l_fn VARCHAR2(100);
  l_ln VARCHAR2(100);
  l_city VARCHAR2(100);
  l_country_id NUMBER;
  l_country_count NUMBER;
BEGIN
  -- First names
  l_first_names(1) := 'James'; l_first_names(2) := 'John'; l_first_names(3) := 'Robert';
  l_first_names(4) := 'Michael'; l_first_names(5) := 'William'; l_first_names(6) := 'David';
  l_first_names(7) := 'Richard'; l_first_names(8) := 'Joseph'; l_first_names(9) := 'Thomas';
  l_first_names(10) := 'Charles'; l_first_names(11) := 'Mary'; l_first_names(12) := 'Patricia';
  l_first_names(13) := 'Jennifer'; l_first_names(14) := 'Linda'; l_first_names(15) := 'Elizabeth';
  l_first_names(16) := 'Barbara'; l_first_names(17) := 'Susan'; l_first_names(18) := 'Jessica';
  l_first_names(19) := 'Sarah'; l_first_names(20) := 'Karen';

  -- Last names
  l_last_names(1) := 'Smith'; l_last_names(2) := 'Johnson'; l_last_names(3) := 'Williams';
  l_last_names(4) := 'Brown'; l_last_names(5) := 'Jones'; l_last_names(6) := 'Garcia';
  l_last_names(7) := 'Miller'; l_last_names(8) := 'Davis'; l_last_names(9) := 'Rodriguez';
  l_last_names(10) := 'Martinez'; l_last_names(11) := 'Hernandez'; l_last_names(12) := 'Lopez';
  l_last_names(13) := 'Gonzalez'; l_last_names(14) := 'Wilson'; l_last_names(15) := 'Anderson';
  l_last_names(16) := 'Thomas'; l_last_names(17) := 'Taylor'; l_last_names(18) := 'Moore';
  l_last_names(19) := 'Jackson'; l_last_names(20) := 'Martin';

  -- Cities
  l_cities(1) := 'New York'; l_cities(2) := 'Los Angeles'; l_cities(3) := 'Chicago';
  l_cities(4) := 'Houston'; l_cities(5) := 'Phoenix'; l_cities(6) := 'Philadelphia';
  l_cities(7) := 'San Antonio'; l_cities(8) := 'San Diego'; l_cities(9) := 'Dallas';
  l_cities(10) := 'San Jose';

  -- Get country IDs
  FOR rec IN (SELECT country_id, ROWNUM rn FROM ${p}countries) LOOP
    l_country_ids(rec.rn) := rec.country_id;
  END LOOP;
  l_country_count := l_country_ids.COUNT;

  FOR i IN 1..${baseCustomers} LOOP
    l_fn := l_first_names(MOD(i, 20) + 1);
    l_ln := l_last_names(MOD(i, 20) + 1);
    l_city := l_cities(MOD(i, 10) + 1);
    l_country_id := l_country_ids(MOD(i, l_country_count) + 1);

    INSERT INTO ${p}customers (first_name, last_name, email, phone, address_line1, city, state_province, postal_code, country_id, credit_limit)
    VALUES (
      l_fn, l_ln,
      LOWER(l_fn) || '.' || LOWER(l_ln) || '.' || i || '@example.com',
      '+1-' || LPAD(FLOOR(DBMS_RANDOM.VALUE(100, 999)), 3, '0') || '-' || LPAD(FLOOR(DBMS_RANDOM.VALUE(1000, 9999)), 4, '0'),
      FLOOR(DBMS_RANDOM.VALUE(1, 9999)) || ' Main Street',
      l_city,
      'State',
      LPAD(FLOOR(DBMS_RANDOM.VALUE(10000, 99999)), 5, '0'),
      l_country_id,
      ROUND(DBMS_RANDOM.VALUE(1000, 10000), 2)
    );
    IF MOD(i, 1000) = 0 THEN
      COMMIT;
      DBMS_OUTPUT.PUT_LINE('Customers inserted: ' || i);
    END IF;
  END LOOP;
  COMMIT;
  DBMS_OUTPUT.PUT_LINE('Customers complete: ${baseCustomers} rows');
END;
/

-- Inventory
DECLARE
  TYPE t_number_arr IS TABLE OF NUMBER INDEX BY PLS_INTEGER;
  l_product_ids t_number_arr;
  l_warehouse_ids t_number_arr;
  l_prod_id NUMBER;
  l_wh_id NUMBER;
  l_prod_count NUMBER;
  l_wh_count NUMBER;
BEGIN
  FOR rec IN (SELECT product_id, ROWNUM rn FROM ${p}products) LOOP
    l_product_ids(rec.rn) := rec.product_id;
  END LOOP;
  l_prod_count := l_product_ids.COUNT;

  FOR rec IN (SELECT warehouse_id, ROWNUM rn FROM ${p}warehouses) LOOP
    l_warehouse_ids(rec.rn) := rec.warehouse_id;
  END LOOP;
  l_wh_count := l_warehouse_ids.COUNT;

  FOR p_idx IN 1..l_prod_count LOOP
    l_prod_id := l_product_ids(p_idx);
    FOR w_idx IN 1..l_wh_count LOOP
      l_wh_id := l_warehouse_ids(w_idx);
      INSERT INTO ${p}inventory (product_id, warehouse_id, quantity_on_hand, quantity_reserved, reorder_level)
      VALUES (
        l_prod_id,
        l_wh_id,
        FLOOR(DBMS_RANDOM.VALUE(100, 1000)),
        FLOOR(DBMS_RANDOM.VALUE(0, 50)),
        FLOOR(DBMS_RANDOM.VALUE(10, 30))
      );
    END LOOP;
    IF MOD(p_idx, 100) = 0 THEN
      COMMIT;
    END IF;
  END LOOP;
  COMMIT;
  DBMS_OUTPUT.PUT_LINE('Inventory complete');
END;
/

-- Orders (${baseOrders} rows)
DECLARE
  TYPE t_number_arr IS TABLE OF NUMBER INDEX BY PLS_INTEGER;
  TYPE t_varchar_arr IS TABLE OF VARCHAR2(50) INDEX BY PLS_INTEGER;
  l_customer_ids t_number_arr;
  l_warehouse_ids t_number_arr;
  l_statuses t_varchar_arr;
  l_shipping t_varchar_arr;
  l_cust_id NUMBER;
  l_wh_id NUMBER;
  l_status VARCHAR2(20);
  l_ship VARCHAR2(50);
  l_cust_count NUMBER;
  l_wh_count NUMBER;
BEGIN
  -- Statuses
  l_statuses(1) := 'PENDING'; l_statuses(2) := 'PROCESSING'; l_statuses(3) := 'SHIPPED';
  l_statuses(4) := 'DELIVERED'; l_statuses(5) := 'CANCELLED';

  -- Shipping methods
  l_shipping(1) := 'Standard'; l_shipping(2) := 'Express'; l_shipping(3) := 'Overnight';

  FOR rec IN (SELECT customer_id, ROWNUM rn FROM ${p}customers) LOOP
    l_customer_ids(rec.rn) := rec.customer_id;
  END LOOP;
  l_cust_count := l_customer_ids.COUNT;

  FOR rec IN (SELECT warehouse_id, ROWNUM rn FROM ${p}warehouses) LOOP
    l_warehouse_ids(rec.rn) := rec.warehouse_id;
  END LOOP;
  l_wh_count := l_warehouse_ids.COUNT;

  FOR i IN 1..${baseOrders} LOOP
    l_cust_id := l_customer_ids(MOD(i, l_cust_count) + 1);
    l_status := l_statuses(MOD(i, 5) + 1);
    l_wh_id := l_warehouse_ids(MOD(i, l_wh_count) + 1);
    l_ship := l_shipping(MOD(i, 3) + 1);

    INSERT INTO ${p}orders (customer_id, status, warehouse_id, shipping_method, notes)
    VALUES (
      l_cust_id,
      l_status,
      l_wh_id,
      l_ship,
      'Order ' || i
    );
    IF MOD(i, 5000) = 0 THEN
      COMMIT;
      DBMS_OUTPUT.PUT_LINE('Orders inserted: ' || i);
    END IF;
  END LOOP;
  COMMIT;
  DBMS_OUTPUT.PUT_LINE('Orders complete: ${baseOrders} rows');
END;
/

-- Order Items (1-3 per order)
DECLARE
  TYPE t_number_arr IS TABLE OF NUMBER INDEX BY PLS_INTEGER;
  l_order_ids t_number_arr;
  l_product_ids t_number_arr;
  l_order_id NUMBER;
  l_prod_id NUMBER;
  l_item_count NUMBER;
  l_quantity NUMBER;
  l_unit_price NUMBER;
  l_counter NUMBER := 0;
  l_order_count NUMBER;
  l_prod_count NUMBER;
BEGIN
  FOR rec IN (SELECT order_id, ROWNUM rn FROM ${p}orders) LOOP
    l_order_ids(rec.rn) := rec.order_id;
  END LOOP;
  l_order_count := l_order_ids.COUNT;

  FOR rec IN (SELECT product_id, ROWNUM rn FROM ${p}products) LOOP
    l_product_ids(rec.rn) := rec.product_id;
  END LOOP;
  l_prod_count := l_product_ids.COUNT;

  FOR o_idx IN 1..l_order_count LOOP
    l_order_id := l_order_ids(o_idx);
    l_item_count := FLOOR(DBMS_RANDOM.VALUE(1, 4));
    FOR j IN 1..l_item_count LOOP
      l_quantity := FLOOR(DBMS_RANDOM.VALUE(1, 6));
      l_unit_price := ROUND(DBMS_RANDOM.VALUE(10, 200), 2);
      l_prod_id := l_product_ids(MOD(o_idx + j, l_prod_count) + 1);

      INSERT INTO ${p}order_items (order_id, product_id, quantity, unit_price, line_total)
      VALUES (
        l_order_id,
        l_prod_id,
        l_quantity,
        l_unit_price,
        l_quantity * l_unit_price
      );
      l_counter := l_counter + 1;
    END LOOP;
    IF MOD(o_idx, 5000) = 0 THEN
      COMMIT;
      DBMS_OUTPUT.PUT_LINE('Order items processed for ' || o_idx || ' orders');
    END IF;
  END LOOP;
  COMMIT;
  DBMS_OUTPUT.PUT_LINE('Order items complete: ' || l_counter || ' rows');
END;
/

`;

    // RAC hot tables
    script += `-- ============================================
-- RAC CONTENTION TABLES
-- ============================================\n`;

    for (let tableNum = 1; tableNum <= racTableCount; tableNum++) {
      const suffix = tableNum > 1 ? `_${tableNum}` : '';
      script += `
-- RAC Hot Block Table ${tableNum}
BEGIN
  FOR i IN 1..1000 LOOP
    INSERT INTO ${p}rac_hotblock${suffix} (slot_id, counter, last_instance) VALUES (i, 0, 0);
  END LOOP;
  COMMIT;
  DBMS_OUTPUT.PUT_LINE('rac_hotblock${suffix}: 1000 rows');
END;
/

-- RAC Hot Index Table ${tableNum}
BEGIN
  FOR i IN 1..5000 LOOP
    INSERT INTO ${p}rac_hotindex${suffix} (id, bucket, value) VALUES (i, MOD(i, 20) + 1, 0);
  END LOOP;
  COMMIT;
  DBMS_OUTPUT.PUT_LINE('rac_hotindex${suffix}: 5000 rows');
END;
/
`;
    }

    script += `
-- ============================================
-- SET TABLES TO LOGGING MODE
-- ============================================
`;

    for (const tableName of Object.keys(tables)) {
      script += `ALTER TABLE ${tableName} LOGGING;\n`;
    }

    script += `
-- ============================================
-- GATHER STATISTICS
-- ============================================
BEGIN
  DBMS_STATS.GATHER_SCHEMA_STATS(USER, CASCADE => TRUE, DEGREE => 4);
END;
/

PROMPT
PROMPT ============================================
PROMPT Schema creation complete!
PROMPT ============================================
PROMPT

SET TIMING OFF
SET ECHO OFF
`;

    return script;
  }
}

module.exports = new SchemaManager();
