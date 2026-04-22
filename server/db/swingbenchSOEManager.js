const fs = require('fs');
const path = require('path');
const oracledb = require('oracledb');

const SWINGBENCH_ROOT = path.join(__dirname, '../../swingbench');
const ORDERENTRY_ROOT = path.join(SWINGBENCH_ROOT, 'orderentry');
const WIZARD_PATH = path.join(SWINGBENCH_ROOT, 'oewizard.xml');
const DATAGEN_PATH = path.join(ORDERENTRY_ROOT, 'soe2.xml');

const SUPPORTED_MODELS = {
  compression: [
    { value: 'none', label: 'No Compression' },
    { value: 'advanced', label: 'Advanced Compression' }
  ],
  partitioning: [
    { value: 'none', label: 'No Partitioning' },
    { value: 'hash', label: 'Hash Partitioning' },
    { value: 'composite', label: 'Composite Partitioning' }
  ],
  indexing: [
    { value: 'all', label: 'All Indexes' },
    { value: 'primary', label: 'Primary Keys Only' },
    { value: 'none', label: 'No Indexes' }
  ]
};

const DEFAULTS = {
  username: 'SOE',
  password: 'soe',
  tablespace: 'SOE',
  tempTablespace: 'TEMP',
  createUser: true,
  createTablespace: false,
  replaceExisting: false,
  datafile: '',
  datafileSize: '2G',
  tablespaceModel: 'smallfile',
  compression: 'none',
  partitioning: 'none',
  indexing: 'all',
  parallelism: 2,
  logging: 'nologging',
  runningInCloud: false
};

const EXPECTED_TABLES = [
  'CUSTOMERS',
  'ADDRESSES',
  'CARD_DETAILS',
  'WAREHOUSES',
  'ORDER_ITEMS',
  'ORDERS',
  'INVENTORIES',
  'PRODUCT_INFORMATION',
  'LOGON',
  'PRODUCT_DESCRIPTIONS',
  'ORDERENTRY_METADATA'
];

const EXPECTED_VIEWS = ['PRODUCTS', 'PRODUCT_PRICES'];
const EXPECTED_SEQUENCES = ['CUSTOMER_SEQ', 'ORDERS_SEQ', 'ADDRESS_SEQ', 'LOGON_SEQ', 'CARD_DETAILS_SEQ'];
const EXPECTED_PACKAGES = ['ORDERENTRY'];

const INSTALL_SCRIPT_LABELS = {
  admin: {
    createTablespace: 'soedgcreatetablespace.sql',
    createUser: 'soedgcreateuser.sql',
    dropUser: 'soedropuser.sql',
    dropTablespace: 'soedroptablespace.sql'
  },
  owner: {
    createTables: {
      none: 'soedgcreatetables2.sql',
      hash: 'soedgcreatetableshash2.sql',
      composite: 'soedgcreatetablescomposite2.sql'
    },
    createViews: 'soedgviews.sql',
    sqlSet: 'soedgsqlset.sql',
    analyze: 'soedganalyzeschema2.sql',
    constraints: {
      all: 'soedgconstraints2.sql',
      primary: 'soedgconstraints_pk_2.sql',
      none: 'soedgconstraints_none_2.sql'
    },
    indexes: {
      all: 'soedgindexes2.sql',
      primary: 'soedgindexes_pk_2.sql',
      none: 'soedgindexes_none_2.sql'
    },
    sequences: 'soedgsequences2.sql',
    packageHeader: 'soedgpackage2_header.sql',
    packageBody: 'soedgpackage2_body.sql',
    metadata: 'soedgsetupmetadata.sql',
    dropObjects: 'soedgdrop2.sql'
  }
};

const IGNORABLE_ORA_CODES = new Set([
  942,   // table or view does not exist
  955,   // name is already used by an existing object
  1918,  // user does not exist
  959,   // tablespace does not exist
  2289,  // sequence does not exist
  4043   // object does not exist
]);

class SwingbenchSOEManager {
  getDefaults() {
    return {
      defaults: { ...DEFAULTS },
      supportedModels: SUPPORTED_MODELS,
      wizardPath: WIZARD_PATH,
      dataGenerationConfigPath: DATAGEN_PATH,
      limitations: this.getLimitations()
    };
  }

  getLimitations() {
    return [
      'DBStress installs the SOE schema objects from the bundled Swingbench SQL scripts.',
      'The XML-driven data load from soe2.xml is not executed yet because the supporting Swingbench sample data files are not present in this repository.',
      'Connect with a privileged account if you want DBStress to create or drop the SOE user and tablespace.'
    ];
  }

  normalizeOptions(options = {}) {
    const username = String(options.username || DEFAULTS.username).trim().toUpperCase();
    const password = String(options.password || DEFAULTS.password);
    const tablespace = String(options.tablespace || DEFAULTS.tablespace).trim().toUpperCase();
    const tempTablespace = String(options.tempTablespace || DEFAULTS.tempTablespace).trim().toUpperCase();
    const partitioning = this.normalizeEnum(options.partitioning, SUPPORTED_MODELS.partitioning, DEFAULTS.partitioning);
    const indexing = this.normalizeEnum(options.indexing, SUPPORTED_MODELS.indexing, DEFAULTS.indexing);
    const compression = this.normalizeEnum(options.compression, SUPPORTED_MODELS.compression, DEFAULTS.compression);
    const parallelism = Math.max(1, Number.parseInt(options.parallelism, 10) || DEFAULTS.parallelism);
    const createUser = options.createUser !== undefined ? !!options.createUser : DEFAULTS.createUser;
    const createTablespace = !!options.createTablespace;
    const replaceExisting = !!options.replaceExisting;
    const datafile = String(options.datafile || DEFAULTS.datafile).trim();
    const datafileSize = String(options.datafileSize || DEFAULTS.datafileSize).trim().toUpperCase();
    const tablespaceModel = String(options.tablespaceModel || DEFAULTS.tablespaceModel).trim().toLowerCase() || 'smallfile';
    const runningInCloud = !!options.runningInCloud;

    if (!/^[A-Z][A-Z0-9_$#]*$/.test(username)) {
      throw new Error('SOE username must be a valid Oracle identifier, for example SOE.');
    }
    if (!/^[A-Z][A-Z0-9_$#]*$/.test(tablespace)) {
      throw new Error('Tablespace must be a valid Oracle identifier, for example SOE.');
    }
    if (!/^[A-Z][A-Z0-9_$#]*$/.test(tempTablespace)) {
      throw new Error('Temporary tablespace must be a valid Oracle identifier, for example TEMP.');
    }
    if (!/^[A-Za-z0-9_$#]+$/.test(password)) {
      throw new Error('SOE password must use simple Oracle identifier characters only: letters, numbers, _, $, #.');
    }
    if (!/^[0-9]+[KMG]$/i.test(datafileSize)) {
      throw new Error('Datafile size must look like 512M, 2G, or 10G.');
    }
    if (createTablespace && !datafile) {
      throw new Error('Datafile path is required when "Create Tablespace" is enabled.');
    }

    return {
      username,
      password,
      tablespace,
      tempTablespace,
      createUser,
      createTablespace,
      replaceExisting,
      datafile,
      datafileSize,
      tablespaceModel,
      compression,
      partitioning,
      indexing,
      parallelism,
      logging: DEFAULTS.logging,
      runningInCloud,
      compressClause: compression === 'advanced' ? 'compress' : '',
      parallelClause: `parallel ${parallelism}`
    };
  }

  normalizeEnum(value, supported, fallback) {
    const normalized = String(value || fallback).trim().toLowerCase();
    const supportedValues = new Set(supported.map((entry) => entry.value));
    if (!supportedValues.has(normalized)) {
      return fallback;
    }
    return normalized;
  }

  getExecutionPlan(options = {}) {
    const config = this.normalizeOptions(options);
    const adminScripts = [];

    if (config.replaceExisting && config.createUser) {
      adminScripts.push(INSTALL_SCRIPT_LABELS.admin.dropUser);
    }
    if (config.replaceExisting && config.createTablespace) {
      adminScripts.push(INSTALL_SCRIPT_LABELS.admin.dropTablespace);
    }
    if (config.createTablespace) {
      adminScripts.push(INSTALL_SCRIPT_LABELS.admin.createTablespace);
    }
    if (config.createUser) {
      adminScripts.push(INSTALL_SCRIPT_LABELS.admin.createUser);
    }

    const ownerScripts = [
      INSTALL_SCRIPT_LABELS.owner.createTables[config.partitioning],
      INSTALL_SCRIPT_LABELS.owner.createViews,
      INSTALL_SCRIPT_LABELS.owner.sqlSet,
      INSTALL_SCRIPT_LABELS.owner.analyze,
      INSTALL_SCRIPT_LABELS.owner.constraints[config.indexing],
      INSTALL_SCRIPT_LABELS.owner.indexes[config.indexing],
      INSTALL_SCRIPT_LABELS.owner.sequences,
      INSTALL_SCRIPT_LABELS.owner.packageHeader,
      INSTALL_SCRIPT_LABELS.owner.packageBody,
      INSTALL_SCRIPT_LABELS.owner.metadata
    ];

    return {
      config,
      adminScripts,
      ownerScripts,
      dataGenerationConfigPath: DATAGEN_PATH,
      limitations: this.getLimitations()
    };
  }

  getVariables(config) {
    return {
      username: config.username,
      password: config.password,
      tablespace: config.tablespace,
      temptablespace: config.tempTablespace,
      datafile: config.datafile ? `'${config.datafile.replace(/'/g, "''")}'` : "''",
      datafilesize: config.datafileSize,
      tablespacemodel: config.tablespaceModel,
      compress: config.compressClause,
      indextablespace: config.tablespace,
      parallelism: String(config.parallelism),
      parallelclause: config.parallelClause,
      logging: config.logging,
      analyzedegree: String(config.parallelism),
      instancecount: '1',
      partitioncount: '32',
      intervalrange: '1000000',
      intervalmax: '1000001',
      running_in_cloud: config.runningInCloud ? 'TRUE' : 'FALSE'
    };
  }

  readScript(scriptName) {
    return fs.readFileSync(path.join(ORDERENTRY_ROOT, scriptName), 'utf8');
  }

  substituteVariables(sqlText, variables) {
    return String(sqlText).replace(/&([A-Za-z0-9_]+)/g, (full, key) => {
      if (!(key in variables)) {
        throw new Error(`Missing Swingbench SQL variable '&${key}'.`);
      }
      return variables[key];
    });
  }

  isSlashDelimitedBlock(firstLine = '') {
    return /^(DECLARE|BEGIN|CREATE(\s+OR\s+REPLACE)?\s+(PACKAGE|PACKAGE BODY|PROCEDURE|FUNCTION|TRIGGER|TYPE))/i.test(firstLine.trim());
  }

  splitSqlStatements(sqlText) {
    const statements = [];
    const lines = String(sqlText).replace(/\r\n/g, '\n').split('\n');
    let buffer = [];
    let slashDelimited = false;

    const flush = () => {
      const statement = buffer.join('\n').trim();
      buffer = [];
      slashDelimited = false;
      if (statement) {
        statements.push(statement.replace(/;\s*$/g, '').trim());
      }
    };

    for (const line of lines) {
      const trimmed = line.trim();

      if (!trimmed || /^--/.test(trimmed) || /^rem\b/i.test(trimmed)) {
        continue;
      }

      if (trimmed === '/') {
        flush();
        continue;
      }

      if (buffer.length === 0) {
        slashDelimited = this.isSlashDelimitedBlock(trimmed);
      }

      buffer.push(line);

      if (!slashDelimited && /;\s*$/.test(trimmed)) {
        flush();
      }
    }

    flush();
    return statements;
  }

  async executeScript(connection, scriptName, variables, options = {}) {
    const sqlText = this.substituteVariables(this.readScript(scriptName), variables);
    const statements = this.splitSqlStatements(sqlText);
    const errors = [];

    for (const statement of statements) {
      try {
        await connection.execute(statement, [], {
          autoCommit: false,
          outFormat: oracledb.OUT_FORMAT_OBJECT
        });
      } catch (error) {
        const ignorable = options.ignoreErrors && IGNORABLE_ORA_CODES.has(error.errorNum);
        if (ignorable) {
          errors.push({ statement, ignored: true, error: error.message });
          continue;
        }
        error.scriptName = scriptName;
        error.statement = statement;
        throw error;
      }
    }

    await connection.commit();
    return { statements: statements.length, ignoredErrors: errors };
  }

  buildInstallScript(options = {}) {
    const plan = this.getExecutionPlan(options);
    const variables = this.getVariables(plan.config);
    const sections = [
      '-- DBStress Swingbench SOE schema installation script',
      `-- Wizard source: ${WIZARD_PATH}`,
      `-- Data generation XML reference: ${DATAGEN_PATH}`,
      '-- NOTE: This script installs the SOE objects from the bundled SQL files.',
      '-- NOTE: The XML-driven data load is intentionally not executed here.',
      ''
    ];

    for (const scriptName of plan.adminScripts) {
      sections.push(`-- Admin script: ${scriptName}`);
      sections.push(this.substituteVariables(this.readScript(scriptName), variables).trim());
      sections.push('');
    }

    for (const scriptName of plan.ownerScripts) {
      sections.push(`-- Owner script: ${scriptName}`);
      sections.push(this.substituteVariables(this.readScript(scriptName), variables).trim());
      sections.push('');
    }

    return {
      ...plan,
      variables,
      script: sections.join('\n')
    };
  }

  async createSchema(oracleDb, options = {}, progressCallback = () => {}) {
    const plan = this.getExecutionPlan(options);
    const variables = this.getVariables(plan.config);
    const totalSteps = Math.max(1, plan.adminScripts.length + plan.ownerScripts.length + 2);
    let completedSteps = 0;

    const emit = (step) => {
      completedSteps += 1;
      progressCallback({
        schemaId: plan.config.username,
        step,
        progress: Math.min(99, Math.round((completedSteps / totalSteps) * 100))
      });
    };

    progressCallback({
      schemaId: plan.config.username,
      step: `Preparing Swingbench SOE install for ${plan.config.username}...`,
      progress: 0
    });

    const adminConnection = await oracleDb.getConnection();
    let ownerConnection;

    try {
      for (const scriptName of plan.adminScripts) {
        emit(`Running admin script ${scriptName}...`);
        await this.executeScript(adminConnection, scriptName, variables, {
          ignoreErrors: plan.config.replaceExisting && /drop/i.test(scriptName)
        });
      }

      emit(`Connecting as ${plan.config.username}...`);
      ownerConnection = await oracleDb.createDirectConnection({
        user: plan.config.username,
        password: plan.config.password
      });

      for (const scriptName of plan.ownerScripts) {
        emit(`Running owner script ${scriptName}...`);
        await this.executeScript(ownerConnection, scriptName, variables);
      }

      progressCallback({
        schemaId: plan.config.username,
        step: `SOE schema ${plan.config.username} installed. Swingbench object load complete; XML data load not executed.`,
        progress: 100
      });

      return {
        success: true,
        config: plan.config,
        adminScripts: plan.adminScripts,
        ownerScripts: plan.ownerScripts,
        limitations: this.getLimitations()
      };
    } catch (error) {
      progressCallback({
        schemaId: plan.config.username,
        step: `Error: ${error.message}`,
        progress: -1
      });
      throw error;
    } finally {
      if (ownerConnection) {
        await ownerConnection.close();
      }
      await adminConnection.close();
    }
  }

  async dropSchema(oracleDb, options = {}, progressCallback = () => {}) {
    const config = this.normalizeOptions(options);
    const variables = this.getVariables(config);
    const scripts = [];

    if (config.replaceExisting || config.createUser) {
      scripts.push(INSTALL_SCRIPT_LABELS.admin.dropUser);
    }
    if (config.createTablespace) {
      scripts.push(INSTALL_SCRIPT_LABELS.admin.dropTablespace);
    }

    progressCallback({
      schemaId: config.username,
      step: `Dropping Swingbench SOE user ${config.username}...`,
      progress: 0
    });

    const adminConnection = await oracleDb.getConnection();
    let completed = 0;

    try {
      for (const scriptName of scripts) {
        completed += 1;
        progressCallback({
          schemaId: config.username,
          step: `Running admin script ${scriptName}...`,
          progress: Math.round((completed / Math.max(1, scripts.length + 1)) * 100)
        });
        await this.executeScript(adminConnection, scriptName, variables, { ignoreErrors: true });
      }

      progressCallback({
        schemaId: config.username,
        step: `SOE schema ${config.username} dropped.`,
        progress: 100
      });

      return {
        success: true,
        scripts
      };
    } catch (error) {
      progressCallback({
        schemaId: config.username,
        step: `Error: ${error.message}`,
        progress: -1
      });
      throw error;
    } finally {
      await adminConnection.close();
    }
  }

  async getStatus(oracleDb, options = {}) {
    const config = this.normalizeOptions(options);
    const userResult = await oracleDb.execute(
      `
        SELECT username, created
        FROM all_users
        WHERE username = :username
      `,
      { username: config.username }
    );

    const userExists = (userResult.rows || []).length > 0;

    const countsResult = userExists
      ? await oracleDb.execute(
          `
            SELECT object_type, COUNT(*) AS object_count
            FROM all_objects
            WHERE owner = :username
              AND object_type IN ('TABLE', 'VIEW', 'SEQUENCE', 'PACKAGE', 'PACKAGE BODY')
            GROUP BY object_type
          `,
          { username: config.username }
        )
      : { rows: [] };

    const objectCounts = Object.fromEntries(
      (countsResult.rows || []).map((row) => [row.OBJECT_TYPE, Number(row.OBJECT_COUNT || 0)])
    );

    const tablesResult = userExists
      ? await oracleDb.execute(
          `
            SELECT table_name
            FROM all_tables
            WHERE owner = :username
              AND table_name IN (${EXPECTED_TABLES.map((_, index) => `:t${index}`).join(', ')})
          `,
          {
            username: config.username,
            ...Object.fromEntries(EXPECTED_TABLES.map((name, index) => [`t${index}`, name]))
          }
        )
      : { rows: [] };

    const existingTables = new Set((tablesResult.rows || []).map((row) => row.TABLE_NAME));

    return {
      username: config.username,
      userExists,
      ready: userExists && EXPECTED_TABLES.every((table) => existingTables.has(table)),
      objectCounts: {
        tables: objectCounts.TABLE || 0,
        views: objectCounts.VIEW || 0,
        sequences: objectCounts.SEQUENCE || 0,
        packages: objectCounts.PACKAGE || 0,
        packageBodies: objectCounts['PACKAGE BODY'] || 0
      },
      expectedObjects: {
        tables: EXPECTED_TABLES,
        views: EXPECTED_VIEWS,
        sequences: EXPECTED_SEQUENCES,
        packages: EXPECTED_PACKAGES
      },
      installedTables: Array.from(existingTables).sort()
    };
  }
}

module.exports = new SwingbenchSOEManager();
