const oracledb = require('oracledb');

class OracleDatabase {
  constructor() {
    this.pool = null;
    this.config = null;
  }

  async testConnection(user, password, connectionString) {
    let connection;
    try {
      // Add connection timeout
      const connectPromise = oracledb.getConnection({
        user,
        password,
        connectionString
      });

      const timeoutPromise = new Promise((_, reject) =>
        setTimeout(() => reject(new Error('Connection timeout after 15 seconds')), 15000)
      );

      connection = await Promise.race([connectPromise, timeoutPromise]);

      const result = await connection.execute(
        `SELECT banner FROM v$version WHERE ROWNUM = 1`
      );

      const instanceResult = await connection.execute(
        `SELECT instance_name, host_name, version FROM v$instance`
      );

      return {
        version: result.rows[0][0],
        instance: instanceResult.rows[0][0],
        host: instanceResult.rows[0][1],
        oracleVersion: instanceResult.rows[0][2]
      };
    } finally {
      if (connection) {
        await connection.close();
      }
    }
  }

  async initialize(user, password, connectionString, poolMin = 4, poolMax = 50) {
    if (this.pool) {
      await this.close();
    }

    this.config = { user, password, connectionString };

    // Add pool creation timeout
    const poolPromise = oracledb.createPool({
      user,
      password,
      connectionString,
      poolMin,
      poolMax,
      poolIncrement: 2,
      poolTimeout: 60,
      queueTimeout: 60000,
      enableStatistics: true
    });

    const timeoutPromise = new Promise((_, reject) =>
      setTimeout(() => reject(new Error('Pool creation timeout after 30 seconds')), 30000)
    );

    this.pool = await Promise.race([poolPromise, timeoutPromise]);

    console.log('Oracle connection pool created');
    return this.pool;
  }

  async getConnection() {
    if (!this.pool) {
      throw new Error('Database pool not initialized');
    }
    return await this.pool.getConnection();
  }

  getCredentials(overrides = {}) {
    if (!this.config) {
      throw new Error('Database not configured. Connect first.');
    }

    return {
      user: overrides.user || this.config.user,
      password: overrides.password || this.config.password,
      connectionString: overrides.connectionString || this.config.connectionString
    };
  }

  async createDirectConnection(overrides = {}) {
    const credentials = this.getCredentials(overrides);
    return await oracledb.getConnection(credentials);
  }

  async execute(sql, binds = [], options = {}) {
    let connection;
    try {
      connection = await this.getConnection();
      const result = await connection.execute(sql, binds, {
        outFormat: oracledb.OUT_FORMAT_OBJECT,
        autoCommit: options.autoCommit !== false,
        ...options
      });
      return result;
    } finally {
      if (connection) {
        await connection.close();
      }
    }
  }

  async executeMany(sql, binds, options = {}) {
    let connection;
    try {
      connection = await this.getConnection();
      const result = await connection.executeMany(sql, binds, {
        autoCommit: options.autoCommit !== false,
        ...options
      });
      return result;
    } finally {
      if (connection) {
        await connection.close();
      }
    }
  }

  getStatus() {
    if (!this.pool) {
      return { connected: false };
    }

    const stats = this.pool.getStatistics();
    return {
      connected: true,
      config: {
        user: this.config.user,
        connectionString: this.config.connectionString
      },
      pool: {
        connectionsOpen: stats?.connectionsOpen || 0,
        connectionsInUse: stats?.connectionsInUse || 0,
        poolMin: this.pool.poolMin,
        poolMax: this.pool.poolMax
      }
    };
  }

  async close() {
    if (this.pool) {
      await this.pool.close(10);
      this.pool = null;
      this.config = null;
      console.log('Oracle connection pool closed');
    }
  }

  // Create a new pool with specified size for stress testing
  async createStressPool(sessionCount, overrides = {}) {
    const credentials = this.getCredentials(overrides);
    const poolMin = Math.max(1, Math.min(
      sessionCount,
      Number.isFinite(Number(overrides.poolMin)) ? Number(overrides.poolMin) : Math.min(sessionCount, 10)
    ));
    const poolIncrement = Math.max(1, Math.min(
      sessionCount,
      Number.isFinite(Number(overrides.poolIncrement)) ? Number(overrides.poolIncrement) : 5
    ));
    const queueTimeout = Math.max(
      1000,
      Number.isFinite(Number(overrides.queueTimeout)) ? Number(overrides.queueTimeout) : 120000
    );

    return await oracledb.createPool({
      user: credentials.user,
      password: credentials.password,
      connectionString: credentials.connectionString,
      poolMin,
      poolMax: sessionCount,
      poolIncrement,
      poolTimeout: 60,
      queueTimeout,
      poolAlias: `stress_pool_${Date.now()}`
    });
  }
}

module.exports = new OracleDatabase();
