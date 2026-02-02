const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');
const path = require('path');
require('dotenv').config();

const oracleDb = require('./db/oracle');
const schemaManager = require('./db/schemaManager');
const stressEngine = require('./stress/engine');
const indexContentionEngine = require('./stress/indexContentionEngine');
const libraryCacheLockEngine = require('./stress/libraryCacheLockEngine');
const hwContentionEngine = require('./stress/hwContentionEngine');
const metricsCollector = require('./metrics/collector');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: {
    origin: "http://localhost:3000",
    methods: ["GET", "POST"]
  }
});

app.use(cors());
app.use(express.json());

// Serve static files from React build in production
app.use(express.static(path.join(__dirname, '../client/build')));

// Store active stress test state - now supports multiple schemas
let stressTestStates = {};  // keyed by schema prefix

// Helper to get combined stress status
const getStressStatus = () => {
  const schemas = Object.keys(stressTestStates);
  const anyRunning = schemas.some(s => stressTestStates[s].isRunning);
  return {
    isRunning: anyRunning,
    schemas: stressTestStates,
    activeSchemas: schemas.filter(s => stressTestStates[s].isRunning)
  };
};

// API Routes

// Health check
app.get('/api/health', (req, res) => {
  res.json({ status: 'ok', timestamp: new Date().toISOString() });
});

// Test database connection
app.post('/api/db/test-connection', async (req, res) => {
  const { user, password, connectionString } = req.body;
  try {
    const result = await oracleDb.testConnection(user, password, connectionString);
    res.json({ success: true, message: 'Connection successful', ...result });
  } catch (error) {
    res.status(500).json({ success: false, message: error.message });
  }
});

// Connect to database
app.post('/api/db/connect', async (req, res) => {
  const { user, password, connectionString } = req.body;
  try {
    await oracleDb.initialize(user, password, connectionString);
    res.json({ success: true, message: 'Connected to database' });
  } catch (error) {
    res.status(500).json({ success: false, message: error.message });
  }
});

// Disconnect from database
app.post('/api/db/disconnect', async (req, res) => {
  try {
    await oracleDb.close();
    res.json({ success: true, message: 'Disconnected from database' });
  } catch (error) {
    res.status(500).json({ success: false, message: error.message });
  }
});

// Get connection status
app.get('/api/db/status', (req, res) => {
  const status = oracleDb.getStatus();
  res.json(status);
});

// Schema Management

// Create schema (with multi-schema support)
app.post('/api/schema/create', async (req, res) => {
  const { scaleFactor = 1, prefix = '', compress = false, compressionType = null, parallelism = 10, racTableCount = 1 } = req.body;
  const schemaId = prefix || 'default';

  try {
    io.emit('schema-progress', { schemaId, step: `Starting schema creation${prefix ? ` '${prefix}'` : ''}...`, progress: 0 });

    await schemaManager.createSchema(oracleDb, { prefix, compress, compressionType, racTableCount }, (progress) => {
      io.emit('schema-progress', { schemaId, ...progress });
    });

    io.emit('schema-progress', { schemaId, step: 'Populating data...', progress: 50 });

    await schemaManager.populateData(oracleDb, { prefix, scaleFactor, parallelism, racTableCount }, (progress) => {
      io.emit('schema-progress', { schemaId, ...progress });
    });

    io.emit('schema-progress', { schemaId, step: 'Schema created successfully!', progress: 100 });
    res.json({ success: true, message: `Schema${prefix ? ` '${prefix}'` : ''} created and populated successfully`, schemaId });
  } catch (error) {
    io.emit('schema-progress', { schemaId, step: `Error: ${error.message}`, progress: -1 });
    res.status(500).json({ success: false, message: error.message });
  }
});

// Drop schema
app.post('/api/schema/drop', async (req, res) => {
  const { prefix = '' } = req.body;
  try {
    await schemaManager.dropSchema(oracleDb, prefix);
    res.json({ success: true, message: `Schema${prefix ? ` '${prefix}'` : ''} dropped successfully` });
  } catch (error) {
    res.status(500).json({ success: false, message: error.message });
  }
});

// Generate SQL script for schema creation (download)
app.post('/api/schema/generate-script', (req, res) => {
  const { prefix = '', compressionType = 'none', scaleFactor = 1, racTableCount = 1 } = req.body;
  try {
    const script = schemaManager.generateScript({ prefix, compressionType, scaleFactor, racTableCount });
    const filename = `dbstress_schema${prefix ? `_${prefix}` : ''}_${scaleFactor}x.sql`;

    res.setHeader('Content-Type', 'application/sql');
    res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
    res.send(script);
  } catch (error) {
    res.status(500).json({ success: false, message: error.message });
  }
});

// Get schema info (for a specific schema)
app.get('/api/schema/info', async (req, res) => {
  const { prefix = '' } = req.query;
  try {
    const info = await schemaManager.getSchemaInfo(oracleDb, prefix);
    res.json({ success: true, ...info });
  } catch (error) {
    res.status(500).json({ success: false, message: error.message });
  }
});

// List all schemas
app.get('/api/schemas/list', async (req, res) => {
  try {
    const schemas = await schemaManager.listSchemas(oracleDb);
    res.json({ success: true, schemas });
  } catch (error) {
    res.status(500).json({ success: false, message: error.message });
  }
});

// Stress Test Management

// Start stress test (supports multiple schemas)
app.post('/api/stress/start', async (req, res) => {
  const config = req.body;
  const schemasToTest = config.schemas || [{ prefix: '' }]; // Array of { prefix: '', ... }

  try {
    // Start stress engine for each schema
    for (const schemaConfig of schemasToTest) {
      const schemaId = schemaConfig.prefix || 'default';

      if (stressTestStates[schemaId]?.isRunning) {
        continue; // Skip if already running
      }

      stressTestStates[schemaId] = {
        isRunning: true,
        config: { ...config, ...schemaConfig },
        startTime: new Date(),
        prefix: schemaConfig.prefix
      };
    }

    // Start the stress engine with multi-schema support
    stressEngine.start(oracleDb, { ...config, schemas: schemasToTest }, io);

    // Start metrics collection
    metricsCollector.start(oracleDb, io);

    // Reset GC baseline for fresh delta stats
    await metricsCollector.resetGCBaseline();

    res.json({ success: true, message: 'Stress test started', schemas: schemasToTest.map(s => s.prefix || 'default') });
  } catch (error) {
    // Reset states on error
    for (const schemaConfig of schemasToTest) {
      const schemaId = schemaConfig.prefix || 'default';
      if (stressTestStates[schemaId]) {
        stressTestStates[schemaId].isRunning = false;
      }
    }
    res.status(500).json({ success: false, message: error.message });
  }
});

// Stop stress test
app.post('/api/stress/stop', async (req, res) => {
  const { prefix } = req.body; // Optional: stop specific schema only

  try {
    if (prefix !== undefined) {
      // Stop specific schema
      const schemaId = prefix || 'default';
      stressEngine.stopSchema(schemaId);
      if (stressTestStates[schemaId]) {
        stressTestStates[schemaId].isRunning = false;
      }
    } else {
      // Stop all
      stressEngine.stop();
      metricsCollector.stop();

      for (const schemaId of Object.keys(stressTestStates)) {
        stressTestStates[schemaId].isRunning = false;
      }
    }

    res.json({ success: true, message: 'Stress test stopped' });
  } catch (error) {
    res.status(500).json({ success: false, message: error.message });
  }
});

// Get stress test status
app.get('/api/stress/status', (req, res) => {
  const status = getStressStatus();

  // Calculate uptime for each schema
  for (const schemaId of Object.keys(stressTestStates)) {
    const state = stressTestStates[schemaId];
    state.uptime = state.startTime ? Math.floor((new Date() - state.startTime) / 1000) : 0;
  }

  res.json({
    ...status,
    uptime: status.activeSchemas.length > 0 && stressTestStates[status.activeSchemas[0]]
      ? stressTestStates[status.activeSchemas[0]].uptime
      : 0
  });
});

// Reset GC wait events baseline (for fresh delta stats)
app.post('/api/metrics/reset-gc-baseline', async (req, res) => {
  try {
    await metricsCollector.resetGCBaseline();
    res.json({ success: true, message: 'GC baseline reset' });
  } catch (error) {
    res.status(500).json({ success: false, message: error.message });
  }
});

// Update stress test config on the fly
app.put('/api/stress/config', async (req, res) => {
  const newConfig = req.body;
  const status = getStressStatus();

  if (!status.isRunning) {
    return res.status(400).json({ success: false, message: 'No stress test running' });
  }

  try {
    stressEngine.updateConfig(newConfig);

    for (const schemaId of status.activeSchemas) {
      stressTestStates[schemaId].config = { ...stressTestStates[schemaId].config, ...newConfig };
    }

    res.json({ success: true, message: 'Configuration updated' });
  } catch (error) {
    res.status(500).json({ success: false, message: error.message });
  }
});

// ============================================
// Index Contention Demo API Routes
// ============================================

// Start index contention demo
app.post('/api/index-contention/start', async (req, res) => {
  const config = req.body;
  try {
    await indexContentionEngine.start(oracleDb, config, io);
    res.json({ success: true, message: 'Index Contention demo started' });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// Stop index contention demo
app.post('/api/index-contention/stop', async (req, res) => {
  try {
    const stats = await indexContentionEngine.stop();
    res.json({ success: true, message: 'Index Contention demo stopped', stats });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// Change index type while running
app.post('/api/index-contention/change-index', async (req, res) => {
  const { indexType, schemaPrefix } = req.body;
  try {
    if (!indexContentionEngine.isRunning) {
      return res.status(400).json({ success: false, error: 'Demo not running' });
    }
    await indexContentionEngine.changeIndex(oracleDb, indexType);
    res.json({ success: true, message: `Index type changed to ${indexType}` });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// Change sequence cache (0 == NOCACHE) at runtime
app.post('/api/index-contention/change-sequence-cache', async (req, res) => {
  const { cache } = req.body; // integer
  try {
    await indexContentionEngine.changeSequenceCache(oracleDb, cache);
    res.json({ success: true, message: `Sequence cache changed to ${cache}` });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// Run an A/B test comparing two sequence cache settings while the demo is running
app.post('/api/index-contention/ab-test-sequence-cache', async (req, res) => {
  const { cacheA = 0, cacheB = 100, duration = 10, warmup = 5 } = req.body;
  try {
    if (!indexContentionEngine.isRunning) {
      return res.status(400).json({ success: false, error: 'Demo not running' });
    }

    const result = await indexContentionEngine.runSequenceCacheABTest(oracleDb, { cacheA, cacheB, duration, warmup });
    res.json({ success: true, result });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// Get index contention demo status
app.get('/api/index-contention/status', (req, res) => {
  res.json({
    isRunning: indexContentionEngine.isRunning,
    config: indexContentionEngine.config,
    stats: indexContentionEngine.stats
  });
});

// ============================================
// Library Cache Lock Demo API Routes
// ============================================

// Start library cache lock demo
app.post('/api/library-cache-lock/start', async (req, res) => {
  const config = req.body;
  try {
    await libraryCacheLockEngine.start(oracleDb, config, io);
    res.json({ success: true, message: 'Library Cache Lock demo started' });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// Stop library cache lock demo
app.post('/api/library-cache-lock/stop', async (req, res) => {
  try {
    const stats = await libraryCacheLockEngine.stop();
    res.json({ success: true, message: 'Library Cache Lock demo stopped', stats });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// Get library cache lock demo status
app.get('/api/library-cache-lock/status', (req, res) => {
  res.json({
    isRunning: libraryCacheLockEngine.isRunning,
    config: libraryCacheLockEngine.config,
    stats: libraryCacheLockEngine.stats
  });
});

// ============================================
// HW Contention Demo API Routes
// ============================================

// Start HW contention demo
app.post('/api/hw-contention/start', async (req, res) => {
  const config = req.body;
  try {
    await hwContentionEngine.start(oracleDb, config, io);
    res.json({ success: true, message: 'HW Contention demo started' });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// Stop HW contention demo
app.post('/api/hw-contention/stop', async (req, res) => {
  try {
    const stats = await hwContentionEngine.stop();
    res.json({ success: true, message: 'HW Contention demo stopped', stats });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// Get HW contention demo status
app.get('/api/hw-contention/status', (req, res) => {
  res.json({
    isRunning: hwContentionEngine.isRunning,
    config: hwContentionEngine.config,
    stats: hwContentionEngine.stats
  });
});

// Socket.IO connection handling
io.on('connection', (socket) => {
  console.log('Client connected:', socket.id);

  // Send current state to newly connected client
  socket.emit('stress-status', getStressStatus());

  socket.on('disconnect', () => {
    console.log('Client disconnected:', socket.id);
  });
});

// Catch-all handler for React Router (in production)
app.get('*', (req, res) => {
  res.sendFile(path.join(__dirname, '../client/build/index.html'));
});

const PORT = process.env.PORT || 3001;

server.listen(PORT, () => {
  console.log(`DBStress server running on port ${PORT}`);
  console.log(`API available at http://localhost:${PORT}/api`);
});

// Graceful shutdown
process.on('SIGTERM', async () => {
  console.log('Shutting down...');
  stressEngine.stop();
  indexContentionEngine.stop();
  libraryCacheLockEngine.stop();
  hwContentionEngine.stop();
  metricsCollector.stop();
  await oracleDb.close();
  process.exit(0);
});

process.on('SIGINT', async () => {
  console.log('Shutting down...');
  stressEngine.stop();
  indexContentionEngine.stop();
  libraryCacheLockEngine.stop();
  hwContentionEngine.stop();
  metricsCollector.stop();
  await oracleDb.close();
  process.exit(0);
});
