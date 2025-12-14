const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');
const path = require('path');
require('dotenv').config();

const oracleDb = require('./db/oracle');
const schemaManager = require('./db/schemaManager');
const stressEngine = require('./stress/engine');
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

// Store active stress test state
let stressTestState = {
  isRunning: false,
  config: null,
  startTime: null
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

// Create schema
app.post('/api/schema/create', async (req, res) => {
  const { scaleFactor = 1 } = req.body;
  try {
    io.emit('schema-progress', { step: 'Starting schema creation...', progress: 0 });
    await schemaManager.createSchema(oracleDb, (progress) => {
      io.emit('schema-progress', progress);
    });
    io.emit('schema-progress', { step: 'Populating data...', progress: 50 });
    await schemaManager.populateData(oracleDb, scaleFactor, (progress) => {
      io.emit('schema-progress', progress);
    });
    io.emit('schema-progress', { step: 'Schema created successfully!', progress: 100 });
    res.json({ success: true, message: 'Schema created and populated successfully' });
  } catch (error) {
    io.emit('schema-progress', { step: `Error: ${error.message}`, progress: -1 });
    res.status(500).json({ success: false, message: error.message });
  }
});

// Drop schema
app.post('/api/schema/drop', async (req, res) => {
  try {
    await schemaManager.dropSchema(oracleDb);
    res.json({ success: true, message: 'Schema dropped successfully' });
  } catch (error) {
    res.status(500).json({ success: false, message: error.message });
  }
});

// Get schema info
app.get('/api/schema/info', async (req, res) => {
  try {
    const info = await schemaManager.getSchemaInfo(oracleDb);
    res.json({ success: true, ...info });
  } catch (error) {
    res.status(500).json({ success: false, message: error.message });
  }
});

// Stress Test Management

// Start stress test
app.post('/api/stress/start', async (req, res) => {
  const config = req.body;

  if (stressTestState.isRunning) {
    return res.status(400).json({ success: false, message: 'Stress test already running' });
  }

  try {
    stressTestState = {
      isRunning: true,
      config,
      startTime: new Date()
    };

    // Start the stress engine
    stressEngine.start(oracleDb, config, io);

    // Start metrics collection
    metricsCollector.start(oracleDb, io);

    res.json({ success: true, message: 'Stress test started' });
  } catch (error) {
    stressTestState.isRunning = false;
    res.status(500).json({ success: false, message: error.message });
  }
});

// Stop stress test
app.post('/api/stress/stop', async (req, res) => {
  try {
    stressEngine.stop();
    metricsCollector.stop();

    stressTestState = {
      isRunning: false,
      config: null,
      startTime: null
    };

    res.json({ success: true, message: 'Stress test stopped' });
  } catch (error) {
    res.status(500).json({ success: false, message: error.message });
  }
});

// Get stress test status
app.get('/api/stress/status', (req, res) => {
  res.json({
    ...stressTestState,
    uptime: stressTestState.startTime
      ? Math.floor((new Date() - stressTestState.startTime) / 1000)
      : 0
  });
});

// Update stress test config on the fly
app.put('/api/stress/config', async (req, res) => {
  const newConfig = req.body;

  if (!stressTestState.isRunning) {
    return res.status(400).json({ success: false, message: 'No stress test running' });
  }

  try {
    stressEngine.updateConfig(newConfig);
    stressTestState.config = { ...stressTestState.config, ...newConfig };
    res.json({ success: true, message: 'Configuration updated' });
  } catch (error) {
    res.status(500).json({ success: false, message: error.message });
  }
});

// Socket.IO connection handling
io.on('connection', (socket) => {
  console.log('Client connected:', socket.id);

  // Send current state to newly connected client
  socket.emit('stress-status', stressTestState);

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
  metricsCollector.stop();
  await oracleDb.close();
  process.exit(0);
});

process.on('SIGINT', async () => {
  console.log('Shutting down...');
  stressEngine.stop();
  metricsCollector.stop();
  await oracleDb.close();
  process.exit(0);
});
