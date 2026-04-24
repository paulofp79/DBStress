const tablespaceManager = require('../db/tablespaceManager');

class DatafileGrowthEngine {
  constructor() {
    this.db = null;
    this.io = null;
    this.rules = new Map();
  }

  configure(db, io) {
    this.db = db;
    this.io = io;
  }

  normalizeRule(config = {}) {
    const fileName = String(config.fileName || '').trim();
    if (!fileName) {
      throw new Error('Datafile name is required.');
    }

    return {
      fileName,
      tablespaceName: String(config.tablespaceName || '').trim() || null,
      incrementMb: Math.max(1, Number.parseInt(config.incrementMb, 10) || 100),
      intervalSeconds: Math.max(1, Number.parseInt(config.intervalSeconds, 10) || 60)
    };
  }

  emitStatus() {
    if (this.io) {
      this.io.emit('datafile-growth-status', this.getStatus());
    }
  }

  getStatus() {
    return {
      isRunning: this.rules.size > 0,
      schedules: Array.from(this.rules.values())
        .map((rule) => ({
          fileName: rule.fileName,
          tablespaceName: rule.tablespaceName,
          incrementMb: rule.incrementMb,
          intervalSeconds: rule.intervalSeconds,
          resizeCount: rule.resizeCount,
          currentSizeMb: rule.currentSizeMb,
          lastRunAt: rule.lastRunAt,
          nextRunAt: rule.nextRunAt,
          lastError: rule.lastError || null
        }))
        .sort((a, b) => a.fileName.localeCompare(b.fileName))
    };
  }

  async resizeDatafile(fileName, incrementMb) {
    if (!this.db) {
      throw new Error('Database connection is not available.');
    }

    const current = await tablespaceManager.getDatafileByName(this.db, fileName);
    const nextSizeMb = Math.ceil(Number(current.sizeMb || 0) + Number(incrementMb || 0));
    const escapedFileName = fileName.replace(/'/g, "''");

    await this.db.execute(`ALTER DATABASE DATAFILE '${escapedFileName}' RESIZE ${nextSizeMb}M`);

    const updated = await tablespaceManager.getDatafileByName(this.db, fileName);
    return {
      previousSizeMb: current.sizeMb,
      currentSizeMb: updated.sizeMb,
      nextSizeMb,
      tablespaceName: updated.tablespaceName
    };
  }

  scheduleNext(rule) {
    rule.nextRunAt = Date.now() + (rule.intervalSeconds * 1000);
    rule.timer = setTimeout(async () => {
      try {
        const result = await this.resizeDatafile(rule.fileName, rule.incrementMb);
        rule.currentSizeMb = result.currentSizeMb;
        rule.tablespaceName = result.tablespaceName || rule.tablespaceName;
        rule.resizeCount += 1;
        rule.lastRunAt = Date.now();
        rule.lastError = null;
      } catch (error) {
        rule.lastRunAt = Date.now();
        rule.lastError = error.message;
      } finally {
        if (this.rules.has(rule.fileName)) {
          this.scheduleNext(rule);
        }
        this.emitStatus();
      }
    }, rule.intervalSeconds * 1000);
  }

  async start(config = {}) {
    const normalized = this.normalizeRule(config);

    if (!this.db) {
      throw new Error('Connect to Oracle first.');
    }

    const current = await tablespaceManager.getDatafileByName(this.db, normalized.fileName);

    if (this.rules.has(normalized.fileName)) {
      await this.stop(normalized.fileName);
    }

    const rule = {
      ...normalized,
      tablespaceName: normalized.tablespaceName || current.tablespaceName,
      currentSizeMb: current.sizeMb,
      resizeCount: 0,
      lastRunAt: null,
      nextRunAt: null,
      lastError: null,
      timer: null
    };

    this.rules.set(rule.fileName, rule);
    this.scheduleNext(rule);
    this.emitStatus();

    return this.getStatus();
  }

  async stop(fileName) {
    const key = String(fileName || '').trim();
    const rule = this.rules.get(key);
    if (rule?.timer) {
      clearTimeout(rule.timer);
    }
    this.rules.delete(key);
    this.emitStatus();
    return this.getStatus();
  }

  async stopAll() {
    for (const rule of this.rules.values()) {
      if (rule.timer) {
        clearTimeout(rule.timer);
      }
    }
    this.rules.clear();
    this.emitStatus();
    return this.getStatus();
  }
}

module.exports = new DatafileGrowthEngine();
