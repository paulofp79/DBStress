#!/usr/bin/env node

/**
 * Controlled Oracle RAC CPU workload.
 *
 * This runner performs read-only, CPU-heavy SQL.  It never creates, changes,
 * or drops database objects.  Use one --target per RAC instance service to
 * place an equal number of sessions on every node.
 *
 * Required environment variables:
 *   ORACLE_USER, ORACLE_PASSWORD
 *
 * Example:
 *   ORACLE_USER=lab ORACLE_PASSWORD=secret node scripts/rac-cpu-burn.js \
 *     --target rac1=host1:1521/labsvc1 --target rac2=host2:1521/labsvc2 \
 *     --sessions-per-target 32 --duration-seconds 300 \
 *     --i-understand-this-can-saturate-cpu
 */

let oracledb;

const defaults = {
  durationSeconds: 60,
  sessionsPerTarget: 4,
  workUnits: 1000,
  payloadBytes: 1024
};

function usage(exitCode = 0) {
  const message = `
Controlled Oracle RAC CPU workload (read-only)

Required:
  ORACLE_USER and ORACLE_PASSWORD environment variables
  --target NAME=CONNECT_STRING     Repeat for each RAC instance service
  --i-understand-this-can-saturate-cpu

Optional:
  --sessions-per-target N          Concurrent sessions per target (default: ${defaults.sessionsPerTarget})
  --duration-seconds N             Finite run duration (default: ${defaults.durationSeconds})
  --work-units N                   Hash operations per SQL call (default: ${defaults.workUnits})
  --payload-bytes N                Bytes hashed per operation, 1-32767 (default: ${defaults.payloadBytes})

Each session is tagged MODULE=DBSTRESS_RAC_CPU and ACTION=<target>-<worker>.
Use dedicated instance services, not a generic SCAN service, if you need an
even CPU load on each RAC node. Stop safely with Ctrl-C.
`;
  console[exitCode === 0 ? 'log' : 'error'](message.trim());
  process.exit(exitCode);
}

function positiveInteger(value, option, maximum) {
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed < 1 || (maximum && parsed > maximum)) {
    throw new Error(`${option} must be an integer from 1 to ${maximum || 'a practical limit'}`);
  }
  return parsed;
}

function parseArgs(argv) {
  const config = { ...defaults, targets: [], acknowledged: false };
  for (let index = 0; index < argv.length; index += 1) {
    const option = argv[index];
    if (option === '--help' || option === '-h') usage();
    if (option === '--i-understand-this-can-saturate-cpu') {
      config.acknowledged = true;
      continue;
    }
    const value = argv[++index];
    if (!value) throw new Error(`Missing value for ${option}`);
    if (option === '--target') {
      const separator = value.indexOf('=');
      if (separator <= 0 || separator === value.length - 1) {
        throw new Error('--target must be NAME=CONNECT_STRING');
      }
      config.targets.push({ name: value.slice(0, separator), connectionString: value.slice(separator + 1) });
    } else if (option === '--sessions-per-target') {
      config.sessionsPerTarget = positiveInteger(value, option);
    } else if (option === '--duration-seconds') {
      config.durationSeconds = positiveInteger(value, option, 86400);
    } else if (option === '--work-units') {
      config.workUnits = positiveInteger(value, option, 1000000);
    } else if (option === '--payload-bytes') {
      config.payloadBytes = positiveInteger(value, option, 32767);
    } else {
      throw new Error(`Unknown option: ${option}`);
    }
  }
  if (!config.acknowledged) throw new Error('Add --i-understand-this-can-saturate-cpu to run this workload');
  if (config.targets.length === 0) throw new Error('Specify at least one --target');
  if (!process.env.ORACLE_USER || !process.env.ORACLE_PASSWORD) {
    throw new Error('Set ORACLE_USER and ORACLE_PASSWORD before running');
  }
  return config;
}

let stopRequested = false;
const workers = [];

async function worker(target, workerNumber, config, deadline) {
  let connection;
  let batches = 0;
  try {
    connection = await oracledb.getConnection({
      user: process.env.ORACLE_USER,
      password: process.env.ORACLE_PASSWORD,
      connectionString: target.connectionString
    });
    const action = `${target.name}-${workerNumber}`.slice(0, 32);
    await connection.execute(
      'BEGIN DBMS_APPLICATION_INFO.SET_MODULE(:module, :action); END;',
      { module: 'DBSTRESS_RAC_CPU', action }
    );
    const instance = await connection.execute(
      "SELECT instance_name, host_name FROM v$instance",
      [], { outFormat: oracledb.OUT_FORMAT_OBJECT }
    );
    const details = instance.rows[0];
    console.log(`[${action}] connected to ${details.INSTANCE_NAME} on ${details.HOST_NAME}`);

    const sql = `
      SELECT COUNT(*) AS HASHES, MAX(hash_value) AS SAMPLE_HASH
      FROM (
        SELECT STANDARD_HASH(
                 RPAD(TO_CHAR(level) || RAWTOHEX(SYS_GUID()), :payloadBytes, 'x'),
                 'SHA512'
               ) AS hash_value
        FROM dual
        CONNECT BY level <= :workUnits
      )`;
    while (!stopRequested && Date.now() < deadline) {
      await connection.execute(sql, { payloadBytes: config.payloadBytes, workUnits: config.workUnits });
      batches += 1;
    }
  } finally {
    if (connection) {
      try {
        await connection.execute('BEGIN DBMS_APPLICATION_INFO.SET_MODULE(NULL, NULL); END;');
      } finally {
        await connection.close();
      }
    }
  }
  return { target: target.name, workerNumber, batches };
}

async function main() {
  let config;
  try {
    config = parseArgs(process.argv.slice(2));
  } catch (error) {
    console.error(`Configuration error: ${error.message}`);
    usage(1);
  }

  try {
    oracledb = require('oracledb');
  } catch (error) {
    throw new Error(`Unable to load the Oracle driver. Run npm install and configure Oracle Client first. (${error.message})`);
  }

  const totalSessions = config.targets.length * config.sessionsPerTarget;
  const deadline = Date.now() + config.durationSeconds * 1000;
  console.log(`Starting ${totalSessions} CPU workers for ${config.durationSeconds}s across ${config.targets.length} target(s).`);
  console.log('Use Ctrl-C to stop. Monitor tagged sessions with MODULE = DBSTRESS_RAC_CPU.');

  for (const target of config.targets) {
    for (let number = 1; number <= config.sessionsPerTarget; number += 1) {
      workers.push(worker(target, number, config, deadline));
    }
  }
  const results = await Promise.allSettled(workers);
  const failures = results.filter((result) => result.status === 'rejected');
  const completedBatches = results
    .filter((result) => result.status === 'fulfilled')
    .reduce((total, result) => total + result.value.batches, 0);
  console.log(`Finished: ${completedBatches} SQL batches; ${failures.length} worker failure(s).`);
  for (const failure of failures) console.error(failure.reason.message);
  process.exitCode = failures.length ? 1 : 0;
}

process.once('SIGINT', () => {
  stopRequested = true;
  console.log('\nStop requested; waiting for active SQL calls to finish.');
});
process.once('SIGTERM', () => {
  stopRequested = true;
  console.log('\nStop requested; waiting for active SQL calls to finish.');
});

main().catch((error) => {
  console.error(error.stack || error.message);
  process.exitCode = 1;
});
