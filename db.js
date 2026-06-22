require('dotenv').config();
const { Pool } = require('pg');

const connectionString = process.env.DATABASE_URL;
const poolBaseConfig = {
  max: Number(process.env.DB_POOL_MAX || 10),
  idleTimeoutMillis: Number(process.env.DB_IDLE_TIMEOUT_MS || 10000),
  connectionTimeoutMillis: Number(process.env.DB_CONNECT_TIMEOUT_MS || 10000),
  ssl: process.env.DB_SSL === "false" ? false : {
    rejectUnauthorized: false // Often required for remote databases
  }
};

const pool = new Pool(connectionString ? {
  ...poolBaseConfig,
  connectionString,
} : {
  ...poolBaseConfig,
  user: process.env.USER_NAME || process.env.DB_USER,
  host: process.env.IP || process.env.DB_HOST,
  database: process.env.DB_NAME,
  password: process.env.PASSWORD || process.env.DB_PASSWORD || process.env.DB_PASS,
  port: Number(process.env.DB_PORT || 5432), // Allows overriding default port
});

let isPoolClosed = false;

function isConfigured() {
  if (connectionString) return true;

  return Boolean(
    (process.env.USER_NAME || process.env.DB_USER) &&
      (process.env.IP || process.env.DB_HOST) &&
      process.env.DB_NAME &&
      (process.env.PASSWORD || process.env.DB_PASSWORD || process.env.DB_PASS),
  );
}

function padDate(n) {
  return String(n).padStart(2, "0");
}

function formatTimestamp(date) {
  return `${date.getFullYear()}-${padDate(date.getMonth() + 1)}-${padDate(date.getDate())} ${padDate(date.getHours())}:${padDate(date.getMinutes())}:${padDate(date.getSeconds())}`;
}

function getFiveMinuteBucket(date) {
  const d = new Date(date);
  const mins = Math.floor(d.getMinutes() / 5) * 5;
  d.setMinutes(mins, 0, 0);
  d.setMilliseconds(0);
  return d;
}

function initDB() {
  return new Promise(async (resolve, reject) => {
    let client;
    try {
      // Connect to verify the connection works
      client = await pool.connect();
      
      const createTableQuery = `
        CREATE TABLE IF NOT EXISTS candles_5m (
          id SERIAL PRIMARY KEY,
          symbol TEXT NOT NULL,
          open REAL NOT NULL,
          high REAL NOT NULL,
          low REAL NOT NULL,
          close REAL NOT NULL,
          volume INTEGER NOT NULL,
          timestamp TIMESTAMP NOT NULL,
          UNIQUE(symbol, timestamp)
        );
        CREATE TABLE IF NOT EXISTS ticks (
          id SERIAL PRIMARY KEY,
          symbol TEXT NOT NULL,
          open REAL NOT NULL,
          high REAL NOT NULL,
          low REAL NOT NULL,
          close REAL NOT NULL,
          volume INTEGER NOT NULL,
          timestamp TIMESTAMP NOT NULL
        );
      `;
      
      await client.query(createTableQuery);
      resolve();
    } catch (err) {
      console.error('[DB] Error initializing Postgres DB:', err);
      reject(err);
    } finally {
      if (client) {
        client.release();
      }
    }
  });
}

function insertTick(symbol, open, high, low, close, volume, timestampStr) {
  return new Promise(async (resolve, reject) => {
    if (isPoolClosed) return resolve(null);
    try {
      const query = `
        INSERT INTO ticks (symbol, open, high, low, close, volume, timestamp)
        VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING id;
      `;
      const res = await pool.query(query, [symbol, open, high, low, close, volume, timestampStr]);
      resolve(res.rows[0].id);
    } catch (err) {
      console.error('[DB] Error inserting tick:', err);
      reject(err);
    }
  });
}

function upsertCandle(symbol, open, high, low, close, volume, timestampStr) {
  return new Promise(async (resolve, reject) => {
    if (isPoolClosed) return resolve(null);
    try {
      const query = `
        INSERT INTO candles_5m (symbol, open, high, low, close, volume, timestamp) 
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT(symbol, timestamp) DO UPDATE SET 
          open=EXCLUDED.open,
          high=EXCLUDED.high,
          low=EXCLUDED.low,
          close=EXCLUDED.close,
          volume=EXCLUDED.volume
        RETURNING id;
      `;
      const res = await pool.query(query, [symbol, open, high, low, close, volume, timestampStr]);
      resolve(res.rows[0].id);
    } catch (err) {
      reject(err);
    }
  });
}

function getTicksInRange(symbol, fromStr, toStr) {
  return Promise.resolve([]);
}

function aggregateOHLCFromTicks(rows) {
  if (!rows || rows.length === 0) return null;
  return {
    open: rows[0].open != null ? rows[0].open : rows[0].close,
    high: Math.max(...rows.map(r => r.high != null ? r.high : r.close)),
    low: Math.min(...rows.map(r => r.low != null ? r.low : r.close)),
    close: rows[rows.length - 1].close,
    volume: rows.reduce((sum, r) => sum + r.volume, 0),
    tick_count: rows.length
  };
}

function cleanOldTicks(hours = 1) {
  return Promise.resolve(0);
}

function getDB() {
  return pool;
}

function closeDB() {
  return new Promise((resolve, reject) => {
    if (isPoolClosed) return resolve();
    isPoolClosed = true;
    pool.end((err) => {
      if (err) return reject(err);
      resolve();
    });
  });
}

module.exports = {
  initDB,
  insertTick,
  upsertCandle,
  getTicksInRange,
  aggregateOHLCFromTicks,
  cleanOldTicks,
  getDB,
  closeDB,
  getFiveMinuteBucket,
  formatTimestamp,
  isConfigured,
  query: (text, params) => isPoolClosed ? null : pool.query(text, params),
};
