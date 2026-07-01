require('dotenv').config();
const { Pool } = require('pg');

const pool = new Pool({
  user: process.env.USER_NAME,
  host: process.env.IP,
  database: process.env.DB_NAME,
  password: process.env.PASSWORD,
  port: process.env.DB_PORT || 5432, // Allows overriding default port
  ssl: {
    rejectUnauthorized: false // Often required for remote databases
  }
});

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
    try {
      // Connect to verify the connection works
      const client = await pool.connect();
      
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
        CREATE TABLE IF NOT EXISTS historical_candles (
          id SERIAL PRIMARY KEY,
          symbol VARCHAR NOT NULL,
          datetime TIMESTAMP NOT NULL,
          open NUMERIC NOT NULL,
          high NUMERIC NOT NULL,
          low NUMERIC NOT NULL,
          close NUMERIC NOT NULL,
          volume BIGINT NOT NULL,
          UNIQUE(symbol, datetime)
        );
        CREATE INDEX IF NOT EXISTS idx_historical_candles_symbol ON historical_candles(symbol);

        CREATE TABLE IF NOT EXISTS symbol_payloads (
          symbol VARCHAR PRIMARY KEY,
          historic_data JSONB NOT NULL,
          updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS prediction_logs (
          id SERIAL PRIMARY KEY,
          symbol VARCHAR NOT NULL,
          tick_data JSONB,
          response_data JSONB,
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
      `;
      
      await client.query(createTableQuery);
      client.release();
      resolve();
    } catch (err) {
      console.error('[DB] Error initializing Postgres DB:', err);
      reject(err);
    }
  });
}

function insertTick(symbol, open, high, low, close, volume, timestampStr) {
  return new Promise(async (resolve, reject) => {
    try {
      if (isPoolClosed) return resolve();
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
    try {
      if (isPoolClosed) return resolve();
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

async function getTicksInRange(symbol, fromStr, toStr) {
  try {
    if (isPoolClosed) return [];
    const res = await pool.query(
      `SELECT open, high, low, close, volume, timestamp FROM ticks
       WHERE symbol = $1 AND timestamp >= $2 AND timestamp < $3
       ORDER BY timestamp ASC`,
      [symbol, fromStr, toStr]
    );
    return res.rows;
  } catch (err) {
    console.error('[DB] Error fetching ticks in range:', err);
    return [];
  }
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

async function cleanOldTicks(hours = 1) {
  try {
    if (isPoolClosed) return 0;
    const res = await pool.query(
      `DELETE FROM ticks WHERE timestamp < NOW() - INTERVAL '${hours} hours'`
    );
    const deleted = res.rowCount || 0;
    if (deleted > 0) console.log(`[DB] Cleaned ${deleted} old ticks (older than ${hours}h)`);
    return deleted;
  } catch (err) {
    console.error('[DB] Error cleaning old ticks:', err);
    return 0;
  }
}

function getDB() {
  return pool;
}

let isPoolClosed = false;
function closeDB() {
  return new Promise((resolve, reject) => {
    if (isPoolClosed) {
      return resolve(); // Already closed
    }
    isPoolClosed = true;
    pool.end(err => {
      if (err) {
        console.error("Error closing PostgreSQL pool:", err);
        reject(err);
      } else {
        console.log("PostgreSQL pool closed.");
        resolve();
      }
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
  query: (text, params) => {
    if (isPoolClosed) {
      // Return a dummy resolved promise to avoid unhandled rejections during shutdown
      return Promise.resolve({ rows: [], rowCount: 0 });
    }
    return pool.query(text, params);
  },
};
