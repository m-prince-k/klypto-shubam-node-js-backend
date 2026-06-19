const db = require('./db');

const collectors = {};
const lastTicks = {}; // symbol -> { price, volume }
const POLL_INTERVAL_MS = 1000;
const MAX_COLLECTOR_RUN_MS = 2 * 60 * 1000; // auto-stop after 2 minutes

function startCollector(symbol, token, smartApi) {
  const key = symbol.toUpperCase();

  if (collectors[key]) {
    return false;
  }

  if (!smartApi || typeof smartApi.marketData !== 'function') {
    console.error(`[TickCollector] Cannot start collector for ${key}: smartApi not available`);
    return false;
  }

  console.log(`[TickCollector] Starting collector for ${key} (token: ${token}, interval: ${POLL_INTERVAL_MS}ms)`);

  const startedAt = Date.now();

  const intervalId = setInterval(async () => {
    try {
      // Auto-stop after max run time
      if (Date.now() - startedAt > MAX_COLLECTOR_RUN_MS) {
        console.log(`[TickCollector/${key}] Auto-stopping after ${MAX_COLLECTOR_RUN_MS / 1000}s`);
        stopCollector(key);
        return;
      }

      const resp = await smartApi.marketData({
        mode: "FULL",
        exchangeTokens: { NSE: [token] },
        tradingsymbol: `${key}-EQ`
      });

      let tick = null;
      if (resp && resp.data && resp.data.fetched && resp.data.fetched.length > 0) {
        tick = resp.data.fetched[0];
      } else if (resp && resp.fetched && resp.fetched.length > 0) {
        tick = resp.fetched[0];
      }

      if (tick && (tick.ltp != null || tick.close != null)) {
        if (!lastTicks[key]) {
          console.log(`[TickCollector/${key}] Raw tick fields:`, Object.keys(tick).join(', '));
          console.log(`[TickCollector/${key}] Sample tick:`, JSON.stringify(tick));
        }

        const now = new Date();
        const fallbackStr = db.formatTimestamp(now);
        const rawTime = tick.exchangeTime || tick.lastTradeTime || tick.exchTradeTime || fallbackStr;
        const price = tick.ltp != null ? tick.ltp : tick.close;
        const volume = tick.lastTradedQuantity || tick.volume || 0;
        if (!tick.lastTradedQuantity && !tick.volume && !lastTicks[key]) {
          console.log(`[TickCollector/${key}] WARNING: getLtpData endpoint returns no volume field. Volume will be 0.`);
        }

        const last = lastTicks[key];
        if (last && last.price === price && last.volume === volume) {
          return;
        }
        lastTicks[key] = { price, volume };

        await db.insertTick(key, price, volume, rawTime);
      }
    } catch (err) {
      // suppress log spam on transient network errors
    }
  }, POLL_INTERVAL_MS);

  collectors[key] = {
    symbol: key,
    token,
    intervalId,
    startedAt,
    pollCount: 0
  };

  return true;
}

function stopCollector(symbol) {
  const key = symbol.toUpperCase();
  const collector = collectors[key];
  if (!collector) return false;

  clearInterval(collector.intervalId);
  delete collectors[key];
  console.log(`[TickCollector] Stopped collector for ${key}`);
  return true;
}

function isCollectorRunning(symbol) {
  const key = symbol.toUpperCase();
  return !!collectors[key];
}

function getActiveCollectors() {
  return Object.keys(collectors);
}

async function getCurrentBucketOHLC(symbol) {
  const key = symbol.toUpperCase();
  const now = new Date();
  const bucketStart = db.getFiveMinuteBucket(now);
  const bucketEnd = new Date(bucketStart);
  bucketEnd.setMinutes(bucketStart.getMinutes() + 5);

  const fromStr = db.formatTimestamp(bucketStart);
  const toStr = db.formatTimestamp(bucketEnd);

  const rows = await db.getTicksInRange(key, fromStr, toStr);
  return db.aggregateOHLCFromTicks(rows);
}

function getBucketBoundaries(date) {
  const now = date || new Date();
  const bucketStart = db.getFiveMinuteBucket(now);
  const bucketEnd = new Date(bucketStart);
  bucketEnd.setMinutes(bucketStart.getMinutes() + 5);
  return {
    start: bucketStart,
    end: bucketEnd,
    startStr: db.formatTimestamp(bucketStart),
    endStr: db.formatTimestamp(bucketEnd)
  };
}

async function getLiveCandleForBucket(symbol, bucketStartDate) {
  const key = symbol.toUpperCase();
  const bucketStart = db.getFiveMinuteBucket(bucketStartDate || new Date());
  const bucketEnd = new Date(bucketStart);
  bucketEnd.setMinutes(bucketStart.getMinutes() + 5);

  const fromStr = db.formatTimestamp(bucketStart);
  const toStr = db.formatTimestamp(bucketEnd);

  const rows = await db.getTicksInRange(key, fromStr, toStr);
  if (!rows || rows.length === 0) return null;

  const ohlc = db.aggregateOHLCFromTicks(rows);
  return {
    datetime: fromStr,
    open: ohlc.open,
    high: ohlc.high,
    low: ohlc.low,
    close: ohlc.close,
    volume: ohlc.volume,
    tickCount: rows.length
  };
}

function stopAllCollectors() {
  Object.keys(collectors).forEach(key => stopCollector(key));
}

module.exports = {
  startCollector,
  stopCollector,
  isCollectorRunning,
  getActiveCollectors,
  getCurrentBucketOHLC,
  getBucketBoundaries,
  getLiveCandleForBucket,
  stopAllCollectors
};
