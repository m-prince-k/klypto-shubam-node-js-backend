const fs = require("fs");
const path = require("path");
const axios = require("axios");
const postgresDb = require("./db");
const angelone = require("./angelone-client");
const symbolsList = require("./symbols.js");

const PREDICT_URL = "http://13.207.78.205:8000/predict";

let LOG_FILE;
let PAYLOAD_LOG;
let logStream;
let payloadLogStream;
let tickLogStream;

const TARGET_HOUR = 9;
const TARGET_MINUTE = 15;

async function asyncPool(concurrency, items, iteratorFn) {
  const ret = [];
  const executing = new Set();
  for (const item of items) {
    const p = Promise.resolve().then(() => iteratorFn(item));
    ret.push(p);
    executing.add(p);
    const clean = () => executing.delete(p);
    p.then(clean).catch(clean);
    if (executing.size >= concurrency) {
      await Promise.race(executing);
    }
  }
  return Promise.all(ret);
}

function logPayload(msg) {
  if (payloadLogStream) {
    const timestamp = new Date().toISOString();
    payloadLogStream.write(`[${timestamp}] ${msg}\n`);
  }
}

async function getLatestTick(symbol, retryCount = 0) {
  const fetchStart = Date.now();
  try {
    const token = symbolsList[symbol];
    if (!token) return { symbol, success: false, error: "Token not found in symbolsList", fetchTimeMs: Date.now() - fetchStart };
    
    const now = new Date();
    const pad = n => String(n).padStart(2, '0');
    const dateStr = `${now.getFullYear()}-${pad(now.getMonth()+1)}-${pad(now.getDate())}`;
    
    const fromStr = `${dateStr} ${String(TARGET_HOUR).padStart(2, '0')}:${String(TARGET_MINUTE).padStart(2, '0')}`;
    const toStr = `${dateStr} ${String(TARGET_HOUR).padStart(2, '0')}:${String(TARGET_MINUTE + 10).padStart(2, '0')}`;

    try {
      const hist = await angelone.fetchHistoricalCandles(symbol, token, "FIVE_MINUTE", fromStr, toStr);
      if (hist && hist.data && hist.data.length > 0) {
        let candle = hist.data.find(c => {
           const d = new Date(c[0]);
           return d.getHours() === TARGET_HOUR && d.getMinutes() === TARGET_MINUTE;
        });
        
        if (!candle) {
           console.log(`  [${symbol}] Exact ${TARGET_HOUR}:${TARGET_MINUTE} candle not found. Using latest available candle in this window.`);
           candle = hist.data[hist.data.length - 1];
        }
        
        if (candle) {
           const fetchTimeMs = Date.now() - fetchStart;
           const fallbackCandle = {
             timestamp: candle[0],
             open: parseFloat(candle[1]),
             high: parseFloat(candle[2]),
             low: parseFloat(candle[3]),
             close: parseFloat(candle[4]),
             volume: parseInt(candle[5], 10)
           };
           return { symbol, success: true, tick: fallbackCandle, dateStr, fetchTimeMs };
        }
      }
      return { symbol, success: false, error: "Candle not found in response", fetchTimeMs: Date.now() - fetchStart };
    } catch (e) {
      const isRateLimit = e.response && (e.response.status === 429 || e.response.status === 403);
      const isNetworkError = e.code === 'ECONNABORTED' || e.code === 'ETIMEDOUT' || e.code === 'ECONNRESET';
      if (isRateLimit || isNetworkError) {
        if (retryCount < 3) {
           const delay = 1000 * Math.pow(1.5, retryCount);
           console.log(`  [${symbol}] ${isRateLimit ? 'Rate limited (403/429)' : 'Network Error'}. Waiting ${delay}ms before retry (Attempt ${retryCount + 1}/3)...`);
           await new Promise(r => setTimeout(r, delay));
           return await getLatestTick(symbol, retryCount + 1);
        }
      }
      return { symbol, success: false, error: e.message, fetchTimeMs: Date.now() - fetchStart };
    }
  } catch (err) {
    return { symbol, success: false, error: err.message, fetchTimeMs: Date.now() - fetchStart };
  }
}

async function processSymbol(symbol, historic_data, latestTickResult, dbPool, metrics) {
  try {
    if (!historic_data || historic_data.length === 0) {
      console.log(`  Skipping ${symbol}: JSON data is empty.`);
      return;
    }

    if (historic_data.length > 300) {
      historic_data = historic_data.slice(-300);
    }

    let tickObj;
    if (latestTickResult && latestTickResult.success) {
      const latestTick = latestTickResult.tick;
      
      const d = new Date(latestTick.timestamp);
      const formattedDatetime = `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')} ${String(d.getHours()).padStart(2, '0')}:${String(d.getMinutes()).padStart(2, '0')}:${String(d.getSeconds()).padStart(2, '0')}`;

      tickObj = {
        datetime: formattedDatetime,
        open: latestTick.open,
        high: latestTick.high,
        low: latestTick.low,
        close: latestTick.close,
        volume: latestTick.volume,
      };
      
      const currentExecutionTime = new Date().toISOString();
      const tickLogMsg = { time_written: currentExecutionTime, symbol, date: latestTickResult.dateStr, tick: latestTick, fetchTimeMs: latestTickResult.fetchTimeMs, source: "api" };
      if (tickLogStream) {
        tickLogStream.write(JSON.stringify(tickLogMsg) + "\n");
      }
      logPayload(`[TICK FETCH] ${JSON.stringify(tickLogMsg)}`);
    } else {
      console.log(`  No live tick found for ${symbol} in API. Using last historical candle.`);
      const lastCandle = historic_data[historic_data.length - 1];
      tickObj = {
        datetime: lastCandle.datetime,
        open: lastCandle.open,
        high: lastCandle.high,
        low: lastCandle.low,
        close: lastCandle.close,
        volume: lastCandle.volume,
      };
    }

    const concatenated_historic_data = [...historic_data];
    if (tickObj) {
      concatenated_historic_data.push(tickObj);
    }

    const payload = {
      historic_data: concatenated_historic_data,
      tick: tickObj,
    };

    logPayload(`[${symbol}] Retrieved tick: ${JSON.stringify(tickObj)}`);
    logPayload(`[${symbol}] Full payload being sent: ${JSON.stringify(payload)}`);

    const maxRetries = 3;
    let response = null;
    let attempt = 0;
    while (attempt < maxRetries) {
      try {
        response = await axios.post(PREDICT_URL, payload, {
          headers: { "Content-Type": "application/json" },
          timeout: 30000,
        });
        break; 
      } catch (err) {
        attempt++;
        const errorMsg = err.response ? JSON.stringify(err.response.data) : err.message;
        if (attempt >= maxRetries) {
          throw new Error(`Failed after ${maxRetries} attempts: ${errorMsg}`);
        }
        console.warn(`  [${symbol}] Error sending payload (${errorMsg}), retrying attempt ${attempt}/${maxRetries}...`);
        await new Promise(res => setTimeout(res, 2000));
      }
    }

    await dbPool.query(`
      INSERT INTO prediction_logs (symbol, tick_data, response_data, created_at)
      VALUES ($1, $2, $3, CURRENT_TIMESTAMP)
    `, [symbol, tickObj, response.data]);
    
    const logMsg = `[${symbol}] Payload sent. Tick: ${JSON.stringify(tickObj)} Response: ${JSON.stringify(response.data)}\n`;
    if (logStream) logStream.write(logMsg);
    
    metrics.successCount++;
    console.log(`  Success [${symbol}]. Signal: ${JSON.stringify(response.data)}`);
  } catch (err) {
    metrics.failCount++;
    const errorMsg = err.response
      ? JSON.stringify(err.response.data)
      : err.message;
    const logErr = `[${symbol}] Error sending payload: ${errorMsg}\n`;
    if (logStream) logStream.write(logErr);
    console.error(`  Error [${symbol}]: ${errorMsg}`);
  }
}

async function main() {
  const now = new Date();
  const hours = now.getHours();
  const minutes = now.getMinutes();
  const timeStr = `${hours}${minutes.toString().padStart(2, '0')}`;
  
  LOG_FILE = path.join(__dirname, "prediction logs", `prediction${timeStr}.log`);
  PAYLOAD_LOG = path.join(__dirname, "prediction logs", `prediction_payloads${timeStr}.log`);
  const TICK_LOG = path.join(__dirname, "prediction logs", `${TARGET_HOUR}_${TARGET_MINUTE}_ticks.log`);

  logStream = fs.createWriteStream(LOG_FILE, { flags: 'a' });
  payloadLogStream = fs.createWriteStream(PAYLOAD_LOG, { flags: 'a' });
  tickLogStream = fs.createWriteStream(TICK_LOG, { flags: 'a' });

  console.log("Starting prediction job...");

  const pool = postgresDb.getDB();
  const res = await pool.query('SELECT symbol, historic_data FROM symbol_payloads');
  const payloadRecords = res.rows;

  console.log(`Found ${payloadRecords.length} payload records in DB.`);

  logStream.write(`--- Prediction Run at ${new Date().toISOString()} ---\n`);

  const CONCURRENCY = 10;
  const startTime = Date.now();
  
  // Phase 1: Fetch Ticks for all symbols with a Sweep Loop
  console.log("\n--- Phase 1: Fetching API Ticks for all symbols ---");
  const allTickResults = {};
  let symbolsToFetch = payloadRecords.map(r => r.symbol);
  let sweepNumber = 1;

  while (symbolsToFetch.length > 0) {
    console.log(`\n--- Tick Sweep ${sweepNumber}: Fetching ${symbolsToFetch.length} symbols ---`);
    const failedSymbols = [];

    await asyncPool(CONCURRENCY, symbolsToFetch, async (symbol) => {
      const result = await getLatestTick(symbol);
      allTickResults[symbol] = result;
      if (!result.success) {
        console.log(`  [${symbol}] TICK FETCH FAILED: ${result.error}`);
        failedSymbols.push(symbol);
      }
    });

    symbolsToFetch = failedSymbols;
    
    if (symbolsToFetch.length > 0) {
      console.log(`\nSweep ${sweepNumber} finished. ${symbolsToFetch.length} ticks failed.`);
      console.log(`Waiting 2 seconds before retrying failed ticks...`);
      await new Promise(r => setTimeout(r, 2000));
    }
    sweepNumber++;
  }
  
  console.log("\n✅ All 206 ticks successfully fetched (or attempted until sweep completion).\n");

  const metrics = {
    successCount: 0,
    failCount: 0,
    maxTime: 0,
    minTime: Infinity
  };

  // Phase 2: Send Predictions using the cached ticks
  console.log("\n--- Phase 2: Sending Predictions ---");
  await asyncPool(CONCURRENCY, payloadRecords, async (record) => {
    const symbol = record.symbol;
    const latestTickResult = allTickResults[symbol];
    
    // Track API fetch metrics (if it was successful)
    if (latestTickResult && latestTickResult.success) {
      if (latestTickResult.fetchTimeMs > metrics.maxTime) metrics.maxTime = latestTickResult.fetchTimeMs;
      if (latestTickResult.fetchTimeMs < metrics.minTime) metrics.minTime = latestTickResult.fetchTimeMs;
    }
    
    await processSymbol(symbol, record.historic_data.historic_data || record.historic_data, latestTickResult, pool, metrics);
  });

  const totalTimeTaken = Date.now() - startTime;
  const rps = (payloadRecords.length / (totalTimeTaken / 1000)).toFixed(2);
  
  console.log("\n--- PREDICTION PIPELINE METRICS ---");
  console.log(`Total Execution Time: ${totalTimeTaken} ms`);
  console.log(`Requests Per Second (RPS): ${rps}`);
  console.log(`Success Count: ${metrics.successCount} / ${payloadRecords.length}`);
  console.log(`Failed Count: ${metrics.failCount} / ${payloadRecords.length}`);
  if (metrics.minTime !== Infinity) {
    console.log(`Fastest API Fetch: ${metrics.minTime} ms`);
    console.log(`Slowest API Fetch: ${metrics.maxTime} ms`);
  }

  logStream.write(`Finished prediction job iteration in ${totalTimeTaken} ms.\n`);
  
  logStream.end();
  payloadLogStream.end();
  tickLogStream.end();
}

let lastRunDate = null;
async function loop() {
  while (true) {
    const now = new Date();
    const h = now.getHours();
    const m = now.getMinutes();
    const dateStr = now.toDateString();

    // Run ONLY between 09:20 and 09:25 AM, once per day.
    if (h === 9 && m >= 20 && m <= 25 && lastRunDate !== dateStr) {
      console.log(`Time is ${String(h).padStart(2, '0')}:${String(m).padStart(2, '0')}. Running daily prediction cycle for 09:15 candle!`);
      try {
        await main();
        lastRunDate = dateStr;
        console.log("Daily prediction cycle finished successfully.");
      } catch (err) {
        console.error("Error in prediction cycle:", err);
      }
    } else {
      // Just waiting silently
    }

    await new Promise((r) => setTimeout(r, 60000));
  }
}

function startPredictionEngine() {
  console.log("Starting Background Prediction Engine...");
  loop().catch(console.error);
}

module.exports = { startPredictionEngine, main };

if (require.main === module) {
  main().then(() => {
    postgresDb.closeDB();
    process.exit(0);
  }).catch(err => {
    console.error(err);
    process.exit(1);
  });
}
