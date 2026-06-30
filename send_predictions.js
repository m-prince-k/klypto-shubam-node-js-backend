const fs = require("fs");
const path = require("path");
const axios = require("axios");
const postgresDb = require("./db");
const csv = require("csv-parser");
const angelone = require("./angelone-client");
const symbolsList = require("./symbols.js");

// Use the folder the user specified for JSON payloads
const JSON_FOLDER = path.join(__dirname, "extractJson");
const PREDICT_URL = "http://43.205.133.183:8000/predict";
let LOG_FILE = path.join(__dirname, "prediction logs", "predictions_response.log");
let PAYLOAD_LOG = path.join(__dirname, "prediction logs", "prediction_payloads.log");

function logPayload(msg) {
  const timestamp = new Date().toISOString();
  fs.appendFileSync(PAYLOAD_LOG, `[${timestamp}] ${msg}\n`);
}

// Promise wrapper for postgres with API fallback
function getLatestTick(symbol) {
  return new Promise(async (resolve, reject) => {
    try {
      const query = `
        SELECT open, high, low, close, volume, timestamp 
        FROM candles_5m 
        WHERE symbol = $1 
          AND EXTRACT(HOUR FROM timestamp) = 9 
          AND EXTRACT(MINUTE FROM timestamp) = 15
        ORDER BY timestamp DESC LIMIT 1
      `;
      const res = await postgresDb.query(query, [symbol]);
      
      let dbCandle = null;
      if (res.rows && res.rows.length > 0) {
        dbCandle = res.rows[0];
      }

      // Check if the DB candle is actually from today
      const todayStr = new Date().toISOString().split('T')[0];
      let isToday = false;
      if (dbCandle) {
         const d = new Date(dbCandle.timestamp);
         const dbDateStr = `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`;
         if (dbDateStr === todayStr) {
             isToday = true;
         }
      }

      if (isToday) {
        return resolve(dbCandle);
      }

      // FALLBACK: Fetch explicitly from Angel One API for today's 09:15
      console.log(`  [${symbol}] 09:15 DB candle is missing for today. Fetching fallback from Angel One API...`);
      const token = symbolsList[symbol];
      
      const now = new Date();
      const pad = n => String(n).padStart(2, '0');
      const dateStr = `${now.getFullYear()}-${pad(now.getMonth()+1)}-${pad(now.getDate())}`;
      const fromStr = `${dateStr} 09:15`;
      const toStr = `${dateStr} 09:30`; // Wider bound to prevent empty array

      if (token) {
        try {
          const hist = await angelone.fetchHistoricalCandles(symbol, token, "FIVE_MINUTE", fromStr, toStr);
          if (hist && hist.data && hist.data.length > 0) {
            // Find EXACTLY the 09:15 candle
            const candle = hist.data.find(c => {
               const d = new Date(c[0]);
               return d.getHours() === 9 && d.getMinutes() === 15;
            });
            if (candle) {
               console.log(`  [${symbol}] API Fallback fetch successful!`);
               const fallbackCandle = {
                 timestamp: candle[0],
                 open: parseFloat(candle[1]),
                 high: parseFloat(candle[2]),
                 low: parseFloat(candle[3]),
                 close: parseFloat(candle[4]),
                 volume: parseInt(candle[5], 10)
               };
               
               // Backfill it into DB so we have it permanently
               const candleDate = new Date(candle[0]);
               const candleDtStr = `${candleDate.getFullYear()}-${pad(candleDate.getMonth()+1)}-${pad(candleDate.getDate())} ${pad(candleDate.getHours())}:${pad(candleDate.getMinutes())}:00`;
               await postgresDb.query(`
                 INSERT INTO candles_5m (symbol, timestamp, open, high, low, close, volume)
                 VALUES ($1, $2, $3, $4, $5, $6, $7)
                 ON CONFLICT(symbol, timestamp) DO NOTHING
               `, [symbol, candleDtStr, fallbackCandle.open, fallbackCandle.high, fallbackCandle.low, fallbackCandle.close, fallbackCandle.volume]);

               return resolve(fallbackCandle);
            }
          }
        } catch (e) {
            console.log(`  [${symbol}] API Fallback fetch failed: ${e.message}`);
            if (e.message.includes('403') || e.message.includes('429')) {
                // Short wait if we hit rate limits so we don't totally spam
                await new Promise(r => setTimeout(r, 2000));
            }
        }
      }

      // If fallback fails, return the old DB candle (from yesterday) or null
      resolve(dbCandle);
    } catch (err) {
      reject(err);
    }
  });
}

function readJSON(filePath) {
  return new Promise((resolve, reject) => {
    fs.readFile(filePath, "utf-8", (err, data) => {
      if (err) return reject(err);
      try {
        resolve(JSON.parse(data));
      } catch (e) {
        reject(e);
      }
    });
  });
}

async function processSymbol(symbol, historic_data) {
  try {
    console.log(`Processing ${symbol}...`);

    if (!historic_data || historic_data.length === 0) {
      console.log(`  Skipping ${symbol}: JSON data is empty.`);
      return;
    }

    if (historic_data.length > 300) {
      historic_data = historic_data.slice(-300);
    }

    // 2. Fetch live tick from Postgres for 09:15
    const latestTick = await getLatestTick(symbol);

    let tickObj;
    if (latestTick) {
      // If we have an OHLCV candle from the DB, map it perfectly
      tickObj = {
        datetime: postgresDb.formatTimestamp(new Date(latestTick.timestamp)),
        open: latestTick.open,
        high: latestTick.high,
        low: latestTick.low,
        close: latestTick.close,
        volume: latestTick.volume,
      };
    } else {
      // Fallback to last candle in history
      console.log(
        `  No live tick found for ${symbol} in DB. Using last historical candle.`,
      );
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

    // 3. Construct Payload
    const payload = {
      historic_data: historic_data,
      tick: tickObj,
    };

    // Log retrieved tick and payload
    logPayload(
      `[${symbol}] Retrieved tick from DB: ${JSON.stringify(latestTick || "None")}`,
    );
    logPayload(
      `[${symbol}] Sending payload with tickObj: ${JSON.stringify(tickObj)}`,
    );
    
    console.log(`  [${symbol}] Payload being sent (historic_data length: ${historic_data.length}, tick: ${JSON.stringify(tickObj)})`);
    logPayload(`[${symbol}] Full payload being sent: ${JSON.stringify(payload)}`);

    // 4. Send to Predict API with retry
    const maxRetries = 3;
    let response = null;
    let attempt = 0;
    while (attempt < maxRetries) {
      try {
        response = await axios.post(PREDICT_URL, payload, {
          headers: { "Content-Type": "application/json" },
          timeout: 30000,
        });
        break; // Success
      } catch (err) {
        attempt++;
        const errorMsg = err.response ? JSON.stringify(err.response.data) : err.message;
        if (attempt >= maxRetries) {
          throw new Error(`Failed after ${maxRetries} attempts: ${errorMsg}`);
        }
        console.warn(`  [${symbol}] Error sending payload (${errorMsg}), retrying attempt ${attempt}/${maxRetries}...`);
        await new Promise(res => setTimeout(res, 2000)); // wait 2s before retry
      }
    }

    // 5. Log response to Database
    const pool = postgresDb.getDB();
    await pool.query(`
      INSERT INTO prediction_logs (symbol, tick_data, response_data, created_at)
      VALUES ($1, $2, $3, CURRENT_TIMESTAMP)
    `, [symbol, tickObj, response.data]);
    
    // Fallback file log
    const logMsg = `[${symbol}] Payload sent. Tick: ${JSON.stringify(tickObj)} Response: ${JSON.stringify(response.data)}\n`;
    fs.appendFileSync(LOG_FILE, logMsg);
    
    console.log(`  Success. Signal: ${JSON.stringify(response.data)}`);
  } catch (err) {
    const errorMsg = err.response
      ? JSON.stringify(err.response.data)
      : err.message;
    const logErr = `[${symbol}] Error sending payload: ${errorMsg}\n`;
    fs.appendFileSync(LOG_FILE, logErr);
    console.error(`  Error: ${errorMsg}`);
  }
}

async function main() {
  const now = new Date();
  const hours = now.getHours();
  const minutes = now.getMinutes();
  const timeStr = `${hours}${minutes.toString().padStart(2, '0')}`;
  
  LOG_FILE = path.join(__dirname, "prediction logs", `prediction${timeStr}.log`);
  PAYLOAD_LOG = path.join(__dirname, "prediction logs", `prediction_payloads${timeStr}.log`);

  console.log("Starting prediction job...");

  // Fetch data from symbol_payloads table instead of JSON files
  const pool = postgresDb.getDB();
  const res = await pool.query('SELECT symbol, historic_data FROM symbol_payloads');
  const payloadRecords = res.rows;

  console.log(`Found ${payloadRecords.length} payload records in DB.`);

  // Initialize log file
  fs.writeFileSync(
    LOG_FILE,
    `--- Prediction Run at ${new Date().toISOString()} ---\n`,
  );

  // Batch process files for faster execution
  const BATCH_SIZE = 20;
  for (let i = 0; i < payloadRecords.length; i += BATCH_SIZE) {
    const batch = payloadRecords.slice(i, i + BATCH_SIZE);
    await Promise.all(batch.map(record => processSymbol(record.symbol, record.historic_data.historic_data || record.historic_data)));
  }

  console.log("Finished prediction job iteration.");
}

let lastRunDate = null;
async function loop() {
  while (true) {
    const now = new Date();
    const h = now.getHours();
    const m = now.getMinutes();
    const dateStr = now.toDateString();

    // Run ONLY between 09:20 and 09:25 AM, once per day.
    // This prevents it from instantly running if the server is restarted at 4 PM.
    if (h === 9 && m >= 20 && m <= 25 && lastRunDate !== dateStr) {
      console.log(`Time is ${h}:${String(m).padStart(2, '0')}. It is 09:20 AM! Running daily prediction cycle...`);
      try {
        await main();
        lastRunDate = dateStr;
      } catch (err) {
        console.error("Error in prediction cycle:", err);
      }
    } else {
      console.log(`Time is ${h}:${m}. Not time yet or already run today. Skipping execution.`);
    }

    // Sleep for 1 minute
    await new Promise((r) => setTimeout(r, 60000));
  }
}

function startPredictionEngine() {
  console.log("Starting Background Prediction Engine. Waiting for 09:20 every day...");
  loop().catch(console.error);
}

module.exports = { startPredictionEngine, main };
