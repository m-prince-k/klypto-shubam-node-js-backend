const fs = require("fs");
const path = require("path");
const axios = require("axios");
const postgresDb = require("./db");
const csv = require("csv-parser");

// Use the folder the user specified for JSON payloads
const JSON_FOLDER = path.join(__dirname, "extractJson");
const PREDICT_URL = "http://43.205.133.183:8000/predict";
let LOG_FILE = path.join(__dirname, "prediction logs", "predictions_response.log");
let PAYLOAD_LOG = path.join(__dirname, "prediction logs", "prediction_payloads.log");

function logPayload(msg) {
  const timestamp = new Date().toISOString();
  fs.appendFileSync(PAYLOAD_LOG, `[${timestamp}] ${msg}\n`);
}

// Promise wrapper for postgres
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
      if (res.rows && res.rows.length > 0) {
        resolve(res.rows[0]);
      } else {
        resolve(null);
      }
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

async function processFile(file) {
  const symbol = path.basename(file, ".json");
  const jsonPath = path.join(JSON_FOLDER, file);

  try {
    console.log(`Processing ${symbol}...`);

    // 1. Read historic data from precomputed JSON
    let parsedData = await readJSON(jsonPath);
    let historic_data = parsedData.historic_data || [];
    
    if (historic_data.length === 0) {
      console.log(`  Skipping ${symbol}: JSON data is empty.`);
      return;
    }

    // (Filtering is no longer strictly needed as process_17jun.js handles it, 
    // but we can ensure it's precisely the last 300)
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

    // 5. Log response
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

  // Check if the directory exists
  if (!fs.existsSync(JSON_FOLDER)) {
    console.error(`Directory not found: ${JSON_FOLDER}`);
    return;
  }

  // Postgres connection is handled by db.js pool

  const files = fs.readdirSync(JSON_FOLDER).filter((f) => f.endsWith(".json"));
  console.log(`Found ${files.length} JSON files.`);

  // Initialize log file
  fs.writeFileSync(
    LOG_FILE,
    `--- Prediction Run at ${new Date().toISOString()} ---\n`,
  );

  // Batch process files for faster execution
  const BATCH_SIZE = 20;
  for (let i = 0; i < files.length; i += BATCH_SIZE) {
    const batch = files.slice(i, i + BATCH_SIZE);
    await Promise.all(batch.map(file => processFile(file)));
  }

  postgresDb.closeDB().then(() => {
    console.log("Finished prediction job iteration.");
  }).catch(err => {
    console.error("Error closing DB:", err);
  });
}

async function loop() {
  while (true) {
    const now = new Date();
    const h = now.getHours();
    const m = now.getMinutes();

    // Run exactly at 09:20
    if (h === 9 && m === 20) {
      console.log(`Time is ${h}:${m}, exactly 09:20. Running prediction cycle...`);
      await main();
    } else {
      console.log(`Time is ${h}:${m}. Not 09:20. Skipping execution.`);
    }

    const nextNow = new Date();
    const msUntilNext5Min = (5 * 60 * 1000) - (nextNow.getTime() % (5 * 60 * 1000));
    console.log(`Waiting ${Math.round(msUntilNext5Min / 1000)} seconds before next prediction cycle...`);
    await new Promise((r) => setTimeout(r, msUntilNext5Min));
  }
}

if (process.argv.includes('--once')) {
  main().then(() => process.exit(0));
} else {
  loop();
}
