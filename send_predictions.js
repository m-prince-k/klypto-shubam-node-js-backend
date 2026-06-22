const fs = require("fs");
const path = require("path");
const axios = require("axios");
const postgresDb = require("./db");
const csv = require("csv-parser");
const { performCalculations } = require("./process_calculations.js");

// Use the folder the user specified for JSON payloads
const JSON_FOLDER = path.join(__dirname, "extractJson");
const PREDICT_URL = "http://43.205.133.183:8000/predict";
const LOG_DIR = path.join(__dirname, "prediction logs");
let LOG_FILE = path.join(LOG_DIR, "predictions_response.log");
let PAYLOAD_LOG = path.join(LOG_DIR, "prediction_payloads.log");
let predictionEngineStarted = false;

function ensureLogDir() {
  if (!fs.existsSync(LOG_DIR)) {
    fs.mkdirSync(LOG_DIR, { recursive: true });
  }
}

function logPayload(msg) {
  ensureLogDir();
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
          AND DATE(timestamp) = CURRENT_DATE
        ORDER BY timestamp DESC LIMIT 1
      `;
      const res = await postgresDb.query(query, [symbol]);
      if (res && res.rows && res.rows.length > 0) {
        resolve(res.rows[0]);
      } else {
        resolve(null);
      }
    } catch (err) {
      console.warn(`[Prediction] Could not read live tick for ${symbol}: ${err.message}`);
      resolve(null);
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
  
  LOG_FILE = path.join(LOG_DIR, `prediction${timeStr}.log`);
  PAYLOAD_LOG = path.join(LOG_DIR, `prediction_payloads${timeStr}.log`);

  console.log("Starting prediction job...");
  ensureLogDir();

  if (!fs.existsSync(JSON_FOLDER)) {
    fs.mkdirSync(JSON_FOLDER, { recursive: true });
  }

  let files = fs.readdirSync(JSON_FOLDER).filter((f) => f.endsWith(".json"));
  if (files.length === 0) {
    console.warn(
      `[Prediction] ${JSON_FOLDER} is empty. Generating prediction inputs from historical CSV files...`,
    );

    try {
      await performCalculations();
    } catch (err) {
      console.error(
        `[Prediction] Failed to generate extractJson data: ${err.message}`,
      );
    }

    files = fs.readdirSync(JSON_FOLDER).filter((f) => f.endsWith(".json"));
  }

  if (files.length === 0) {
    console.warn(
      `[Prediction] No JSON files available in ${JSON_FOLDER}. Skipping this prediction cycle.`,
    );
    return;
  }

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

  console.log("Finished prediction job iteration.");
}

const LAST_RUN_FILE = path.join(__dirname, 'last_prediction_date.txt');
let lastRunDate = null;
if (fs.existsSync(LAST_RUN_FILE)) {
  lastRunDate = fs.readFileSync(LAST_RUN_FILE, 'utf8').trim();
}

async function loop() {
  while (true) {
    const now = new Date();
    const h = now.getHours();
    const m = now.getMinutes();
    const dateStr = now.toDateString();

    // Run at or after 09:20, once per day
    if ((h > 9 || (h === 9 && m >= 20)) && lastRunDate !== dateStr) {
      console.log(`Time is ${h}:${m}, >= 09:20. Running prediction cycle for today...`);
      try {
        await main();
        lastRunDate = dateStr;
        fs.writeFileSync(LAST_RUN_FILE, dateStr);
      } catch (err) {
        console.error("Error in prediction cycle:", err);
      }
    } else {
      // console.log(`Time is ${h}:${m}. Not time yet or already run today. Skipping execution.`);
    }

    // Sleep for 1 minute
    await new Promise((r) => setTimeout(r, 60000));
  }
}

function startPredictionEngine() {
  if (predictionEngineStarted) {
    return;
  }

  predictionEngineStarted = true;
  console.log("Starting Background Prediction Engine. Waiting for 09:20 every day...");
  loop().catch(console.error);
}

module.exports = { startPredictionEngine, main };
