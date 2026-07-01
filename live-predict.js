const path = require("path");
require("dotenv").config({ path: path.join(__dirname, ".env") });

const express = require("express");
const cors = require("cors");
const axios = require("axios");
const fs = require("fs");
const csv = require("csv-parser");
const { generate_payload } = require("./calculate_parameters.js");
const db = require("./db");
const tickCollector = require("./tick-collector");
const angelone = require("./angelone-client");
const symbols = require("./symbols.js");
const { startPredictionEngine } = require("./send_predictions.js");
const { startPostMarketCalculations } = require("./process_calculations.js");

const ANGELONE_LOG = path.join(__dirname, 'angelone_ticks.log');
const DB_LOG = path.join(__dirname, 'db_ticks.log');

function logToFile(file, msg) {
  const timestamp = new Date().toISOString();
  fs.appendFile(file, `[${timestamp}] ${msg}\n`, (err) => {
    if (err) console.error("Log error:", err);
  });
}

const app = express();
app.use(cors());
app.use(express.json({ limit: "50mb" }));
app.use(express.urlencoded({ limit: "50mb", extended: true }));

const PORT = process.env.LIVE_PORT || 3001;
const PREDICT_URL = "http://13.207.78.205:8000/predict";

const tokenCache = {};
const ohlcvCache = {};

function padDate(n) {
  return String(n).padStart(2, "0");
}

function formatTimestamp(date) {
  const d = date instanceof Date ? date : new Date(date);
  if (isNaN(d.getTime())) return String(date);
  return `${d.getFullYear()}-${padDate(d.getMonth() + 1)}-${padDate(d.getDate())} ${padDate(d.getHours())}:${padDate(d.getMinutes())}:${padDate(d.getSeconds())}`;
}

function getFiveMinuteBucketStart(date) {
  const d = new Date(date);
  const mins = Math.floor(d.getMinutes() / 5) * 5;
  d.setMinutes(mins, 0, 0);
  d.setMilliseconds(0);
  return d;
}


function readCSV(filePath, stock_code) {
  return new Promise((resolve, reject) => {
    const data = [];
    fs.createReadStream(filePath)
      .pipe(csv())
      .on("data", (row) => {
        data.push({
          datetime: row.datetime,
          exchange_code: "NSE",
          stock_code: stock_code,
          open: Number(row.open),
          high: Number(row.high),
          low: Number(row.low),
          close: Number(row.close),
          volume: Number(row.volume || 0),
        });
      })
      .on("end", () => resolve(data))
      .on("error", reject);
  });
}

// Option 2: On-demand gap fill and predict endpoint
app.all("/api/predict-ondemand", async (req, res) => {
  try {
    const symbol = (req.query.symbol || (req.body && req.body.symbol) || "BOSCHLTD").toUpperCase();
    const HIST_FOLDER = path.join(__dirname, "historical_csv");
    const csvPath = path.join(HIST_FOLDER, `${symbol}.csv`);

    if (!fs.existsSync(csvPath)) {
      return res.status(404).json({ success: false, error: `CSV for ${symbol} not found.` });
    }

    let rawData = await readCSV(csvPath, symbol);

    if (rawData.length > 0) {
      const lastRow = rawData[rawData.length - 1];
      const d = new Date(lastRow.datetime);
      if (!isNaN(d.getTime())) {
        const pad = n => String(n).padStart(2, "0");
        const fromDateStr = `${d.getFullYear()}-${pad(d.getMonth()+1)}-${pad(d.getDate())} ${d.getHours()}:${pad(d.getMinutes())}`;
        
        const now = new Date();
        const toDateStr = `${now.getFullYear()}-${pad(now.getMonth()+1)}-${pad(now.getDate())} ${now.getHours()}:${pad(now.getMinutes())}`;
        
        if (fromDateStr < toDateStr) {
          try {
            const token = symbols[symbol];
            if (token) {
              const hist = await angelone.fetchHistoricalCandles(symbol, token, "FIVE_MINUTE", fromDateStr, toDateStr);
              if (hist && hist.data) {
                for (const candle of hist.data) {
                  const candleDate = new Date(candle[0]);
                  const candleDtStr = `${candleDate.getFullYear()}-${pad(candleDate.getMonth()+1)}-${pad(candleDate.getDate())} ${pad(candleDate.getHours())}:${pad(candleDate.getMinutes())}:00`;
                  
                  if (candleDate > d) {
                    const newRow = {
                      datetime: candleDtStr,
                      exchange_code: "NSE",
                      stock_code: symbol,
                      open: parseFloat(candle[1]),
                      high: parseFloat(candle[2]),
                      low: parseFloat(candle[3]),
                      close: parseFloat(candle[4]),
                      volume: parseInt(candle[5], 10),
                    };
                    rawData.push(newRow);
                    const csvLine = `${candleDtStr},${newRow.open},${newRow.high},${newRow.low},${newRow.close},${newRow.volume},0,0,0,0,0\n`;
                    fs.appendFileSync(csvPath, csvLine);
                  }
                }
              }
            }
          } catch (e) {
            console.warn(`[OnDemand] Gap fetch failed for ${symbol}:`, e.message);
          }
        }
      }
    }

    const processed = await generate_payload(rawData);
    const last300 = processed.slice(-300);
    const tick = last300[last300.length - 1];

    const payload = {
      historic_data: last300,
      tick: {
        datetime: tick.datetime,
        open: tick.open,
        high: tick.high,
        low: tick.low,
        close: tick.close,
      }
    };

    const targetUrl = req.query.url || (req.body && req.body.url) || PREDICT_URL;
    const response = await axios.post(targetUrl, payload, { timeout: 15000 });

    return res.json({
      success: true,
      symbol: symbol,
      latest_candle: tick.datetime,
      python_response: response.data
    });

  } catch (err) {
    console.error("[PredictOnDemand] Error:", err.message);
    return res.status(500).json({ success: false, error: err.message });
  }
});
// Predict result endpoint
app.get("/api/predictResult", async (req, res) => {
  try {
    const resDB = await db.query(`
      SELECT DISTINCT ON (symbol) symbol, tick_data as tick, response_data as response 
      FROM prediction_logs 
      WHERE DATE(created_at) = CURRENT_DATE
      ORDER BY symbol, created_at DESC
    `);
    
    // Filter to only successful signals
    const results = resDB.rows.filter(r => r.response && r.response.signal !== null && r.response.signal !== undefined);
    
    return res.json({ success: true, data: results, source: 'DB' });
  } catch (err) {
    console.error("Error in /api/predictResult:", err);
    return res.status(500).json({ success: false, error: err.message });
  }
});

// Health check
app.get("/health", (req, res) => {
  res.json({
    status: "ok",
    collectors: tickCollector.getActiveCollectors(),
    uptime: process.uptime(),
  });
});

// Status endpoint
app.get("/api/status", (req, res) => {
  res.json({
    success: true,
    collectors: tickCollector.getActiveCollectors(),
    tokenCache: Object.keys(tokenCache),
  });
});

// Graceful shutdown
process.on("SIGINT", async () => {
  console.log("\n[Server] Shutting down...");
  tickCollector.stopAllCollectors();
  await db.closeDB();
  process.exit(0);
});

process.on("SIGTERM", async () => {
  console.log("\n[Server] Shutting down...");
  tickCollector.stopAllCollectors();
  await db.closeDB();
  process.exit(0);
});

// Start server
async function start() {
  try {
    await db.initDB();
    console.log("[Server] DB initialized");

    // Start global tick collector for all symbols
    startGlobalTickCollector();
    // Start daily prediction engine (runs at 09:20)
    startPredictionEngine();

    // Start post-market calculations engine (runs at 15:45)
    startPostMarketCalculations();

    // Clean old ticks every 30 minutes
    setInterval(() => {
      db.cleanOldTicks(1).catch((err) =>
        console.warn("[Server] Cleanup error:", err.message)
      );
    }, 30 * 60 * 1000);

    // Regular interval check to clear prediction logs at exactly 15:45 (3:45 PM)
    setInterval(() => {
      const now = new Date();
      if (now.getHours() === 15 && now.getMinutes() === 45) {
        try {
          const logsDir = require('path').join(__dirname, "prediction logs");
          if (require('fs').existsSync(logsDir)) {
            const files = require('fs').readdirSync(logsDir);
            let cleared = false;
            for (const file of files) {
              if (file.endsWith(".log") && file !== "predictions_response.log") {
                require('fs').unlinkSync(require('path').join(logsDir, file));
                cleared = true;
              }
            }
            if (cleared) console.log("[Server] ✅ Cleared prediction logs at 3:45 PM.");
          }
        } catch (err) {
          console.error("[Server] ❌ Error clearing prediction logs:", err);
        }
      }
    }, 60 * 1000);

    app.listen(PORT, () => {
      console.log(`[Server] Live Predict server running on port ${PORT}`);
    });
  } catch (err) {
    console.error("[Server] Failed to start:", err);
    process.exit(1);
  }
}

async function startGlobalTickCollector() {
  console.log("[GlobalTickCollector] Initializing tokens for all symbols...");
  const validSymbols = [];
  
  for (const sym of Object.keys(symbols)) {
    try {
      let token = tokenCache[sym] || tokenCache[`${sym}:NSE`];
      if (!token) {
         token = symbols[sym];
         if (token) {
           tokenCache[sym] = token;
           tokenCache[`${sym}:NSE`] = token;
         }
      }
      if (token) {
        validSymbols.push({ symbol: sym, token });
      }
    } catch (e) {
      console.warn(`[GlobalTickCollector] Failed to get token for ${sym}: ${e.message}`);
    }
    // Small delay to prevent rate limiting during initialization
    await new Promise(r => setTimeout(r, 100));
  }

  const CHUNK_SIZE = 50;
  const chunks = [];
  for (let i = 0; i < validSymbols.length; i += CHUNK_SIZE) {
    chunks.push(validSymbols.slice(i, i + CHUNK_SIZE));
  }

  // Global cache to prevent duplicate timestamp entries in CSV
  global.lastWrittenTimestamp = global.lastWrittenTimestamp || {};

  // console.log(`[GlobalTickCollector] Starting batched polling for ${validSymbols.length} symbols in ${chunks.length} chunks...`);

  // Run infinite loop
  (async function loop() {
    while (true) {
      const now = new Date();
      const timeStr = `${padDate(now.getHours())}:${padDate(now.getMinutes())}:${padDate(now.getSeconds())}`;

      // Don't collect before market opens at 09:15
      if (timeStr < "09:15:00") {
        const msUntilOpen = new Date().setHours(9, 15, 0, 0) - Date.now();
        const waitMs = msUntilOpen > 0 ? msUntilOpen : 60000;
        console.log(`[GlobalTickCollector] Market not open yet (${timeStr}). Waiting until 09:15...`);
        await new Promise(r => setTimeout(r, waitMs));
        continue;
      }

      if (timeStr > "15:30:00") {
        console.log("[GlobalTickCollector] Market closed (after 15:30). Sleeping for 1 minute...");
        await new Promise(r => setTimeout(r, 60000));
        continue;
      }

      const currentBucketTime = getFiveMinuteBucketStart(Date.now()).getTime();

      for (const chunk of chunks) {
        try {
          const tokens = chunk.map(item => item.token);
          const response = await angelone.fetchMarketDataBatch(tokens);
          
          if (response && response.status && response.data && response.data.fetched) {
            for (const item of response.data.fetched) {
              const symbolObj = chunk.find(c => c.token === item.symbolToken);
              if (!symbolObj) continue;
              const sym = symbolObj.symbol;
              
              const price = item.ltp;
              const vol = parseInt(item.lastTradeQty, 10) || 0;
              
              if (price == null || isNaN(price)) continue;

              global.liveTicksCache = global.liveTicksCache || {};
              const lastT = global.liveTicksCache[sym];
              if (!lastT || lastT.price !== price || lastT.volume !== vol) {
                global.liveTicksCache[sym] = { price, volume: vol };
                await db.insertTick(sym, price, price, price, price, vol, formatTimestamp(new Date()));
              }

              // Initialize or update the cache
              if (!ohlcvCache[sym] || ohlcvCache[sym].bucketTime !== currentBucketTime) {
                if (ohlcvCache[sym]) {
                  // A 5-minute bucket has just closed. Write it directly to CSV!
                  const oldBucket = ohlcvCache[sym];
                  const csvPath = path.join(__dirname, "historical_csv", `${sym}.csv`);
                  if (fs.existsSync(csvPath)) {
                    const lastWritten = global.lastWrittenTimestamp[sym] || "";
                    if (oldBucket.datetime > lastWritten) {
                      const timePart = oldBucket.datetime.split(' ')[1];
                      if (timePart >= "09:15:00" && timePart <= "15:25:00") {
                        const csvLine = `${oldBucket.datetime},${oldBucket.open},${oldBucket.high},${oldBucket.low},${oldBucket.close},${oldBucket.volume},0,0,0,0,0\n`;
                        fs.appendFileSync(csvPath, csvLine);
                        global.lastWrittenTimestamp[sym] = oldBucket.datetime;
                      }
                    }
                  }
                }
                
                ohlcvCache[sym] = {
                  bucketTime: currentBucketTime,
                  open: price,
                  high: price,
                  low: price,
                  close: price,
                  volume: vol,
                  datetime: formatTimestamp(new Date(currentBucketTime))
                };
              } else {
                const c = ohlcvCache[sym];
                c.high = Math.max(c.high, price);
                c.low = Math.min(c.low, price);
                c.close = price;
                c.volume += vol;
              }
              
              // Continuously UPSERT the aggregated OHLCV into SQLite
              await db.upsertCandle(sym, ohlcvCache[sym].open, ohlcvCache[sym].high, ohlcvCache[sym].low, ohlcvCache[sym].close, ohlcvCache[sym].volume, ohlcvCache[sym].datetime);
            }
          }
        } catch (e) {
          console.warn(`[GlobalTickCollector] Error fetching batch:`, e.message);
        }
        
        // Wait 1000ms between batch requests to reduce Angel One load
        await new Promise(r => setTimeout(r, 1000));
      }
    }
  })();
}

start();

async function startBackgroundGapFiller() {
  console.log("[BackgroundGapFiller] Initializing Startup Run...");
  const HIST_FOLDER = path.join(__dirname, "historical_csv");
  
  async function runGapFillerCycle() {
    console.log(`[BackgroundGapFiller] Starting gap fill cycle at ${new Date().toLocaleTimeString()}`);
    try {
      if (!fs.existsSync(HIST_FOLDER)) {
        console.warn("[BackgroundGapFiller] historical_csv folder not found!");
        return;
      }

      const files = fs.readdirSync(HIST_FOLDER).filter(f => f.endsWith('.csv'));
      let addedCount = 0;

      for (const file of files) {
        const symbol = path.basename(file, '.csv');
        const csvPath = path.join(HIST_FOLDER, file);
        
        try {
          let rawData = await readCSV(csvPath, symbol);
          if (rawData.length > 0) {
            const lastRow = rawData[rawData.length - 1];
            
            // Initialize duplicate protection without DOWNGRADING it
            if (!global.lastWrittenTimestamp) global.lastWrittenTimestamp = {};
            if (!global.lastWrittenTimestamp[symbol] || lastRow.datetime > global.lastWrittenTimestamp[symbol]) {
              global.lastWrittenTimestamp[symbol] = lastRow.datetime;
            }
            
            // Use the safest maximum known timestamp
            const safeLastTime = global.lastWrittenTimestamp[symbol];
            const d = new Date(safeLastTime);
            if (!isNaN(d.getTime())) {
              const pad = n => String(n).padStart(2, "0");
              const fromDateStr = `${d.getFullYear()}-${pad(d.getMonth()+1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}`;
              const now = new Date();
              const toDateStr = `${now.getFullYear()}-${pad(now.getMonth()+1)}-${pad(now.getDate())} ${pad(now.getHours())}:${pad(now.getMinutes())}`;
              
              if (fromDateStr < toDateStr) {
                const token = symbols[symbol];
                if (token) {
                  const hist = await angelone.fetchHistoricalCandles(symbol, token, "FIVE_MINUTE", fromDateStr, toDateStr);
                  if (hist && hist.data) {
                    for (const candle of hist.data) {
                      const candleDate = new Date(candle[0]);
                      const candleDtStr = `${candleDate.getFullYear()}-${pad(candleDate.getMonth()+1)}-${pad(candleDate.getDate())} ${pad(candleDate.getHours())}:${pad(candleDate.getMinutes())}:00`;
                      
                      if (candleDate > d) {
                        const timePart = candleDtStr.split(' ')[1];
                        if (timePart >= "09:15:00" && timePart <= "15:25:00") {
                          const newRow = {
                            datetime: candleDtStr,
                            exchange_code: "NSE",
                            stock_code: symbol,
                            open: parseFloat(candle[1]),
                            high: parseFloat(candle[2]),
                            low: parseFloat(candle[3]),
                            close: parseFloat(candle[4]),
                            volume: parseInt(candle[5], 10),
                          };
                          const csvLine = `${candleDtStr},${newRow.open},${newRow.high},${newRow.low},${newRow.close},${newRow.volume},0,0,0,0,0\n`;
                          fs.appendFileSync(csvPath, csvLine);
                          global.lastWrittenTimestamp[symbol] = candleDtStr;
                          addedCount++;
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        } catch (e) {
          console.warn(`[BackgroundGapFiller] Error processing ${symbol}:`, e.message);
          if (e.message.includes("403") || e.message.includes("429") || (e.response && (e.response.status === 403 || e.response.status === 429))) {
            console.log("[BackgroundGapFiller] API Limit Hit! Sleeping for 5 seconds to cool down...");
            await new Promise(r => setTimeout(r, 5000));
          }
        }
        
        // Wait 2500ms (2.5 seconds) between stocks to strictly respect Angel One's minute-level limits
        await new Promise(r => setTimeout(r, 2500));
      }
      
      console.log(`[BackgroundGapFiller] Cycle completed. Appended ${addedCount} total candles.`);
    } catch (err) {
      console.error(`[BackgroundGapFiller] Cycle error:`, err.message);
    }
  }

  // Run continuously in the background as a safety net to ensure NO entries are missed
  (async function loop() {
    while (true) {
      await runGapFillerCycle();
      
      console.log("[BackgroundGapFiller] Cycle finished. Sleeping for 5 minutes before checking again...");
      // Wait exactly 5 minutes before the next cycle
      await new Promise(r => setTimeout(r, 5 * 60 * 1000));
    }
  })();
}
