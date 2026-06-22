require("dotenv").config();

const axios = require("axios");
const express = require("express");
const fs = require("fs");
const path = require("path");
const authenticator = require("authenticator");
const cors = require("cors");
const { generate_payload } = require("./calculate_parameters.js");
const db = require("./db");
const tickCollector = require("./tick-collector");
const { startPredictionEngine } = require("./send_predictions.js");

const app = express();
app.use(cors());
app.options(/.*/, cors());

const HOST = process.env.HOST || "0.0.0.0";
const PORT = Number(process.env.PORT || 3000);
const startupState = {
  databaseReady: false,
  startupError: null,
};

// We no longer rely exclusively on the static predictData.json file.
// We will generate the historical data dynamically using calculate_parameters.js generate_payload.
let historicalData = { historic_data: [], tick: {} };

const ANGELONE_CLIENT_ID = process.env.ANGEL_CLIENT_CODE;
const ANGELONE_CLIENT_SECRET = process.env.ANGEL_API_KEY;
const ANGELONE_PASSWORD = process.env.ANGEL_PASSWORD;
const ANGELONE_TOTP = process.env.ANGEL_TOTP_SECRET;
const ANGELONE_API_URL =
  process.env.ANGELONE_API_URL || "https://api.angelone.in";
const ANGELONE_LTP_URL =
  process.env.ANGELONE_LTP_URL || `${ANGELONE_API_URL}/rest/secure/marketdata`;
const ANGELONE_BOSCHLTD_TOKEN = process.env.ANGELONE_BOSCHLTD_TOKEN;

const marketStore = {
  symbolToTokenMaster: {
    BOSCHLTD: ANGELONE_BOSCHLTD_TOKEN,
    "BOSCHLTD:NSE": ANGELONE_BOSCHLTD_TOKEN,
  },
  latestMarketData: {},
};

let jwtToken = null;
let jwtTokenExpiry = 0;

const csvCache = {}; // Cache for parsed CSV data

function pad2(value) {
  return String(value).padStart(2, "0");
}

function formatDateTime(date) {
  const d = date instanceof Date ? date : new Date(date);
  return `${d.getFullYear()}-${pad2(d.getMonth() + 1)}-${pad2(d.getDate())} ${pad2(d.getHours())}:${pad2(d.getMinutes())}:${pad2(d.getSeconds())}`;
}

function getFiveMinuteBucketStart(date) {
  const d = new Date(date);
  d.setSeconds(0, 0);
  d.setMinutes(Math.floor(d.getMinutes() / 5) * 5);
  return d;
}

async function loadHistoricalFromCsv(symbol) {
  if (csvCache[symbol]) {
    return csvCache[symbol];
  }

  const csvPath = path.join(__dirname, "historical_csv", `${symbol}.csv`);
  if (!fs.existsSync(csvPath)) {
    return [];
  }

  const formattedHistorical = [];
  const csvContent = fs.readFileSync(csvPath, "utf-8");
  const lines = csvContent
    .split("\n")
    .filter((line) => line.trim().length > 0);
  const normalizeCsvValue = (value) => value.trim().replaceAll('"', "");

  for (let i = 1; i < lines.length; i++) {
    const cols = lines[i].split(",");
    if (cols.length >= 6) {
      formattedHistorical.push({
        datetime: normalizeCsvValue(cols[0]),
        exchange_code: "NSE",
        stock_code: symbol,
        open: parseFloat(normalizeCsvValue(cols[1])),
        high: parseFloat(normalizeCsvValue(cols[2])),
        low: parseFloat(normalizeCsvValue(cols[3])),
        close: parseFloat(normalizeCsvValue(cols[4])),
        volume: parseInt(normalizeCsvValue(cols[5]), 10),
      });
    }
  }

  csvCache[symbol] = formattedHistorical;
  return formattedHistorical;
}

async function loadHistoricalFromAngelOne(symbol, token, interval) {
  const now = new Date();
  const from = new Date(now);
  const lookbackDays = Number(process.env.HISTORY_LOOKBACK_DAYS || 6);

  from.setDate(from.getDate() - lookbackDays);
  from.setHours(9, 15, 0, 0);

  const fromdate = `${from.getFullYear()}-${pad2(from.getMonth() + 1)}-${pad2(from.getDate())} 09:15`;
  const todate = `${now.getFullYear()}-${pad2(now.getMonth() + 1)}-${pad2(now.getDate())} ${pad2(now.getHours())}:${pad2(now.getMinutes())}`;

  const historicalRes = await smartApi.getCandleData({
    exchange: "NSE",
    symboltoken: token,
    interval,
    fromdate,
    todate,
  });

  const currentBucketStart = formatDateTime(getFiveMinuteBucketStart(now));
  const candles = Array.isArray(historicalRes?.data) ? historicalRes.data : [];

  return candles
    .map((candle) => ({
      datetime: formatDateTime(new Date(candle[0])),
      exchange_code: "NSE",
      stock_code: symbol,
      open: parseFloat(candle[1]),
      high: parseFloat(candle[2]),
      low: parseFloat(candle[3]),
      close: parseFloat(candle[4]),
      volume: parseInt(candle[5], 10),
    }))
    .filter((row) => row.datetime < currentBucketStart);
}

async function loadHistoricalData(symbol, token, interval) {
  try {
    const csvData = await loadHistoricalFromCsv(symbol);
    if (csvData.length > 0) {
      return csvData;
    }
  } catch (err) {
    console.warn(`[History] CSV load failed for ${symbol}:`, err.message);
  }

  console.warn(
    `[History] CSV missing for ${symbol}. Falling back to Angel One historical candles.`,
  );

  try {
    const marketHistory = await loadHistoricalFromAngelOne(symbol, token, interval);
    console.log(
      `[History] Loaded ${marketHistory.length} fallback candles from Angel One for ${symbol}.`,
    );
    return marketHistory;
  } catch (err) {
    console.warn(`[History] Angel One fallback failed for ${symbol}:`, err.message);
    return [];
  }
}

let scripMaster = null;
async function getTokenForSymbol(symbol) {
  if (marketStore.symbolToTokenMaster[symbol]) {
    return marketStore.symbolToTokenMaster[symbol];
  }
  if (marketStore.symbolToTokenMaster[`${symbol}:NSE`]) {
    return marketStore.symbolToTokenMaster[`${symbol}:NSE`];
  }

  if (!scripMaster) {
    try {
      console.log("Downloading Angel One Scrip Master...");
      const res = await axios.get(
        "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json",
      );
      scripMaster = res.data;
      console.log("Scrip Master downloaded.");
    } catch (e) {
      console.error("Failed to fetch Scrip Master", e.message);
      return null;
    }
  }

  const searchSymbol = `${symbol}-EQ`;
  const found = scripMaster.find(
    (item) => item.symbol === searchSymbol && item.exch_seg === "NSE",
  );
  if (found) {
    console.log(
      `[getTokenForSymbol] Found token ${found.token} for symbol ${symbol}`,
    );
    marketStore.symbolToTokenMaster[symbol] = found.token;
    return found.token;
  }
  console.log(`[getTokenForSymbol] Token NOT FOUND for symbol ${symbol}`);
  return null;
}

const smartApi = {
  marketData: async ({ mode, exchangeTokens, tradingsymbol }) => {
    if (
      !ANGELONE_CLIENT_ID ||
      !ANGELONE_PASSWORD ||
      !ANGELONE_CLIENT_SECRET ||
      !ANGELONE_TOTP
    ) {
      throw new Error(
        "Missing Angel One credentials. Set ANGEL_CLIENT_CODE, ANGEL_PASSWORD, ANGEL_API_KEY, and ANGEL_TOTP_SECRET.",
      );
    }

    if (!jwtToken || Date.now() > jwtTokenExpiry) {
      const totpCode = authenticator.generateToken(ANGELONE_TOTP);
      const loginRes = await axios.post(
        "https://apiconnect.angelbroking.com/rest/auth/angelbroking/user/v1/loginByPassword",
        {
          clientcode: ANGELONE_CLIENT_ID,
          password: ANGELONE_PASSWORD,
          totp: totpCode,
        },
        {
          headers: {
            "Content-Type": "application/json",
            Accept: "application/json",
            "X-UserType": "USER",
            "X-SourceID": "WEB",
            "X-ClientLocalIP": "127.0.0.1",
            "X-ClientPublicIP": "127.0.0.1",
            "X-MACAddress": "01-01-01-01-01-01",
            "X-PrivateKey": ANGELONE_CLIENT_SECRET,
          },
        },
      );
      if (loginRes.data.status && loginRes.data.data) {
        jwtToken = loginRes.data.data.jwtToken;
        jwtTokenExpiry = Date.now() + 23 * 60 * 60 * 1000;
      } else {
        throw new Error(
          "Angel One Login failed: " + JSON.stringify(loginRes.data),
        );
      }
    }

    const token = exchangeTokens.NSE ? exchangeTokens.NSE[0] : null;

    const response = await axios.post(
      "https://apiconnect.angelbroking.com/rest/secure/angelbroking/order/v1/getLtpData",
      {
        exchange: "NSE",
        tradingsymbol: tradingsymbol || "BOSCHLTD-EQ",
        symboltoken: token,
      },
      {
        headers: {
          "Content-Type": "application/json",
          Accept: "application/json",
          "X-UserType": "USER",
          "X-SourceID": "WEB",
          "X-ClientLocalIP": "127.0.0.1",
          "X-ClientPublicIP": "127.0.0.1",
          "X-MACAddress": "01-01-01-01-01-01",
          "X-PrivateKey": ANGELONE_CLIENT_SECRET,
          Authorization: `Bearer ${jwtToken}`,
        },
        timeout: 25000,
      },
    );

    // Format the response to match what the code expects (resp.data.fetched[0])
    return {
      status: true,
      data: {
        fetched: response.data.data ? [response.data.data] : [],
      },
    };
  },

  getCandleData: async ({
    exchange,
    symboltoken,
    interval,
    fromdate,
    todate,
  }) => {
    if (!jwtToken || Date.now() > jwtTokenExpiry) {
      await smartApi.marketData({ mode: "LTP", exchangeTokens: { NSE: [] } }); // triggers login
    }
    const response = await axios.post(
      "https://apiconnect.angelbroking.com/rest/secure/angelbroking/historical/v1/getCandleData",
      {
        exchange: exchange || "NSE",
        symboltoken,
        interval,
        fromdate,
        todate,
      },
      {
        headers: {
          "Content-Type": "application/json",
          Accept: "application/json",
          "X-UserType": "USER",
          "X-SourceID": "WEB",
          "X-ClientLocalIP": "127.0.0.1",
          "X-ClientPublicIP": "127.0.0.1",
          "X-MACAddress": "01-01-01-01-01-01",
          "X-PrivateKey": ANGELONE_CLIENT_SECRET,
          Authorization: `Bearer ${jwtToken}`,
        },
        timeout: 25000,
      },
    );
    return response.data;
  },
};

app.use(express.json({ limit: "50mb" }));
app.use(express.urlencoded({ limit: "50mb", extended: true }));

app.get("/", (req, res) => {
  res.send("Klypto Shubham backend is running");
});

app.get("/health", (req, res) => {
  res.status(200).json({
    status: startupState.startupError
      ? "degraded"
      : startupState.databaseReady
        ? "ready"
        : "starting",
    databaseReady: startupState.databaseReady,
    startupError: startupState.startupError,
    collectors: tickCollector.getActiveCollectors(),
    uptime: process.uptime(),
  });
});

app.get("/ready", (req, res) => {
  const isReady = startupState.databaseReady && !startupState.startupError;

  res.status(isReady ? 200 : 503).json({
    status: isReady ? "ready" : "not-ready",
    databaseReady: startupState.databaseReady,
    startupError: startupState.startupError,
  });
});

app.post("/api/strategy/predict", async (req, res) => {
  await forwardToPredict(req, res);
});

app.get("/api/strategy/predict", async (req, res) => {
  await forwardToPredict(req, res);
});

const symbolsList = require("./symbols.js");

const forwardToPredict = async (req, res) => {
  try {
    const targetUrl =
      req.body?.url || req.query.url || "http://43.205.133.183:8000/predict";
    const symbol = (
      req.body?.symbol ||
      req.query.symbol ||
      "BOSLIM"
    ).toUpperCase();
    const interval = (
      req.body?.interval ||
      req.query.interval ||
      "FIVE_MINUTE"
    ).toUpperCase();
    const limitParam = req.body?.limit || req.query.limit;
    const limit = limitParam ? parseInt(limitParam) : null;

    let token =
      marketStore.symbolToTokenMaster &&
      (marketStore.symbolToTokenMaster[symbol] ||
        marketStore.symbolToTokenMaster[`${symbol}:NSE`]);

    // Dynamically fetch from Scrip Master if not in marketStore
    if (!token) {
      token = await getTokenForSymbol(symbol);
      if (token) {
        if (!marketStore.symbolToTokenMaster)
          marketStore.symbolToTokenMaster = {};
        marketStore.symbolToTokenMaster[symbol] = token;
        marketStore.symbolToTokenMaster[`${symbol}:NSE`] = token;
      }
    }

    if (!token) {
      return res.status(400).json({
        success: false,
        error: `Symbol ${symbol} not found in master`,
      });
    }

    const formattedHistorical = await loadHistoricalData(symbol, token, interval);

    // Generate indicators
    let boslim = await generate_payload(formattedHistorical);
    if (limit && boslim.length > limit) {
      boslim = boslim.slice(-limit);
    }

    // Fetch live tick
    let tick = null;
    try {
      const resp = await smartApi.marketData({
        mode: "FULL",
        exchangeTokens: { NSE: [token] },
      });
      if (
        resp &&
        resp.data &&
        resp.data.fetched &&
        resp.data.fetched.length > 0
      ) {
        tick = resp.data.fetched[0];
      }
    } catch (e) {
      console.warn(
        "[Strategy.forwardToPredict] Angel One LTP fetch failed for " +
          symbol +
          ":",
        e.message,
      );
    }

    if (!tick) {
      tick =
        (marketStore.latestMarketData &&
          (marketStore.latestMarketData[symbol] ||
            marketStore.latestMarketData[`${symbol}:NSE`])) ||
        {};
    }

    // Fallback if tick is missing
    if (!tick.close && formattedHistorical.length > 0) {
      const lastCandle = formattedHistorical[formattedHistorical.length - 1];
      tick = {
        low: lastCandle.low,
        high: lastCandle.high,
        open: lastCandle.open,
        close: lastCandle.close,
        exchangeTime: lastCandle.datetime,
      };
    }

    const pad2 = (n) => String(n).padStart(2, "0");
    const now = new Date();
    const formattedNow = `${now.getFullYear()}-${pad2(now.getMonth() + 1)}-${pad2(now.getDate())} ${pad2(now.getHours())}:${pad2(now.getMinutes())}:${pad2(now.getSeconds())}`;

    const filterTick = {
      low: tick.low,
      high: tick.high,
      open: tick.open,
      close: tick.close,
      datetime:
        tick.exchangeTime ||
        tick.lastTradeTime ||
        tick.exchTradeTime ||
        formattedNow,
    };

    console.log(filterTick, "Filter Tick");
    const payload = {
      historic_data: boslim,
      tick: filterTick,
    };

    try {
      const response = await axios.post(targetUrl, payload, {
        headers: { "Content-Type": "application/json" },
        timeout: 15000,
      });

      console.log(response?.data, "_____---878987");

      // Save payload ONLY if a signal is found (no 'reason' returned)
      if (response?.data && !response.data.reason) {
        try {
          const payloadFile = require("path").join(
            __dirname,
            `payload_${symbol}.json`,
          );
          require("fs").writeFileSync(
            payloadFile,
            JSON.stringify(payload, null, 2),
          );
          console.log(
            `[Signal Found] Saved payload for ${symbol} to ${payloadFile}`,
          );
        } catch (saveErr) {
          console.warn(
            `Could not save payload for ${symbol} to file:`,
            saveErr.message,
          );
        }
      }

      return res.json(response?.data);
    } catch (error) {
      const pythonError = error.response ? error.response.data : error.message;
      console.log("Error from Python API:", pythonError);
      return res.status(500).json({ success: false, error: pythonError });
    }
  } catch (err) {
    console.error("[Strategy.forwardToPredict] Error:", err.message);
    return res.status(500).json({ success: false, error: err.message });
  }
};

async function fetchLiveTick(symbol, reqToken) {
  const tickSymbol = (symbol || "BOSCHLTD").toUpperCase();
  let tick = null;

  try {
    const token = reqToken || (await getTokenForSymbol(tickSymbol));

    if (token && smartApi && typeof smartApi.marketData === "function") {
      const resp = await smartApi.marketData({
        mode: "LTP",
        exchangeTokens: { NSE: [token] },
        tradingsymbol: `${tickSymbol}-EQ`,
      });

      if (resp && resp.fetched && resp.fetched.length > 0) {
        tick = resp.fetched[0];
      } else if (
        resp &&
        resp.data &&
        resp.data.fetched &&
        resp.data.fetched.length > 0
      ) {
        tick = resp.data.fetched[0];
      } else {
        tick = resp;
      }
    }
  } catch (e) {
    console.warn(
      "[Strategy.fetchBOSCHLTDLiveTick] Angel One LTP fetch failed for BOSCHLTD:",
      e.message,
    );
  }

  if (!tick || (typeof tick === "object" && Object.keys(tick).length === 0)) {
    tick =
      (marketStore.latestMarketData &&
        (marketStore.latestMarketData[tickSymbol] ||
          marketStore.latestMarketData[`${tickSymbol}:NSE`])) ||
      {};
  }

  return tick;
}

// Mount the TCS endpoint
// require('./tcs_endpoint')(app, marketStore, smartApi, getTokenForSymbol);

app.get("/api/predictResult", async (req, res) => {
  try {
    const logsDir = path.join(__dirname, "prediction logs");
    if (!fs.existsSync(logsDir)) {
      return res.json({
        success: true,
        data: [],
        signals: [],
        totalEntries: 0,
        signalEntries: 0,
        logFile: null,
        message: "Prediction log directory not found on this server.",
      });
    }

    const files = fs
      .readdirSync(logsDir)
      .filter(
        (f) =>
          f.startsWith("prediction") &&
          f.endsWith(".log") &&
          !f.startsWith("prediction_payloads") &&
          f !== "predictions_response.log",
      );

    if (files.length === 0) {
      return res.json({
        success: true,
        data: [],
        signals: [],
        totalEntries: 0,
        signalEntries: 0,
        logFile: null,
        message: "No prediction log files found yet.",
      });
    }

    // Get the most recently modified file
    let latestFile = null;
    let maxTime = 0;
    for (const f of files) {
      const stats = fs.statSync(path.join(logsDir, f));
      if (stats.mtime.getTime() > maxTime) {
        maxTime = stats.mtime.getTime();
        latestFile = f;
      }
    }

    if (!latestFile) {
      return res.json({
        success: true,
        data: [],
        signals: [],
        totalEntries: 0,
        signalEntries: 0,
        logFile: null,
        message: "Prediction log files were found, but no latest file could be selected.",
      });
    }

    const filePath = path.join(logsDir, latestFile);
    const content = fs.readFileSync(filePath, "utf-8");
    const lines = content.split("\n");

    const allEntries = [];
    const signalEntries = [];
    let parsedLines = 0;
    let ignoredLines = 0;
    const signalOnly = req.query.onlySignals === "true";

    for (const line of lines) {
      if (!line.trim()) continue;
      // Match the pattern: [SYMBOL] Payload sent. Tick: {tickObj} Response: {responseData}
      const match = line.match(
        /^\[(.*?)\] Payload sent\. Tick: (.*?) Response: (.*)$/,
      );
      if (match) {
        const symbol = match[1];
        try {
          const tick = JSON.parse(match[2]);
          const response = JSON.parse(match[3]);
          parsedLines += 1;

          const entry = {
            symbol,
            tick,
            response,
            hasSignal:
              response &&
              response.signal !== null &&
              response.signal !== undefined,
          };

          allEntries.push(entry);

          if (entry.hasSignal) {
            signalEntries.push(entry);
          }
        } catch (e) {
          ignoredLines += 1;
        }
      } else {
        ignoredLines += 1;
      }
    }

    return res.json({
      success: true,
      data: signalOnly ? signalEntries : allEntries,
      signals: signalEntries,
      totalEntries: allEntries.length,
      signalEntries: signalEntries.length,
      parsedLines,
      ignoredLines,
      showing: signalOnly ? "signals" : "all",
      logFile: latestFile,
      message:
        allEntries.length === 0
          ? "Prediction log file exists, but no entries could be parsed."
          : signalEntries.length === 0
            ? "Entries were found, but none contained a trade signal."
            : undefined,
    });
  } catch (err) {
    console.error("Error in /api/predictResult:", err);
    return res.status(500).json({ success: false, error: err.message });
  }
});

function startHttpServer() {
  app.listen(PORT, HOST, () => {
    console.log(`Express server listening at http://${HOST}:${PORT}`);
    console.log(`Health endpoint available at http://${HOST}:${PORT}/health`);
  });
}

async function bootstrapBackground() {
  if (!db.isConfigured()) {
    console.warn(
      "[Startup] PostgreSQL environment variables are missing. Starting HTTP server without database initialization.",
    );
    if (process.env.ENABLE_PREDICTION_ENGINE !== "false") {
      console.log("[Startup] Starting prediction engine without PostgreSQL.");
      startPredictionEngine();
    }
    return;
  }

  try {
    console.log("[Startup] Initializing PostgreSQL in background...");
    await db.initDB();
    startupState.databaseReady = true;
    console.log("✅ Successfully connected to PostgreSQL Database!");
  } catch (err) {
    startupState.startupError = err.message;
    console.error("❌ Failed to connect to PostgreSQL Database:", err.message);
  }

  if (process.env.ENABLE_PREDICTION_ENGINE !== "false") {
    console.log("[Startup] Starting prediction engine for this server.");
    startPredictionEngine();
  }
}

startHttpServer();
bootstrapBackground();
