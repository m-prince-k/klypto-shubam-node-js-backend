const path = require("path");
require("dotenv").config({ path: path.join(__dirname, ".env") });
const axios = require("axios");
const https = require("https");
const authenticator = require("authenticator");

// Create a single keep-alive Axios instance for all Angel One requests
const agent = new https.Agent({ keepAlive: true, maxSockets: 100 });
const api = axios.create({ httpsAgent: agent });
const ANGELONE_CLIENT_ID = process.env.ANGEL_CLIENT_CODE;
const ANGELONE_CLIENT_SECRET = process.env.ANGEL_API_KEY;
const ANGELONE_PASSWORD = process.env.ANGEL_PASSWORD;
const ANGELONE_TOTP = process.env.ANGEL_TOTP_SECRET;

let jwtToken = null;
let jwtTokenExpiry = 0;
let scripMaster = null;
let loginPromise = null; // Mutex to prevent multiple concurrent logins

async function ensureLoggedIn() {
  if (jwtToken && Date.now() < jwtTokenExpiry) return;

  if (loginPromise) {
    await loginPromise;
    return;
  }

  if (!ANGELONE_CLIENT_ID || !ANGELONE_PASSWORD || !ANGELONE_CLIENT_SECRET || !ANGELONE_TOTP) {
    throw new Error("Missing Angel One credentials. Set ANGEL_CLIENT_CODE, ANGEL_PASSWORD, ANGEL_API_KEY, and ANGEL_TOTP_SECRET.");
  }

  loginPromise = (async () => {
    try {

      const totpCode = authenticator.generateToken(ANGELONE_TOTP);
      const loginRes = await api.post(
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
    }
  );

      if (loginRes.data.status && loginRes.data.data) {
        jwtToken = loginRes.data.data.jwtToken;
        jwtTokenExpiry = Date.now() + 23 * 60 * 60 * 1000;
        require('fs').writeFileSync(path.join(__dirname, 'jwt.txt'), jwtToken);
        console.log("=> WROTE JWT TOKEN TO jwt.txt");
      } else {
        throw new Error("Angel One Login failed: " + JSON.stringify(loginRes.data));
      }
    } finally {
      loginPromise = null; // Release the lock
    }
  })();

  await loginPromise;
}

async function getTokenForSymbol(symbol) {
  if (!scripMaster) {
    console.log("[AngelOne] Downloading Scrip Master...");
    try {
      const res = await api.get(
        "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json",
        { timeout: 15000 }
      );
      scripMaster = res.data;
      console.log("[AngelOne] Scrip Master downloaded.");
    } catch (e) {
      throw new Error(`Failed to fetch Scrip Master: ${e.message}`);
    }
  }

  const searchSymbol = `${symbol}-EQ`;
  const found = scripMaster.find(
    (item) => item.symbol === searchSymbol && item.exch_seg === "NSE"
  );

  if (found) {
    console.log(`[AngelOne] Found token ${found.token} for ${symbol}`);
    return found.token;
  }

  throw new Error(`Token NOT FOUND for symbol ${symbol}`);
}

async function fetchLTP(symbol, token) {
  await ensureLoggedIn();

  const response = await api.post(
    "https://apiconnect.angelbroking.com/rest/secure/angelbroking/order/v1/getLtpData",
    {
      exchange: "NSE",
      tradingsymbol: `${symbol}-EQ`,
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
    }
  );

  if (response.data && response.data.data) {
    return response.data.data;
  }
  return null;
}

async function fetchMarketData(symbol, token) {
  await ensureLoggedIn();

  try {
    const response = await api.post(
      "https://apiconnect.angelbroking.com/rest/secure/angelbroking/order/v1/getLtpData",
      {
        exchange: "NSE",
        tradingsymbol: `${symbol}-EQ`,
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
      }
    );

    return {
      status: true,
      data: {
        fetched: response.data && response.data.data ? [response.data.data] : [],
      },
    };
  } catch (error) {
    if (error.response && error.response.status === 401) {
      jwtToken = null;
      jwtTokenExpiry = 0;
    }
    throw error;
  }
}

async function fetchHistoricalCandles(symbol, token, interval, fromdate, todate) {
  await ensureLoggedIn();

  try {
    const response = await api.post(
      "https://apiconnect.angelbroking.com/rest/secure/angelbroking/historical/v1/getCandleData",
      {
        exchange: "NSE",
        symboltoken: token,
        interval: interval || "FIVE_MINUTE",
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
      }
    );

    return response.data;
  } catch (error) {
    if (error.response && error.response.status === 401) {
      jwtToken = null;
      jwtTokenExpiry = 0;
    }
    throw error;
  }
}

async function fetchMarketDataBatch(tokens) {
  await ensureLoggedIn();

  try {
    const response = await api.post(
      "https://apiconnect.angelbroking.com/rest/secure/angelbroking/market/v1/quote",
      {
        mode: "FULL",
        exchangeTokens: {
          NSE: tokens,
        },
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
      }
    );

    return response.data;
  } catch (error) {
    if (error.response && error.response.status === 401) {
      jwtToken = null;
      jwtTokenExpiry = 0;
    }
    throw error;
  }
}

module.exports = {
  getTokenForSymbol,
  fetchLTP,
  fetchMarketData,
  fetchMarketDataBatch,
  fetchHistoricalCandles,
};
