const fs = require("fs");
const path = require("path");
const csv = require("csv-parser");

async function loadBOSLIM() {
  return new Promise((resolve, reject) => {
    const candles = [];
    // resolve boslim.csv relative to this file so requires from elsewhere still work
    const boslimPath = path.join(__dirname, "..", "boslim.csv");

    fs.createReadStream(boslimPath)
      .pipe(csv())
      .on("data", (row) => {
        candles.push({
          datetime: row.datetime,

          exchange_code: row.exchange_code,
          stock_code: row.stock_code,

          open: Number(row.open),
          high: Number(row.high),
          low: Number(row.low),
          close: Number(row.close),

          volume: Number(row.volume || 0),
        });
      })
      .on("end", () => {
        resolve(candles);
      })
      .on("error", reject);
  });
}

// ======================================
// SMA
// ======================================

function rollingMean(arr, period, minPeriods = period) {
  const result = new Array(arr.length).fill(NaN);

  for (let i = 0; i < arr.length; i++) {
    const start = Math.max(0, i - period + 1);
    const slice = arr.slice(start, i + 1);
    const valid = slice.filter((x) => !Number.isNaN(x));

    if (valid.length === 0) continue;

    if (valid.length >= minPeriods) {
      const sum = valid.reduce((a, b) => a + b, 0);
      result[i] = sum / valid.length;
    }
  }

  return result;
}

function computeSMA(df) {
  const closes = df.map((x) => x.close);

  const sma20 = rollingMean(closes, 20);
  const sma50 = rollingMean(closes, 50);
  // allow SMA_100 and SMA_200 to be calculated using available data
  // (so early rows won't remain NaN)
  const sma100 = rollingMean(closes, 100, 1);
  const sma200 = rollingMean(closes, 200, 1);

  for (let i = 0; i < df.length; i++) {
    df[i].SMA_20 = sma20[i];
    df[i].SMA_50 = sma50[i];
    df[i].SMA_100 = sma100[i];
    df[i].SMA_200 = sma200[i];
  }

  return df;
}

// ======================================
// RSI HELPERS
// ======================================

function rollingMeanMin(arr, period, minPeriods) {
  const result = new Array(arr.length).fill(NaN);

  for (let i = 0; i < arr.length; i++) {
    const count = i + 1;

    if (count < minPeriods) {
      continue;
    }

    const start = Math.max(0, i - period + 1);

    const slice = arr.slice(start, i + 1);

    const valid = slice.filter((x) => !Number.isNaN(x));

    if (valid.length === 0) {
      continue;
    }

    result[i] = valid.reduce((a, b) => a + b, 0) / valid.length;
  }

  return result;
}

// ======================================
// RSI (WILDER)
// ======================================

function computeRSI(df) {
  // Price Change
  for (let i = 0; i < df.length; i++) {
    if (i === 0) {
      df[i].Price_change = NaN;
      continue;
    }

    df[i].Price_change = df[i].close - df[i - 1].close;
  }

  // Gain / Loss
  for (let i = 0; i < df.length; i++) {
    const pc = df[i].Price_change;

    if (Number.isNaN(pc)) {
      df[i].Gain = NaN;
      df[i].Loss = NaN;

      continue;
    }

    df[i].Gain = Math.max(pc, 0);

    df[i].Loss = Math.max(-pc, 0);
  }

  const gains = df.map((x) => x.Gain);

  const losses = df.map((x) => x.Loss);

  // Initial Avg_Gain/Loss
  const avgGain = rollingMeanMin(gains, 14, 13);

  const avgLoss = rollingMeanMin(losses, 14, 13);

  for (let i = 0; i < df.length; i++) {
    df[i].Avg_Gain = avgGain[i];

    df[i].Avg_Loss = avgLoss[i];

    df[i].RMA_Gain = NaN;
    df[i].RMA_Loss = NaN;
  }

  // Wilder RMA
  if (df.length > 14) {
    df[14].RMA_Gain = (avgGain[13] * 13 + df[14].Gain) / 14;

    df[14].RMA_Loss = (avgLoss[13] * 13 + df[14].Loss) / 14;

    for (let i = 15; i < df.length; i++) {
      df[i].RMA_Gain = (df[i - 1].RMA_Gain * 13 + df[i].Gain) / 14;

      df[i].RMA_Loss = (df[i - 1].RMA_Loss * 13 + df[i].Loss) / 14;
    }
  }

  // RS + RSI
  for (let i = 0; i < df.length; i++) {
    df[i].RS = df[i].RMA_Gain / df[i].RMA_Loss;

    df[i].RSI = Number.isNaN(df[i].RS) ? NaN : 100 - 100 / (1 + df[i].RS);
  }

  return df;
}

// ======================================
// EMA
// ======================================

function EMA(series, period) {
  const result = new Array(series.length).fill(NaN);

  if (series.length === 0) {
    return result;
  }

  const alpha = 2 / (period + 1);

  result[0] = series[0];

  for (let i = 1; i < series.length; i++) {
    result[i] = alpha * series[i] + (1 - alpha) * result[i - 1];
  }

  return result;
}

// ======================================
// WMA
// ======================================

function WMA(series, period) {
  const result = new Array(series.length).fill(NaN);

  const weightSum = (period * (period + 1)) / 2;

  for (let i = period - 1; i < series.length; i++) {
    let weighted = 0;
    let weight = 1;

    for (let j = i - period + 1; j <= i; j++) {
      weighted += series[j] * weight;

      weight++;
    }

    result[i] = weighted / weightSum;
  }

  return result;
}

// ======================================
// SMA SERIES
// ======================================

function SMA(series, period) {
  return rollingMean(series, period);
}

// ======================================
// HMA
// Python:
//
// half = int(period / 2)
// sqrt_len = int(np.sqrt(period))
//
// wma1 = WMA(series, half)
// wma2 = WMA(series, period)
//
// return WMA(
//      2*wma1 - wma2,
//      sqrt_len
// )
// ======================================

function HMA(series, period) {
  const half = Math.floor(period / 2);

  const sqrtLen = Math.floor(Math.sqrt(period));

  const wma1 = WMA(series, half);

  const wma2 = WMA(series, period);

  const diff = new Array(series.length).fill(NaN);

  for (let i = 0; i < series.length; i++) {
    if (Number.isNaN(wma1[i]) || Number.isNaN(wma2[i])) {
      continue;
    }

    diff[i] = 2 * wma1[i] - wma2[i];
  }

  return WMA(diff, sqrtLen);
}

// ======================================
// GENERIC MA
// ======================================

function computeMA(series, maType, length) {
  switch (maType) {
    case "SMA":
      return SMA(series, length);

    case "EMA":
      return EMA(series, length);

    case "WMA":
      return WMA(series, length);

    case "HMA":
      return HMA(series, length);

    default:
      throw new Error(`Unsupported MA Type: ${maType}`);
  }
}

function computeATR(df, period = 14, multiplier = 2) {
  const tr = new Array(df.length).fill(NaN);

  for (let i = 0; i < df.length; i++) {
    if (i === 0) continue;

    const highLow = df[i].high - df[i].low;
    const highClose = Math.abs(df[i].high - df[i - 1].close);
    const lowClose = Math.abs(df[i].low - df[i - 1].close);

    tr[i] = Math.max(highLow, highClose, lowClose);
  }

  const atr = new Array(df.length).fill(NaN);

  for (let i = 0; i < df.length; i++) {
    // consider TRs in the current window (start from 1 since tr[0] is undefined)
    const start = Math.max(1, i - period + 1);
    const window = tr.slice(start, i + 1).filter((x) => !Number.isNaN(x));

    if (window.length === 0) {
      atr[i] = NaN;
      continue;
    }

    const sum = window.reduce((a, b) => a + b, 0);

    // use available average when fewer than `period` TR values exist
    atr[i] = sum / window.length;
  }

  for (let i = 0; i < df.length; i++) {
    df[i].ATR = atr[i];

    // ATR CHANNELS
    if (Number.isFinite(df[i].ATR) && df[i].close != null) {
      df[i].ATR_Upper = df[i].close + df[i].ATR * multiplier;
      df[i].ATR_Lower = df[i].close - df[i].ATR * multiplier;
    } else {
      df[i].ATR_Upper = NaN;
      df[i].ATR_Lower = NaN;
    }
  }

  return df;
}

function computeHLV(df, period = 10) {
  for (let i = 0; i < df.length; i++) {
    const slice = df.slice(Math.max(0, i - period + 1), i + 1);

    const avgHigh = slice.reduce((a, b) => a + b.high, 0) / slice.length;

    const avgLow = slice.reduce((a, b) => a + b.low, 0) / slice.length;

    df[i].Baseline = (avgHigh + avgLow) / 2;
  }

  return df;
}

function calculateSSL(data) {
  const requiredCols = [
    "emaHigh",
    "emaLow",
    "maHigh2",
    "maLow2",
    "exitHigh",
    "exitLow",
    "Baseline",
    "ATR",
    "ATR_Upper",
    "ATR_Lower",
    "HLV1",
    "HLV2",
    "HLV3",
    "SSL_Line",
    "SSL_Trend",
    "SSL2_Line",
    "SSL2_Trend",
    "SSL_Exit",
  ];

  data.forEach((row) => {
    requiredCols.forEach((col) => {
      if (!(col in row)) row[col] = null;
    });
  });

  if (data.length < 60) {
    return data;
  }

  const needsInit = data.every((r) => r.emaHigh == null);

  // ==================================
  // HISTORICAL INITIALIZATION
  // ==================================
  if (needsInit) {
    const emaHigh = HMA(
      data.map((x) => x.high),
      60,
    );
    const emaLow = HMA(
      data.map((x) => x.low),
      60,
    );

    const maHigh2 = HMA(
      data.map((x) => x.high),
      5,
    );
    const maLow2 = HMA(
      data.map((x) => x.low),
      5,
    );

    const exitHigh = HMA(
      data.map((x) => x.high),
      15,
    );
    const exitLow = HMA(
      data.map((x) => x.low),
      15,
    );

    const baseline = HMA(
      data.map((x) => x.close),
      60,
    );

    for (let i = 0; i < data.length; i++) {
      data[i].emaHigh = emaHigh[i];
      data[i].emaLow = emaLow[i];

      data[i].maHigh2 = maHigh2[i];
      data[i].maLow2 = maLow2[i];

      data[i].exitHigh = exitHigh[i];
      data[i].exitLow = exitLow[i];

      data[i].Baseline = baseline[i];
    }

    // ATR
    const trList = [];

    for (let i = 0; i < data.length; i++) {
      let tr;

      if (i === 0) {
        tr = data[i].high - data[i].low;
      } else {
        tr = Math.max(
          data[i].high - data[i].low,
          Math.abs(data[i].high - data[i - 1].close),
          Math.abs(data[i].low - data[i - 1].close),
        );
      }

      trList.push(tr);
      data[i].TR = tr;
    }

    // ATR Seed
    for (let i = 0; i < data.length; i++) {
      if (i < 13) {
        data[i].ATR = null;
        continue;
      }

      if (i === 13) {
        let sum = 0;

        for (let j = 0; j <= 13; j++) {
          sum += trList[j];
        }

        data[i].ATR = sum / 14;
      } else {
        data[i].ATR = (data[i - 1].ATR * 13 + trList[i]) / 14;
      }

      data[i].ATR_Upper = data[i].close + data[i].ATR;

      data[i].ATR_Lower = data[i].close - data[i].ATR;
    }

    let prev1 = 1;
    let prev2 = 1;
    let prev3 = 1;

    for (let i = 0; i < data.length; i++) {
      const c = data[i].close;

      // SSL1
      if (c > data[i].emaHigh) prev1 = 1;
      else if (c < data[i].emaLow) prev1 = -1;

      data[i].HLV1 = prev1;

      // SSL2
      if (c > data[i].maHigh2) prev2 = 1;
      else if (c < data[i].maLow2) prev2 = -1;

      data[i].HLV2 = prev2;

      // SSL3
      if (c > data[i].exitHigh) prev3 = 1;
      else if (c < data[i].exitLow) prev3 = -1;

      data[i].HLV3 = prev3;
    }

    for (let i = 0; i < data.length; i++) {
      data[i].SSL_Line = data[i].HLV1 === 1 ? data[i].emaLow : data[i].emaHigh;

      data[i].SSL_Trend = data[i].HLV1 === 1 ? "UP" : "DOWN";

      data[i].SSL2_Line = data[i].HLV2 === 1 ? data[i].maLow2 : data[i].maHigh2;

      data[i].SSL2_Trend = data[i].HLV2 === 1 ? "UP" : "DOWN";

      data[i].SSL_Exit =
        data[i].HLV3 === 1 ? data[i].exitLow : data[i].exitHigh;
    }
  }

  // ==================================
  // INCREMENTAL UPDATE
  // ==================================
  else {
    const curr = data.length - 1;
    const prev = data.length - 2;

    data[curr].emaHigh = HMA(
      data.map((x) => x.high),
      60,
    ).at(-1);

    data[curr].emaLow = HMA(
      data.map((x) => x.low),
      60,
    ).at(-1);

    data[curr].maHigh2 = HMA(
      data.map((x) => x.high),
      5,
    ).at(-1);

    data[curr].maLow2 = HMA(
      data.map((x) => x.low),
      5,
    ).at(-1);

    data[curr].exitHigh = HMA(
      data.map((x) => x.high),
      15,
    ).at(-1);

    data[curr].exitLow = HMA(
      data.map((x) => x.low),
      15,
    ).at(-1);

    data[curr].Baseline = HMA(
      data.map((x) => x.close),
      60,
    ).at(-1);

    const tr = Math.max(
      data[curr].high - data[curr].low,
      Math.abs(data[curr].high - data[prev].close),
      Math.abs(data[curr].low - data[prev].close),
    );

    data[curr].ATR = (data[prev].ATR * 13 + tr) / 14;

    data[curr].ATR_Upper = data[curr].close + data[curr].ATR;

    data[curr].ATR_Lower = data[curr].close - data[curr].ATR;

    // SSL1
    const hlv1 =
      data[curr].close > data[curr].emaHigh
        ? 1
        : data[curr].close < data[curr].emaLow
          ? -1
          : data[prev].HLV1;

    data[curr].HLV1 = hlv1;

    data[curr].SSL_Line = hlv1 === 1 ? data[curr].emaLow : data[curr].emaHigh;

    data[curr].SSL_Trend = hlv1 === 1 ? "UP" : "DOWN";

    // SSL2
    const hlv2 =
      data[curr].close > data[curr].maHigh2
        ? 1
        : data[curr].close < data[curr].maLow2
          ? -1
          : data[prev].HLV2;

    data[curr].HLV2 = hlv2;

    data[curr].SSL2_Line = hlv2 === 1 ? data[curr].maLow2 : data[curr].maHigh2;

    data[curr].SSL2_Trend = hlv2 === 1 ? "UP" : "DOWN";

    // SSL3
    const hlv3 =
      data[curr].close > data[curr].exitHigh
        ? 1
        : data[curr].close < data[curr].exitLow
          ? -1
          : data[prev].HLV3;

    data[curr].HLV3 = hlv3;

    data[curr].SSL_Exit = hlv3 === 1 ? data[curr].exitLow : data[curr].exitHigh;
  }

  return data;
}

function calculateSSL(data) {
  const requiredCols = [
    "emaHigh",
    "emaLow",
    "maHigh2",
    "maLow2",
    "exitHigh",
    "exitLow",
    "Baseline",
    "ATR",
    "ATR_Upper",
    "ATR_Lower",
    "HLV1",
    "HLV2",
    "HLV3",
    "SSL_Line",
    "SSL_Trend",
    "SSL2_Line",
    "SSL2_Trend",
    "SSL_Exit",
  ];

  data.forEach((row) => {
    requiredCols.forEach((col) => {
      if (!(col in row)) row[col] = null;
    });
  });

  if (data.length < 60) {
    return data;
  }

  const needsInit = data.every((r) => r.emaHigh == null);

  // ==================================
  // HISTORICAL INITIALIZATION
  // ==================================
  if (needsInit) {
    const emaHigh = HMA(
      data.map((x) => x.high),
      60,
    );
    const emaLow = HMA(
      data.map((x) => x.low),
      60,
    );

    const maHigh2 = HMA(
      data.map((x) => x.high),
      5,
    );
    const maLow2 = HMA(
      data.map((x) => x.low),
      5,
    );

    const exitHigh = HMA(
      data.map((x) => x.high),
      15,
    );
    const exitLow = HMA(
      data.map((x) => x.low),
      15,
    );

    const baseline = HMA(
      data.map((x) => x.close),
      60,
    );

    for (let i = 0; i < data.length; i++) {
      data[i].emaHigh = emaHigh[i];
      data[i].emaLow = emaLow[i];

      data[i].maHigh2 = maHigh2[i];
      data[i].maLow2 = maLow2[i];

      data[i].exitHigh = exitHigh[i];
      data[i].exitLow = exitLow[i];

      data[i].Baseline = baseline[i];
    }

    // ATR
    const trList = [];

    for (let i = 0; i < data.length; i++) {
      let tr;

      if (i === 0) {
        tr = data[i].high - data[i].low;
      } else {
        tr = Math.max(
          data[i].high - data[i].low,
          Math.abs(data[i].high - data[i - 1].close),
          Math.abs(data[i].low - data[i - 1].close),
        );
      }

      trList.push(tr);
      data[i].TR = tr;
    }

    // ATR Seed
    for (let i = 0; i < data.length; i++) {
      if (i < 13) {
        data[i].ATR = null;
        continue;
      }

      if (i === 13) {
        let sum = 0;

        for (let j = 0; j <= 13; j++) {
          sum += trList[j];
        }

        data[i].ATR = sum / 14;
      } else {
        data[i].ATR = (data[i - 1].ATR * 13 + trList[i]) / 14;
      }

      data[i].ATR_Upper = data[i].close + data[i].ATR;

      data[i].ATR_Lower = data[i].close - data[i].ATR;
    }

    let prev1 = 1;
    let prev2 = 1;
    let prev3 = 1;

    for (let i = 0; i < data.length; i++) {
      const c = data[i].close;

      // SSL1
      if (c > data[i].emaHigh) prev1 = 1;
      else if (c < data[i].emaLow) prev1 = -1;

      data[i].HLV1 = prev1;

      // SSL2
      if (c > data[i].maHigh2) prev2 = 1;
      else if (c < data[i].maLow2) prev2 = -1;

      data[i].HLV2 = prev2;

      // SSL3
      if (c > data[i].exitHigh) prev3 = 1;
      else if (c < data[i].exitLow) prev3 = -1;

      data[i].HLV3 = prev3;
    }

    for (let i = 0; i < data.length; i++) {
      data[i].SSL_Line = data[i].HLV1 === 1 ? data[i].emaLow : data[i].emaHigh;

      data[i].SSL_Trend = data[i].HLV1 === 1 ? "UP" : "DOWN";

      data[i].SSL2_Line = data[i].HLV2 === 1 ? data[i].maLow2 : data[i].maHigh2;

      data[i].SSL2_Trend = data[i].HLV2 === 1 ? "UP" : "DOWN";

      data[i].SSL_Exit =
        data[i].HLV3 === 1 ? data[i].exitLow : data[i].exitHigh;
    }
  }

  // ==================================
  // INCREMENTAL UPDATE
  // ==================================
  else {
    const curr = data.length - 1;
    const prev = data.length - 2;

    data[curr].emaHigh = HMA(
      data.map((x) => x.high),
      60,
    ).at(-1);

    data[curr].emaLow = HMA(
      data.map((x) => x.low),
      60,
    ).at(-1);

    data[curr].maHigh2 = HMA(
      data.map((x) => x.high),
      5,
    ).at(-1);

    data[curr].maLow2 = HMA(
      data.map((x) => x.low),
      5,
    ).at(-1);

    data[curr].exitHigh = HMA(
      data.map((x) => x.high),
      15,
    ).at(-1);

    data[curr].exitLow = HMA(
      data.map((x) => x.low),
      15,
    ).at(-1);

    data[curr].Baseline = HMA(
      data.map((x) => x.close),
      60,
    ).at(-1);

    const tr = Math.max(
      data[curr].high - data[curr].low,
      Math.abs(data[curr].high - data[prev].close),
      Math.abs(data[curr].low - data[prev].close),
    );

    data[curr].ATR = (data[prev].ATR * 13 + tr) / 14;

    data[curr].ATR_Upper = data[curr].close + data[curr].ATR;

    data[curr].ATR_Lower = data[curr].close - data[curr].ATR;

    // SSL1
    const hlv1 =
      data[curr].close > data[curr].emaHigh
        ? 1
        : data[curr].close < data[curr].emaLow
          ? -1
          : data[prev].HLV1;

    data[curr].HLV1 = hlv1;

    data[curr].SSL_Line = hlv1 === 1 ? data[curr].emaLow : data[curr].emaHigh;

    data[curr].SSL_Trend = hlv1 === 1 ? "UP" : "DOWN";

    // SSL2
    const hlv2 =
      data[curr].close > data[curr].maHigh2
        ? 1
        : data[curr].close < data[curr].maLow2
          ? -1
          : data[prev].HLV2;

    data[curr].HLV2 = hlv2;

    data[curr].SSL2_Line = hlv2 === 1 ? data[curr].maLow2 : data[curr].maHigh2;

    data[curr].SSL2_Trend = hlv2 === 1 ? "UP" : "DOWN";

    // SSL3
    const hlv3 =
      data[curr].close > data[curr].exitHigh
        ? 1
        : data[curr].close < data[curr].exitLow
          ? -1
          : data[prev].HLV3;

    data[curr].HLV3 = hlv3;

    data[curr].SSL_Exit = hlv3 === 1 ? data[curr].exitLow : data[curr].exitHigh;
  }

  return data;
}

function computeSSLExit(df) {
  for (let i = 0; i < df.length; i++) {
    if (i === 0) {
      df[i].SSL_Exit = df[i].close;
      df[i].SSL_Exit_Trend = df[i].SSL_Trend || 0;
      continue;
    }

    const prev = df[i - 1];

    if (prev.SSL_Trend === 1 && df[i].SSL_Trend === -1) {
      df[i].SSL_Exit = df[i].close;
      df[i].SSL_Exit_Trend = -1;
    } else if (prev.SSL_Trend === -1 && df[i].SSL_Trend === 1) {
      df[i].SSL_Exit = df[i].close;
      df[i].SSL_Exit_Trend = 1;
    } else {
      // carry forward previous exit so we don't leave NaN
      df[i].SSL_Exit = prev.SSL_Exit;
      df[i].SSL_Exit_Trend = prev.SSL_Exit_Trend || 0;
    }
  }

  return df;
}

async function generate_payload(rawData = null) {
  let boslim;
  if (rawData && Array.isArray(rawData)) {
    boslim = rawData;
  } else {
    boslim = await loadBOSLIM();
  }

  boslim = computeHLV(boslim);
  boslim = calculateSSL(boslim);
  boslim = computeSSLExit(boslim);
  boslim = computeATR(boslim);

  // STEP 2
  boslim = computeSMA(boslim);

  // STEP 3
  boslim = computeRSI(boslim);

  // STEP 5: Format to match the Python API exact requirements
  const fmt = (val) =>
    Number.isNaN(val) || val == null ? "NaN" : val.toString();

  boslim = boslim.map((row, index) => {
    let Vol_chng = 0;
    let Vol_pct_chng = "0.0";
    if (index > 0) {
      Vol_chng = row.volume - boslim[index - 1].volume;
      if (boslim[index - 1].volume !== 0) {
        Vol_pct_chng = (Vol_chng / boslim[index - 1].volume).toString();
      }
    }

    return {
      datetime: row.datetime,
      exchange_code: row.exchange_code,
      stock_code: row.stock_code,
      high: row.high,
      low: row.low,
      open: row.open,
      close: row.close,
      volume: row.volume,
      SMA_20: fmt(row.SMA_20),
      SMA_50: fmt(row.SMA_50),
      SMA_100: fmt(row.SMA_100),
      SMA_200: fmt(row.SMA_200),
      Price_change: fmt(row.Price_change),
      Gain: fmt(row.Gain),
      Loss: fmt(row.Loss),
      Avg_Gain: fmt(row.Avg_Gain),
      Avg_Loss: fmt(row.Avg_Loss),
      RMA_Gain: fmt(row.RMA_Gain),
      RMA_Loss: fmt(row.RMA_Loss),
      RS: fmt(row.RS),
      RSI: fmt(row.RSI),
      Baseline: fmt(row.Baseline),
      SSL_Line: fmt(row.SSL_Line),
      SSL_Trend: row.SSL_Trend || "NaN",
      SSL2_Line: fmt(row.SSL2_Line),
      SSL2_Trend: row.SSL2_Trend || "NaN",
      SSL_Exit: fmt(row.SSL_Exit),
      SSL_Exit_Trend: row.SSL_Exit_Trend || "NaN",
      ATR: fmt(row.ATR),
      ATR_Upper: fmt(row.ATR_Upper),
      ATR_Lower: fmt(row.ATR_Lower),
      Vol_chng: Vol_chng,
      Vol_pct_chng: Vol_pct_chng,
    };
  });

  return boslim;
}

module.exports = { generate_payload };

if (require.main === module) {
  (async () => {
    const boslim = await generate_payload();
    console.log(JSON.stringify({ boslim }));
  })();
}
