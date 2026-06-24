const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');
const { generate_payload } = require('./calculate_parameters.js');
const angelone = require('./angelone-client.js');
const db = require('./db');

const HIST_FOLDER = path.join(__dirname, 'historical_csv');

function emptyFolder(folderPath) {
    if (fs.existsSync(folderPath)) {
        fs.readdirSync(folderPath).forEach(file => {
            const curPath = path.join(folderPath, file);
            if (!fs.lstatSync(curPath).isDirectory()) {
                fs.unlinkSync(curPath);
            }
        });
        console.log(`[CLEANUP] Emptied folder: ${folderPath}`);
    }
}

async function getRawDataFromDB(symbol) {
  const pool = db.getDB();
  const res = await pool.query('SELECT datetime, open, high, low, close, volume FROM historical_candles WHERE symbol = $1 ORDER BY datetime ASC', [symbol]);
  return res.rows.map(r => ({
    datetime: db.formatTimestamp(new Date(r.datetime)),
    exchange_code: "NSE",
    stock_code: symbol,
    open: Number(r.open),
    high: Number(r.high),
    low: Number(r.low),
    close: Number(r.close),
    volume: Number(r.volume || 0),
  }));
}

async function fillGapForSymbol(symbol, rawData) {
  if (rawData.length === 0) return rawData;
  const lastRow = rawData[rawData.length - 1];
  let lastDt = lastRow.datetime;
  
  const d = new Date(lastDt);
  if (isNaN(d.getTime())) return rawData;
  
  const pad = n => String(n).padStart(2, '0');
  const fromDateStr = `${d.getFullYear()}-${pad(d.getMonth()+1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}`;
  
  const now = new Date();
  let toDateStr = `${now.getFullYear()}-${pad(now.getMonth()+1)}-${pad(now.getDate())} ${pad(now.getHours())}:${pad(now.getMinutes())}`;
  const maxToDateStr = `${now.getFullYear()}-${pad(now.getMonth()+1)}-${pad(now.getDate())} 15:25`;
  if (toDateStr > maxToDateStr) {
      toDateStr = maxToDateStr;
  }
  
  if (fromDateStr >= toDateStr) return rawData;
  
  try {
    const token = await angelone.getTokenForSymbol(symbol);
    if (!token) return rawData;
    
    const hist = await angelone.fetchHistoricalCandles(symbol, token, "FIVE_MINUTE", fromDateStr, toDateStr);
    if (hist && hist.data) {
        const pool = db.getDB();
        for (const candle of hist.data) {
            const candleDate = new Date(candle[0]);
            const candleDtStr = `${candleDate.getFullYear()}-${pad(candleDate.getMonth()+1)}-${pad(candleDate.getDate())} ${pad(candleDate.getHours())}:${pad(candleDate.getMinutes())}:00`;
            const timeStr = `${pad(candleDate.getHours())}:${pad(candleDate.getMinutes())}:00`;
            if (candleDate > d && timeStr >= "09:15:00" && timeStr <= "15:25:00") {
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
                
                await pool.query(`
                  INSERT INTO historical_candles (symbol, datetime, open, high, low, close, volume) 
                  VALUES ($1, $2, $3, $4, $5, $6, $7)
                  ON CONFLICT(symbol, datetime) DO NOTHING
                `, [symbol, candleDtStr, newRow.open, newRow.high, newRow.low, newRow.close, newRow.volume]);
            }
        }
        console.log(`Filled gaps for ${symbol}: up to ${toDateStr}`);
    }
  } catch (e) {
    console.warn(`Could not fill gap for ${symbol}:`, e.message);
  }
  
  return rawData;
}

async function processSymbol(symbol) {
    try {
        let rawData = await getRawDataFromDB(symbol);
        rawData = await fillGapForSymbol(symbol, rawData);
        
        // Filter strictly to 09:15:00 - 15:25:00
        rawData = rawData.filter(row => {
            const timePart = row.datetime.split(' ')[1];
            if (!timePart) return true;
            return timePart >= "09:15:00" && timePart <= "15:25:00";
        });
        
        // Deep scan and calculation using existing calculate_parameters.js
        const processed = await generate_payload(rawData);
        
        // Save last 300 rows as JSON to DB symbol_payloads
        const last300 = processed.slice(-300);
        const pool = db.getDB();
        await pool.query(`
          INSERT INTO symbol_payloads (symbol, historic_data, updated_at) 
          VALUES ($1, $2, CURRENT_TIMESTAMP)
          ON CONFLICT(symbol) DO UPDATE SET historic_data = EXCLUDED.historic_data, updated_at = CURRENT_TIMESTAMP
        `, [symbol, JSON.stringify({ historic_data: last300 })]);
        
        console.log(`Processed ${symbol}`);
    } catch(e) {
        console.error(`Error processing ${symbol}:`, e);
    }
}

async function performCalculations() {
    const pool = db.getDB();
    const res = await pool.query('SELECT DISTINCT symbol FROM historical_candles');
    const symbols = res.rows.map(r => r.symbol);
    console.log(`[${new Date().toLocaleTimeString()}] Found ${symbols.length} symbols in DB. Starting calculations...`);
    
    const BATCH_SIZE = 10;
    for (let i = 0; i < symbols.length; i += BATCH_SIZE) {
        const batch = symbols.slice(i, i + BATCH_SIZE);
        for (const symbol of batch) {
            await processSymbol(symbol);
        }
    }
    console.log(`[${new Date().toLocaleTimeString()}] Done all calculations and saves for today!`);
}

async function loop() {
    const LAST_RUN_FILE = path.join(__dirname, 'last_calculated_date.txt');
    let lastProcessedDate = null;
    if (fs.existsSync(LAST_RUN_FILE)) {
        lastProcessedDate = fs.readFileSync(LAST_RUN_FILE, 'utf8').trim();
    }
    
    console.log("Starting Post-Market Calculation Engine. Waiting for 15:45 every day...");
    
    while (true) {
        const now = new Date();
        const pad = n => String(n).padStart(2, '0');
        const currentDateStr = `${now.getFullYear()}-${pad(now.getMonth() + 1)}-${pad(now.getDate())}`;
        
        const isPastTriggerTime = now.getHours() > 15 || (now.getHours() === 15 && now.getMinutes() >= 45);
        const alreadyProcessedToday = (lastProcessedDate === currentDateStr);
        
        if (isPastTriggerTime && !alreadyProcessedToday) {
            console.log(`\n=================================================`);
            console.log(`[${now.toLocaleTimeString()}] Triggering Post-Market Calculations!`);
            console.log(`=================================================`);
            
            // Empty both directories daily before populating new ones
            emptyFolder(OUT_CSV_FOLDER);
            emptyFolder(OUT_JSON_FOLDER);
            
            try {
                await performCalculations();
                lastProcessedDate = currentDateStr; // Mark as done for today in memory
                fs.writeFileSync(LAST_RUN_FILE, currentDateStr); // Save it so restarts don't re-trigger
            } catch (err) {
                console.error("Error during calculations:", err);
            }
        }
        
        // Check time every 60 seconds
        await new Promise(r => setTimeout(r, 60000));
    }
}

module.exports = {
    startPostMarketCalculations: loop,
    performCalculations,
    emptyFolder,
    OUT_CSV_FOLDER,
    OUT_JSON_FOLDER
};
