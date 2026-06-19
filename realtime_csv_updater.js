const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');
const angelone = require('./angelone-client.js');

const HIST_FOLDER = path.join(__dirname, 'historical_csv');

function readCSV(filePath, stock_code) {
  return new Promise((resolve, reject) => {
    const data = [];
    fs.createReadStream(filePath)
      .pipe(csv())
      .on('data', (row) => {
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
      .on('end', () => resolve(data))
      .on('error', reject);
  });
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
  const toDateStr = `${now.getFullYear()}-${pad(now.getMonth()+1)}-${pad(now.getDate())} ${pad(now.getHours())}:${pad(now.getMinutes())}`;
  
  if (fromDateStr >= toDateStr) return rawData;
  
  try {
    const token = await angelone.getTokenForSymbol(symbol);
    if (!token) return rawData;
    
    const hist = await angelone.fetchHistoricalCandles(symbol, token, "FIVE_MINUTE", fromDateStr, toDateStr);
    if (hist && hist.data) {
        const histCsvPath = path.join(HIST_FOLDER, `${symbol}.csv`);
        let added = 0;
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
                fs.appendFileSync(histCsvPath, csvLine);
                added++;
            }
        }
        if (added > 0) {
            console.log(`[${symbol}] Filled ${added} gaps up to ${toDateStr}`);
        }
    }
  } catch (e) {
    console.warn(`[${symbol}] Could not fill gap:`, e.message);
  }
  
  return rawData;
}

async function updateAllFiles() {
    console.log(`\n[${new Date().toLocaleTimeString()}] Starting real-time historical_csv gap fill...`);
    const files = fs.readdirSync(HIST_FOLDER).filter(f => f.endsWith('.csv'));
    const BATCH_SIZE = 10;
    for (let i = 0; i < files.length; i += BATCH_SIZE) {
        const batch = files.slice(i, i + BATCH_SIZE);
        for (const file of batch) {
            const symbol = path.basename(file, '.csv');
            const inputPath = path.join(HIST_FOLDER, file);
            try {
                let rawData = await readCSV(inputPath, symbol);
                await fillGapForSymbol(symbol, rawData);
            } catch (e) {
                console.error(`Error processing ${symbol}:`, e);
            }
        }
    }
    console.log(`[${new Date().toLocaleTimeString()}] Historical gap fill check complete.`);
}

async function loop() {
  while (true) {
    await updateAllFiles();
    const now = new Date();
    // Align tightly to the 5-minute clock boundary
    const msUntilNext5Min = (5 * 60 * 1000) - (now.getTime() % (5 * 60 * 1000));
    console.log(`Waiting ${Math.round(msUntilNext5Min / 1000)} seconds before next sync...`);
    await new Promise(r => setTimeout(r, msUntilNext5Min));
  }
}

loop().catch(console.error);
