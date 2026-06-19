const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');
const { generate_payload } = require('./calculate_parameters.js');
const angelone = require('./angelone-client.js');

const HIST_FOLDER = path.join(__dirname, 'historical_csv');
const OUT_CSV_FOLDER = path.join(__dirname, 'calculated_parameters');
const OUT_JSON_FOLDER = path.join(__dirname, 'extractJson');

if (!fs.existsSync(OUT_CSV_FOLDER)) fs.mkdirSync(OUT_CSV_FOLDER);
if (!fs.existsSync(OUT_JSON_FOLDER)) fs.mkdirSync(OUT_JSON_FOLDER);

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

function writeCSV(filePath, data) {
  if (data.length === 0) return Promise.resolve();
  const headers = Object.keys(data[0]);
  const stream = fs.createWriteStream(filePath);
  
  stream.write(headers.join(',') + '\n');
  for (const row of data) {
    const line = headers.map(h => {
        let val = row[h];
        if (val === null || val === undefined) val = "NaN";
        return `"${val}"`;
    }).join(',');
    stream.write(line + '\n');
  }
  stream.end();
  
  return new Promise(resolve => stream.on('finish', resolve));
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
        const histCsvPath = path.join(HIST_FOLDER, `${symbol}.csv`);
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
                
                // Append exactly matching original columns, padding with zeroes for unused metrics
                const csvLine = `${candleDtStr},${newRow.open},${newRow.high},${newRow.low},${newRow.close},${newRow.volume},0,0,0,0,0\n`;
                fs.appendFileSync(histCsvPath, csvLine);
            }
        }
        console.log(`Filled gaps for ${symbol}: up to ${toDateStr}`);
    }
  } catch (e) {
    console.warn(`Could not fill gap for ${symbol}:`, e.message);
  }
  
  return rawData;
}

async function processFile(file) {
    const symbol = path.basename(file, '.csv');
    const inputPath = path.join(HIST_FOLDER, file);
    const csvOutPath = path.join(OUT_CSV_FOLDER, file);
    const jsonOutPath = path.join(OUT_JSON_FOLDER, `${symbol}.json`);
    
    try {
        let rawData = await readCSV(inputPath, symbol);
        rawData = await fillGapForSymbol(symbol, rawData);
        
        // Filter strictly to 09:15:00 - 15:25:00
        rawData = rawData.filter(row => {
            const timePart = row.datetime.split(' ')[1];
            if (!timePart) return true;
            return timePart >= "09:15:00" && timePart <= "15:25:00";
        });
        
        // Deep scan and calculation using existing calculate_parameters.js
        const processed = await generate_payload(rawData);
        
        // Save full calculated data to calculated_parameters (NO DATA IS DELETED)
        await writeCSV(csvOutPath, processed);
        
        // Save last 300 rows as JSON to extractJson folder
        const last300 = processed.slice(-300);
        fs.writeFileSync(jsonOutPath, JSON.stringify({ historic_data: last300 }, null, 2));
        
        console.log(`Processed ${symbol}`);
    } catch(e) {
        console.error(`Error processing ${symbol}:`, e);
    }
}

async function performCalculations() {
    const files = fs.readdirSync(HIST_FOLDER).filter(f => f.endsWith('.csv'));
    console.log(`[${new Date().toLocaleTimeString()}] Found ${files.length} files. Starting calculations...`);
    const BATCH_SIZE = 10;
    for (let i = 0; i < files.length; i += BATCH_SIZE) {
        const batch = files.slice(i, i + BATCH_SIZE);
        for (const file of batch) {
            await processFile(file);
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
    startPostMarketCalculations: loop
};
