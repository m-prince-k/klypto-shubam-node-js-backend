const fs = require('fs');
const path = require('path');

const HIST_DIR = path.join(__dirname, 'historical_csv');
const OUT_FILE = path.join(__dirname, 'today_915_candles.csv');

const files = fs.readdirSync(HIST_DIR).filter(f => f.endsWith('.csv'));

const now = new Date();
const todayStr = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}-${String(now.getDate()).padStart(2, '0')} 09:15:00`;

// CSV Header matching candles_5m table structure (excluding id as it's serial)
let csvContent = "symbol,open,high,low,close,volume,timestamp\n";

for (const file of files) {
    const symbol = path.basename(file, '.csv');
    
    // Read the last close price to make the mock candle realistic
    const csvLines = fs.readFileSync(path.join(HIST_DIR, file), 'utf-8').trim().split('\n');
    if (csvLines.length > 1) {
        const lastLine = csvLines[csvLines.length - 1];
        const parts = lastLine.split(',');
        
        // historical_csv format: "datetime","exchange_code","stock_code","open","high","low","close","volume"
        if (parts.length >= 8) {
            const lastClose = parseFloat(parts[6].replace(/"/g, ''));
            
            // Generate a slight variation for the 9:15 candle
            const open = (lastClose * 1.001).toFixed(2);
            const high = (lastClose * 1.005).toFixed(2);
            const low = (lastClose * 0.998).toFixed(2);
            const close = (lastClose * 1.003).toFixed(2);
            const volume = Math.floor(Math.random() * 50000) + 10000; // Random volume between 10k-60k
            
            csvContent += `${symbol},${open},${high},${low},${close},${volume},${todayStr}\n`;
        }
    }
}

fs.writeFileSync(OUT_FILE, csvContent);
console.log(`Created ${OUT_FILE} with today's 9:15 candles for ${files.length} stocks.`);
