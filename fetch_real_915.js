const fs = require('fs');
const path = require('path');
const { fetchHistoricalCandles } = require('./angelone-client');
const symbolsMap = require('./symbols');

const symbols = Object.keys(symbolsMap);
const OUT_FILE = path.join(__dirname, 'today_915_candles.csv');

const now = new Date();
const todayDateStr = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}-${String(now.getDate()).padStart(2, '0')}`;
// Angel One historical API fromdate format: "yyyy-mm-dd HH:MM"
const fromdate = `${todayDateStr} 09:15`;
const todate = `${todayDateStr} 09:20`;

async function fetchReal915() {
    let existingContent = "";
    if (fs.existsSync(OUT_FILE)) {
        existingContent = fs.readFileSync(OUT_FILE, 'utf-8');
    }
    
    let csvContent = existingContent.length > 0 ? (existingContent.endsWith('\n') ? '' : '\n') : "symbol,open,high,low,close,volume,timestamp\n";
    let count = 0;
    
    console.log(`Fetching missing 9:15 candles for ${todayDateStr}...`);

    for (const symbol of symbols) {
        if (existingContent.includes(`\n${symbol},`) || existingContent.startsWith(`${symbol},`)) {
            continue; // Skip if already fetched
        }
        try {
            const token = symbolsMap[symbol];
            if (!token) continue;
            
            console.log(`Fetching for ${symbol}...`);
            const response = await fetchHistoricalCandles(symbol, token, "FIVE_MINUTE", fromdate, todate);
            
            if (response && response.status && response.data && response.data.length > 0) {
                const candle915 = response.data.find(c => c[0].includes('09:15:00'));
                
                if (candle915) {
                    const [ts, open, high, low, close, volume] = candle915;
                    const formattedTs = `${todayDateStr} 09:15:00`;
                    csvContent += `${symbol},${open},${high},${low},${close},${volume},${formattedTs}\n`;
                    console.log(`✅ Fetched 9:15 for ${symbol}`);
                    count++;
                } else {
                    console.log(`⚠️ No 9:15 candle found for ${symbol}`);
                }
            } else {
                console.log(`❌ No data for ${symbol}: ${response?.message || 'unknown'}`);
            }
            
            await new Promise(r => setTimeout(r, 1500)); // sleep longer to avoid rate limit
        } catch (e) {
            console.log(`💥 Error fetching ${symbol}: ${e.message}`);
            await new Promise(r => setTimeout(r, 2000)); // longer sleep on error
        }
    }
    
    if (count > 0) {
        fs.appendFileSync(OUT_FILE, csvContent);
        console.log(`\n🎉 Successfully appended ${count} new candles to ${OUT_FILE}`);
    } else {
        console.log(`\n🎉 All symbols already fetched!`);
    }
}

fetchReal915();
