const fs = require('fs');
const path = require('path');
const { fetchHistoricalCandles } = require('./angelone-client');
const symbolsMap = require('./symbols');

const symbols = Object.keys(symbolsMap);
const HIST_DIR = path.join(__dirname, 'historical_csv');

// Dates
const now = new Date();
const todayDateStr = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}-${String(now.getDate()).padStart(2, '0')}`;
const fromdate = `${todayDateStr} 09:15`;
const todate = `${todayDateStr} 15:30`;

const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function fetchTodayAll() {
    let successCount = 0;
    
    console.log(`Fetching today's candles (${fromdate} to ${todate}) for ${symbols.length} symbols...`);

    if (!fs.existsSync(HIST_DIR)) {
        fs.mkdirSync(HIST_DIR);
    }

    for (const symbol of symbols) {
        let success = false;
        let attempts = 0;
        const maxAttempts = 3;

        while (!success && attempts < maxAttempts) {
            attempts++;
            try {
                const token = symbolsMap[symbol];
                if (!token) {
                    success = true;
                    break;
                }

                const response = await fetchHistoricalCandles(symbol, token, 'FIVE_MINUTE', fromdate, todate);
                
                if (response && response.data) {
                    const csvPath = path.join(HIST_DIR, `${symbol}.csv`);
                    let existingContent = fs.existsSync(csvPath) ? fs.readFileSync(csvPath, 'utf-8') : "datetime,exchange_code,stock_code,open,high,low,close,volume\n";
                    
                    let addedCount = 0;
                    for (const candle of response.data) {
                        const candleDate = new Date(candle[0]);
                        const dtStr = `${candleDate.getFullYear()}-${String(candleDate.getMonth()+1).padStart(2, '0')}-${String(candleDate.getDate()).padStart(2, '0')} ${String(candleDate.getHours()).padStart(2, '0')}:${String(candleDate.getMinutes()).padStart(2, '0')}:00`;
                        
                        // We only want 9:15 to 15:25
                        const timePart = dtStr.split(' ')[1];
                        if (timePart >= "09:15:00" && timePart <= "15:25:00") {
                            if (!existingContent.includes(`"${dtStr}"`)) {
                                const newRow = `"${dtStr}","NSE","${symbol}","${candle[1]}","${candle[2]}","${candle[3]}","${candle[4]}","${candle[5]}"\n`;
                                existingContent = existingContent.trimEnd() + '\n' + newRow;
                                addedCount++;
                            }
                        }
                    }
                    
                    if (addedCount > 0) {
                        fs.writeFileSync(csvPath, existingContent.trimEnd() + '\n');
                        console.log(`[${symbol}] Appended ${addedCount} missing candles.`);
                    } else {
                        console.log(`[${symbol}] No missing candles.`);
                    }
                    
                    successCount++;
                    success = true;
                } else {
                    console.log(`[${symbol}] No data returned. Attempt ${attempts}`);
                }
            } catch (err) {
                console.warn(`[${symbol}] Error: ${err.message}. Attempt ${attempts}/${maxAttempts}`);
                if (err.message.includes('429') || err.message.includes('403')) {
                    console.log(`[${symbol}] API limit hit. Sleeping for 10 seconds...`);
                    await delay(10000);
                } else {
                    // Sleep a bit on other errors too
                    await delay(3000);
                }
            }
        }
        
        // Wait 2s between symbols to avoid hitting the 1 request / second strict limit or minute limits
        await delay(2000);
    }
    
    console.log(`Finished processing. Successfully fetched data for ${successCount}/${symbols.length} symbols.`);
}

module.exports = { fetchTodayAll };
