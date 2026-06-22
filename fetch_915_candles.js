const angelone = require('./angelone-client.js');
const symbolsMap = require('./symbols.js');
const db = require('./db.js');

const symbols = Object.keys(symbolsMap);

const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function fetchAndStore915Candles() {
    console.log("=> Starting 9:15 AM candle fetch for all symbols...");
    const now = new Date();
    const pad = (n) => String(n).padStart(2, '0');
    const todayStr = `${now.getFullYear()}-${pad(now.getMonth() + 1)}-${pad(now.getDate())}`;
    
    // We want the 09:15 to 09:16 slice to strictly get the 9:15 candle
    const fromdate = `${todayStr} 09:15`;
    const todate = `${todayStr} 09:16`;

    for (const symbol of symbols) {
        let success = false;
        let attempts = 0;

        while (!success && attempts < 3) {
            attempts++;
            try {
                const token = symbolsMap[symbol];
                if (!token) break;

                const response = await angelone.fetchHistoricalCandles(symbol, token, 'FIVE_MINUTE', fromdate, todate);
                
                if (response && response.data && response.data.length > 0) {
                    const candle = response.data[0];
                    // candle format: [timestamp, open, high, low, close, volume]
                    const open = parseFloat(candle[1]);
                    const high = parseFloat(candle[2]);
                    const low = parseFloat(candle[3]);
                    const close = parseFloat(candle[4]);
                    const volume = parseInt(candle[5], 10);
                    
                    // Insert perfectly into DB for send_predictions.js to read
                    const timestampStr = `${todayStr} 09:15:00`;
                    await db.upsertCandle(symbol, open, high, low, close, volume, timestampStr);
                    console.log(`[${symbol}] 9:15 Candle saved to DB: O=${open} H=${high} L=${low} C=${close} V=${volume}`);
                    success = true;
                } else {
                    console.log(`[${symbol}] No 9:15 data returned. Attempt ${attempts}`);
                }
            } catch (err) {
                console.warn(`[${symbol}] Error fetching 9:15 candle: ${err.message}`);
                if (err.message.includes('429') || err.message.includes('403')) {
                    await delay(5000);
                }
            }
        }
        await delay(1000); // 1s delay to respect rate limit
    }
    console.log("=> Finished 9:15 AM candle fetch for all symbols.");
}

module.exports = { fetchAndStore915Candles };
