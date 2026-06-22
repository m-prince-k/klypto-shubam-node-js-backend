const fs = require('fs');
const csv = require('csv-parser');
const db = require('./db.js');

async function run() {
  try {
    console.log("Initializing DB and re-creating table...");
    await db.initDB();

    console.log("Reading CSV...");
    const rows = [];
    fs.createReadStream('today_915_candles.csv')
      .pipe(csv())
      .on('data', (data) => rows.push(data))
      .on('end', async () => {
        console.log(`Found ${rows.length} rows. Inserting...`);
        let count = 0;
        
        for (const row of rows) {
          // Using your existing upsertCandle function
          await db.upsertCandle(
            row.symbol,
            parseFloat(row.open),
            parseFloat(row.high),
            parseFloat(row.low),
            parseFloat(row.close),
            parseInt(row.volume, 10),
            row.timestamp
          );
          count++;
        }
        
        console.log(`Successfully inserted ${count} rows!`);
        process.exit(0);
      });
  } catch (err) {
    console.error(err);
    process.exit(1);
  }
}

run();
