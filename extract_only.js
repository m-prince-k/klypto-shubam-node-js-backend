const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');

const OUT_CSV_FOLDER = path.join(__dirname, 'calculated_parameters');
const OUT_JSON_FOLDER = path.join(__dirname, 'extractJson');

if (!fs.existsSync(OUT_JSON_FOLDER)) fs.mkdirSync(OUT_JSON_FOLDER);

function padMissingCandles(data) {
    if (!data || data.length === 0) return data;

    const days = new Set();
    data.forEach(row => {
        const datePart = row.datetime.split(' ')[0];
        if (datePart) days.add(datePart);
    });
    
    const sortedDays = Array.from(days).sort();
    
    const times = [];
    for (let h = 9; h <= 15; h++) {
        for (let m = 0; m < 60; m += 5) {
            const timeStr = `${String(h).padStart(2, '0')}:${String(m).padStart(2, '0')}:00`;
            if (timeStr >= "09:15:00" && timeStr <= "15:25:00") {
                times.push(timeStr);
            }
        }
    }
    
    const dataMap = new Map();
    data.forEach(row => {
        dataMap.set(row.datetime, row);
    });

    const paddedData = [];
    let lastRow = null;

    for (const day of sortedDays) {
        let firstRowForDay = null;
        for (const timeStr of times) {
            const dt = `${day} ${timeStr}`;
            if (dataMap.has(dt)) {
                firstRowForDay = dataMap.get(dt);
                break;
            }
        }

        for (const timeStr of times) {
            const dt = `${day} ${timeStr}`;
            if (dataMap.has(dt)) {
                lastRow = dataMap.get(dt);
                paddedData.push(lastRow);
            } else {
                let referenceRow = lastRow || firstRowForDay;
                if (referenceRow) {
                    const newRow = { ...referenceRow };
                    newRow.datetime = dt;
                    newRow.open = referenceRow.close;
                    newRow.high = referenceRow.close;
                    newRow.low = referenceRow.close;
                    newRow.volume = 0;
                    paddedData.push(newRow);
                    lastRow = newRow;
                }
            }
        }
    }
    
    return paddedData;
}

function readCSV(filePath) {
  return new Promise((resolve, reject) => {
    const data = [];
    fs.createReadStream(filePath)
      .pipe(csv())
      .on('data', (row) => {
        const parsedRow = {};
        for (const key in row) {
          const val = row[key];
          if (key === 'datetime' || key === 'exchange_code' || key === 'stock_code' || key === 'SSL_Trend' || key === 'SSL2_Trend' || key === 'SSL_Exit_Trend') {
            parsedRow[key] = val;
          } else {
            const num = Number(val);
            parsedRow[key] = isNaN(num) ? val : num;
          }
        }
        data.push(parsedRow);
      })
      .on('end', () => resolve(data))
      .on('error', reject);
  });
}

async function run() {
    const files = fs.readdirSync(OUT_CSV_FOLDER).filter(f => f.endsWith('.csv'));
    console.log(`Found ${files.length} files to extract.`);
    for (const file of files) {
        const symbol = path.basename(file, '.csv');
        const csvPath = path.join(OUT_CSV_FOLDER, file);
        const jsonPath = path.join(OUT_JSON_FOLDER, `${symbol}.json`);
        
        try {
            let data = await readCSV(csvPath);
            // filter strictly
            data = data.filter(row => {
                const timePart = row.datetime.split(' ')[1];
                if (!timePart) return false;
                return timePart >= "09:15:00" && timePart <= "15:25:00";
            });
            // pad missing candles to keep 75 per day
            data = padMissingCandles(data);
            
            const last300 = data.slice(-300);
            fs.writeFileSync(jsonPath, JSON.stringify({ historic_data: last300 }, null, 2));
            console.log(`Extracted 300 padded rows for ${symbol}`);
        } catch (e) {
            console.error(`Error processing ${symbol}:`, e);
        }
    }
    console.log("Extraction complete.");
}

run();
