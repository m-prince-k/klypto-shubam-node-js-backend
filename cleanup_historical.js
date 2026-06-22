const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');

const HIST_FOLDER = path.join(__dirname, 'historical_csv');
const CALC_FOLDER = path.join(__dirname, 'calculated_parameters');

function readCSV(filePath) {
  return new Promise((resolve, reject) => {
    const data = [];
    fs.createReadStream(filePath)
      .pipe(csv())
      .on('data', (row) => data.push(row))
      .on('end', () => resolve(data))
      .on('error', reject);
  });
}

function writeCSV(filePath, data) {
  if (data.length === 0) return Promise.resolve();
  const headers = Object.keys(data[0]);
  const tmpPath = filePath + '.tmp';
  const stream = fs.createWriteStream(tmpPath);
  stream.write(headers.join(',') + '\n');
  for (const row of data) {
    const line = headers.map(h => {
        let val = row[h];
        if (val === null || val === undefined) val = 'NaN';
        return `"${val}"`;
    }).join(',');
    stream.write(line + '\n');
  }
  stream.end();
  return new Promise((resolve, reject) => {
    stream.on('finish', () => {
      try {
        fs.renameSync(tmpPath, filePath);
        resolve();
      } catch (err) {
        reject(err);
      }
    });
    stream.on('error', reject);
  });
}

async function run() {
  const files = fs.readdirSync(HIST_FOLDER).filter(f => f.endsWith('.csv'));
  let cleanedFiles = 0;
  for (const file of files) {
    const filePath = path.join(HIST_FOLDER, file);
    const data = await readCSV(filePath);
    
    // Filter conditions:
    // 1. Keep if datetime <= '2026-06-22 09:20:00'
    // 2. Keep if NOT (open == high && high == low && low == close && volume == 0)
    const filtered = data.filter(row => {
      // time check
      if (row.datetime > '2026-06-19 15:25:00') return false;
      
      // flat candle check
      const o = parseFloat(row.open);
      const h = parseFloat(row.high);
      const l = parseFloat(row.low);
      const c = parseFloat(row.close);
      const v = parseFloat(row.volume);
      
      if (o === 0 && h === 0 && l === 0 && c === 0 && v === 0) {
        return false;
      }
      
      return true;
    });
    
    if (filtered.length < data.length) {
      await writeCSV(filePath, filtered);
      cleanedFiles++;
    }
  }
  console.log('Cleaned ' + cleanedFiles + ' historical files.');

  const calcFiles = fs.readdirSync(CALC_FOLDER).filter(f => f.endsWith('.csv'));
  let cleanedCalcFiles = 0;
  for (const file of calcFiles) {
    const filePath = path.join(CALC_FOLDER, file);
    const data = await readCSV(filePath);
    
    const filtered = data.filter(row => {
      if (row.datetime > '2026-06-19 15:25:00') return false;
      return true;
    });
    
    if (filtered.length < data.length) {
      await writeCSV(filePath, filtered);
      cleanedCalcFiles++;
    }
  }
  console.log('Cleaned ' + cleanedCalcFiles + ' calculated_parameter files.');
}
run().catch(console.error);


