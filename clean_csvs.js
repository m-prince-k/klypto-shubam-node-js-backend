const fs = require('fs');
const path = require('path');

const HIST_FOLDER = path.join(__dirname, 'historical_csv');
const files = fs.readdirSync(HIST_FOLDER).filter(f => f.endsWith('.csv'));

for (const file of files) {
  const filePath = path.join(HIST_FOLDER, file);
  try {
    const content = fs.readFileSync(filePath, 'utf-8');
    const lines = content.split('\n');
    const newLines = [];
    
    for (let i = 0; i < lines.length; i++) {
      const line = lines[i].trim();
      if (!line) continue;
      
      // Header
      if (i === 0 || !line.match(/^\d{4}-\d{2}-\d{2}/)) {
        newLines.push(line);
        continue;
      }
      
      const parts = line.split(',');
      const datetime = parts[0]; // e.g., "2026-06-19 18:55:00"
      const time = datetime.split(' ')[1];
      
      if (time >= "09:15:00" && time <= "15:25:00") {
        newLines.push(line);
      }
    }
    
    const uniqueLines = [];
    const seenDates = new Set();
    
    uniqueLines.push(newLines[0]);
    
    for (let i = 1; i < newLines.length; i++) {
      const line = newLines[i];
      const dt = line.split(',')[0];
      if (!seenDates.has(dt)) {
        seenDates.add(dt);
        uniqueLines.push(line);
      }
    }

    fs.writeFileSync(filePath, uniqueLines.join('\n') + '\n');
  } catch (err) {
    console.log(`Skipped ${file} due to error: ${err.message}`);
  }
}
console.log('Cleaned ' + files.length + ' CSV files');
