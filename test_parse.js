const fs = require('fs');
const path = require('path');

const logsDir = path.join(__dirname, "prediction logs");
const filePath = path.join(logsDir, "prediction920.log");
const content = fs.readFileSync(filePath, "utf-8");
const lines = content.split("\n");

const results = [];

for (const line of lines) {
  if (!line.trim()) continue;
  const match = line.match(
    /^\[(.*?)\] Payload sent\. Tick: (.*?) Response: (.*)$/,
  );
  if (match) {
    const symbol = match[1];
    try {
      const tick = JSON.parse(match[2]);
      const response = JSON.parse(match[3]);

      if (
        response &&
        response.signal !== null &&
        response.signal !== undefined
      ) {
        results.push({
          symbol,
          tick,
          response,
        });
      }
    } catch (e) {
      console.log(`Error parsing for ${symbol}:`, e.message, "Line:", line.substring(0, 100));
    }
  } else {
    // console.log("No match:", line.substring(0, 100));
  }
}

console.log("Total results:", results.length);
