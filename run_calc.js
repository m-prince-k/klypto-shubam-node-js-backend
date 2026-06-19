const p = require('./process_calculations.js');
console.log('Running calculations...');
p.emptyFolder(p.OUT_CSV_FOLDER);
p.emptyFolder(p.OUT_JSON_FOLDER);
p.performCalculations().then(() => {
    console.log('Done running calculations!');
}).catch(console.error);
