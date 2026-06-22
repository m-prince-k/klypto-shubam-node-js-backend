const { performCalculations } = require('./process_calculations.js');

async function run() {
    console.log("Starting calculations...");
    await performCalculations();
    console.log("Calculations done.");
    process.exit(0);
}

run().catch(err => {
    console.error(err);
    process.exit(1);
});
