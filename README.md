# Option Chain Backend (klypto-shubam-node-js-backend)

A Node.js backend service designed for live stock market data collection, historical data management, and strategy prediction integration using the Angel One SmartAPI.

## Features

- **Live Market Data Collection**: Continuously fetches live ticks for configured stock symbols using the Angel One SmartAPI.
- **Historical Data Management**: Maintains historical CSV data for various stocks, automatically filling missing gaps.
- **Prediction Integration**: Formats and forwards aggregated 5-minute candle data and live ticks to an external Python prediction engine.
- **Post-Market Calculations**: Runs a daily script at 15:45 to process all historical data and compute technical indicators.
- **Database Storage**: Uses SQLite for high-speed live tick recording and PostgreSQL for persistent storage.

## Project Structure

- `index.js`: Main Express server handling authentication and primary API routes for forwarding prediction requests.
- `live-predict.js`: Live prediction engine that manages continuous background tick collection, gap filling, and exposes live prediction endpoints.
- `tick-collector.js`: Module responsible for polling Angel One API for live ticks and storing them into the SQLite database.
- `process_calculations.js`: Post-market script that runs daily. It reads historical CSVs, fills data gaps, calculates technical parameters, and outputs the results.
- `angelone-client.js`: Helper module abstracting Angel One API calls (login, fetch tokens, market data, historical candles).
- `db.js`: Database configuration for SQLite (`live_ticks.db`) and PostgreSQL.
- `calculate_parameters.js`: Utility for calculating technical indicators on historical data.

### Important Directories

- `historical_csv/`: Raw historical 5-minute candle data for stocks.
- `calculated_parameters/`: Output folder for processed CSV files with technical indicators.
- `extractJson/`: Output folder containing the last 300 rows of processed data in JSON format.
- `prediction logs/`: Directory where prediction request/response logs are stored.

```

## Installation

1. Install dependencies:
   ```bash
   npm install
   ```

2. Set up the `.env` file as described above.

3. Make sure the database files and required directories exist (the app handles creation for some, but ensure permissions are correct).

## Scripts

- **`npm start`**: Starts the main Express server (`index.js`) using nodemon.
- **`npm run live`**: Starts the live prediction engine (`live-predict.js`).
- **`npm run live:dev`**: Starts the live prediction engine using nodemon for development.

## API Endpoints

- **`POST /api/strategy/predict`** (in `index.js`): Fetches historical and live data for a given symbol and forwards it to the Python prediction server.
- **`GET /api/strategy/predict`** (in `live-predict.js`): Triggers the live predict workflow, handling live ticks and historical fallback.
- **`GET /api/predictResult`**: Retrieves the latest prediction results from the log files.
- **`GET /health`**: Returns the health status and active collectors.
- **`GET /api/status`**: Returns the active tick collectors and token cache.

## Technologies Used

- **Node.js & Express**: Core framework.
- **Axios**: For making HTTP requests to Angel One API and the prediction server.
- **SQLite3 & PostgreSQL**: Database storage for live ticks.
- **CSV Parser**: For reading and managing historical data.
- **otplib / totp-generator**: For generating Angel One TOTP tokens.
