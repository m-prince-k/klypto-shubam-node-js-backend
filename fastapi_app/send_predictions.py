import os
import json
import asyncio
import aiohttp
from datetime import datetime
from database import database, format_timestamp

PREDICT_URL = "http://43.205.133.183:8000/predict"
LOGS_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "prediction logs")
os.makedirs(LOGS_DIR, exist_ok=True)

async def get_latest_tick(symbol: str):
    query = """
        SELECT open, high, low, close, volume, timestamp 
        FROM candles_5m 
        WHERE symbol = :symbol 
          AND EXTRACT(HOUR FROM timestamp) = 9 
          AND EXTRACT(MINUTE FROM timestamp) = 15
        ORDER BY timestamp DESC LIMIT 1
    """
    row = await database.fetch_one(query=query, values={"symbol": symbol})
    if row:
        return dict(row)
    return None

def log_payload(payload_log: str, msg: str):
    with open(payload_log, 'a') as f:
        f.write(f"[{datetime.now().isoformat()}] {msg}\n")

async def process_symbol(symbol: str, historic_data: list, log_file: str, payload_log: str):
    try:
        print(f"Processing {symbol}...")
        
        if not historic_data:
            print(f"  Skipping {symbol}: JSON data is empty.")
            return
            
        if len(historic_data) > 300:
            historic_data = historic_data[-300:]
            
        latest_tick = await get_latest_tick(symbol)
        
        if latest_tick:
            tick_obj = {
                "datetime": format_timestamp(latest_tick["timestamp"]),
                "open": latest_tick["open"],
                "high": latest_tick["high"],
                "low": latest_tick["low"],
                "close": latest_tick["close"],
                "volume": latest_tick["volume"]
            }
        else:
            print(f"  No live tick found for {symbol} in DB. Using last historical candle.")
            last_candle = historic_data[-1]
            tick_obj = {
                "datetime": last_candle["datetime"],
                "open": last_candle["open"],
                "high": last_candle["high"],
                "low": last_candle["low"],
                "close": last_candle["close"],
                "volume": last_candle["volume"]
            }
            
        payload = {
            "historic_data": historic_data,
            "tick": tick_obj
        }
        
        log_payload(payload_log, f"[{symbol}] Retrieved tick from DB: {latest_tick or 'None'}")
        log_payload(payload_log, f"[{symbol}] Sending payload with tickObj: {json.dumps(tick_obj)}")
        print(f"  [{symbol}] Payload being sent (historic_data length: {len(historic_data)}, tick: {json.dumps(tick_obj)})")
        log_payload(payload_log, f"[{symbol}] Full payload being sent: {json.dumps(payload)}")
        
        max_retries = 3
        response_data = None
        
        async with aiohttp.ClientSession() as session:
            for attempt in range(1, max_retries + 1):
                try:
                    async with session.post(PREDICT_URL, json=payload, timeout=30) as resp:
                        response_data = await resp.json()
                        break
                except Exception as e:
                    if attempt >= max_retries:
                        raise Exception(f"Failed after {max_retries} attempts: {e}")
                    print(f"  [{symbol}] Error sending payload ({e}), retrying attempt {attempt}/{max_retries}...")
                    await asyncio.sleep(2)
                    
        # Log response to Database
        query = """
          INSERT INTO prediction_logs (symbol, tick_data, response_data, created_at)
          VALUES (:symbol, :tick_data, :response_data, CURRENT_TIMESTAMP)
        """
        values = {
            "symbol": symbol,
            "tick_data": json.dumps(tick_obj),
            "response_data": json.dumps(response_data)
        }
        await database.execute(query=query, values=values)
        
        log_msg = f"[{symbol}] Payload sent. Tick: {json.dumps(tick_obj)} Response: {json.dumps(response_data)}\n"
        with open(log_file, 'a') as f:
            f.write(log_msg)
            
        print(f"  Success. Signal: {json.dumps(response_data)}")
        
    except Exception as err:
        log_err = f"[{symbol}] Error sending payload: {err}\n"
        with open(log_file, 'a') as f:
            f.write(log_err)
        print(f"  Error: {err}")

async def run_predictions():
    now = datetime.now()
    time_str = now.strftime("%H%M")
    
    log_file = os.path.join(LOGS_DIR, f"prediction{time_str}.log")
    payload_log = os.path.join(LOGS_DIR, f"prediction_payloads{time_str}.log")
    
    print("Starting prediction job...")
    
    res = await database.fetch_all("SELECT symbol, historic_data FROM symbol_payloads")
    print(f"Found {len(res)} payload records in DB.")
    
    with open(log_file, 'w') as f:
        f.write(f"--- Prediction Run at {now.isoformat()} ---\n")
        
    BATCH_SIZE = 20
    for i in range(0, len(res), BATCH_SIZE):
        batch = res[i:i+BATCH_SIZE]
        tasks = []
        for record in batch:
            hist = json.loads(record["historic_data"])
            hist_data = hist.get("historic_data", hist)
            tasks.append(process_symbol(record["symbol"], hist_data, log_file, payload_log))
        await asyncio.gather(*tasks)
        
    print("Finished prediction job iteration.")

# Cron is handled by APScheduler in main.py
