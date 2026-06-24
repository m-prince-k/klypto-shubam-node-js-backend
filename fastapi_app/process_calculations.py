import asyncio
import json
import os
from datetime import datetime, timedelta
import angelone_client
from database import database, format_timestamp
from calculate_parameters import generate_payload

async def fetch_with_retry(symbol: str, token: str, interval: str, fromdate: str, todate: str, retries: int = 3):
    for i in range(retries):
        try:
            return await angelone_client.fetch_historical_candles(symbol, token, interval, fromdate, todate)
        except Exception as e:
            print(f"[Retry {i+1}/{retries}] AngelOne API error for {symbol}: {e}")
            if i < retries - 1:
                await asyncio.sleep(5)
    return None

async def get_raw_data_from_db(symbol: str):
    query = "SELECT datetime, open, high, low, close, volume FROM historical_candles WHERE symbol = :symbol ORDER BY datetime ASC"
    rows = await database.fetch_all(query=query, values={"symbol": symbol})
    
    return [{
        "datetime": format_timestamp(r["datetime"]),
        "exchange_code": "NSE",
        "stock_code": symbol,
        "open": float(r["open"]),
        "high": float(r["high"]),
        "low": float(r["low"]),
        "close": float(r["close"]),
        "volume": int(r["volume"] or 0)
    } for r in rows]

def pad(n: int) -> str:
    return str(n).zfill(2)

async def fill_gap_for_symbol(symbol: str, raw_data: list, to_date_str: str):
    if not raw_data:
        return raw_data
        
    last_row = raw_data[-1]
    last_dt_str = last_row["datetime"]
    
    try:
        d = datetime.strptime(last_dt_str, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return raw_data
        
    from_date_str = d.strftime("%Y-%m-%d %H:%M")
    
    if from_date_str >= to_date_str:
        return raw_data
        
    try:
        token = await angelone_client.get_token_for_symbol(symbol)
        if not token:
            return raw_data
            
        hist = await fetch_with_retry(symbol, token, "FIVE_MINUTE", from_date_str, to_date_str)
        
        if hist and hist.get("data"):
            inserted = 0
            for candle in hist["data"]:
                # candle is [timestamp, open, high, low, close, volume]
                candle_date = datetime.fromisoformat(candle[0].replace('+', '+00:00')[:19])
                candle_dt_str = candle_date.strftime("%Y-%m-%d %H:%M:%S")
                time_str = candle_date.strftime("%H:%M:%S")
                
                if candle_date > d and "09:15:00" <= time_str <= "15:25:00":
                    new_row = {
                        "datetime": candle_dt_str,
                        "exchange_code": "NSE",
                        "stock_code": symbol,
                        "open": float(candle[1]),
                        "high": float(candle[2]),
                        "low": float(candle[3]),
                        "close": float(candle[4]),
                        "volume": int(candle[5])
                    }
                    raw_data.append(new_row)
                    
                    query = """
                      INSERT INTO historical_candles (symbol, datetime, open, high, low, close, volume) 
                      VALUES (:symbol, :datetime, :open, :high, :low, :close, :volume)
                      ON CONFLICT(symbol, datetime) DO NOTHING
                    """
                    values = {
                        "symbol": symbol, "datetime": candle_dt_str, "open": new_row["open"],
                        "high": new_row["high"], "low": new_row["low"], "close": new_row["close"], "volume": new_row["volume"]
                    }
                    await database.execute(query=query, values=values)
                    inserted += 1
                    
            if inserted > 0:
                print(f"Filled gaps for {symbol}: inserted {inserted} new candles up to {to_date_str}")
    except Exception as e:
        print(f"Could not fill gap for {symbol}: {e}")
        
    return raw_data

async def process_symbol(symbol: str, target_date_str: str):
    try:
        raw_data = await get_raw_data_from_db(symbol)
        max_to_date_str = f"{target_date_str} 15:25"
        
        raw_data = await fill_gap_for_symbol(symbol, raw_data, max_to_date_str)
        
        # Filter strictly to 09:15:00 - 15:25:00
        filtered_data = [row for row in raw_data if "09:15:00" <= row["datetime"].split(" ")[1] <= "15:25:00"]
        
        processed = generate_payload(filtered_data)
        target_payload = processed[-300:]
        
        query = """
          INSERT INTO symbol_payloads (symbol, historic_data, updated_at) 
          VALUES (:symbol, :historic_data, CURRENT_TIMESTAMP)
          ON CONFLICT(symbol) DO UPDATE SET historic_data = EXCLUDED.historic_data, updated_at = CURRENT_TIMESTAMP
        """
        values = {
            "symbol": symbol,
            "historic_data": json.dumps({"historic_data": target_payload})
        }
        await database.execute(query=query, values=values)
        
        print(f"Processed {symbol}: Payload has exactly {len(target_payload)} candles ending at {target_date_str} 15:25")
    except Exception as e:
        print(f"Error processing {symbol}: {e}")

async def perform_calculations():
    print("Starting perform_calculations()")
    res = await database.fetch_all("SELECT DISTINCT symbol FROM historical_candles")
    symbols = [r["symbol"] for r in res]
    
    res_payloads = await database.fetch_all("SELECT symbol FROM symbol_payloads WHERE DATE(updated_at) = CURRENT_DATE")
    processed_symbols = {r["symbol"] for r in res_payloads}
    
    remaining = [s for s in symbols if s not in processed_symbols]
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Found {len(symbols)} total symbols. {len(processed_symbols)} already processed today. Starting Deep Scan for remaining {len(remaining)}...")
    
    now = datetime.now()
    target_date_str = now.strftime("%Y-%m-%d")
    
    BATCH_SIZE = 5
    for i in range(0, len(remaining), BATCH_SIZE):
        batch = remaining[i:i+BATCH_SIZE]
        for symbol in batch:
            await process_symbol(symbol, target_date_str)
            await asyncio.sleep(1.5)
            
    print("\n=================================================")
    print("✅ [DEEP SCAN COMPLETE] symbol_payloads has been fully updated for today!")
    print("=================================================\n")

# Note: The cron scheduling will be handled centrally by APScheduler in main.py, 
# so we don't need the infinite while-loop here.
