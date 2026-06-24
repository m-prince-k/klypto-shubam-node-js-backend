import asyncio
import time
import json
import os
from datetime import datetime, timedelta
import angelone_client
from database import insert_tick, format_timestamp, get_five_minute_bucket

collectors = {}
last_ticks = {}
POLL_INTERVAL_MS = 1000
MAX_COLLECTOR_RUN_MS = 2 * 60 * 1000

async def collector_task(symbol: str, token: str):
    key = symbol.upper()
    started_at = time.time() * 1000
    
    while True:
        if key not in collectors:
            break
            
        if (time.time() * 1000) - started_at > MAX_COLLECTOR_RUN_MS:
            print(f"[TickCollector/{key}] Auto-stopping after {MAX_COLLECTOR_RUN_MS / 1000}s")
            stop_collector(key)
            break
            
        try:
            # We call fetchMarketData manually via aiohttp here or through angelone_client
            # Wait, angelone_client doesn't have fetchMarketData exported. I need to use the endpoint.
            await angelone_client.ensure_logged_in()
            import aiohttp
            
            headers = {
                "Content-Type": "application/json",
                "Accept": "application/json",
                "X-UserType": "USER",
                "X-SourceID": "WEB",
                "X-ClientLocalIP": "127.0.0.1",
                "X-ClientPublicIP": "127.0.0.1",
                "X-MACAddress": "01-01-01-01-01-01",
                "X-PrivateKey": angelone_client.ANGELONE_CLIENT_SECRET,
                "Authorization": f"Bearer {angelone_client.jwt_token}",
            }
            payload = {
                "exchange": "NSE",
                "tradingsymbol": f"{key}-EQ",
                "symboltoken": token
            }
            async with aiohttp.ClientSession() as session:
                async with session.post("https://apiconnect.angelbroking.com/rest/secure/angelbroking/order/v1/getLtpData", json=payload, headers=headers) as resp:
                    resp_data = await resp.json()
            
            tick = None
            if resp_data and resp_data.get("data"):
                tick = resp_data["data"]
                
            if tick and (tick.get("ltp") is not None or tick.get("close") is not None):
                now = datetime.now()
                raw_time = tick.get("exchangeTime", tick.get("lastTradeTime", format_timestamp(now)))
                price = float(tick.get("ltp", tick.get("close", 0)))
                open_p = float(tick.get("open", price))
                high = float(tick.get("high", price))
                low = float(tick.get("low", price))
                close = float(tick.get("close", price))
                volume = int(tick.get("lastTradedQuantity", tick.get("volume", 0)))
                
                last = last_ticks.get(key)
                if last and last["close"] == close and last["volume"] == volume and last["open"] == open_p and last["high"] == high and last["low"] == low:
                    pass
                else:
                    last_ticks[key] = {"open": open_p, "high": high, "low": low, "close": close, "volume": volume}
                    await insert_tick(key, open_p, high, low, close, volume, raw_time)
                    msg = f"[TickCollector] Received Live Tick -> {key}: Open={open_p}, High={high}, Low={low}, Close={close}, Vol={volume}, Time={raw_time}"
                    print(msg)
                    with open(os.path.join(os.path.dirname(__file__), 'tick_collector.log'), 'a') as f:
                        f.write(f"[{datetime.now().isoformat()}] {msg}\n")
                        
        except Exception as e:
            pass # suppress transient network errors
            
        await asyncio.sleep(POLL_INTERVAL_MS / 1000.0)

def start_collector(symbol: str, token: str):
    key = symbol.upper()
    if key in collectors:
        return False
        
    print(f"[TickCollector] Starting collector for {key} (token: {token}, interval: {POLL_INTERVAL_MS}ms)")
    task = asyncio.create_task(collector_task(key, token))
    collectors[key] = {
        "symbol": key,
        "token": token,
        "task": task,
        "startedAt": time.time() * 1000
    }
    return True

def stop_collector(symbol: str):
    key = symbol.upper()
    if key in collectors:
        collectors[key]["task"].cancel()
        del collectors[key]
        print(f"[TickCollector] Stopped collector for {key}")
        return True
    return False

def is_collector_running(symbol: str):
    return symbol.upper() in collectors

def get_active_collectors():
    return list(collectors.keys())

def stop_all_collectors():
    keys = list(collectors.keys())
    for key in keys:
        stop_collector(key)
