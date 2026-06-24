import os
import glob
from contextlib import asynccontextmanager
from fastapi import FastAPI
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from database import init_db, close_db, database
import angelone_client
from symbols import symbols_map
from tick_collector import start_collector, get_active_collectors, stop_all_collectors
from process_calculations import perform_calculations
from send_predictions import run_predictions

scheduler = AsyncIOScheduler()

async def global_tick_collector():
    print("[GlobalTickCollector] Starting morning initialization...")
    try:
        await angelone_client.ensure_logged_in()
        for symbol in symbols_map.keys():
            try:
                token = await angelone_client.get_token_for_symbol(symbol)
                start_collector(symbol, token)
            except Exception as e:
                print(f"[GlobalTickCollector] Error starting {symbol}: {e}")
        print("[GlobalTickCollector] All collectors started for the day.")
    except Exception as e:
        print(f"[GlobalTickCollector] Fatal error: {e}")

async def cleanup_logs():
    print("[LogCleanup] Automatically deleting prediction logs...")
    logs_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "prediction logs")
    if os.path.exists(logs_dir):
        files = glob.glob(os.path.join(logs_dir, "*.log"))
        for f in files:
            try:
                os.remove(f)
            except Exception as e:
                print(f"Failed to delete {f}: {e}")
        print("[LogCleanup] prediction logs directory cleared.")

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    print("Initializing Database...")
    await init_db()
    
    # Schedule cron jobs
    # 1. Start tick collector at 09:15 AM Monday-Friday
    scheduler.add_job(global_tick_collector, CronTrigger(day_of_week='mon-fri', hour=9, minute=15))
    
    # 2. Stop tick collector at 15:30 PM Monday-Friday
    scheduler.add_job(stop_all_collectors, CronTrigger(day_of_week='mon-fri', hour=15, minute=30))
    
    # 3. Send Predictions at 09:20 AM Monday-Friday
    scheduler.add_job(run_predictions, CronTrigger(day_of_week='mon-fri', hour=9, minute=20))
    
    # 4. Process calculations at 15:45 PM Monday-Friday
    scheduler.add_job(perform_calculations, CronTrigger(day_of_week='mon-fri', hour=15, minute=45))
    
    # 5. Cleanup logs at 15:45 PM Monday-Friday
    scheduler.add_job(cleanup_logs, CronTrigger(day_of_week='mon-fri', hour=15, minute=45))
    
    scheduler.start()
    print("APScheduler started with cron jobs registered.")
    
    yield
    
    # Shutdown
    print("Shutting down...")
    stop_all_collectors()
    scheduler.shutdown()
    await close_db()

app = FastAPI(lifespan=lifespan)

@app.get("/health")
async def health_check():
    return {"status": "ok", "message": "FastAPI trading backend is running."}

@app.get("/api/status")
async def get_status():
    return {
        "active_collectors_count": len(get_active_collectors()),
        "symbols": get_active_collectors()
    }

@app.get("/api/predictResult")
async def predict_result():
    query = """
        SELECT symbol, tick_data, response_data, created_at 
        FROM prediction_logs 
        WHERE DATE(created_at) = CURRENT_DATE 
        ORDER BY created_at DESC
    """
    rows = await database.fetch_all(query=query)
    
    # Optional: logic to parse JSON response and format output as requested by frontend
    # For now, return raw rows translated to dicts
    results = [dict(r) for r in rows]
    return {"status": "success", "data": results}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=3000, reload=True)
