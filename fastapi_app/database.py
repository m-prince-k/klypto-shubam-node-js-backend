import os
import datetime
from dotenv import load_dotenv
from databases import Database

load_dotenv()

DATABASE_URL = f"postgresql://{os.getenv('USER_NAME')}:{os.getenv('PASSWORD')}@{os.getenv('IP')}:{os.getenv('DB_PORT', '5432')}/{os.getenv('DB_NAME')}"

# Initialize Database instance
database = Database(DATABASE_URL)

async def init_db():
    try:
        await database.connect()
        
        # Create tables using direct execution
        queries = """
        CREATE TABLE IF NOT EXISTS candles_5m (
          id SERIAL PRIMARY KEY,
          symbol TEXT NOT NULL,
          open REAL NOT NULL,
          high REAL NOT NULL,
          low REAL NOT NULL,
          close REAL NOT NULL,
          volume INTEGER NOT NULL,
          timestamp TIMESTAMP NOT NULL,
          UNIQUE(symbol, timestamp)
        );
        CREATE TABLE IF NOT EXISTS ticks (
          id SERIAL PRIMARY KEY,
          symbol TEXT NOT NULL,
          open REAL NOT NULL,
          high REAL NOT NULL,
          low REAL NOT NULL,
          close REAL NOT NULL,
          volume INTEGER NOT NULL,
          timestamp TIMESTAMP NOT NULL
        );
        CREATE TABLE IF NOT EXISTS historical_candles (
          id SERIAL PRIMARY KEY,
          symbol VARCHAR NOT NULL,
          datetime TIMESTAMP NOT NULL,
          open NUMERIC NOT NULL,
          high NUMERIC NOT NULL,
          low NUMERIC NOT NULL,
          close NUMERIC NOT NULL,
          volume BIGINT NOT NULL,
          UNIQUE(symbol, datetime)
        );
        CREATE INDEX IF NOT EXISTS idx_historical_candles_symbol ON historical_candles(symbol);

        CREATE TABLE IF NOT EXISTS symbol_payloads (
          symbol VARCHAR PRIMARY KEY,
          historic_data JSONB NOT NULL,
          updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS prediction_logs (
          id SERIAL PRIMARY KEY,
          symbol VARCHAR NOT NULL,
          tick_data JSONB,
          response_data JSONB,
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        for query in queries.split(';'):
            if query.strip():
                await database.execute(query=query)
                
    except Exception as e:
        print(f"[DB] Error initializing Postgres DB: {e}")
        raise e

async def insert_tick(symbol: str, open_p: float, high: float, low: float, close: float, volume: int, timestamp_str: str):
    query = """
        INSERT INTO ticks (symbol, open, high, low, close, volume, timestamp)
        VALUES (:symbol, :open, :high, :low, :close, :volume, :timestamp) RETURNING id;
    """
    values = {
        "symbol": symbol, "open": open_p, "high": high, "low": low, "close": close, 
        "volume": volume, "timestamp": timestamp_str
    }
    return await database.execute(query=query, values=values)

async def upsert_candle(symbol: str, open_p: float, high: float, low: float, close: float, volume: int, timestamp_str: str):
    query = """
        INSERT INTO candles_5m (symbol, open, high, low, close, volume, timestamp) 
        VALUES (:symbol, :open, :high, :low, :close, :volume, :timestamp)
        ON CONFLICT(symbol, timestamp) DO UPDATE SET 
          open=EXCLUDED.open,
          high=EXCLUDED.high,
          low=EXCLUDED.low,
          close=EXCLUDED.close,
          volume=EXCLUDED.volume
        RETURNING id;
    """
    values = {
        "symbol": symbol, "open": open_p, "high": high, "low": low, "close": close, 
        "volume": volume, "timestamp": timestamp_str
    }
    return await database.execute(query=query, values=values)

def get_five_minute_bucket(date_obj: datetime.datetime) -> datetime.datetime:
    mins = (date_obj.minute // 5) * 5
    return date_obj.replace(minute=mins, second=0, microsecond=0)

def format_timestamp(date_obj: datetime.datetime) -> str:
    return date_obj.strftime("%Y-%m-%d %H:%M:%S")

async def close_db():
    if database.is_connected:
        await database.disconnect()
        print("PostgreSQL pool closed.")
