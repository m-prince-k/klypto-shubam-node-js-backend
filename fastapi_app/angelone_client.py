import os
import time
import json
import pyotp
import aiohttp
from dotenv import load_dotenv

load_dotenv()

ANGELONE_CLIENT_ID = os.getenv("ANGEL_CLIENT_CODE")
ANGELONE_CLIENT_SECRET = os.getenv("ANGEL_API_KEY")
ANGELONE_PASSWORD = os.getenv("ANGEL_PASSWORD")
ANGELONE_TOTP = os.getenv("ANGEL_TOTP_SECRET")

jwt_token = None
jwt_token_expiry = 0
scrip_master = None

async def ensure_logged_in():
    global jwt_token, jwt_token_expiry
    if jwt_token and time.time() * 1000 < jwt_token_expiry:
        return

    if not all([ANGELONE_CLIENT_ID, ANGELONE_PASSWORD, ANGELONE_CLIENT_SECRET, ANGELONE_TOTP]):
        raise Exception("Missing Angel One credentials. Set ANGEL_CLIENT_CODE, ANGEL_PASSWORD, ANGEL_API_KEY, and ANGEL_TOTP_SECRET.")

    totp_code = pyotp.TOTP(ANGELONE_TOTP).now()
    
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "X-UserType": "USER",
        "X-SourceID": "WEB",
        "X-ClientLocalIP": "127.0.0.1",
        "X-ClientPublicIP": "127.0.0.1",
        "X-MACAddress": "01-01-01-01-01-01",
        "X-PrivateKey": ANGELONE_CLIENT_SECRET,
    }
    
    payload = {
        "clientcode": ANGELONE_CLIENT_ID,
        "password": ANGELONE_PASSWORD,
        "totp": totp_code,
    }

    async with aiohttp.ClientSession() as session:
        async with session.post("https://apiconnect.angelbroking.com/rest/auth/angelbroking/user/v1/loginByPassword", json=payload, headers=headers) as resp:
            data = await resp.json()
            if data.get("status") and data.get("data"):
                jwt_token = data["data"]["jwtToken"]
                jwt_token_expiry = (time.time() * 1000) + (23 * 60 * 60 * 1000)
                
                # Write to jwt.txt for other modules if needed
                jwt_path = os.path.join(os.path.dirname(__file__), 'jwt.txt')
                with open(jwt_path, 'w') as f:
                    f.write(jwt_token)
                print("=> WROTE JWT TOKEN TO jwt.txt")
            else:
                raise Exception(f"Angel One Login failed: {data}")

async def get_token_for_symbol(symbol: str) -> str:
    global scrip_master
    if not scrip_master:
        print("[AngelOne] Downloading Scrip Master...")
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get("https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json") as resp:
                    scrip_master = await resp.json()
                    print("[AngelOne] Scrip Master downloaded.")
        except Exception as e:
            raise Exception(f"Failed to fetch Scrip Master: {e}")

    search_symbol = f"{symbol}-EQ"
    found = next((item for item in scrip_master if item.get("symbol") == search_symbol and item.get("exch_seg") == "NSE"), None)

    if found:
        print(f"[AngelOne] Found token {found['token']} for {symbol}")
        return found["token"]

    raise Exception(f"Token NOT FOUND for symbol {symbol}")

async def fetch_historical_candles(symbol: str, token: str, interval: str, fromdate: str, todate: str):
    global jwt_token, jwt_token_expiry
    await ensure_logged_in()

    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "X-UserType": "USER",
        "X-SourceID": "WEB",
        "X-ClientLocalIP": "127.0.0.1",
        "X-ClientPublicIP": "127.0.0.1",
        "X-MACAddress": "01-01-01-01-01-01",
        "X-PrivateKey": ANGELONE_CLIENT_SECRET,
        "Authorization": f"Bearer {jwt_token}",
    }
    
    payload = {
        "exchange": "NSE",
        "symboltoken": token,
        "interval": interval or "FIVE_MINUTE",
        "fromdate": fromdate,
        "todate": todate,
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post("https://apiconnect.angelbroking.com/rest/secure/angelbroking/historical/v1/getCandleData", json=payload, headers=headers) as resp:
                if resp.status == 401:
                    jwt_token = None
                    jwt_token_expiry = 0
                    raise Exception("Unauthorized 401. Token cleared.")
                data = await resp.json()
                return data
    except Exception as e:
        raise e
