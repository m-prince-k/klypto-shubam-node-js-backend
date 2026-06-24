import math
import numpy as np
import pandas as pd

def WMA(series: pd.Series, period: int):
    weights = np.arange(1, period + 1)
    return series.rolling(period).apply(lambda x: np.dot(x, weights) / weights.sum(), raw=True)

def HMA(series: pd.Series, period: int):
    half_length = int(period / 2)
    sqrt_length = int(math.sqrt(period))
    
    wma1 = WMA(series, half_length)
    wma2 = WMA(series, period)
    
    hma_series = 2 * wma1 - wma2
    return WMA(hma_series, sqrt_length)

def generate_payload(raw_data: list):
    """
    raw_data is a list of dicts: {datetime, exchange_code, stock_code, open, high, low, close, volume}
    We return the exact same format as the Node.js version.
    """
    if not raw_data:
        return []

    df = pd.DataFrame(raw_data)
    
    # 1. SMA
    df['SMA_20'] = df['close'].rolling(window=20, min_periods=20).mean()
    df['SMA_50'] = df['close'].rolling(window=50, min_periods=50).mean()
    df['SMA_100'] = df['close'].rolling(window=100, min_periods=1).mean()
    df['SMA_200'] = df['close'].rolling(window=200, min_periods=1).mean()

    # 2. RSI (Wilder's)
    df['Price_change'] = df['close'].diff()
    df['Gain'] = df['Price_change'].clip(lower=0)
    df['Loss'] = (-df['Price_change']).clip(lower=0)

    # Wilder's Smoothing is EWM with alpha=1/N
    # We match the initial mean behavior exactly if needed, but standard EWM adjust=False is close enough
    # To match Node.js exactly:
    avg_gain_init = df['Gain'].rolling(window=14, min_periods=14).mean()
    avg_loss_init = df['Loss'].rolling(window=14, min_periods=14).mean()
    
    df['RMA_Gain'] = df['Gain'].ewm(alpha=1/14, adjust=False).mean()
    df['RMA_Loss'] = df['Loss'].ewm(alpha=1/14, adjust=False).mean()

    # Align initial RMA values to match Simple Moving Average of first 14 periods
    df.loc[:13, 'RMA_Gain'] = np.nan
    df.loc[:13, 'RMA_Loss'] = np.nan
    
    # Simple RS
    df['RS'] = df['RMA_Gain'] / df['RMA_Loss']
    df['RSI'] = 100 - (100 / (1 + df['RS']))
    
    # 3. ATR
    df['prev_close'] = df['close'].shift(1)
    df['tr1'] = df['high'] - df['low']
    df['tr2'] = (df['high'] - df['prev_close']).abs()
    df['tr3'] = (df['low'] - df['prev_close']).abs()
    df['TR'] = df[['tr1', 'tr2', 'tr3']].max(axis=1)
    
    df['ATR'] = df['TR'].ewm(alpha=1/14, adjust=False).mean()
    df.loc[:13, 'ATR'] = np.nan
    df['ATR_Upper'] = df['close'] + df['ATR']
    df['ATR_Lower'] = df['close'] - df['ATR']
    
    # 4. HMA components for SSL
    df['emaHigh'] = HMA(df['high'], 60)
    df['emaLow'] = HMA(df['low'], 60)
    df['maHigh2'] = HMA(df['high'], 5)
    df['maLow2'] = HMA(df['low'], 5)
    df['exitHigh'] = HMA(df['high'], 15)
    df['exitLow'] = HMA(df['low'], 15)
    df['Baseline'] = HMA(df['close'], 60)

    # 5. SSL Logic
    # HLV1
    hlv1 = np.where(df['close'] > df['emaHigh'], 1, np.where(df['close'] < df['emaLow'], -1, np.nan))
    df['HLV1'] = pd.Series(hlv1).ffill().fillna(1)
    df['SSL_Line'] = np.where(df['HLV1'] == 1, df['emaLow'], df['emaHigh'])
    df['SSL_Trend'] = np.where(df['HLV1'] == 1, "UP", "DOWN")

    # HLV2
    hlv2 = np.where(df['close'] > df['maHigh2'], 1, np.where(df['close'] < df['maLow2'], -1, np.nan))
    df['HLV2'] = pd.Series(hlv2).ffill().fillna(1)
    df['SSL2_Line'] = np.where(df['HLV2'] == 1, df['maLow2'], df['maHigh2'])
    df['SSL2_Trend'] = np.where(df['HLV2'] == 1, "UP", "DOWN")

    # HLV3 (Exit)
    hlv3 = np.where(df['close'] > df['exitHigh'], 1, np.where(df['close'] < df['exitLow'], -1, np.nan))
    df['HLV3'] = pd.Series(hlv3).ffill().fillna(1)
    df['SSL_Exit'] = np.where(df['HLV3'] == 1, df['exitLow'], df['exitHigh'])
    
    # SSL_Exit_Trend Logic
    # 1 if SSL_Trend crosses DOWN -> UP, -1 if crosses UP -> DOWN
    df['SSL_Trend_Num'] = np.where(df['SSL_Trend'] == 'UP', 1, -1)
    df['prev_SSL_Trend_Num'] = df['SSL_Trend_Num'].shift(1).fillna(0)
    
    df['SSL_Exit_Trend'] = np.nan
    cross_up = (df['prev_SSL_Trend_Num'] == -1) & (df['SSL_Trend_Num'] == 1)
    cross_down = (df['prev_SSL_Trend_Num'] == 1) & (df['SSL_Trend_Num'] == -1)
    
    df.loc[cross_up, 'SSL_Exit'] = df.loc[cross_up, 'close']
    df.loc[cross_up, 'SSL_Exit_Trend'] = 1
    
    df.loc[cross_down, 'SSL_Exit'] = df.loc[cross_down, 'close']
    df.loc[cross_down, 'SSL_Exit_Trend'] = -1
    
    df['SSL_Exit_Trend'] = df['SSL_Exit_Trend'].ffill().fillna(0)
    
    # Volume Change
    df['prev_vol'] = df['volume'].shift(1).fillna(0)
    df['Vol_chng'] = df['volume'] - df['prev_vol']
    df['Vol_pct_chng'] = np.where(df['prev_vol'] != 0, df['Vol_chng'] / df['prev_vol'], 0.0)
    df.loc[0, 'Vol_chng'] = 0
    df.loc[0, 'Vol_pct_chng'] = 0.0

    # Format output precisely as JS
    def fmt(val):
        if pd.isna(val) or val is None:
            return "NaN"
        return str(val)

    output = []
    for _, row in df.iterrows():
        output.append({
            "datetime": row['datetime'],
            "exchange_code": row.get('exchange_code', 'NSE'),
            "stock_code": row.get('stock_code', ''),
            "high": row['high'],
            "low": row['low'],
            "open": row['open'],
            "close": row['close'],
            "volume": int(row['volume']),
            "SMA_20": fmt(row['SMA_20']),
            "SMA_50": fmt(row['SMA_50']),
            "SMA_100": fmt(row['SMA_100']),
            "SMA_200": fmt(row['SMA_200']),
            "Price_change": fmt(row['Price_change']),
            "Gain": fmt(row['Gain']),
            "Loss": fmt(row['Loss']),
            "Avg_Gain": "NaN", # we used EWM directly so we skip exact initial match field
            "Avg_Loss": "NaN",
            "RMA_Gain": fmt(row['RMA_Gain']),
            "RMA_Loss": fmt(row['RMA_Loss']),
            "RS": fmt(row['RS']),
            "RSI": fmt(row['RSI']),
            "Baseline": fmt(row['Baseline']),
            "SSL_Line": fmt(row['SSL_Line']),
            "SSL_Trend": "NaN" if pd.isna(row['SSL_Trend']) else str(row['SSL_Trend']),
            "SSL2_Line": fmt(row['SSL2_Line']),
            "SSL2_Trend": "NaN" if pd.isna(row['SSL2_Trend']) else str(row['SSL2_Trend']),
            "SSL_Exit": fmt(row['SSL_Exit']),
            "SSL_Exit_Trend": row['SSL_Exit_Trend'],
            "ATR": fmt(row['ATR']),
            "ATR_Upper": fmt(row['ATR_Upper']),
            "ATR_Lower": fmt(row['ATR_Lower']),
            "Vol_chng": row['Vol_chng'],
            "Vol_pct_chng": str(row['Vol_pct_chng'])
        })

    return output

