import requests, pandas as pd, os, pytz
from datetime import datetime

headers = {'User-Agent': 'Mozilla/5.0'}
commodity_symbols = [
    {"name": "Dow Jones", "symbol": "OANDA:US30USD"},
    {"name": "S&P 500", "symbol": "CME_MINI:ES1!"},
    {"name": "NASDAQ 100", "symbol": "CME_MINI:NQ1!"},
    {"name": "VIX", "symbol": "CBOE:VX1!"},
    {"name": "Dollar Index", "symbol": "TVC:DXY"},
    {"name": "US03Y", "symbol": "TVC:US03Y"},
    {"name": "US05Y", "symbol": "TVC:US05Y"},
    {"name": "US10Y", "symbol": "TVC:US10Y"},
    {"name": "US30Y", "symbol": "TVC:US30Y"},
    {"name": "Nikkei 225", "symbol": "CME:NKD1!"},
    {"name": "Euro Stoxx 50", "symbol": "TVC:SX5E"},
    {"name": "DAX", "symbol": "EUREX:FDAX1!"},
    {"name": "FTSE 100", "symbol": "TVC:UKX"},
    {"name": "Bitcoin", "symbol": "CRYPTO:BTCUSD"},
    {"name": "USD/INR", "symbol": "FX_IDC:USDINR"},
    {"name": "USD/JPY", "symbol": "OANDA:USDJPY"}
]

def format_value(value, key, name):
    if value is None: return "0"
    try:
        if key == '%': return f"{float(value):.2f}%"
        if name in ["VIX", "Dollar Index", "US03Y", "US05Y","US10Y", "US30Y","USD/INR", "USD/JPY"] and key in ['LTP', 'Chng', 'Prev.', 'Yr Hi', 'Yr Lo']:
            return f"{float(value):.2f}"
        if key in ['LTP', 'Chng', 'Prev.', 'Yr Hi', 'Yr Lo']:
            val = float(value)
            return str(int(val))
        return str(float(value))
    except: return "0"

commodity_data = []
for c in commodity_symbols:
    try:
        data = requests.get(f"https://scanner.tradingview.com/symbol?symbol={c['symbol']}&fields=close[1],change_abs,price_52_week_high,price_52_week_low,close,change&no_404=true", headers=headers, timeout=5).json()
        commodity_data.append({
            'Index': c["name"],
            'LTP': format_value(data.get('close'), 'LTP', c["name"]),
            'Chng': format_value(data.get('change_abs'), 'Chng', c["name"]),
            '%': format_value(data.get('change'), '%', c["name"]),
            'Prev.': format_value(data.get('close[1]'), 'Prev.', c["name"]),
            'Yr Hi': format_value(data.get('price_52_week_high'), 'Yr Hi', c["name"]),
            'Yr Lo': format_value(data.get('price_52_week_low'), 'Yr Lo', c["name"])
        })
    except:
        commodity_data.append({
            'Index': c["name"],
            'LTP': "0", 'Chng': "0", '%': "0.00%",
            'Prev.': "0", 'Yr Hi': "0", 'Yr Lo': "0"
        })

commodity_data.append({
    'Index': '', 'LTP': '', 'Chng': '', '%': '',
    'Prev.': '', 'Yr Hi': 'Update Time', 'Yr Lo': datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%d-%b %H:%M')
})

os.makedirs('Data', exist_ok=True)
pd.DataFrame(commodity_data).to_csv('Data/GLOBAL_DATA.csv', index=False)
