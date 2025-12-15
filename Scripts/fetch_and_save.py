import requests
import pandas as pd
import json
from datetime import datetime
import pytz
import os

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0'
}

# TradingView API base URL
TV_BASE_URL = "https://scanner.tradingview.com/symbol?symbol={symbol}&fields=close[1],change_abs,price_52_week_high,price_52_week_low,close,change&no_404=true&label-product=symbols-performance"

# TradingView symbols mapping
TV_SYMBOLS = {
    "USD/INR": "FX_IDC:USDINR",
    "GIFT-NIFTY": "NSEIX:NIFTY1!",
    "GOLD": "MCX:GOLD1!",
    "SILVER": "MCX:SILVER1!",
    "IND 5Y": "TVC:IN05Y",
    "IND 10Y": "TVC:IN10Y",
    "IND 30Y": "TVC:IN30Y"
}

# FIRST API: Fetch all indices data from NSE (maintained as per your requirement)
url_indices = "https://www.nseindia.com/api/allIndices"
response_indices = requests.get(url_indices, headers=headers)
data_indices = response_indices.json()

# SECOND API: Fetch market status data from NSE
url_market = "https://www.nseindia.com/api/marketStatus"
response_market = requests.get(url_market, headers=headers)
data_market = response_market.json()

# Target indices list
target_indices = [
    "NIFTY 50",
    "INDIA VIX",
    "GIFT-NIFTY",
    "USD/INR",
    "GOLD",
    "SILVER",
    "IND 5Y",
    "IND 10Y",
    "IND 30Y",
    "NIFTY NEXT 50",
    "NIFTY MIDCAP SELECT",
    "NIFTY MIDCAP 50",
    "NIFTY SMALLCAP 50",
    "NIFTY 500",
    "NIFTY ALPHA 50",
    "NIFTY IT",
    "NIFTY BANK",
    "NIFTY FINANCIAL SERVICES",
    "NIFTY PSU BANK",
    "NIFTY PRIVATE BANK",
    "NIFTY FMCG",
    "NIFTY CONSUMER DURABLES",
    "NIFTY PHARMA",
    "NIFTY HEALTHCARE INDEX",
    "NIFTY METAL",
    "NIFTY AUTO",
    "NIFTY SERVICES SECTOR",
    "NIFTY OIL & GAS",
    "NIFTY CHEMICALS",
    "NIFTY COMMODITIES",
    "NIFTY INDIA CONSUMPTION",
    "NIFTY PSE"
]

index_dict = {}

# Helper function to fetch data from TradingView API
def fetch_tradingview_data(symbol_name, tv_symbol):
    """Fetch data from TradingView API for the given symbol"""
    url = TV_BASE_URL.format(symbol=tv_symbol)
    
    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code == 200:
            data = response.json()
            
            # Extract required fields
            close = data.get('close', '-')
            change_abs = data.get('change_abs', '-')
            change = data.get('change', '-')
            prev_close = data.get('close[1]', '-')
            year_high = data.get('price_52_week_high', '-')
            year_low = data.get('price_52_week_low', '-')
            
            # Format percentage change
            if change != '-':
                percent_change_str = f"{change:.2f}%" if isinstance(change, (int, float)) else f"{change}%"
            else:
                percent_change_str = "-"
            
            return {
                'Index': symbol_name,
                'LTP': round(close, 2) if isinstance(close, (int, float)) else close,
                'Chng': round(change_abs, 2) if isinstance(change_abs, (int, float)) else change_abs,
                '% Chng': percent_change_str,
                'Previous': round(prev_close, 2) if isinstance(prev_close, (int, float)) else prev_close,
                'Adv:Dec': '-',
                'Yr Hi': round(year_high, 2) if isinstance(year_high, (int, float)) else year_high,
                'Yr Lo': round(year_low, 2) if isinstance(year_low, (int, float)) else year_low
            }
        else:
            print(f"Error fetching {symbol_name}: Status code {response.status_code}")
            return None
    except Exception as e:
        print(f"Error fetching {symbol_name}: {str(e)}")
        return None

# Fetch TradingView data for specified symbols
for index_name, tv_symbol in TV_SYMBOLS.items():
    print(f"Fetching {index_name} data from TradingView...")
    tv_data = fetch_tradingview_data(index_name, tv_symbol)
    if tv_data:
        index_dict[index_name] = tv_data

# Process all indices data from NSE for non-TradingView indices
for item in data_indices['data']:
    index_name = item.get('index')
    
    # Skip indices already fetched from TradingView
    if index_name in TV_SYMBOLS:
        continue
    
    if index_name in target_indices:
        try:
            advances = int(item.get('advances', 0))
        except (ValueError, TypeError):
            advances = 0
        
        try:
            declines = int(item.get('declines', 0))
        except (ValueError, TypeError):
            declines = 0
        
        try:
            unchanged = int(item.get('unchanged', 0))
        except (ValueError, TypeError):
            unchanged = 0
        
        if declines != 0:
            adv_dec_ratio = advances / declines
            adv_dec_ratio_str = f"{adv_dec_ratio:.2f}"
        else:
            if advances > 0:
                adv_dec_ratio_str = "Max"
            else:
                adv_dec_ratio_str = "-"
        
        percent_change = item.get('percentChange')
        if percent_change is not None:
            percent_change_str = f"{percent_change}%"
        else:
            percent_change_str = "-"
        
        index_dict[index_name] = {
            'Index': index_name,
            'LTP': item.get('last'),
            'Chng': item.get('variation'),
            '% Chng': percent_change_str,
            'Previous': item.get('previousClose'),
            'Adv:Dec': adv_dec_ratio_str,
            'Yr Hi': item.get('yearHigh'),
            'Yr Lo': item.get('yearLow')
        }

# Create records in the specified order
records = []
for index_name in target_indices:
    if index_name in index_dict:
        records.append(index_dict[index_name])
    else:
        # Create empty entry for missing indices
        records.append({
            'Index': index_name,
            'LTP': '-',
            'Chng': '-',
            '% Chng': '-',
            'Previous': '-',
            'Adv:Dec': '-',
            'Yr Hi': '-',
            'Yr Lo': '-'
        })

# Add timestamp row
ist = pytz.timezone('Asia/Kolkata')
current_time = datetime.now(ist).strftime('%d-%b %H:%M')

# Add timestamp as last row with proper formatting
records.append({
    'Index': '',
    'LTP': '',
    'Chng': '',
    '% Chng': '',
    'Previous': '',
    'Adv:Dec': '',
    'Yr Hi': 'Updated Time:',
    'Yr Lo': current_time
})

# Save to CSV
os.makedirs('Data', exist_ok=True)
df = pd.DataFrame(records)
df.to_csv('Data/nse_all_indices.csv', index=False)

# Print summary
print(f"\nCSV created successfully! Updated at {current_time} IST")
print(f"Total indices processed: {len(index_dict)} out of {len(target_indices)}")

# Print details of TradingView data fetched
print("\nTradingView Data Fetched:")
for symbol in TV_SYMBOLS.keys():
    if symbol in index_dict:
        data = index_dict[symbol]
        print(f"  {symbol}: LTP={data['LTP']}, Change={data['Chng']}, %Change={data['% Chng']}")
    else:
        print(f"  {symbol}: Failed to fetch")
