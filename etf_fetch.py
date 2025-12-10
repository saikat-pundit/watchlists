import requests
import pandas as pd
from datetime import datetime
import pytz

# Headers to mimic a browser request
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0'
}

# URL for ETF data
url = "https://www.nseindia.com/api/etf"

try:
    response = requests.get(url, headers=headers)
    response.raise_for_status()  # Raise an error for bad status codes
    data = response.json()
except requests.exceptions.RequestException as e:
    print(f"Error fetching data: {e}")
    exit(1)

# List to store extracted records
records = []

# Process each ETF in the data
for item in data.get('data', []):
    # Extract required fields with fallback values
    symbol = item.get('symbol', '-')
    assets = item.get('assets', '-')
    
    # Get the last traded price
    ltP = item.get('ltP', '-')
    
    # Get the change amount and percentage
    chn = item.get('chn', '-')
    per = item.get('per', '-')
    
    # Format percentage change with % sign if it's a valid number
    if per != '-' and per is not None:
        try:
            percent_change_str = f"{per}%"
        except (ValueError, TypeError):
            percent_change_str = "-"
    else:
        percent_change_str = "-"
    
    # Get previous close and 52-week high/low
    prev_close = item.get('prevClose', '-')
    wkhi = item.get('wkhi', '-')
    wklo = item.get('wklo', '-')
    
    # Append to records with the specified CSV header names
    records.append({
        'SYMBOL': symbol,
        'ASSETS': assets,
        'LAST': ltP,
        'CHANGE': chn,
        '%CHANGE': percent_change_str,
        'PREVIOUS CLOSE': prev_close,
        '52w High': wkhi,
        '52w Low': wklo
    })

# Add timestamp row at the end
ist = pytz.timezone('Asia/Kolkata')
current_time = datetime.now(ist).strftime('%d-%b %H:%M')

# Add Update Time row with time in the last column
records.append({
    'SYMBOL': '',
    'ASSETS': '',
    'LAST': '',
    'CHANGE': '',
    '%CHANGE': '',
    'PREVIOUS CLOSE': '',
    '52w High': 'Update Time:',
    '52w Low': current_time  # Time in the last column
})

# Create DataFrame and save to CSV
df = pd.DataFrame(records)
df.to_csv('etf.csv', index=False)

# Print success message
print(f"ETF data saved to etf.csv successfully!")
print(f"Total records: {len(records)-1} ETFs + 1 timestamp row")
print(f"Last update: {current_time} IST")
