import requests
import pandas as pd
import json
from datetime import datetime

# NSE blocks bots, so we pretend to be a browser
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0'
}

url = "https://www.nseindia.com/api/allIndices"

response = requests.get(url, headers=headers)
data = response.json()

# Convert to simple list
records = []
for item in data['data']:
    records.append({
        'Date': datetime.now().strftime("%Y-%m-%d %H:%M"),
        'Index Name': item.get('index'),
        'Symbol': item.get('indexSymbol'),
        'Last': item.get('last'),
        'Change': item.get('variation'),
        '% Change': item.get('percentChange'),
        'Open': item.get('open'),
        'High': item.get('high'),
        'Low': item.get('low'),
        'Previous Close': item.get('previousClose')
    })

# Save as CSV
df = pd.DataFrame(records)
df.to_csv('nse_all_indices.csv', index=False)
print("CSV created successfully!")
