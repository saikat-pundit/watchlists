import requests
import pandas as pd

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0'
}

url = "https://www.nseindia.com/api/marketStatus"

response = requests.get(url, headers=headers)
data = response.json()

records = []

# Find currencyfuture data
for item in data['marketState']:
    if item.get('market') == 'currencyfuture':
        records.append({
            'SYMBOL': 'USDINR',
            'LAST PRICE': item.get('last', '')
        })
        break

# Get giftnifty data
if 'giftnifty' in data:
    gift = data['giftnifty']
    records.append({
        'SYMBOL': 'GIFT-NIFTY',
        'LAST PRICE': gift.get('LASTPRICE', '')
    })

df = pd.DataFrame(records)
df.to_csv('market_data.csv', index=False)
print("CSV created successfully!")
