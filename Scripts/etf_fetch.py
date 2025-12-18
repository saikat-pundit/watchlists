import requests, pandas as pd, os, pytz
from datetime import datetime

headers = {'User-Agent': 'Mozilla/5.0'}
url = "https://www.nseindia.com/api/etf"
target_symbols = ["NIFTYBEES", "METALIETF", "PVTBANIETF", "ALPHA", "GOLDBEES", "SILVERBEES", "PHARMABEES", "ITBEES", "BANKBEES"]

try:
    data = requests.get(url, headers=headers).json()
except:
    data = {}

symbol_dict = {}
for item in data.get('data', []):
    symbol = item.get('symbol')
    if symbol in target_symbols:
        per = item.get('per', '-')
        percent = f"{per}%" if per != '-' and per is not None else '-'
        symbol_dict[symbol] = {
            'SYMBOL': symbol,
            'LTP': item.get('ltP', '-'),
            'CHNG': item.get('chn', '-'),
            '%CHNG': percent,
            'PREVIOUS': item.get('prevClose', '-'),
            'Yr Hi': item.get('wkhi', '-'),
            'Yr Lo': item.get('wklo', '-')
        }

records = []
for symbol in target_symbols:
    if symbol in symbol_dict:
        records.append(symbol_dict[symbol])
    else:
        records.append({
            'SYMBOL': symbol,
            'LTP': '-', 'CHNG': '-', '%CHNG': '-',
            'PREVIOUS': '-', 'Yr Hi': '-', 'Yr Lo': '-'
        })

records.append({
    'SYMBOL': '', 'LTP': '', 'CHNG': '', '%CHNG': '',
    'PREVIOUS': '', 'Yr Hi': 'Update Time', 'Yr Lo': datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%d-%b %H:%M')
})

os.makedirs('Data', exist_ok=True)
pd.DataFrame(records).to_csv('Data/etf.csv', index=False)
