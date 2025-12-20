import requests
import pandas as pd
from datetime import datetime, timedelta
import pytz
import os

HOLIDAYS = [
    "2025-02-26", "2025-03-14", "2025-03-31", "2025-04-10",
    "2025-04-14", "2025-04-18", "2025-05-01", "2025-08-15",
    "2025-08-27", "2025-10-02", "2025-10-21", "2025-10-22",
    "2025-11-05", "2025-12-26", "2026-01-27", "2026-03-04", 
    "2026-03-27", "2026-04-01", "2026-04-04", "2026-04-15", 
    "2026-05-02", "2026-05-29", "2026-06-27", "2026-09-15", 
    "2026-10-03", "2026-10-21", "2026-11-11", "2026-11-25", 
    "2026-12-26"
]

target_funds = [
    "Aditya Birla Sun Life PSU Equity Fund-Direct Plan-Growth",
    "Axis Focused Fund - Direct Plan - Growth Option",
    "Axis Large & Mid Cap Fund - Direct Plan - Growth",
    "Axis Large Cap Fund - Direct Plan - Growth",
    "Axis Small Cap Fund - Direct Plan - Growth",
    "ICICI Prudential Banking and PSU Debt Fund - Direct Plan -  Growth",
    "ICICI Prudential Corporate Bond Fund - Direct Plan - Growth",
    "ICICI Prudential Gilt Fund - Direct Plan - Growth",
    "ICICI Prudential Nifty 50 Index Fund - Direct Plan Cumulative Option",
    "ICICI PRUDENTIAL SILVER ETF FUND OF FUND - Direct Plan - Growth",
    "ICICI Prudential Technology Fund - Direct Plan -  Growth",
    "Mahindra Manulife Consumption Fund - Direct Plan -Growth",
    "Mirae Asset Arbitrage Fund Direct Growth",
    "Mirae Asset ELSS Tax Saver Fund - Direct Plan - Growth",
    "Mirae Asset Healthcare Fund Direct Growth",
    "Nippon India Gold Savings Fund - Direct Plan Growth Plan - Growth Option",
    "Nippon India Nifty Next 50 Junior BeES FoF - Direct Plan - Growth Plan - Growth Option",
    "Nippon India Nivesh Lakshya Long Duration Fund- Direct Plan- Growth Option",
    "quant ELSS Tax Saver Fund - Growth Option - Direct Plan",
    "SBI GILT FUND - DIRECT PLAN - GROWTH"
]

ist = pytz.timezone('Asia/Kolkata')
today = datetime.now(ist)

if today.weekday() in [0, 6] or today.strftime('%Y-%m-%d') in HOLIDAYS:
    exit()

def extract_name(full):
    if "FoF" in full:
        parts = full.split("FoF")
        result = parts[0] + "FoF"
    elif "Fund" in full:
        parts = full.split("Fund")
        result = parts[0] + "Fund"
    else:
        result = full.split('-')[0]
    return ' '.join(result.split()).upper()

display_names = [extract_name(fund) for fund in target_funds]
fund_mapping = dict(zip(display_names, target_funds))

if today.weekday() == 0:
    target_date = today - timedelta(days=3)
elif today.weekday() == 6:
    target_date = today - timedelta(days=2)
else:
    target_date = today - timedelta(days=1)

target_date_str = target_date.strftime('%Y-%m-%d')
url = f"https://www.amfiindia.com/api/nav-history?query_type=all_for_date&from_date={target_date_str}"

response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
data = response.json()

nav_data = {}
for fund in data['data']:
    for scheme in fund['schemes']:
        for nav in scheme['navs']:
            if nav['NAV_Name'] in target_funds:
                name = extract_name(nav['NAV_Name'])
                time_str = nav['hNAV_Upload_display']
                date_only = ' '.join(time_str.split()[:2]) if time_str else '-'
                nav_data[name] = {
                    'NAV': nav['hNAV_Amt'],
                    'Update Time': date_only
                }

records = []
for name in display_names:
    if name in nav_data:
        records.append({
            'Fund Name': name,
            'NAV': nav_data[name]['NAV'],
            'Update Time': nav_data[name]['Update Time']
        })
    else:
        records.append({
            'Fund Name': name,
            'NAV': '-',
            'Update Time': '-'
        })

timestamp = today.strftime('%d-%b %H:%M')
records.append({
    'Fund Name': '',
    'NAV': 'LAST UPDATED:',
    'Update Time': timestamp
})

os.makedirs('Data', exist_ok=True)
pd.DataFrame(records).to_csv('Data/Daily_NAV.csv', index=False)

print(f"Funds found: {len(nav_data)} out of {len(display_names)}")
print(f"Timestamp: {timestamp} IST")
