import requests
import pandas as pd
from datetime import datetime, timedelta
import pytz

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0',
    'Accept': 'application/json',
    'Accept-Language': 'en-US,en;q=0.9',
    'Referer': 'https://www.nseindia.com/option-chain'
}

def get_nearest_thursday():
    ist = pytz.timezone('Asia/Kolkata')
    today = datetime.now(ist)
    
    days_ahead = 3 - today.weekday()
    if days_ahead <= 0:
        days_ahead += 7
    
    expiry_date = today + timedelta(days=days_ahead)
    expiry_str = expiry_date.strftime('%d-%b-%Y')
    
    return expiry_str

def get_option_chain(symbol="NIFTY"):
    expiry = get_nearest_thursday()
    url = f"https://www.nseindia.com/api/option-chain-v3?type=Indices&symbol={symbol}&expiry={expiry}"
    
    session = requests.Session()
    session.headers.update(headers)
    session.get("https://www.nseindia.com")
    
    response = session.get(url)
    data = response.json()
    
    return data, expiry

def create_option_chain_dataframe(data, expiry):
    records = data['records']
    timestamp = records['timestamp']
    underlying_value = records['underlyingValue']
    
    option_data = []
    
    for item in records['data']:
        strike_price = item['strikePrice']
        ce_data = item.get('CE', {})
        pe_data = item.get('PE', {})
        
        option_row = {
            'OI': pe_data.get('openInterest', 0),
            'OI_CHANGE': pe_data.get('changeinOpenInterest', 0),
            'VOLUME': pe_data.get('totalTradedVolume', 0),
            'CHANGE': pe_data.get('change', 0),
            'LTP': pe_data.get('lastPrice', 0),
            'STRIKE': strike_price,
            'C_LTP': ce_data.get('lastPrice', 0),
            'C_CHANGE': ce_data.get('change', 0),
            'C_VOLUME': ce_data.get('totalTradedVolume', 0),
            'C_OI_CHANGE': ce_data.get('changeinOpenInterest', 0),
            'C_OI': ce_data.get('openInterest', 0)
        }
        option_data.append(option_row)
    
    if not option_data:
        print("No option data found in response")
        return pd.DataFrame()
    
    df = pd.DataFrame(option_data)
    
    if 'STRIKE' in df.columns:
        df = df.sort_values('STRIKE')
    else:
        print("STRIKE column not found in data")
        print("Available columns:", df.columns.tolist())
        return pd.DataFrame()
    
    expiry_row = {
        'OI': '',
        'OI_CHANGE': '',
        'VOLUME': '',
        'CHANGE': '',
        'LTP': '',
        'STRIKE': f'EXPIRY: {expiry}',
        'C_LTP': f'TIMESTAMP: {timestamp}',
        'C_CHANGE': f'UNDERLYING: {underlying_value}',
        'C_VOLUME': '',
        'C_OI_CHANGE': '',
        'C_OI': ''
    }
    
    df = pd.concat([df, pd.DataFrame([expiry_row])], ignore_index=True)
    
    return df

def main():
    ist = pytz.timezone('Asia/Kolkata')
    
    try:
        data, expiry = get_option_chain()
        
        if data and 'records' in data and 'data' in data['records']:
            df = create_option_chain_dataframe(data, expiry)
            
            if not df.empty:
                import os
                os.makedirs('Data', exist_ok=True)
                
                df.to_csv('Data/Option.csv', index=False)
                
                timestamp = datetime.now(ist).strftime('%d-%b %H:%M')
                print(f"Option chain saved to Data/Option.csv")
                print(f"Expiry: {expiry}")
                print(f"Timestamp: {timestamp} IST")
                print(f"Underlying Value: {data['records']['underlyingValue']}")
            else:
                print("Failed to create option chain dataframe")
        else:
            print("Invalid or empty API response")
            if data:
                print("Response keys:", data.keys())
            
    except Exception as e:
        print(f"Error occurred: {str(e)}")
        print("Full traceback will be shown below")

if __name__ == "__main__":
    main()
