import pandas as pd
from tradingview_screener import Query
import os
from datetime import datetime

# Market data configuration
MARKETS = [
    ('DJ:DJI', 'Dow Jones'),
    ('SP:SPX', 'S&P 500'),
    ('NASDAQ:NDX', 'NASDAQ 100'),
    ('CBOE:VIX', 'VIX'),
    ('TVC:DXY', 'Dollar Index'),
    ('TVC:US10Y', 'US10Y'),
    ('INDEX:NKY', 'Nikkei 225'),
    ('STOXX50:SX5E', 'Euro Stoxx 50'),
    ('XETR:DAX', 'DAX'),
    ('FTSE:UKX', 'FTSE 100'),
    ('CRYPTOCAP:BTC', 'Bitcoin'),
    ('FX:USDINR', 'USD/INR'),
    ('FX:USDJPY', 'USD/JPY')
]

def fetch_market_data():
    """Fetch market data from TradingView"""
    symbols = [symbol for symbol, _ in MARKETS]
    
    _, df = (Query()
            .symbols({'tickers': symbols})
            .select('close', 'change_abs', 'change', 'close[1]',
                   'price_52_week_high', 'price_52_week_low')
            .get_scanner_data())
    
    return df

def process_data(df):
    """Process and format the data"""
    results = []
    for symbol, name in MARKETS:
        row = df[df['ticker'] == symbol]
        if not row.empty:
            r = row.iloc[0]
            results.append({
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'index': name,
                'symbol': symbol,
                'ltp': r.get('close'),
                'change': r.get('change_abs'),
                'change_percent': r.get('change'),
                'previous_close': r.get('close[1]'),
                'year_high': r.get('price_52_week_high'),
                'year_low': r.get('price_52_week_low')
            })
    
    return pd.DataFrame(results)

def save_to_csv(df, filepath='Data/GLOBAL.csv'):
    """Save data to CSV, append if file exists"""
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    
    if os.path.exists(filepath):
        existing_df = pd.read_csv(filepath)
        df = pd.concat([existing_df, df], ignore_index=True)
    
    df.to_csv(filepath, index=False)
    print(f"✅ Data saved to {filepath} ({len(df)} records)")

def main():
    """Main execution function"""
    print("📊 Fetching global market data...")
    
    try:
        raw_data = fetch_market_data()
        processed_data = process_data(raw_data)
        
        print(f"\n📈 Latest data:")
        print(processed_data[['index', 'ltp', 'change_percent']].to_string(index=False))
        
        save_to_csv(processed_data)
        
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    main()
