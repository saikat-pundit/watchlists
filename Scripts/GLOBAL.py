import pandas as pd
from tradingview_screener import Query
import os
from datetime import datetime

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

def main():
    print("📊 Fetching global market data...")
    
    try:
        # Fetch ALL data first
        total_count, all_data = Query().select(
            'name', 'close', 'change_abs', 'change', 'close[1]',
            'price_52_week_high', 'price_52_week_low', 'ticker'
        ).get_scanner_data()
        
        print(f"Total instruments available: {total_count}")
        
        # Filter for our specific symbols
        symbols = [s for s,_ in MARKETS]
        df = all_data[all_data['ticker'].isin(symbols)]
        
        if df.empty:
            print("❌ No matching symbols found")
            print("Available tickers sample:", all_data['ticker'].head(10).tolist())
            return
        
        # Process results
        results = []
        for symbol, name in MARKETS:
            row = df[df['ticker'] == symbol]
            if not row.empty:
                r = row.iloc[0]
                results.append({
                    'timestamp': datetime.now().isoformat(),
                    'index': name,
                    'symbol': symbol,
                    'ltp': r.get('close'),
                    'change': r.get('change_abs'),
                    'change_percent': r.get('change'),
                    'previous_close': r.get('close[1]'),
                    'year_high': r.get('price_52_week_high'),
                    'year_low': r.get('price_52_week_low')
                })
        
        # Save to CSV
        os.makedirs('Data', exist_ok=True)
        result_df = pd.DataFrame(results)
        
        if os.path.exists('Data/GLOBAL.csv'):
            existing = pd.read_csv('Data/GLOBAL.csv')
            result_df = pd.concat([existing, result_df], ignore_index=True)
        
        result_df.to_csv('Data/GLOBAL.csv', index=False)
        print(f"✅ Saved {len(results)} records to Data/GLOBAL.csv")
        print("\n📈 Latest data:")
        for r in results:
            print(f"  {r['index']}: {r['ltp']} ({r['change_percent']}%)")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
