import yfinance as yf
import pandas as pd
from datetime import datetime
import concurrent.futures

tickers = {
    "SP500": "^GSPC",
    "DJI": "^DJI",
    "VIX": "^VIX", 
    "AAPL": "AAPL",
    "META": "META"
}

def fetch_single_ticker(name_ticker_tuple):
    name, ticker = name_ticker_tuple
    try:
        data = yf.download(ticker, period="1d", interval="1d", progress=False)
        if data.empty:
            return None
            
        data.reset_index(inplace=True)
        data["Symbol"] = name
        data["Ticker"] = ticker
        return data
        
    except Exception as e:
        print(f"Error fetching {name}: {e}")
        return None

def fetch_data_multithreaded():
    print("Fetching market data (multi-threaded)...")
    
    # Use ThreadPoolExecutor for parallel downloads
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        results = list(executor.map(fetch_single_ticker, tickers.items()))
    
    # Filter out None results
    all_data = [r for r in results if r is not None]
    
    if not all_data:
        print("No data fetched!")
        return
        
    df = pd.concat(all_data, ignore_index=True)
    column_order = ["Date", "Symbol", "Ticker", "Open", "High", "Low", "Close", "Volume"]
    df = df[column_order]
    
    filename = f"market_data_{datetime.now().strftime('%Y-%m-%d')}.csv"
    df.to_csv(filename, index=False)
    
    print(f"\nSaved {len(df)} records to {filename}")
    return df

if __name__ == "__main__":
    fetch_data_multithreaded()
