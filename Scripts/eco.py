import requests
import pandas as pd
from datetime import datetime
import pytz
import os

# Sensibull API URL
API_URL = "https://oxide.sensibull.com/v1/compute/market_global_events"

# Headers based on the request headers you provided
headers = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Encoding': 'gzip, deflate, br, zstd',
    'Accept-Language': 'en-US,en;q=0.5',
    'Connection': 'keep-alive',
    'Content-Length': '118',
    'Content-Type': 'application/json',
    'DNT': '1',
    'Host': 'oxide.sensibull.com',
    'Origin': 'null',
    'Priority': 'u=0, i',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'cross-site',
    'TE': 'trailers',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:136.0) Gecko/20100101 Firefox/136.0',
    'Cache-Control': 'no-store, no-cache, must-revalidate, max-age=0, no-transform'
}

# Request payload
payload = {
    "from_date": "2025-12-01",  # You can adjust this date
    "to_date": "2025-12-31",    # You can adjust this date
    "countries": ["India", "China", "Japan", "Euro Area", "USA"],
    "impacts": []  # Empty means all impacts
}

def fetch_sensibull_data():
    """Fetch data from Sensibull API"""
    try:
        response = requests.post(
            API_URL, 
            headers=headers, 
            json=payload,
            timeout=10
        )
        
        if response.status_code == 200:
            data = response.json()
            if data.get('success') and 'payload' in data:
                return data['payload'].get('data', [])
            else:
                print(f"API returned unsuccessful response: {data}")
                return []
        else:
            print(f"HTTP Error: {response.status_code}")
            print(f"Response: {response.text[:200]}")
            return []
            
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
        return []
    except Exception as e:
        print(f"Unexpected error: {e}")
        return []

def process_data(raw_data):
    """Process the API data and extract required fields"""
    records = []
    
    for item in raw_data:
        # Extract only the required fields
        record = {
            'Date': item.get('date', ''),
            'Time': item.get('time', ''),
            'Country': item.get('country', ''),
            'Title': item.get('title', ''),
            'Impact': item.get('impact', '').capitalize(),
            'Actual': item.get('actual', ''),
            'Expected': item.get('expected', ''),
            'Previous': item.get('previous', '')
        }
        records.append(record)
    
    return records

def save_to_csv(data, filename='Data/Economic.csv'):
    """Save processed data to CSV"""
    if not data:
        print("No data to save. Creating empty CSV with timestamp.")
        # Create empty dataframe with headers
        data = [{
            'Date': '', 'Time': '', 'Country': 'No Data Available',
            'Title': '', 'Impact': '', 'Actual': '', 'Expected': '', 'Previous': ''
        }]
    
    # Convert to DataFrame
    df = pd.DataFrame(data)
    
    # Sort by date and time
    if not df.empty and 'Date' in df.columns and 'Time' in df.columns:
        df['DateTime'] = pd.to_datetime(df['Date'] + ' ' + df['Time'], errors='coerce')
        df = df.sort_values('DateTime')
        df = df.drop('DateTime', axis=1)
    
    # Add timestamp row
    ist = pytz.timezone('Asia/Kolkata')
    current_time = datetime.now(ist).strftime('%d-%b %H:%M')
    
    # Create timestamp row
    timestamp_row = pd.DataFrame([{
        'Date': '', 'Time': '', 'Country': '', 'Title': '',
        'Impact': 'Updated:', 'Actual': '', 'Expected': '', 'Previous': current_time
    }])
    
    # Concatenate data with timestamp row
    df = pd.concat([df, timestamp_row], ignore_index=True)
    
    # Ensure Data directory exists
    os.makedirs('Data', exist_ok=True)
    
    # Save to CSV
    df.to_csv(filename, index=False)
    
    return len(data) - 1  # Return count excluding timestamp row

def main():
    print("Fetching economic data from Sensibull API...")
    
    # Fetch data
    raw_data = fetch_sensibull_data()
    
    if raw_data:
        print(f"Successfully fetched {len(raw_data)} economic events")
        
        # Process data
        processed_data = process_data(raw_data)
        
        # Save to CSV
        record_count = save_to_csv(processed_data)
        
        # Print summary
        print(f"✓ Economic data saved to Data/Economic.csv")
        print(f"Total records: {record_count} economic events")
        
        ist = pytz.timezone('Asia/Kolkata')
        current_time = datetime.now(ist).strftime('%d-%b %H:%M')
        print(f"Last update: {current_time} IST")
    else:
        print("Failed to fetch data from API")
        
        # Save empty CSV with timestamp
        save_to_csv([])
        print("Created empty Economic.csv file")

if __name__ == "__main__":
    main()
