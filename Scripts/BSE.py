import requests
import csv
import json
import os
from datetime import datetime

def fetch_bse_data():
    """Fetch data from BSE API for all categories"""
    all_data = []
    
    # Define categories to fetch
    categories = [1, 2, 3]
    
    for cat in categories:
        url = f"https://api.bseindia.com/BseIndiaAPI/api/MktCapBoard_indstream/w?cat={cat}&type=2"
        
        headers = {
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:136.0) Gecko/20100101 Firefox/136.0",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.5",
            "Referer": "https://www.bseindia.com/"
        }
        
        try:
            print(f"Fetching data for category {cat}...")
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            if "RealTime" in data:
                for item in data["RealTime"]:
                    # Add category and fetch timestamp
                    item["Category"] = cat
                    item["Fetch_Time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    all_data.append(item)
                
                print(f"  Found {len(data['RealTime'])} records")
            else:
                print(f"  No 'RealTime' data found for category {cat}")
                
        except Exception as e:
            print(f"  Error fetching category {cat}: {e}")
            continue
    
    return all_data

def save_to_csv(data, filename):
    """Save data to CSV file"""
    if not data:
        print("No data to save!")
        return
    
    # Create directory if it doesn't exist
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    
    # Get all unique keys from data
    fieldnames = set()
    for item in data:
        fieldnames.update(item.keys())
    
    # Convert set to list and sort
    fieldnames = sorted(list(fieldnames))
    
    # Write to CSV
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)
    
    print(f"Data saved to {filename}")
    print(f"Total records: {len(data)}")
    print(f"Fields: {', '.join(fieldnames)}")

def main():
    print("Starting BSE Data Fetch...")
    print("-" * 50)
    
    # Fetch data from all categories
    data = fetch_bse_data()
    
    if data:
        # Save to CSV
        csv_filename = "Data/BSE.csv"
        save_to_csv(data, csv_filename)
        
        # Also save a timestamped copy for backup
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_filename = f"Data/Backup/BSE_{timestamp}.csv"
        os.makedirs("Data/Backup", exist_ok=True)
        save_to_csv(data, backup_filename)
        
        print("\nSummary:")
        print(f"Total indices fetched: {len(data)}")
        print(f"Unique indices: {len(set(item['INDX_CD'] for item in data))}")
    else:
        print("No data was fetched!")

if __name__ == "__main__":
    main()
