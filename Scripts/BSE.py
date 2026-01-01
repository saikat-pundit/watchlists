import requests
import csv
from datetime import datetime
import os

def fetch_bse_data():
    urls = [
        "https://api.bseindia.com/BseIndiaAPI/api/MktCapBoard_indstream/w?cat=1&type=2",
        "https://api.bseindia.com/BseIndiaAPI/api/MktCapBoard_indstream/w?cat=2&type=2",
        "https://api.bseindia.com/BseIndiaAPI/api/MktCapBoard_indstream/w?cat=3&type=2"
    ]
    
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:136.0) Gecko/20100101 Firefox/136.0",
        "Referer": "https://www.bseindia.com/"
    }
    
    all_data = []
    for url in urls:
        try:
            response = requests.get(url, headers=headers, timeout=10)
            if response.status_code == 200:
                all_data.extend(response.json().get("RealTime", []))
        except:
            continue
    
    return all_data

def transform_data(original_data):
    if not original_data:
        return []
    
    transformed = []
    for item in original_data:
        row = [
            item.get("IndexName", ""),
            f'{item.get("Curvalue", 0):.2f}',
            f'{item.get("Chg", 0):.2f}',
            f'{item.get("ChgPer", 0):.2f}',
            f'{item.get("Prev_Close", 0):.2f}',
            f'{item.get("Week52High", 0):.2f}',
            f'{item.get("Week52Low", 0):.2f}'
        ]
        transformed.append(row)
    
    return transformed

def save_to_csv(data, filename="Data/BSE.csv"):
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    
    csv_headers = ["Index", "LTP", "CHNG", "%", "PREV.", "YR HI", "YR LO"]
    
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(csv_headers)
        writer.writerows(data)
        
        timestamp = datetime.now().strftime("%d-%b %H:%M")
        writer.writerow(["", "", "", "", "", "Update Time", timestamp])

if __name__ == "__main__":
    raw_data = fetch_bse_data()
    processed_data = transform_data(raw_data)
    save_to_csv(processed_data)
    print(f"CSV saved with {len(processed_data)} records")
