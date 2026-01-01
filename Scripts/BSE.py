import requests
import csv
from datetime import datetime, timedelta
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
                data = response.json()
                # Handle RealTime data (cat 1 & 2)
                if "RealTime" in data:
                    all_data.extend(data["RealTime"])
                # Handle EOD data (cat 3)
                elif "EOD" in data:
                    # Convert EOD format to match RealTime format
                    for item in data["EOD"]:
                        converted = {
                            "IndexName": item.get("IndicesWatchName", "").strip(),
                            "Curvalue": item.get("Curvalue", 0),
                            "Chg": item.get("CHNG", 0),
                            "ChgPer": item.get("CHNGPER", 0),
                            "Prev_Close": item.get("PrevDayClose", 0),
                            "Week52High": "-",
                            "Week52Low": "-"
                        }
                        all_data.append(converted)
        except:
            continue
    
    return all_data

def transform_data(original_data):
    if not original_data:
        return []
    
    transformed = []
    for item in original_data:
        # Handle missing values with defaults
        week52high = item.get("Week52High", "-")
        week52low = item.get("Week52Low", "-")
        
        # Format week52 values
        if week52high != "-" and week52high != "":
            week52high = f'{float(week52high):.2f}'
        if week52low != "-" and week52low != "":
            week52low = f'{float(week52low):.2f}'
        
        row = [
            item.get("IndexName", "-"),
            f'{float(item.get("Curvalue", 0)):.2f}',
            f'{float(item.get("Chg", 0)):.2f}',
            f'{float(item.get("ChgPer", 0)):.2f}',
            f'{float(item.get("Prev_Close", 0)):.2f}',
            week52high,
            week52low
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
        
        # IST timestamp
        timestamp = (datetime.now() + timedelta(hours=5, minutes=30)).strftime("%d-%b %H:%M")
        writer.writerow(["", "", "", "", "", "Update Time", timestamp])

if __name__ == "__main__":
    raw_data = fetch_bse_data()
    processed_data = transform_data(raw_data)
    save_to_csv(processed_data)
    print(f"CSV saved with {len(processed_data)} records")
