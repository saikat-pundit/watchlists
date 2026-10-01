import requests
import csv
from datetime import datetime, timedelta
import os


def fetch_bse_data():
    urls = [
        "https://api.bseindia.com/BseIndiaAPI/api/MktCapBoard_indstream/w?type=2&cat=1",
        "https://api.bseindia.com/BseIndiaAPI/api/MktCapBoard_indstream/w?type=2&cat=2",
        "https://api.bseindia.com/BseIndiaAPI/api/MktCapBoard_indstream/w?type=2&cat=3",
    ]

    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:155.0) Gecko/20100101 Firefox/155.0",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Referer": "https://www.bseindia.com/",
        "Origin": "https://www.bseindia.com",
        "Connection": "keep-alive",
        "Host": "api.bseindia.com",
        "Priority": "u=4",
        "TE": "trailers",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-site",
    }

    cookies = {
        "_ga": "GA1.1.932471926.1790820492",
        "_ga_2VVED3VX1X": "GS2.1.s1790820491$o1$g1$t1790820556$j58$l0$h0",
    }

    session = requests.Session()
    session.headers.update(headers)
    session.cookies.update(cookies)

    all_data = []
    seen = set()

    for url in urls:
        try:
            response = session.get(url, timeout=15)
            print(f"URL: {url} -> Status: {response.status_code}")
            if response.status_code != 200:
                print(f"  Non-200 body: {response.text[:200]}")
                continue

            data = response.json()

            # --- RealTime ---
            for item in data.get("RealTime", []):
                name = (item.get("IndexName") or "").strip()
                if not name or name in seen:
                    continue
                seen.add(name)
                all_data.append({
                    "IndexName": name,
                    "Curvalue": item.get("Curvalue", 0),
                    "Chg": item.get("Chg", 0),
                    "ChgPer": item.get("ChgPer", 0),
                    "Prev_Close": item.get("Prev_Close", 0),
                    "Week52High": item.get("Week52High", 0),
                    "Week52Low": item.get("Week52Low", 0),
                })

            # --- EOD ---
            for item in data.get("EOD", []):
                name = (item.get("IndicesWatchName") or "").strip()
                if not name or name in seen:
                    continue
                seen.add(name)
                all_data.append({
                    "IndexName": name,
                    "Curvalue": item.get("Curvalue", 0),
                    "Chg": item.get("CHNG", 0),
                    "ChgPer": item.get("CHNGPER", 0),
                    "Prev_Close": item.get("PrevDayClose", 0),
                    "Week52High": "-",
                    "Week52Low": "-",
                })

        except Exception as e:
            print(f"Error fetching {url}: {e}")
            continue

    return all_data


def transform_data(original_data):
    if not original_data:
        return []

    transformed = []
    for item in original_data:
        week52high = item.get("Week52High", "-")
        week52low = item.get("Week52Low", "-")

        def fmt(v):
            if v in ("-", "", None):
                return "-"
            try:
                return f"{float(v):.2f}"
            except (ValueError, TypeError):
                return "-"

        week52high = fmt(week52high)
        week52low = fmt(week52low)

        try:
            row = [
                item.get("IndexName", "-"),
                f'{float(item.get("Curvalue", 0)):.2f}',
                f'{float(item.get("Chg", 0)):.2f}',
                f'{float(item.get("ChgPer", 0)):.2f}',
                f'{float(item.get("Prev_Close", 0)):.2f}',
                week52high,
                week52low,
            ]
            transformed.append(row)
        except (ValueError, TypeError):
            continue

    return transformed


def save_to_csv(data, filename="Data/BSE.csv"):
    os.makedirs(os.path.dirname(filename), exist_ok=True)

    csv_headers = ["Index", "LTP", "CHNG", "%", "PREV.", "YR HI", "YR LO"]

    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(csv_headers)
        writer.writerows(data)

        timestamp = (datetime.now() + timedelta(hours=5, minutes=30)).strftime(
            "%d-%b %H:%M"
        )
        writer.writerow(["", "", "", "", "", "Update Time", timestamp])


if __name__ == "__main__":
    raw_data = fetch_bse_data()
    print(f"Total records fetched: {len(raw_data)}")
    processed_data = transform_data(raw_data)
    save_to_csv(processed_data)
    print(f"CSV saved with {len(processed_data)} records")
