import requests
import pandas as pd
from datetime import datetime, timedelta
import pytz
import os
from pathlib import Path

HOLIDAYS = [
    "2025-02-26", "2025-03-14", "2025-03-31", "2025-04-10",
    "2025-04-14", "2025-04-18", "2025-05-01", "2025-08-15",
    "2025-08-27", "2025-10-02", "2025-10-21", "2025-10-22",
    "2025-11-05", "2025-12-26", "2026-01-27", "2026-03-04",
    "2026-03-27", "2026-04-01", "2026-04-04", "2026-04-14",
    "2026-05-02", "2026-05-29", "2026-06-27", "2026-09-15",
    "2026-10-03", "2026-10-21", "2026-11-11", "2026-11-25",
    "2026-12-26", "2026-01-16"
]

target_funds = [
    "Aditya Birla Sun Life PSU Equity Fund-Direct Plan-Growth",
    "Axis Focused Fund - Direct Plan - Growth Option",
    "Axis Large & Mid Cap Fund - Direct Plan - Growth",
    "Axis Large Cap Fund - Direct Plan - Growth",
    "Axis Small Cap Fund - Direct Plan - Growth",
    "ICICI Prudential Banking and PSU Debt Fund - Direct Plan - Growth",
    "ICICI Prudential Corporate Bond Fund - Direct Plan - Growth",
    "ICICI Prudential Gilt Fund - Direct Plan - Growth",
    "ICICI Prudential Nifty 50 Index Fund - Direct Plan Cumulative Option",
    "ICICI Prudential Silver ETF FOF - Direct Plan - Growth",
    "ICICI Prudential Technology Fund - Direct Plan -  Growth",
    "Mahindra Manulife Consumption Fund - Direct Plan -Growth",
    "Mirae Asset Arbitrage Fund - Direct Plan - Growth",
    "Mirae Asset ELSS Tax Saver Fund - Direct Plan - Growth",
    "Mirae Asset Healthcare Fund - Direct Plan - Growth",
    "Nippon India Gold Savings Fund - Direct Plan Growth Plan - Growth Option",
    "Nippon India Nifty Next 50 Junior BeES FoF - Direct Plan - Growth Plan - Growth Option",
    "Nippon India Nivesh Lakshya Long Term Fund- Direct Plan- Growth Option",
    "quant ELSS Tax Saver Fund - Growth Option - Direct Plan",
    "SBI GILT FUND - DIRECT PLAN - GROWTH"
]

NAVALL_URL = "https://portal.amfiindia.com/spages/NAVAll.txt"


def extract_name(full):
    """Extract display name from full fund name"""
    if "FoF" in full:
        parts = full.split("FoF")
        result = parts[0] + "FoF"
    elif "Fund" in full:
        parts = full.split("Fund")
        result = parts[0] + "Fund"
    else:
        result = full.split('-')[0]
    return ' '.join(result.split()).upper()


def load_old_data():
    """Load existing NAV data from CSV if exists"""
    csv_path = Path('Data/Daily_NAV.csv')
    old_data = {}

    if csv_path.exists():
        try:
            df_old = pd.read_csv(csv_path)
            # Exclude the timestamp row
            df_funds = df_old[~df_old['Fund Name'].str.contains('LAST UPDATED', na=False)]

            for _, row in df_funds.iterrows():
                fund_name = row['Fund Name']
                # Only store if we have valid NAV data (not '-')
                if str(row['NAV']).strip() not in ['-', 'nan', '']:
                    old_data[fund_name] = {
                        'NAV': str(row['NAV']).strip(),
                        'Update Time': str(row['Update Time']).strip()
                    }
            print(f"Loaded old data for {len(old_data)} funds")
        except Exception as e:
            print(f"Error loading old data: {e}")

    return old_data


def fetch_from_api(target_date_str):
    """Fetch NAV data from AMFI API for a specific date"""
    url = f"https://www.amfiindia.com/api/nav-history?query_type=all_for_date&from_date={target_date_str}"

    try:
        response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=30)
        response.raise_for_status()
        data = response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching API data: {e}")
        return {}

    new_nav_data = {}
    if 'data' in data:
        for fund in data['data']:
            if 'schemes' in fund:
                for scheme in fund['schemes']:
                    if 'navs' in scheme:
                        for nav in scheme['navs']:
                            if nav['NAV_Name'] in target_funds:
                                name = extract_name(nav['NAV_Name'])
                                time_str = nav.get('hNAV_Upload_display', '')
                                date_only = ' '.join(time_str.split()[:2]) if time_str else '-'
                                new_nav_data[name] = {
                                    'NAV': str(nav.get('hNAV_Amt', '-')).strip(),
                                    'Update Time': date_only
                                }

    print(f"API data fetched for {len(new_nav_data)} funds")
    return new_nav_data


def _normalize(text):
    """Normalize a string for fuzzy matching"""
    return ' '.join(str(text).lower().split())


def _build_navall_lookup(target_date_str):
    """
    Download NAVAll.txt and build a lookup of {fund_name: nav} for Direct Plan Growth
    schemes matching a given date.
    Returns: dict keyed by normalized 'Scheme Name' -> {NAV, Update Time, raw_name}
    """
    try:
        resp = requests.get(NAVALL_URL, headers={'User-Agent': 'Mozilla/5.0'}, timeout=60)
        resp.raise_for_status()
        text = resp.text
    except requests.exceptions.RequestException as e:
        print(f"Error fetching NAVAll.txt: {e}")
        return {}

    # Convert expected date format dd-Mon-YYYY (e.g. 26-Oct-2026) for comparison
    try:
        expected_date_obj = datetime.strptime(target_date_str, '%Y-%m-%d')
        expected_date_display = expected_date_obj.strftime('%d-%b-%Y')
    except Exception:
        expected_date_display = target_date_str

    lookup = {}
    for line in text.splitlines():
        line = line.strip()
        if not line or ';' not in line:
            continue
        parts = line.split(';')
        if len(parts) < 8:
            continue

        # Skip header
        if parts[0].strip().lower() == 'scheme code':
            continue

        scheme_code = parts[0].strip()
        scheme_name = parts[3].strip()
        plan = parts[4].strip()
        option = parts[5].strip()
        nav_amt = parts[6].strip()
        nav_date = parts[7].strip()

        # Only Direct Plan Growth
        if _normalize(plan) != 'direct plan':
            continue
        if _normalize(option) not in ('growth', 'growth option'):
            continue

        # Optional: prefer the exact target date if available
        # (still store others as fallback candidates in case date differs slightly)
        record = {
            'NAV': nav_amt,
            'Update Time': nav_date,
            'raw_name': scheme_name,
            'date': nav_date
        }

        key = _normalize(scheme_name)
        # Prefer exact date match; otherwise keep first available
        if key not in lookup or nav_date == expected_date_display:
            lookup[key] = record

    return lookup


def _resolve_targets_from_lookup(lookup):
    """
    Match each target fund against the NAVAll lookup.
    Strategy: exact normalized match first, then startswith on the
    'Scheme Name' portion extracted from the target full name.
    Returns dict {display_name: {NAV, Update Time}}
    """
    matched = {}
    if not lookup:
        return matched

    for full_name in target_funds:
        display = extract_name(full_name)

        # Try to find Scheme Name candidates by stripping plan/option suffixes
        scheme_candidates = []
        for sep in [' - Direct Plan', '-Direct Plan', ' Direct Plan', '- Direct', '-Direct']:
            if sep in full_name:
                scheme_candidates.append(full_name.split(sep)[0].strip())
        # Also try entire name
        scheme_candidates.append(full_name)

        found = None
        # 1) exact match on any candidate
        for cand in scheme_candidates:
            key = _normalize(cand)
            if key in lookup:
                found = lookup[key]
                break

        # 2) startswith match
        if not found:
            for cand in scheme_candidates:
                norm_cand = _normalize(cand)
                for key, rec in lookup.items():
                    if key == norm_cand or key.startswith(norm_cand):
                        found = rec
                        break
                if found:
                    break

        if found:
            matched[display] = {
                'NAV': found['NAV'],
                'Update Time': found['Update Time']
            }

    print(f"NAVAll.txt matched {len(matched)} funds")
    return matched


def fetch_from_navall(target_date_str):
    """Fetch NAV data from NAVAll.txt as a fallback"""
    lookup = _build_navall_lookup(target_date_str)
    return _resolve_targets_from_lookup(lookup)


def main():
    ist = pytz.timezone('Asia/Kolkata')
    today = datetime.now(ist)

    # Check if today is holiday or weekend
    if today.weekday() in [0, 7] or today.strftime('%Y-%m-%d') in HOLIDAYS:
        print(f"{today.strftime('%Y-%m-%d')} is holiday/weekend. Exiting.")
        exit()

    # Load old data before fetching new data
    old_data = load_old_data()

    # Calculate target date for NAV fetch
    if today.weekday() == 0:  # Monday
        target_date = today - timedelta(days=3)
    elif today.weekday() == 6:  # Sunday (shouldn't happen due to check above)
        target_date = today - timedelta(days=2)
    else:  # Tuesday to Friday
        target_date = today - timedelta(days=1)

    target_date_str = target_date.strftime('%Y-%m-%d')
    print(f"Fetching NAV data for date: {target_date_str}")

    display_names = [extract_name(fund) for fund in target_funds]

    # --- Primary: AMFI API ---
    new_nav_data = fetch_from_api(target_date_str)
    print(f"New data fetched from API for {len(new_nav_data)} funds")

    # --- Fallback: NAVAll.txt for funds still missing ---
    missing_after_api = [name for name in display_names if name not in new_nav_data]
    if missing_after_api:
        print(f"{len(missing_after_api)} fund(s) missing from API. Trying NAVAll.txt fallback...")
        navall_data = fetch_from_navall(target_date_str)
        for name in missing_after_api:
            if name in navall_data:
                new_nav_data[name] = navall_data[name]

    # --- Prepare records ---
    records = []
    funds_with_new_data = 0
    funds_with_old_data = 0
    funds_missing = 0

    for name in display_names:
        if name in new_nav_data:
            records.append({
                'Fund Name': name,
                'NAV': new_nav_data[name]['NAV'],
                'Update Time': new_nav_data[name]['Update Time']
            })
            funds_with_new_data += 1
        elif name in old_data:
            records.append({
                'Fund Name': name,
                'NAV': old_data[name]['NAV'],
                'Update Time': old_data[name]['Update Time']
            })
            funds_with_old_data += 1
        else:
            records.append({
                'Fund Name': name,
                'NAV': '-',
                'Update Time': '-'
            })
            funds_missing += 1

    # Add timestamp row
    timestamp = today.strftime('%d-%b %H:%M')
    records.append({
        'Fund Name': '',
        'NAV': 'LAST UPDATED:',
        'Update Time': timestamp
    })

    # Ensure Data directory exists
    os.makedirs('Data', exist_ok=True)

    # Save to CSV
    df = pd.DataFrame(records)
    df.to_csv('Data/Daily_NAV.csv', index=False)

    print(f"File saved: Data/Daily_NAV.csv")
    print(f"Summary -> new: {funds_with_new_data}, old-retained: {funds_with_old_data}, missing: {funds_missing}")


if __name__ == "__main__":
    main()
