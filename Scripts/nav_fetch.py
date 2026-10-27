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
    if "FoF" in full:
        result = full.split("FoF")[0] + "FoF"
    elif "Fund" in full:
        result = full.split("Fund")[0] + "Fund"
    else:
        result = full.split('-')[0]
    return ' '.join(result.split()).upper()


def norm(s):
    return ' '.join(str(s).lower().replace('-', ' ').split())


def load_old_data():
    p = Path('Data/Daily_NAV.csv')
    old = {}
    if p.exists():
        try:
            df = pd.read_csv(p)
            df = df[~df['Fund Name'].str.contains('LAST UPDATED', na=False)]
            for _, r in df.iterrows():
                if str(r['NAV']).strip() not in ['-', 'nan', '']:
                    old[r['Fund Name']] = {
                        'NAV': str(r['NAV']).strip(),
                        'Update Time': str(r['Update Time']).strip()
                    }
            print(f"Loaded old data for {len(old)} funds")
        except Exception as e:
            print(f"Error loading old data: {e}")
    return old


def fetch_from_api(date_str):
    url = f"https://www.amfiindia.com/api/nav-history?query_type=all_for_date&from_date={date_str}"
    try:
        r = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=30)
        r.raise_for_status()
        data = r.json()
    except requests.exceptions.RequestException as e:
        print(f"API error: {e}")
        return {}

    out = {}
    for fund in data.get('data', []):
        for scheme in fund.get('schemes', []):
            for nav in scheme.get('navs', []):
                if nav.get('NAV_Name') in target_funds:
                    name = extract_name(nav['NAV_Name'])
                    t = nav.get('hNAV_Upload_display', '')
                    out[name] = {
                        'NAV': str(nav.get('hNAV_Amt', '-')).strip(),
                        'Update Time': ' '.join(t.split()[:2]) if t else '-'
                    }
    print(f"API fetched: {len(out)}")
    return out


def parse_navall(text):
    rows = []
    for line in text.splitlines():
        parts = line.strip().split(';')
        if len(parts) < 8 or parts[0].strip().lower() == 'scheme code':
            continue
        rows.append({
            'isin': parts[1].strip(),
            'name': parts[3].strip(),
            'plan': norm(parts[4]),
            'option': norm(parts[5]),
            'nav': parts[6].strip(),
            'date': parts[7].strip()
        })
    return rows


def is_growth(option):
    o = option.replace(' option', '').strip()
    return o in ('growth', 'cumulative')


def target_scheme_name(full):
    s = full
    for sep in ['- direct plan', '-direct plan', ' direct plan', '- direct', '-direct']:
        low = s.lower()
        if sep in low:
            s = s[:low.index(sep)]
            break
    s = s.replace('Growth Option', '').replace('- Growth', '').replace('Cumulative Option', '')
    return ' '.join(s.strip(' -').split())


def fetch_from_navall(date_str):
    try:
        r = requests.get(NAVALL_URL, headers={'User-Agent': 'Mozilla/5.0'}, timeout=60)
        r.raise_for_status()
        rows = parse_navall(r.text)
    except requests.exceptions.RequestException as e:
        print(f"NAVAll error: {e}")
        return {}

    try:
        target_disp = datetime.strptime(date_str, '%Y-%m-%d').strftime('%d-%b-%Y')
    except Exception:
        target_disp = date_str

    direct_growth = [x for x in rows if x['plan'] == 'direct plan' and is_growth(x['option'])]
    out = {}

    for full in target_funds:
        display = extract_name(full)
        scheme = target_scheme_name(full)
        scheme_n = norm(scheme)

        best = None
        best_score = None
        for row in direct_growth:
            name_n = norm(row['name'])
            if name_n == scheme_n:
                exact = 2
            elif name_n.startswith(scheme_n) or scheme_n.startswith(name_n):
                exact = 1
            else:
                continue

            has_isin = row['isin'] not in ('-', '')
            score = (exact, 1 if row['date'] == target_disp else 0, 1 if has_isin else 0)
            if best_score is None or score > best_score:
                best_score = score
                best = row

        if best:
            out[display] = {'NAV': best['nav'], 'Update Time': best['date']}

    print(f"NAVAll matched: {len(out)}")
    return out


def main():
    ist = pytz.timezone('Asia/Kolkata')
    today = datetime.now(ist)

    if today.weekday() in [0, 7] or today.strftime('%Y-%m-%d') in HOLIDAYS:
        print(f"{today.strftime('%Y-%m-%d')} is holiday/weekend. Exiting.")
        exit()

    old_data = load_old_data()

    if today.weekday() == 0:
        target_date = today - timedelta(days=3)
    elif today.weekday() == 6:
        target_date = today - timedelta(days=2)
    else:
        target_date = today - timedelta(days=1)

    date_str = target_date.strftime('%Y-%m-%d')
    print(f"Target date: {date_str}")

    display_names = [extract_name(f) for f in target_funds]

    new_data = fetch_from_api(date_str)

    missing = [n for n in display_names if n not in new_data]
    if missing:
        print(f"{len(missing)} missing from API, trying NAVAll.txt...")
        navall = fetch_from_navall(date_str)
        for n in missing:
            if n in navall:
                new_data[n] = navall[n]

    records = []
    c_new = c_old = c_miss = 0
    for name in display_names:
        if name in new_data:
            records.append({'Fund Name': name, 'NAV': new_data[name]['NAV'], 'Update Time': new_data[name]['Update Time']})
            c_new += 1
        elif name in old_data:
            records.append({'Fund Name': name, 'NAV': old_data[name]['NAV'], 'Update Time': old_data[name]['Update Time']})
            c_old += 1
        else:
            records.append({'Fund Name': name, 'NAV': '-', 'Update Time': '-'})
            c_miss += 1

    records.append({'Fund Name': '', 'NAV': 'LAST UPDATED:', 'Update Time': today.strftime('%d-%b %H:%M')})

    os.makedirs('Data', exist_ok=True)
    pd.DataFrame(records).to_csv('Data/Daily_NAV.csv', index=False)

    print(f"Saved: Data/Daily_NAV.csv")
    print(f"new={c_new} old={c_old} missing={c_miss}")


if __name__ == "__main__":
    main()
