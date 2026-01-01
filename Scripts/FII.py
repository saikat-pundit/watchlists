import requests, csv, re
from pathlib import Path

url = "https://www.fpi.nsdl.co.in/web/StaticReports/Fortnightly_Sector_wise_FII_Investment_Data/FIIInvestSector_Dec152025.html"

try:
    # Fetch data
    html = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=30).text
    
    # Find table
    table = html[html.find('<table'):html.find('</table>')+8]
    
    # Parse rows and cells
    rows = []
    for tr in re.findall(r'<tr.*?>(.*?)</tr>', table, re.DOTALL):
        cells = []
        for td in re.findall(r'<t[dh].*?>(.*?)</t[dh]>', tr, re.DOTALL):
            # Clean cell: remove tags, commas, extra spaces
            clean = re.sub(r'<.*?>', ' ', td).replace(',', '').strip()
            clean = re.sub(r'\s+', ' ', clean)
            cells.append(clean)
        if cells:
            rows.append(cells)
    
    # Save CSV
    csv_path = Path(__file__).parent.parent / 'Data' / 'FII.csv'
    csv_path.parent.mkdir(exist_ok=True)
    
    with open(csv_path, 'w', newline='', encoding='utf-8') as f:
        csv.writer(f).writerows(rows)
    
    print(f"✅ Saved {len(rows)} rows to {csv_path}")
    
except Exception as e:
    print(f"❌ Error: {e}")
