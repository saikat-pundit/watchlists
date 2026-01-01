import requests
import csv
import os
from datetime import datetime

def fetch_nsdl_data():
    """Fetch FII investment data from NSDL website and save as CSV."""
    
    url = "https://www.fpi.nsdl.co.in/web/StaticReports/Fortnightly_Sector_wise_FII_Investment_Data/FIIInvestSector_Dec152025.html"
    
    try:
        print("Fetching FII data from NSDL...")
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        # Extract table data
        html_content = response.text
        table_start = html_content.find('<table')
        
        if table_start == -1:
            print("No table found in the HTML")
            return False
        
        # Extract table content
        table_end = html_content.find('</table>', table_start)
        table_html = html_content[table_start:table_end+8]
        
        # Parse table rows
        rows = []
        row_start = 0
        
        while True:
            # Find next table row
            tr_start = table_html.find('<tr', row_start)
            if tr_start == -1:
                break
                
            tr_end = table_html.find('</tr>', tr_start)
            if tr_end == -1:
                break
                
            row_html = table_html[tr_start:tr_end+5]
            
            # Extract cells from this row
            cells = []
            cell_start = 0
            
            while True:
                td_start = row_html.find('<td', cell_start)
                if td_start == -1:
                    td_start = row_html.find('<th', cell_start)  # Also check for header cells
                    if td_start == -1:
                        break
                
                # Find cell end
                td_end = row_html.find('>', td_start)
                if td_end == -1:
                    break
                    
                # Find closing tag
                td_close = row_html.find('</td>', td_end)
                if td_close == -1:
                    td_close = row_html.find('</th>', td_end)
                    if td_close == -1:
                        break
                
                # Extract cell content (strip tags)
                cell_content = row_html[td_end+1:td_close]
                cell_content = cell_content.replace('<br>', ' ').replace('&nbsp;', ' ').strip()
                
                # Remove any remaining HTML tags
                while '<' in cell_content and '>' in cell_content:
                    tag_start = cell_content.find('<')
                    tag_end = cell_content.find('>', tag_start)
                    if tag_end != -1:
                        cell_content = cell_content[:tag_start] + cell_content[tag_end+1:]
                
                cells.append(cell_content.strip())
                cell_start = td_close + 5
            
            if cells:  # Only add non-empty rows
                rows.append(cells)
            
            row_start = tr_end + 5
        
        if not rows:
            print("No data extracted from table")
            return False
        
        # Ensure Data directory exists
        os.makedirs('Data', exist_ok=True)
        
        # Save to CSV
        csv_path = 'Data/FII.csv'
        with open(csv_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerows(rows)
        
        print(f"Data saved to {csv_path}")
        print(f"Total rows: {len(rows)}")
        print(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        return True
        
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        return False
    except Exception as e:
        print(f"Error processing data: {e}")
        return False

if __name__ == "__main__":
    success = fetch_nsdl_data()
    if success:
        print("Script completed successfully")
    else:
        print("Script failed")
        exit(1)
