import pandas as pd
import requests
import re
import zipfile
import json
from io import BytesIO

# Read the CSV
df = pd.read_csv("https://docs.google.com/spreadsheets/d/e/2PACX-1vTBuDewVgTDoc_zaWYQyaWKpBt0RwtFPhnBrpqr1v6Y5wfAmPpEYvTsaWd64bsHhH68iYNtLMSRpOQ0/pub?gid=1012340291&single=true&output=csv")

# Filter to get only rows with SCHOOL NAME and Documents Zip
data = df[['SCHOOL NAME', 'Documents Zip']].dropna(subset=['SCHOOL NAME'])

def get_filename(file_id):
    """Get original filename from Google Drive"""
    try:
        meta_url = f"https://drive.google.com/file/d/{file_id}/view"
        resp = requests.get(meta_url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=10)
        
        # Try JSON-LD
        json_ld = re.search(r'<script type="application/ld\+json">(.*?)</script>', resp.text, re.DOTALL)
        if json_ld:
            data = json.loads(json_ld.group(1))
            name = data.get('name', f"file_{file_id}.pdf")
            return name if name.lower().endswith('.pdf') else f"{name}.pdf"
            
        # Try HTML title
        title = re.search(r'<title>(.*?) - Google Drive</title>', resp.text)
        if title:
            name = title.group(1).strip()
            return name if name.lower().endswith('.pdf') else f"{name}.pdf"
    except:
        pass
    return f"file_{file_id}.pdf"

def create_school_zip(school_name, links_str):
    """Create zip file for a school"""
    if not links_str or str(links_str).lower() == 'nan':
        return False
    
    # Clean school name for filename
    clean_name = re.sub(r'[<>:"/\\|?*]', '_', school_name)
    zip_filename = f"{clean_name}.zip"
    
    links = [l.strip() for l in str(links_str).split(';') if l.strip()]
    print(f"\n📦 Processing: {school_name}")
    print(f"   Found {len(links)} document(s)")
    
    success = 0
    with zipfile.ZipFile(zip_filename, 'w') as zipf:
        for i, link in enumerate(links):
            # Extract file ID
            match = re.search(r'id=([a-zA-Z0-9_-]+)', link)
            if not match:
                match = re.search(r'/d/([a-zA-Z0-9_-]+)', link)
            
            if not match:
                continue
                
            file_id = match.group(1)
            filename = get_filename(file_id)
            
            # Ensure PDF extension
            if not filename.lower().endswith('.pdf'):
                filename = f"{filename}.pdf"
            
            # Download file
            try:
                dl_url = f"https://drive.google.com/uc?export=download&id={file_id}"
                session = requests.Session()
                response = session.get(dl_url, stream=True, timeout=30)
                
                # Handle large file confirmation
                if "confirm=" in response.url:
                    token = re.search(r'confirm=([0-9A-Za-z_]+)', response.url).group(1)
                    response = session.get(f"{dl_url}&confirm={token}", stream=True, timeout=30)
                
                # Read content and add to zip
                content = response.content
                zipf.writestr(filename, content)
                success += 1
                print(f"   ✓ {filename}")
                
            except Exception as e:
                print(f"   ✗ Error downloading {file_id}: {str(e)[:50]}")
                continue
    
    if success > 0:
        print(f"   ✅ Created: {zip_filename} ({success}/{len(links)} files)")
        return True
    return False

# Process each school
print(f"🏫 Processing {len(data)} schools...")
for idx, row in data.iterrows():
    school_name = row['SCHOOL NAME']
    documents_zip = row['Documents Zip']
    create_school_zip(school_name, documents_zip)

print("\n🎉 All schools processed!")
