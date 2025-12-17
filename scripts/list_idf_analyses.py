import sys
import os
import urllib.parse
# Ensure the workspace root is on sys.path so the local `test.py` is imported
workspace_root = os.path.dirname(os.path.dirname(__file__))
if workspace_root not in sys.path:
    sys.path.insert(0, workspace_root)
from test import get

path = r"\\RMS-MD1-PIAF\\Early Warning System MD1\\MD1\\Unit 1\\Boiler A\\Induced Draft Fans\\IDF-A"
parts = [p for p in path.split("\\") if p]
if len(parts) < 3:
    print("Invalid path")
    sys.exit(1)

asset_server_name = parts[0]
db_name = parts[1]
element_names = parts[2:]

servers = get('/assetservers').get('Items', [])
asw = next((s for s in servers if s.get('Name') == asset_server_name), None)
if not asw:
    print(f"Asset server not found: {asset_server_name}")
    sys.exit(1)
print('AssetServer:', asw.get('Name'))
as_webid = asw['WebId']

dbs = get(f"/assetservers/{as_webid}/assetdatabases").get('Items', [])
db = next((d for d in dbs if d.get('Name') == db_name), None)
if not db:
    print(f"Asset database not found: {db_name}")
    sys.exit(1)
print('Database:', db.get('Name'))

# find root element
resp = get(f"/assetdatabases/{db['WebId']}/elements?name=" + urllib.parse.quote(element_names[0]))
items = resp.get('Items', [])
parent = next((it for it in items if it.get('Name') == element_names[0]), None)
if not parent:
    print(f"Root element not found: {element_names[0]}")
    sys.exit(1)

parent_local = parent['WebId']

# traverse remaining path
for name in element_names[1:]:
    resp = get(f"/elements/{parent_local}/elements")
    child = next((c for c in resp.get('Items', []) if c.get('Name') == name), None)
    if not child:
        print(f"Missing child: {name}")
        sys.exit(1)
    parent_local = child['WebId']

print('\nFinal Element WebId:', parent_local)

analyses = get(f"/elements/{parent_local}/analyses").get('Items', [])
print('\nAnalyses:')
for a in analyses:
    print(a.get('Name'), a.get('WebId'))
print('Total analyses:', len(analyses))
