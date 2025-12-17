import os
import sys
import urllib.parse

# Ensure local workspace is importable
workspace_root = os.path.dirname(os.path.dirname(__file__))
if workspace_root not in sys.path:
    sys.path.insert(0, workspace_root)

from test import get

path = r"\\RMS-MD1-PIAF\\Early Warning System MD1\\MD1\\Unit 1\\Boiler A\\Induced Draft Fans\\IDF-A"
parts = [p for p in path.split('\\') if p]
asset_server_name = parts[0]
db_name = parts[1]
element_names = parts[2:]

# find asset server
servers = get('/assetservers').get('Items', [])
asw = next((s for s in servers if s.get('Name') == asset_server_name), None)
if not asw:
    raise SystemExit('Asset server not found')
as_webid = asw['WebId']

# find database
dbs = get(f"/assetservers/{as_webid}/assetdatabases").get('Items', [])
db = next((d for d in dbs if d.get('Name') == db_name), None)
if not db:
    raise SystemExit('Asset database not found')

# find root element
resp = get(f"/assetdatabases/{db['WebId']}/elements?name=" + urllib.parse.quote(element_names[0]))
items = resp.get('Items', [])
parent = next((it for it in items if it.get('Name') == element_names[0]), None)
if not parent:
    raise SystemExit('Root element not found')

parent_local = parent['WebId']
# traverse remaining parts
for name in element_names[1:]:
    resp = get(f"/elements/{parent_local}/elements")
    child = next((c for c in resp.get('Items', []) if c.get('Name') == name), None)
    if not child:
        raise SystemExit(f'Missing child: {name}')
    parent_local = child['WebId']

print('Element WebId:', parent_local)

# list analyses and find ones matching 'machine' (case-insensitive)
analyses = get(f"/elements/{parent_local}/analyses").get('Items', [])
matches = [a for a in analyses if 'machine' in (a.get('Name','').lower())]

if not matches:
    print('No analyses matching "machine" found under element.')
    sys.exit(0)

for a in matches:
    name = a.get('Name')
    webid = a.get('WebId')
    print('\nFound analysis:', name, webid)
    # fetch analysis details
    details = get(f"/analyses/{webid}")
    # print keys to inspect where AnalysisRule reference may be
    print('Analysis detail keys:', list(details.keys()))

    # Try finding a direct AnalysisRule reference
    ar_webid = None
    # Common places: Details may include 'AnalysisRule' as an object or 'Links'
    if isinstance(details.get('AnalysisRule'), dict):
        ar_webid = details['AnalysisRule'].get('WebId')
    # Check Links for AnalysisRule or AnalysisRule link
    links = details.get('Links') or {}
    for k, v in links.items():
        if 'analysisrule' in k.lower() or 'analysisrules' in k.lower():
            # v may be a URL or href; try to extract last path component as WebId
            if isinstance(v, str) and '/analysisrules/' in v:
                ar_webid = v.split('/analysisrules/')[-1]
                break
    # If still none, check other fields
    if not ar_webid:
        # some APIs include 'AnalysisRuleWebId' or similar
        ar_webid = details.get('AnalysisRuleWebId') or details.get('AnalysisRuleId')

    if ar_webid:
        print('Associated AnalysisRule WebId:', ar_webid)
        # fetch analysisrule details
        ar_details = get(f"/analysisrules/{ar_webid}")
        cfg = ar_details.get('ConfigString')
        if cfg is None:
            print('No ConfigString present on AnalysisRule or inaccessible.')
        else:
            print('\n--- ConfigString ---')
            print(cfg)
            print('--- end ConfigString ---')
            print('\nBrowse URL: https://10.32.194.4/piwebapi/analysisrules/' + ar_webid)
    else:
        print('No AnalysisRule link found in analysis details. Full details keys printed above.')
