import os
import sys
import re
import urllib.parse

# Ensure local workspace is importable
workspace_root = os.path.dirname(os.path.dirname(__file__))
if workspace_root not in sys.path:
    sys.path.insert(0, workspace_root)

from test import get, update_analysis_rule_config

path = r"\\RMS-MD1-PIAF\\Early Warning System MD1\\MD1\\Unit 1\\Boiler A\\Induced Draft Fans\\IDF-A"
# match analysis name exactly 'Machine_learning' (case-insensitive)
analysis_name_exact = 'machine_learning'
# New Variable1 expression as requested
new_expr = "(1.7051 * 'BRG TEMP AVG') + (0.0634 * 'FLUE GAS AH OUTL TEMP_AVG')"

# locate element WebId from path
parts = [p for p in path.split('\\') if p]
asset_server_name = parts[0]
db_name = parts[1]
element_names = parts[2:]

servers = get('/assetservers').get('Items', [])
asw = next((s for s in servers if s.get('Name') == asset_server_name), None)
if not asw:
    raise SystemExit('Asset server not found')
as_webid = asw['WebId']

dbs = get(f"/assetservers/{as_webid}/assetdatabases").get('Items', [])
db = next((d for d in dbs if d.get('Name') == db_name), None)
if not db:
    raise SystemExit('Asset database not found')

resp = get(f"/assetdatabases/{db['WebId']}/elements?name=" + urllib.parse.quote(element_names[0]))
items = resp.get('Items', [])
parent = next((it for it in items if it.get('Name') == element_names[0]), None)
if not parent:
    raise SystemExit('Root element not found')

parent_local = parent['WebId']
for name in element_names[1:]:
    resp = get(f"/elements/{parent_local}/elements")
    child = next((c for c in resp.get('Items', []) if c.get('Name') == name), None)
    if not child:
        raise SystemExit(f'Missing child: {name}')
    parent_local = child['WebId']

print('Element WebId:', parent_local)

# find exact analysis
analyses = get(f"/elements/{parent_local}/analyses").get('Items', [])
match = next((a for a in analyses if a.get('Name','').lower() == analysis_name_exact), None)
if not match:
    print('No exact analysis named Machine_learning found under element.')
    sys.exit(0)

name = match.get('Name')
awid = match.get('WebId')
print('Found analysis:', name, awid)

# fetch analysis details and analysisrule webid
details = get(f"/analyses/{awid}")
ar_webid = None
if isinstance(details.get('AnalysisRule'), dict):
    ar_webid = details['AnalysisRule'].get('WebId')
links = details.get('Links') or {}
for k, v in links.items():
    if 'analysisrule' in k.lower() and isinstance(v, str) and '/analysisrules/' in v:
        ar_webid = v.split('/analysisrules/')[-1]
        break
ar_webid = ar_webid or details.get('AnalysisRuleWebId') or details.get('AnalysisRuleId')
if not ar_webid:
    print('No AnalysisRule associated with this analysis; aborting.')
    sys.exit(0)

print('AnalysisRule WebId:', ar_webid)

# fetch current config
ar_details = get(f"/analysisrules/{ar_webid}")
cfg = ar_details.get('ConfigString', '')
print('\nCurrent Variable1 snippet:')
mm = re.search(r"(Variable1\s*:=\s*)(.*?);", cfg, flags=re.DOTALL|re.IGNORECASE)
if mm:
    print(mm.group(0))
else:
    print('Variable1 not present - will prepend new Variable1')

# build new config by replacing Variable1 safely
if mm:
    start_pos = mm.start(1)
    nxt = re.search(r"\n\s*Variable2\s*:=" , cfg, flags=re.IGNORECASE)
    if nxt:
        new_cfg = cfg[:start_pos] + "Variable1 := " + new_expr + ";" + cfg[nxt.start():]
    else:
        new_cfg = re.sub(r"(Variable1\s*:=\s*)(.*?);", r"\1" + new_expr + ";", cfg, flags=re.DOTALL|re.IGNORECASE)
else:
    new_cfg = new_expr + ';\n' + cfg

print('\nApplying PATCH (backup=True) ...')
res = update_analysis_rule_config(ar_webid, new_cfg, dry_run=False, backup=True)
print('Result:', res)
print('\nDone.')
