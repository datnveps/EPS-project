import os
import sys
import re
import urllib.parse

# Ensure local workspace is importable
workspace_root = os.path.dirname(os.path.dirname(__file__))
if workspace_root not in sys.path:
    sys.path.insert(0, workspace_root)

from test import get, update_analysis_rule_config

# Element path and target analysis name
path = r"\\RMS-MD1-PIAF\\Early Warning System MD1\\MD1\\Unit 1\\Boiler A\\Induced Draft Fans\\IDF-A"
analysis_name_match = 'machine_learning'  # case-insensitive substring match

# Desired new expression for Variable1 (from user)
new_expr = "-24.9018 + (0.1745 * 'CCW CLR OUT CCW TEMP') + (-0.0234 * 'FAN NDE-X') + (-0.0652 * 'FAN NDE-Y') + (-0.0019 * 'GROSS MW') + (-0.1776 * 'L/O TK TEMP') + (0.2828 * 'MOT CUR') + (0.1229 * 'MTR DE-X') + (0.0125 * 'MTR DE-Y') + (-0.2313 * 'MTR NDE-X') + (0.1471 * 'MTR NDE-Y') + (-0.5973 * 'SPEED FEEDBACK') + (1.7051 * 'BRG TEMP AVG') + (0.0634 * 'FLUE GAS AH OUTL TEMP_AVG')"

# --- locate element WebId from path ---
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

# find analysis matching name
analyses = get(f"/elements/{parent_local}/analyses").get('Items', [])
matches = [a for a in analyses if analysis_name_match in a.get('Name','').lower()]
if not matches:
    print('No matching analyses found.')
    sys.exit(0)

for a in matches:
    name = a.get('Name')
    awid = a.get('WebId')
    print('\nFound analysis:', name, awid)
    details = get(f"/analyses/{awid}")

    # find AnalysisRule webid
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
        print('No AnalysisRule associated with this analysis; skipping.')
        continue

    print('AnalysisRule WebId:', ar_webid)
    ar_details = get(f"/analysisrules/{ar_webid}")
    cfg = ar_details.get('ConfigString', '')
    print('\nCurrent Variable1 snippet (first 200 chars):')
    m = re.search(r"(Variable1\s*:=\s*)(.*?);", cfg, flags=re.DOTALL|re.IGNORECASE)
    if m:
        print(m.group(0)[:200])
    else:
        print('Variable1 assignment not found in ConfigString; will prepend new Variable1.')

    # construct new ConfigString by replacing Variable1 := ...; or prepending if not found
    if m:
        # safer replacement: find the start of Variable1 and the start of Variable2, then replace the span in between
        start_pos = m.start(1)
        nxt = re.search(r"\n\s*Variable2\s*:=" , cfg, flags=re.IGNORECASE)
        if nxt:
            new_cfg = cfg[:start_pos] + "Variable1 := " + new_expr + ";" + cfg[nxt.start():]
        else:
            # fallback to regex replace (may fail if semicolons are inside tag refs)
            new_cfg = re.sub(r"(Variable1\s*:=\s*)(.*?);", r"\1" + new_expr + ";", cfg, flags=re.DOTALL|re.IGNORECASE)
    else:
        new_cfg = new_expr + ';\n' + cfg

    # dry-run update (no backup, no patch)
    print('\nRunning dry-run update...')
    res = update_analysis_rule_config(ar_webid, new_cfg, dry_run=True, backup=False)
    print('\nDry-run status:', res.get('status'))
    if res.get('diff'):
        print('\nDiff:\n', res['diff'])
    else:
        print('No diff produced.')

print('\nDone.')
