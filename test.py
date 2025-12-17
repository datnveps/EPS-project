import json
import os
import difflib
from datetime import datetime
import requests
import urllib3
from requests_ntlm import HttpNtlmAuth
import re

# PI Web API base URL (shared)
BASE_URL = "https://10.32.194.4/piwebapi"


def get(endpoint, params=None):
    USERNAME = "MONGDUONG01PIAF"
    PASSWORD = "AccountForReadOnly@MD1"

    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    session = requests.Session()
    session.verify = False
    session.auth = HttpNtlmAuth(USERNAME, PASSWORD)

    url = BASE_URL + endpoint
    r = session.get(url, params=params)
    r.raise_for_status()
    return r.json()


def patch(endpoint, json_body=None):
    USERNAME = "MONGDUONG01PIAF"
    PASSWORD = "AccountForReadOnly@MD1"

    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    session = requests.Session()
    session.verify = False
    session.auth = HttpNtlmAuth(USERNAME, PASSWORD)

    url = BASE_URL + endpoint
    r = session.patch(url, json=json_body)
    r.raise_for_status()
    try:
        return r.json()
    except ValueError:
        return {'status_code': r.status_code, 'text': r.text}


def update_analysis_rule_config(rule_webid, new_config_string, dry_run=True, backup=True, backup_file='analysisrule_backups.json'):
    rule = get(f"/analysisrules/{rule_webid}")
    old_config = rule.get('ConfigString', '')

    old_lines = (old_config or '').splitlines(keepends=True)
    new_lines = (new_config_string or '').splitlines(keepends=True)
    diff = ''.join(difflib.unified_diff(old_lines, new_lines, fromfile='old', tofile='new'))
    print(f"AnalysisRule WebId: {rule_webid}")
    if diff:
        print('--- Diff ---')
        print(diff)
    else:
        print('No changes detected between old and new ConfigString.')

    if dry_run:
        print('Dry-run enabled — no change will be applied.')
        return {'status': 'dry_run', 'diff': diff}

    if backup:
        entry = {
            'timestamp': datetime.utcnow().isoformat() + 'Z',
            'webid': rule_webid,
            'old_config': old_config,
            'new_config': new_config_string
        }
        try:
            if os.path.exists(backup_file):
                with open(backup_file, 'r', encoding='utf-8') as bf:
                    data = json.load(bf)
            else:
                data = []
        except Exception:
            data = []
        data.append(entry)
        with open(backup_file, 'w', encoding='utf-8') as bf:
            json.dump(data, bf, indent=2, ensure_ascii=False)
        print(f"Backup written to {backup_file}")

    payload = {'ConfigString': new_config_string}
    resp = patch(f"/analysisrules/{rule_webid}", json_body=payload)
    print('Patch response:', resp)
    return {'status': 'patched', 'response': resp}


def find_analysis_rule_webids(rule_name, scope_webid=None, exact=True, return_all=False, max_results=10, include_fields=None):
    results = []
    if scope_webid:
        resp = get(f"/elements/{scope_webid}/analyses")
        items = resp.get('Items', [])
    else:
        try:
            resp = get(f"/analysisrules?search={rule_name}")
            items = resp.get('Items', [])
        except Exception:
            resp = get('/analysisrules')
            items = resp.get('Items', [])

    for it in items:
        name = it.get('Name', '')
        match = (name == rule_name) if exact else (rule_name in name)
        if match:
            results.append({'WebId': it.get('WebId'), 'Name': name, 'Description': it.get('Description', None)})
            if not return_all and len(results) >= max_results:
                break

    if include_fields and results:
        for r in results:
            try:
                details = get(f"/analysisrules/{r['WebId']}")
                for fld in include_fields:
                    r[fld] = details.get(fld)
            except Exception:
                r.update({fld: None for fld in include_fields})
    return results


def get_analysis_rule_config(rule_webid):
    details = get(f"/analysisrules/{rule_webid}")
    return details.get('ConfigString')


def get_analysis_rule_webid(rule_name, scope_webid=None, exact=True, return_all=False, max_results=10):
    items = []
    try:
        if scope_webid:
            resp = get(f"/elements/{scope_webid}/analyses")
            items = resp.get('Items', [])
        else:
            try:
                resp = get(f"/analysisrules?search={rule_name}")
                items = resp.get('Items', [])
            except Exception:
                resp = get('/analysisrules')
                items = resp.get('Items', [])
    except Exception as e:
        raise RuntimeError(f"Failed to retrieve analysis rules: {e}")

    matches = []
    for it in items:
        name = it.get('Name', '')
        matched = (name == rule_name) if exact else (rule_name in name)
        if matched:
            matches.append(it.get('WebId'))
            if not return_all and len(matches) >= max_results:
                break

    if return_all:
        return matches
    return matches[0] if matches else None


# -----------------------------
# Helpers: element & analysis helpers
# -----------------------------


def get_element_webid_by_hierarchy(unit_name, boiler_name, idf_name,
                                   asset_server_index=0, database_index=5):
    asset_servers = get("/assetservers").get("Items", [])
    if len(asset_servers) <= asset_server_index:
        raise RuntimeError("Asset server index out of range")
    asset_server_webid = asset_servers[asset_server_index]["WebId"]

    databases = get(f"/assetservers/{asset_server_webid}/assetdatabases").get("Items", [])
    if len(databases) <= database_index:
        raise RuntimeError("Database index out of range")
    db_item = databases[database_index]
    db_webid = db_item["WebId"]

    root_resp = get(f"/assetdatabases/{db_webid}/elements?name=MD1")
    items = root_resp.get("Items", [])
    if not items:
        raise RuntimeError("MD1 root element not found")
    root_el = items[1] if len(items) > 1 else items[0]
    parent_webid = root_el["WebId"]

    resp = get(f"/elements/{parent_webid}/elements")
    unit = next((e for e in resp.get("Items", []) if e.get("Name") == unit_name), None)
    if not unit:
        raise RuntimeError(f"Unit not found: {unit_name}")
    parent_webid = unit["WebId"]

    resp = get(f"/elements/{parent_webid}/elements")
    boiler = next((e for e in resp.get("Items", []) if e.get("Name") == boiler_name), None)
    if not boiler:
        raise RuntimeError(f"Boiler not found: {boiler_name}")
    parent_webid = boiler["WebId"]

    resp = get(f"/elements/{parent_webid}/elements")
    idf_group = next((e for e in resp.get("Items", []) if e.get("Name") == "Induced Draft Fans"), None)
    if not idf_group:
        raise RuntimeError("Induced Draft Fans group not found under boiler")
    parent_webid = idf_group["WebId"]

    resp = get(f"/elements/{parent_webid}/elements")
    idf = next((e for e in resp.get("Items", []) if e.get("Name") == idf_name), None)
    if not idf:
        raise RuntimeError(f"IDF element not found: {idf_name}")
    return idf["WebId"]



def find_analysis_in_element(element_webid, analysis_name='Machine_learning', exact=True):
    items = get(f"/elements/{element_webid}/analyses").get('Items', [])
    for it in items:
        name = it.get('Name','')
        matched = (name == analysis_name) if exact else (analysis_name.lower() in name.lower())
        if matched:
            return it
    return None


def replace_variable1_in_analysisrule(ar_webid, new_expr, dry_run=True, backup=True):
    ar = get(f"/analysisrules/{ar_webid}")
    cfg = ar.get('ConfigString','')
    m = re.search(r"(Variable1\s*:=\s*)(.*?);", cfg, flags=re.DOTALL|re.IGNORECASE)
    if m:
        start_pos = m.start(1)
        nxt = re.search(r"\n\s*Variable2\s*:=" , cfg, flags=re.IGNORECASE)
        if nxt:
            new_cfg = cfg[:start_pos] + "Variable1 := " + new_expr + ";" + cfg[nxt.start():]
        else:
            new_cfg = re.sub(r"(Variable1\s*:=\s*)(.*?);", r"\1" + new_expr + ";", cfg, flags=re.DOTALL|re.IGNORECASE)
    else:
        new_cfg = new_expr + ';\n' + cfg
    return update_analysis_rule_config(ar_webid, new_cfg, dry_run=dry_run, backup=backup)


def apply_variable1_to_machine_learning(unit='Unit 1', boiler='Boiler A', idf='IDF-A',
                                        new_expr=None, dry_run=True, backup=True):
    if not new_expr:
        raise ValueError('new_expr is required')
    element_webid = get_element_webid_by_hierarchy(unit, boiler, idf)
    analysis = find_analysis_in_element(element_webid, analysis_name='Machine_learning', exact=True)
    if not analysis:
        raise RuntimeError('Machine_learning analysis not found under specified element')
    details = get(f"/analyses/{analysis['WebId']}")
    ar_webid = None
    if isinstance(details.get('AnalysisRule'), dict):
        ar_webid = details['AnalysisRule'].get('WebId')
    links = details.get('Links') or {}
    for k,v in links.items():
        if 'analysisrule' in k.lower() and isinstance(v, str) and '/analysisrules/' in v:
            ar_webid = v.split('/analysisrules/')[-1]
            break
    ar_webid = ar_webid or details.get('AnalysisRuleWebId') or details.get('AnalysisRuleId')
    if not ar_webid:
        raise RuntimeError('AnalysisRule WebId not found for Machine_learning')
    return replace_variable1_in_analysisrule(ar_webid, new_expr, dry_run=dry_run, backup=backup)


if __name__ == "__main__":
    def get_analysisrule_webid_from_analysis(analysis_webid):
        details = get(f"/analyses/{analysis_webid}")
        ar_webid = None
        if isinstance(details.get('AnalysisRule'), dict):
            ar_webid = details['AnalysisRule'].get('WebId')
        links = details.get('Links') or {}
        for k, v in links.items():
            if 'analysisrule' in k.lower() and isinstance(v, str) and '/analysisrules/' in v:
                ar_webid = v.split('/analysisrules/')[-1]
                break
        return ar_webid or details.get('AnalysisRuleWebId') or details.get('AnalysisRuleId')


    def interactive_update_variable():
        try:
            unit = input("Unit (default 'Unit 1'): ") or "Unit 1"
            boiler = input("Boiler (default 'Boiler A'): ") or "Boiler A"
            idf = input("IDF (default 'IDF-A'): ") or "IDF-A"
            el = get_element_webid_by_hierarchy(unit, boiler, idf)
            items = get(f"/elements/{el}/analyses").get("Items", [])
            if not items:
                print("No analyses found under element.")
                return
            print(f"Found {len(items)} analyses:")
            for i, it in enumerate(items, start=1):
                print(f"{i}. {it.get('Name')}  | WebId: {it.get('WebId')}")

            sel = input("Select analysis by number (or 'q' to quit): ")
            if sel.lower() == 'q':
                return
            try:
                idx = int(sel) - 1
                analysis = items[idx]
            except Exception:
                print("Invalid selection")
                return

            print(f"Selected: {analysis.get('Name')} (WebId: {analysis.get('WebId')})")
            ar_webid = get_analysisrule_webid_from_analysis(analysis.get('WebId'))
            if not ar_webid:
                print('AnalysisRule WebId not found for selected analysis')
                return

            cfg = get_analysis_rule_config(ar_webid) or ''
            # find variables
            pattern = re.compile(r'(?:^|\n)\s*(Variable\d+)\s*:=\s*(.*?);', flags=re.DOTALL | re.IGNORECASE)
            matches = list(pattern.finditer(cfg))
            if not matches:
                print('No VariableN patterns found in ConfigString.')
                return

            print('Variables found:')
            variables_dump = []
            for i, m in enumerate(matches, start=1):
                varname = m.group(1)
                expr = m.group(2).strip()
                # Print full expression (preserve as single line)
                expr_full = expr.replace('\n', ' ')
                print(f"{i}. {varname} := {expr_full}")
                variables_dump.append((varname, expr))

            # Also write full variable expressions to a file for exact inspection
            try:
                import hashlib
                safe_name = hashlib.md5(ar_webid.encode('utf-8')).hexdigest()
                dump_name = f"analysis_variables_{safe_name}.txt"
                with open(dump_name, 'w', encoding='utf-8') as df:
                    df.write(f"AnalysisRule: {ar_webid}\n\n")
                    for varname, expr in variables_dump:
                        df.write(varname + ' := ' + expr + '\n\n')
                print(f"Full variable expressions written to: {os.path.abspath(dump_name)}")
            except Exception as _e:
                print('Failed to write variables file:', _e)

            selv = input("Select variable by number (or name, e.g. Variable1): ")
            if not selv:
                print('No variable selected')
                return
            # resolve selection to match
            chosen_match = None
            if selv.isdigit():
                idxv = int(selv) - 1
                if 0 <= idxv < len(matches):
                    chosen_match = matches[idxv]
            else:
                for m in matches:
                    if m.group(1).lower() == selv.lower():
                        chosen_match = m
                        break
            if not chosen_match:
                print('Variable selection not found')
                return

            print('Enter new expression for', chosen_match.group(1), "(finish with an empty line):")
            lines = []
            while True:
                ln = input()
                if ln == '':
                    break
                lines.append(ln)
            new_expr = '\n'.join(lines).strip()
            if not new_expr:
                print('Empty new expression; aborting')
                return

            # build new config
            s_start, s_end = chosen_match.span(2)
            new_cfg = cfg[:s_start] + new_expr + cfg[s_end:]

            print('\n--- Showing diff (dry-run) ---')
            resp = update_analysis_rule_config(ar_webid, new_cfg, dry_run=True, backup=False)
            confirm = input('Apply changes? (y/N): ')
            if confirm.lower() == 'y':
                print('Applying change (backup will be created)...')
                res = update_analysis_rule_config(ar_webid, new_cfg, dry_run=False, backup=True)
                print('Result:', res)
            else:
                print('Aborted; no change applied.')

        except Exception as e:
            print('Error in interactive updater:', e)


    # Main prompt: demo or interactive
    print("Choose action:")
    print("1) Demo: list analyses under Unit 1 > Boiler A > IDF-A")
    print("2) Interactive updater (select unit/boiler/idf, analysis, variable, apply)")
    choice = input("Choose [1/2] (default 1): ") or '1'
    if choice.strip() == '2':
        interactive_update_variable()
    else:
        try:
            print("Demo: listing analyses under Unit 1 > Boiler A > IDF-A")
            el = get_element_webid_by_hierarchy("Unit 1", "Boiler A", "IDF-A")
            print("Element WebId:", el)
            items = get(f"/elements/{el}/analyses").get("Items", [])
            print(f"Total analyses: {len(items)}")
            for it in items:
                print("-", it.get("Name"), "| WebId:", it.get("WebId"))
        except Exception as e:
            print("Error:", e)
