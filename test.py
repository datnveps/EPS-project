#/streams/F1AbEY5NASxI4YUq_ILrgW5Hzzwvq__ASJu7RGFjbR68eTYMQpcYk37CRIVsiRtJGazqZHAUk1TLU1EMS1QSUFGXEVBUkxZIFdBUk5JTkcgU1lTVEVNIE1EMVxNRDFcVU5JVCAxXEJPSUxFUiBBXElORFVDRUQgRFJBRlQgRkFOU1xJREYtQXxCT0lMRVIgREVNQU5EIEJJQVM/interpolated
import json
import os
import difflib
from datetime import datetime
import pandas as pd
import requests
from bs4 import BeautifulSoup
from sqlalchemy import create_engine, text
import requests
import urllib3
from requests_ntlm import HttpNtlmAuth
import psycopg2
import sqlalchemy.types as types
import re

# PI Web API base URL (shared)
BASE_URL = "https://10.32.194.4/piwebapi"
# ----------------------------------------------------
# 1. DATABASE UTILITY FUNCTIONS (Provided by User)
# ----------------------------------------------------

def pgconnect(credential_filepath, db_schema="public"):
    with open(credential_filepath) as f:
        db_conn_dict = json.load(f)
        host = db_conn_dict['host']
        db_user = db_conn_dict['user']
        db_pw = db_conn_dict['password']
        default_db = db_conn_dict['user']
        port = db_conn_dict['port']
        try:
            db = create_engine(f'postgresql+psycopg2://{db_user}:{db_pw}@{host}:{port}/{default_db}', echo=False)
            conn = db.connect()
            print('Connected successfully.')
        except Exception as e:
            print("Unable to connect to the database.")
            print(e)
            db, conn = None, None
        return db,conn
    
def query(conn, sqlcmd, args=None, df=True):
    result = pd.DataFrame() if df else None
    try:
        if df:
            result = pd.read_sql_query(sqlcmd, conn, params=args)
        else:
            result = conn.execute(text(sqlcmd), args).fetchall()
            result = result[0] if len(result) == 1 else result
    except Exception as e:
        print("Error encountered: ", e, sep='\n')
    return result

def get(endpoint, params=None):
    USERNAME = "MONGDUONG01PIAF"  
    PASSWORD = "AccountForReadOnly@MD1" 

    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    session = requests.Session()
    session.verify = False
    session.auth = HttpNtlmAuth(USERNAME, PASSWORD)

    """Helper function to make GET requests to the PI Web API."""
    url = BASE_URL + endpoint
    r = session.get(url, params=params)
    r.raise_for_status()
    return r.json()


def patch(endpoint, json_body=None):
    """Helper to send PATCH requests to the PI Web API using NTLM auth."""
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
    """Update the ConfigString of an analysis rule.

    - dry_run=True: only fetch and show diff, do not PATCH.
    - backup=True: save old ConfigString to `backup_file` before patching.
    """
    # Fetch existing rule
    rule = get(f"/analysisrules/{rule_webid}")
    old_config = rule.get('ConfigString', '')

    print(f"AnalysisRule WebId: {rule_webid}")
    print("--- Old ConfigString ---")
    print(old_config)
    print("--- New ConfigString ---")
    print(new_config_string)

    # Show unified diff
    old_lines = (old_config or '').splitlines(keepends=True)
    new_lines = (new_config_string or '').splitlines(keepends=True)
    diff = ''.join(difflib.unified_diff(old_lines, new_lines, fromfile='old', tofile='new'))
    if diff:
        print("--- Diff ---")
        print(diff)
    else:
        print("No changes detected between old and new ConfigString.")

    if dry_run:
        print("Dry-run enabled — no change will be applied.")
        return {'status': 'dry_run', 'diff': diff}

    # Backup old config if requested
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

    # Perform PATCH
    payload = {'ConfigString': new_config_string}
    resp = patch(f"/analysisrules/{rule_webid}", json_body=payload)
    print("Patch response:", resp)
    return {'status': 'patched', 'response': resp}


def find_analysis_rule_webids(rule_name, scope_webid=None, exact=True, return_all=False, max_results=10, include_fields=None):
    """Find analysis rule WebIds by name.

    - rule_name: exact name or substring to search for.
    - scope_webid: if provided, search under the element's analyses (`/elements/{webid}/analyses`).
    - exact: True for exact match, False for substring match.
    - return_all: if True returns up to `max_results`; if False returns early when limit reached.
    - include_fields: optional list of fields to fetch per matched rule (e.g. ['ConfigString']).

    Returns a list of dicts: [{'WebId':..., 'Name':..., 'Description':..., ...}, ...]
    """
    results = []

    if scope_webid:
        try:
            resp = get(f"/elements/{scope_webid}/analyses")
            items = resp.get('Items', [])
        except Exception as e:
            raise RuntimeError(f"Failed to fetch analyses for scope {scope_webid}: {e}")

        for it in items:
            name = it.get('Name', '')
            match = (name == rule_name) if exact else (rule_name in name)
            if match:
                results.append({'WebId': it.get('WebId'), 'Name': name, 'Description': it.get('Description', None)})
                if not return_all and len(results) >= max_results:
                    break
    else:
        # Try server-side search first, fallback to listing all
        items = []
        try:
            resp = get(f"/analysisrules?search={rule_name}")
            items = resp.get('Items', [])
        except Exception:
            try:
                resp = get('/analysisrules')
                items = resp.get('Items', [])
            except Exception as e:
                raise RuntimeError(f"Failed to list analysis rules: {e}")

        for it in items:
            name = it.get('Name', '')
            match = (name == rule_name) if exact else (rule_name in name)
            if match:
                results.append({'WebId': it.get('WebId'), 'Name': name, 'Description': it.get('Description', None)})
                if not return_all and len(results) >= max_results:
                    break

    # Optionally fetch extra fields for each matched rule
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
    """Return the ConfigString for the analysis rule identified by rule_webid."""
    try:
        details = get(f"/analysisrules/{rule_webid}")
        return details.get('ConfigString')
    except Exception as e:
        raise RuntimeError(f"Failed to fetch analysis rule {rule_webid}: {e}")


def get_analysis_rule_webid(rule_name, scope_webid=None, exact=True, return_all=False, max_results=10):
    """Return the WebId of analysis rule(s) matching `rule_name`.

    - If `scope_webid` is provided, search analyses under that element (`/elements/{scope_webid}/analyses`).
    - If not provided, attempt server-side search `/analysisrules?search=` then fallback to listing `/analysisrules`.
    - `exact` selects exact vs substring matching.
    - `return_all=True` returns a list of WebIds (up to `max_results`), otherwise returns the first match or None.
    """
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
    """Return the WebId for element path MD1 -> unit_name -> boiler_name -> Induced Draft Fans -> idf_name.

    Uses the same asset server and database selection as `setup()` (by index).
    """
    # find asset server
    asset_servers = get("/assetservers").get("Items", [])
    if len(asset_servers) <= asset_server_index:
        raise RuntimeError("Asset server index out of range")
    asset_server_webid = asset_servers[asset_server_index]["WebId"]

    # find database (use provided index)
    databases = get(f"/assetservers/{asset_server_webid}/assetdatabases").get("Items", [])
    if len(databases) <= database_index:
        raise RuntimeError("Database index out of range")
    db_item = databases[database_index]
    db_webid = db_item["WebId"]

    # find root MD1 element (the original code used ?name=MD1 and index 1)
    root_resp = get(f"/assetdatabases/{db_webid}/elements?name=MD1")
    items = root_resp.get("Items", [])
    if not items:
        raise RuntimeError("MD1 root element not found")
    # choose the second item if present (keeps behavior from setup())
    root_el = items[1] if len(items) > 1 else items[0]
    parent_webid = root_el["WebId"]

    # traverse Unit -> Boiler -> Induced Draft Fans -> IDF
    # Unit
    resp = get(f"/elements/{parent_webid}/elements")
    unit = next((e for e in resp.get("Items", []) if e.get("Name") == unit_name), None)
    if not unit:
        raise RuntimeError(f"Unit not found: {unit_name}")
    parent_webid = unit["WebId"]

    # Boiler
    resp = get(f"/elements/{parent_webid}/elements")
    boiler = next((e for e in resp.get("Items", []) if e.get("Name") == boiler_name), None)
    if not boiler:
        raise RuntimeError(f"Boiler not found: {boiler_name}")
    parent_webid = boiler["WebId"]

    # Induced Draft Fans group
    resp = get(f"/elements/{parent_webid}/elements")
    idf_group = next((e for e in resp.get("Items", []) if e.get("Name") == "Induced Draft Fans"), None)
    if not idf_group:
        raise RuntimeError("Induced Draft Fans group not found under boiler")
    parent_webid = idf_group["WebId"]

    # IDF-A / IDF-B
    resp = get(f"/elements/{parent_webid}/elements")
    idf = next((e for e in resp.get("Items", []) if e.get("Name") == idf_name), None)
    if not idf:
        raise RuntimeError(f"IDF element not found: {idf_name}")
    return idf["WebId"]


def find_analysis_in_element(element_webid, analysis_name='Machine_learning', exact=True):
    """Return analysis dict under element with matching name (or None)."""
    items = get(f"/elements/{element_webid}/analyses").get('Items', [])
    for it in items:
        name = it.get('Name','')
        matched = (name == analysis_name) if exact else (analysis_name.lower() in name.lower())
        if matched:
            return it
    return None


def replace_variable1_in_analysisrule(ar_webid, new_expr, dry_run=True, backup=True):
    """Replace Variable1 assignment in AnalysisRule's ConfigString and optionally PATCH it.

    Returns the result from `update_analysis_rule_config`.
    """
    ar = get(f"/analysisrules/{ar_webid}")
    cfg = ar.get('ConfigString','')

    # attempt robust replace: find Variable1 := ...; and replace up to Variable2
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
    """High-level: find Machine_learning analysis under chosen element and update Variable1.

    - `new_expr` must be provided (string expression).
    - `dry_run=True` will only show diff.
    """
    if not new_expr:
        raise ValueError('new_expr is required')
    element_webid = get_element_webid_by_hierarchy(unit, boiler, idf)
    analysis = find_analysis_in_element(element_webid, analysis_name='Machine_learning', exact=True)
    if not analysis:
        raise RuntimeError('Machine_learning analysis not found under specified element')
    # get analysis details to find AnalysisRule WebId
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

# ----------------------------------------------------
# 2. PI WEB API INTERACTION
# ----------------------------------------------------
def setup():
    def find_child_element_webid(parent_webid, child_name, get_func):
        """Fetches the WebId of a specific child element under a given parent."""
        children = get_func(f"/elements/{parent_webid}/elements")
        for el in children["Items"]:
            if el['Name'] == child_name:
                return el['WebId']
        return None
    
    

    # === REQUIRED INPUT: SET THE TARGET CATEGORY NAME ===
    TARGET_CATEGORY_NAME = "Raw"  
    # ====================================================

    # -----------------------------
    # 1 & 2. ASSET SERVER & DATABASE
    # -----------------------------
    asset_servers = get("/assetservers")
    asset_server_webid = asset_servers["Items"][0]["WebId"]
    databases = get(f"/assetservers/{asset_server_webid}/assetdatabases")
    # Using index 5 as specified in your original code
    early_warning_system_db_item = databases["Items"][5]
    early_warning_system_id = early_warning_system_db_item["WebId"] 
    db_name = early_warning_system_db_item["Name"]
    print(f"Target DB: {db_name}, WebId: {early_warning_system_id}")

    # -----------------------------
    # 3. ROOT ELEMENTS (Two levels: MD1 )
    # -----------------------------
    root_element_webid = get(f"/assetdatabases/{early_warning_system_id}/elements?name=MD1")["Items"][1]["WebId"]



    # -----------------------------
    # 4. ITERATE AND COLLECT ATTRIBUTE WEBIDS
    # -----------------------------
    # Define the structure based on the image
    target_elements = {
        "Unit 1": ["Boiler A", "Boiler B"],
        "Unit 2": ["Boiler A", "Boiler B"], 
    }
    fan_names = ["IDF-A", "IDF-B"]
    
    all_raw_attribute_webids = {} 

    print(f"\nSearching for ALL attributes with category '{TARGET_CATEGORY_NAME}'...")

    for unit_name, boiler_list in target_elements.items():
        # Step: Unit
        unit_webid = find_child_element_webid(root_element_webid, unit_name, get)
        if not unit_webid: continue
        
        for boiler_name in boiler_list:
            # Step: Boiler
            boiler_webid = find_child_element_webid(unit_webid, boiler_name, get)
            if not boiler_webid: continue

            # Step: Induced Draft Fans Group
            idf_group_webid = find_child_element_webid(boiler_webid, "Induced Draft Fans", get)
            if not idf_group_webid: continue
            
            for fan_name in fan_names:
                # Step: IDF-A / IDF-B Element
                fan_webid = find_child_element_webid(idf_group_webid, fan_name, get)
                if not fan_webid: continue

                # Step: Find ALL Raw Attributes
                raw_attributes = get(f"/elements/{fan_webid}/attributes?categoryName={TARGET_CATEGORY_NAME}")
                # Store both WebId and the attribute Id (if provided by API)
                raw_attributes = {
                    attr['Name']: {
                        'WebId': attr.get('WebId'),
                        'Id': attr.get('Id')
                    }
                    for attr in raw_attributes["Items"]
                }

                # Store the found attributes with full path keys
                for attr_name, attr_info in raw_attributes.items():
                    path = f"MD1|{unit_name}|{boiler_name}|Induced Draft Fans|{fan_name}|{attr_name}"
                    all_raw_attribute_webids[path] = {
                        'WebId': attr_info.get('WebId'),
                        'Id': attr_info.get('Id')
                    }
    return all_raw_attribute_webids
                

    # -----------------------------
    # 5. READ DATA (SINGLE STREAM - Example for verification)
    # -----------------------------
    
def populate_data(all_raw_attribute_webids, db_engine):
    if not all_raw_attribute_webids:
        print("No attributes found to process.")
        return

    # --- Step 1: Attribute to Schema Mapping (Crucial Step - Define this clearly) ---
    # This dictionary maps the PI Attribute name to the final target column (table.column)
    # NOTE: You MUST verify and complete this mapping based on your 28+ attribute names.
    ATTRIBUTE_MAPPING = {
        'FAN_BEARING_TEMP': 'fan.temp',
        'FAN_VIB_X': 'fan.x',
        'FAN_VIB_Y': 'fan.y',
        'MOTOR_BEARING_TEMP': 'motor.temp',
        'MOTOR_VIB_X': 'motor.x',
        'MOTOR_VIB_Y': 'motor.y',
        'BOILER_DEMAND_BIAS': 'IDF.lub_temp', # Assuming lub_temp is a proxy column for a demand value
        'SPEED_FEEDBACK_RPM': 'speed.speed_feed',
        'VFD_SPEED_COMMAND': 'speed.VFD_speed',
        'COIL_U1_VOLTAGE': 'coil.u1',
        'COIL_U_CURRENT': 'coil.u',
        'COIL_V1_VOLTAGE': 'coil.v1',
        'COIL_V_CURRENT': 'coil.v',
        'COIL_W1_VOLTAGE': 'coil.w1',
        'COIL_W_CURRENT': 'coil.w',
        # Add all 28+ attributes here
    }

    # --- Step 2: Fetch all Time-Series Data ---
    # Extract list of WebIds for bulk API calls
    webids_list = [v['WebId'] for v in all_raw_attribute_webids.values()]
    
    # Batch the requests to avoid URL length limit, max 100 WebIds per call
    MAX_BATCH_SIZE = 50 
    all_ts_data = []

    print("\n--- Fetching Interpolated Data (1-minute intervals for 2 months) ---")
    for i in range(0, len(webids_list), MAX_BATCH_SIZE):
        batch_webids = webids_list[i:i + MAX_BATCH_SIZE]
        webid_params = [f"webid={wid}" for wid in batch_webids]

        bulk_url = "/streams/interpolated?" + "&".join(webid_params)
        
        recorded = get(bulk_url, params={
            "startTime": "*-2mo",
            "endTime": "*",
            "interval": "1m"
        })
        
        # Structure the data into a flat list of dictionaries
        for item in recorded["Items"]:
            webid = item["WebId"]
            
            # Map WebId back to path to get hierarchy
            path = next(k for k, v in all_raw_attribute_webids.items() if v['WebId'] == webid)
            parts = path.split('|')
            attr_name = parts[-1]
            unit_name = parts[1]
            boiler_name = parts[2]
            fan_name = parts[4]

            for value_item in item["Items"]:
                if value_item.get('Good'):
                    all_ts_data.append({
                        'timestamp': pd.to_datetime(value_item['Timestamp'], utc=True),
                        'value': pd.to_numeric(value_item['Value'], errors='coerce'),
                        'attribute_name': attr_name,
                        'unit_name': unit_name,
                        'boiler_name': boiler_name,
                        'fan_name': fan_name,
                        'webid': webid # Used for debugging/mapping
                    })

    if not all_ts_data:
        print("No valid time series data retrieved.")
        return

    df_ts = pd.DataFrame(all_ts_data).dropna(subset=['value'])
    print(f"Total time-series records fetched: {len(df_ts)}")

    # Pivot the data so that each row represents one time slice and contains all component values
    df_pivot = df_ts.pivot_table(
        index=['timestamp', 'unit_name', 'boiler_name', 'fan_name'],
        columns='attribute_name',
        values='value'
    ).reset_index()

    # --- Step 3: Insert Static/Unique Dimensions (unit, boiler) ---
    with db_engine.connect() as conn:
        conn.execution_options(autocommit=True)

        # 3a. UNIT Table Insertion
        unit_data = df_pivot[['unit_name']].drop_duplicates()
        print("\nInserting/Updating Unit Dimension...")
        for _, row in unit_data.iterrows():
            # Check if exists, insert if not, return ID
            sql = text("""
                INSERT INTO unit (unit_name) VALUES (:unit_name)
                ON CONFLICT (unit_name) DO UPDATE SET unit_name=EXCLUDED.unit_name -- Dummy update to return ID
                RETURNING unit_id;
            """)
            result = conn.execute(sql, {'unit_name': row['unit_name']}).fetchone()
            row['unit_id'] = result[0]
        
        # 3b. BOILER Table Insertion
        boiler_data = df_pivot[['unit_name', 'boiler_name']].drop_duplicates()
        print("Inserting/Updating Boiler Dimension...")
        
        # Join boiler_data with the unit_id obtained above
        boiler_data = pd.merge(boiler_data, unit_data, on='unit_name', how='left')

        for _, row in boiler_data.iterrows():
            sql = text("""
                INSERT INTO boiler (boiler_name, unit_id) VALUES (:boiler_name, :unit_id)
                ON CONFLICT (boiler_name) DO NOTHING -- Assuming boiler_name is unique/used as unique identifier
                RETURNING boiler_id;
            """)
            try:
                result = conn.execute(sql, {'boiler_name': row['boiler_name'], 'unit_id': row['unit_id']}).fetchone()
                row['boiler_id'] = result[0] if result else query(conn, "SELECT boiler_id FROM boiler WHERE boiler_name = :boiler_name", {'boiler_name': row['boiler_name']}, df=False)
            except Exception as e:
                # Handle cases where the row already existed and RETURNING failed on CONFLICT DO NOTHING
                print(f"Warning: Conflict on boiler {row['boiler_name']}. Retrieving existing ID.")
                row['boiler_id'] = query(conn, "SELECT boiler_id FROM boiler WHERE boiler_name = :boiler_name", {'boiler_name': row['boiler_name']}, df=False)


    # --- Step 4: Prepare DataFrames for Component Tables (fan, motor, speed, coil) ---
    # Merge the unit/boiler IDs back into the pivot table
    df_pivot = pd.merge(df_pivot, unit_data[['unit_name', 'unit_id']], on='unit_name', how='left')
    df_pivot = pd.merge(df_pivot, boiler_data[['boiler_name', 'boiler_id']], on='boiler_name', how='left')

    # Prepare component dataframes (one row per fan/motor configuration, not per timestamp)
    
    # 4a. FAN Component (assuming type is the fan_name)
    df_fan = df_pivot.rename(columns={'FAN_BEARING_TEMP': 'temp', 'FAN_VIB_X': 'x', 'FAN_VIB_Y': 'y'})[['fan_name', 'temp', 'x', 'y']].drop_duplicates()
    df_fan = df_fan.rename(columns={'fan_name': 'type'})
    
    # 4b. MOTOR Component
    df_motor = df_pivot.rename(columns={'MOTOR_BEARING_TEMP': 'temp', 'MOTOR_VIB_X': 'x', 'MOTOR_VIB_Y': 'y'})[['fan_name', 'temp', 'x', 'y']].drop_duplicates()
    df_motor = df_motor.rename(columns={'fan_name': 'type'})

    # 4c. SPEED Component
    df_speed = df_pivot.rename(columns={'SPEED_FEEDBACK_RPM': 'speed_feed', 'VFD_SPEED_COMMAND': 'VFD_speed'})[['speed_feed', 'VFD_speed', 'timestamp']].drop_duplicates(subset=['timestamp']) # Speed is time-series

    # 4d. COIL Component
    df_coil = df_pivot.rename(columns={
        'COIL_U1_VOLTAGE': 'u1', 'COIL_U_CURRENT': 'u', 
        'COIL_V1_VOLTAGE': 'v1', 'COIL_V_CURRENT': 'v', 
        'COIL_W1_VOLTAGE': 'w1', 'COIL_W_CURRENT': 'w'
    })[['u1', 'u', 'v1', 'v', 'w1', 'w', 'timestamp']].drop_duplicates(subset=['timestamp']) # Coil is time-series

    # --- Step 5: Insert Time-Series Dimensions (speed, coil) ---
    print("\nInserting Speed and Coil Time-Series Data...")
    
    # 5a. SPEED Insertion (Insert and Retrieve IDs)
    df_speed.to_sql('speed', db_engine, if_exists='append', index=False, method='multi')
    # Retrieve the generated IDs (This is difficult without a known unique key other than time)
    # Simplified approach: Reload the data with IDs, matching on timestamp and value (risky, but standard for this schema type)
    df_speed_ids = query(db_engine, "SELECT speed_id, speed_feed, VFD_speed FROM speed")
    df_speed_with_ids = pd.merge(df_pivot, df_speed_ids, on=['speed_feed', 'VFD_speed'], how='left')
    
    # 5b. COIL Insertion (Insert and Retrieve IDs)
    df_coil.to_sql('coil', db_engine, if_exists='append', index=False, method='multi')
    df_coil_ids = query(db_engine, "SELECT coil_id, u1, u, v1, v, w1, w FROM coil")
    df_coil_with_ids = pd.merge(df_speed_with_ids, df_coil_ids, on=['u1', 'u', 'v1', 'v', 'w1', 'w'], how='left')

    # --- Step 6: Final Fact Table Preparation (IDF) ---
    
    # Insert FAN/MOTOR configurations (assuming only a few unique static configs)
    # The current schema forces FAN/MOTOR to be time-series, which is unusual. 
    # Sticking to the schema: insert unique configurations and retrieve their IDs.

    # 6a. FAN Insertion
    df_fan.to_sql('fan', db_engine, if_exists='append', index=False, method='multi')
    df_fan_ids = query(db_engine, "SELECT fan_id, type, temp, x, y FROM fan")
    df_fan_final = pd.merge(df_coil_with_ids, df_fan_ids, on=['type', 'temp', 'x', 'y'], how='left')

    # 6b. MOTOR Insertion
    df_motor.to_sql('motor', db_engine, if_exists='append', index=False, method='multi')
    df_motor_ids = query(db_engine, "SELECT motor_id, type, temp, x, y FROM motor")
    df_motor_final = pd.merge(df_fan_final, df_motor_ids, on=['type', 'temp', 'x', 'y'], how='left')


    # 6c. Final IDF Fact Table DataFrame
    df_idf_fact = df_motor_final.rename(columns={'BOILER_DEMAND_BIAS': 'lub_temp', 'timestamp': 'time_stamp'})[[
        'boiler_id', 'fan_id', 'motor_id', 'coil_id', 'time_stamp', 'speed_id', 'lub_temp'
    ]]

    # --- Step 7: Insert into Final Fact Table (IDF) ---
    print("\nInserting into final IDF Fact Table...")
    
    # Specify dtypes to prevent insertion errors with float/numeric columns
    idf_dtypes = {
        'boiler_id': types.Integer,
        'fan_id': types.Integer,
        'motor_id': types.Integer,
        'coil_id': types.Integer,
        'time_stamp': types.DateTime,
        'speed_id': types.Integer,
        'lub_temp': types.Float, # Use Float for SQL to handle pandas float64
    }

    df_idf_fact.to_sql(
        'idf', 
        db_engine, 
        if_exists='append', 
        index=False, 
        method='multi',
        chunksize=5000,
        dtype=idf_dtypes
    )
    print(f"Successfully inserted {len(df_idf_fact)} rows into the IDF fact table.")


def create_schema():
    credential_filepath = 'db_credentials.json'
    db, conn = pgconnect(credential_filepath, db_schema="public")


    conn.execute(text("""
    CREATE TABLE IF NOT EXISTS unit (
        unit_id SERIAL PRIMARY KEY,
        unit_name VARCHAR(50) UNIQUE NOT NULL
    );
    CREATE TABLE IF NOT EXISTS boiler (
        boiler_id SERIAL PRIMARY KEY,
        boiler_name VARCHAR(50) UNIQUE NOT NULL,
        unit_id INT REFERENCES unit(unit_id)
    );
    CREATE TABLE IF NOT EXISTS fan (
        fan_id SERIAL UNIQUE ,
        type VARCHAR(50),
        PRIMARY KEY (fan_id, type),
        temp FLOAT,
        x FLOAT,
        y FLOAT
    );
    CREATE TABLE IF NOT EXISTS motor (
        motor_id SERIAL UNIQUE,
        type VARCHAR(50),
        PRIMARY KEY (motor_id, type),
        temp FLOAT,
        x FLOAT,
        y FLOAT
    );
    CREATE TABLE IF NOT EXISTS speed (
        speed_id SERIAL PRIMARY KEY,
        speed_feed FLOAT,
        VFD_speed FLOAT
    );
    CREATE TABLE IF NOT EXISTS coil (
        coil_id SERIAL PRIMARY KEY,
        u1 FLOAT,
        u FLOAT,
        v1 FLOAT,
        v FLOAT,
        w1 FLOAT,
        w FLOAT
    );
    CREATE TABLE IF NOT EXISTS IDF (
        idf_id SERIAL PRIMARY KEY,
        boiler_id INT REFERENCES boiler(boiler_id),
        fan_id INT REFERENCES fan(fan_id),
        motor_id INT REFERENCES motor(motor_id),
        coil_id INT REFERENCES coil(coil_id),
        time_stamp TIMESTAMP NOT NULL,
        speed_id INT REFERENCES speed(speed_id),
        lub_temp FLOAT
    );
    """))

    conn.commit()
    return conn

def main():
    all_raw_attribute_webids=setup()
    print(f"Total Raw Attributes Found: {all_raw_attribute_webids}")
    #conn=create_schema()
    #populate_data(all_raw_attribute_webids, conn)
    

if __name__ == "__main__":
    main()