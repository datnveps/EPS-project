#/streams/F1AbEY5NASxI4YUq_ILrgW5Hzzwvq__ASJu7RGFjbR68eTYMQk9S8C_HxAVQ7x6nPOoAewQUk1TLU1EMS1QSUFGXEVBUkxZIFdBUk5JTkcgU1lTVEVNIE1EMVxNRDFcVU5JVCAxXEJPSUxFUiBBXElORFVDRUQgRFJBRlQgRkFOU1xJREYtQXxCT0lMRVIgQk0gT1VU/interpolated
import json
import pandas as pd
import requests
from bs4 import BeautifulSoup
from sqlalchemy import create_engine, text
import requests
import urllib3
from requests_ntlm import HttpNtlmAuth
import psycopg2

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

    BASE_URL = "https://10.32.194.4/piwebapi"
    """Helper function to make GET requests to the PI Web API."""
    url = BASE_URL + endpoint
    r = session.get(url, params=params)
    r.raise_for_status()
    return r.json()

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
                raw_attributes = {attr['Name']: attr['WebId'] for attr in raw_attributes["Items"]}
                
                # Store the found attributes
                for attr_name, attr_webid in raw_attributes.items():
                    # Update path to include the new MD1 level
                    path = f"MD1|{unit_name}|{boiler_name}|Induced Draft Fans|{fan_name}|{attr_name}"
                    all_raw_attribute_webids[path] = attr_webid
                
                return all_raw_attribute_webids

    # -----------------------------
    # 5. READ DATA (SINGLE STREAM - Example for verification)
    # -----------------------------
    
def populate_data(all_raw_attribute_webids):
    first_path = list(all_raw_attribute_webids.keys())[0]
    attribute_webid = all_raw_attribute_webids[first_path]

    print(f"\n--- Reading Recorded Data (10 values) for the first attribute found: {first_path} ---")

    for i in range(len(all_raw_attribute_webids)):
        path = list(all_raw_attribute_webids.keys())[i]
        attribute_webid = all_raw_attribute_webids[path]

        recorded_data = get(f"/streams/{attribute_webid}/interpolated",
                                params={"startTime":"*-1y","endTime":"*","interval":"1h"})
        print(f"/streams/{attribute_webid}/interpolated")
        # for item in recorded_data["Items"]:
        #     timestamp = item["Timestamp"]
        #     value = item["Value"]
        #     print(f"Path: {path}, Timestamp: {timestamp}, Value: {value}")

def create_schema():
    credential_filepath = 'db_credentials.json'
    db, conn = pgconnect(credential_filepath, db_schema="public")


    conn.execute(text("""
    CREATE TABLE IF NOT EXISTS pi_raw_data (
        id SERIAL PRIMARY KEY,
        path VARCHAR(255),
        webid VARCHAR(255),
        timestamp TIMESTAMP,
        value FLOAT
    );
    """))

def main():
    all_raw_attribute_webids=setup()
    create_schema()
    populate_data(all_raw_attribute_webids)
    

if __name__ == "__main__":
    main()