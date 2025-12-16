#/streams/F1AbEY5NASxI4YUq_ILrgW5Hzzwvq__ASJu7RGFjbR68eTYMQpcYk37CRIVsiRtJGazqZHAUk1TLU1EMS1QSUFGXEVBUkxZIFdBUk5JTkcgU1lTVEVNIE1EMVxNRDFcVU5JVCAxXEJPSUxFUiBBXElORFVDRUQgRFJBRlQgRkFOU1xJREYtQXxCT0lMRVIgREVNQU5EIEJJQVM/interpolated
import json
import pandas as pd
import requests
from bs4 import BeautifulSoup
from sqlalchemy import create_engine, text
import requests
import urllib3
from requests_ntlm import HttpNtlmAuth
import psycopg2
import sqlalchemy.types as types

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
    
def populate_data(all_raw_attribute_webids, db_engine):


    # --- Step 2: Fetch all Time-Series Data ---
    webids_list = list(all_raw_attribute_webids.values())
    
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
            path = next(k for k, v in all_raw_attribute_webids.items() if v == webid)
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
    conn=create_schema()
    populate_data(all_raw_attribute_webids, conn)
    

if __name__ == "__main__":
    main()