import json
import pandas as pd
import requests
from bs4 import BeautifulSoup
from sqlalchemy import create_engine, text
import requests
import urllib3
from requests_ntlm import HttpNtlmAuth
def pgconnect(credential_filepath, db_schema="public"):
    with open(credential_filepath) as f:
        db_conn_dict = json.load(f)
        host       = db_conn_dict['host']
        db_user    = db_conn_dict['user']
        db_pw      = db_conn_dict['password']
        default_db = db_conn_dict['user']
        port       = db_conn_dict['port']
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
def main():
    

    # -----------------------------
    # CONFIG
    # -----------------------------
    BASE_URL = "https://10.32.194.4/piwebapi"

    # If PI uses Windows Auth (most common)
    USERNAME = "MONGDUONG01PIAF"   # change if needed
    PASSWORD = "AccountForReadOnly@MD1"           # change if needed

    # Disable SSL warnings (internal PI certs)
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    session = requests.Session()
    session.verify = False
    session.auth = HttpNtlmAuth(USERNAME, PASSWORD)

    # -----------------------------
    # HELPER FUNCTION
    # -----------------------------
    def get(endpoint, params=None):
        url = BASE_URL + endpoint
        r = session.get(url, params=params)
        r.raise_for_status()
        return r.json()

    # -----------------------------
    # 1. ROOT (overview)
    # -----------------------------
    root = get("")
    print("PI Web API version:", root.get("Version"))
    print("Available links:")
    for k in root.get("Links", {}):
        print(" -", k)

    # -----------------------------
    # 2. ASSET SERVERS
    # -----------------------------
    asset_servers = get("/assetservers")
    print("\nAsset Servers:")
    for s in asset_servers["Items"]:
        print(f"- {s['Name']} | WebId={s['WebId']}")

    asset_server_webid = asset_servers["Items"][0]["WebId"]

    # -----------------------------
    # 3. ASSET DATABASES
    # -----------------------------
    databases = get(f"/assetservers/{asset_server_webid}/assetdatabases")
    print("\nAsset Databases:")
    for db in databases["Items"]:
        print(f"- {db['Name']} | WebId={db['WebId']}")

    database_webid = databases["Items"][0]["WebId"]

    # -----------------------------
    # 4. ELEMENTS
    # -----------------------------
    elements = get("/elements", params={
        "databaseWebId": database_webid,
        "maxCount": 5
    })

    print("\nElements (sample):")
    for el in elements["Items"]:
        print(f"- {el['Name']} | WebId={el['WebId']}")

    element_webid = elements["Items"][0]["WebId"]

    # -----------------------------
    # 5. ATTRIBUTES
    # -----------------------------
    attributes = get("/attributes", params={
        "elementWebId": element_webid
    })

    print("\nAttributes:")
    for attr in attributes["Items"]:
        print(f"- {attr['Name']} | WebId={attr['WebId']}")

    attribute_webid = attributes["Items"][0]["WebId"]

    # -----------------------------
    # 6. READ DATA (STREAM)
    # -----------------------------
    recorded = get(f"/streams/{attribute_webid}/recorded", params={
        "startTime": "*-1h",
        "endTime": "*",
        "maxCount": 10
    })

    print("\nRecorded Values (last 1 hour):")
    for item in recorded["Items"]:
        print(item["Timestamp"], "=", item["Value"])


if __name__ == "__main__":
    main()