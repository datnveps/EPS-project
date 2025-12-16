import json
import pandas as pd
import requests
import urllib3
from bs4 import BeautifulSoup
from sqlalchemy import create_engine, text
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
    params = {
        'username': 'VINHTAN02PIAF',
        'password': 'Rms@vt02idpiaf'
    }
    
    url = 'https://10.156.8.181/PIwebapi/attributes/F1AbEwFu1bfjucEGZoa1kZRPslArX6XxKUh7BG5xbR68TLwKAgzZ-iw2D314SAezDFZnp4AUk1TLVZUMi1QSUFGXEVBUkxZIFdBUk5JTkcgU1lTVEVNIFZUMlxVTklUIDJ8QUxMIEZVRUwgT0ZGIFRSUCBGTw/attributes?startIndex=0'

    # WARNING: turning off TLS verification is insecure. Prefer fixing certs.
    verify = False
    # If you want to suppress the InsecureRequestWarning (not recommended), set to True
    suppress_insecure_warning = True

    if verify is False and suppress_insecure_warning:
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    def print_response_debug(resp):
        # Hàm tiện ích in thông tin phản hồi để biết vì sao JSON parsing thất bại.
        print('\n--- Response debug ---')
        print('Status code:', resp.status_code)
        print('Content-Type:', resp.headers.get('Content-Type'))
        body = resp.text or ''
        print('Body (first 1000 chars):')
        print(body[:1000])
        try:
            print('Parsed JSON:', resp.json())
        except ValueError as e:
            # Thông thường JSONDecodeError kế thừa ValueError
            print('Response is not valid JSON:', e)

    # 1) Thử Basic Authentication: nhiều API yêu cầu header Authorization: Basic <base64>
    #    Sử dụng `auth=(username,password)` hoặc HTTPBasicAuth để requests tự tạo header phù hợp.
    print('\nThử Basic Auth...')
    try:
        resp_basic = requests.get(url, auth=(params['username'], params['password']), verify=verify, timeout=30)
    except requests.exceptions.RequestException as e:
        print('Yêu cầu Basic Auth thất bại:', e)
        return
    print_response_debug(resp_basic)

    if resp_basic.status_code == 200:
        # Nếu Basic thành công, xong.
        return

    # 2) Nếu Basic trả 401, nhiều service dùng Bearer token thay vì Basic.
    #    Dưới là ví dụ cách thêm header Authorization: Bearer <TOKEN>.
    #    Thực tế bạn cần lấy token từ endpoint auth (login) hoặc từ admin.
    print('\nBasic Auth không thành công, thử Bearer token (ví dụ, cần thay TOKEN thực tế)...')
    headers = {
        'Authorization': 'Bearer YOUR_TOKEN_HERE'  # Thay YOUR_TOKEN_HERE bằng token thật nếu có
    }
    try:
        resp_bearer = requests.get(url, headers=headers, verify=verify, timeout=30)
    except requests.exceptions.RequestException as e:
        print('Yêu cầu Bearer thất bại:', e)
        return
    print_response_debug(resp_bearer)

    # Nếu vẫn 401, cần kiểm tra docs API hoặc liên hệ admin để biết phương thức auth chính xác.


if __name__ == "__main__":
    main()