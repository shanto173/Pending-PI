import os
import json
import base64
import requests
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from gspread_dataframe import set_with_dataframe
from datetime import datetime
import pytz
from dotenv import load_dotenv

load_dotenv()

# --------- Config from Environment ---------
ODOO_URL = os.getenv("ODOO_URL")
ODOO_DB = os.getenv("ODOO_DB")
ODOO_USERNAME = os.getenv("ODOO_USERNAME")
ODOO_PASSWORD = os.getenv("ODOO_PASSWORD")
GOOGLE_CREDENTIALS_BASE64 = os.getenv("GOOGLE_CREDENTIALS_BASE64")
GOOGLE_SHEET_ID = "1acV7UrmC8ogC54byMrKRTaD9i1b1Cf9QZ-H1qHU5ZZc"

# Decode Google Service Account credentials
creds_json = json.loads(base64.b64decode(GOOGLE_CREDENTIALS_BASE64))
creds = Credentials.from_service_account_info(
    creds_json,
    scopes=["https://www.googleapis.com/auth/spreadsheets"]
)
gc = gspread.authorize(creds)

session = requests.Session()
session.headers.update({"Content-Type": "application/json"})

# --------- Login ---------
def odoo_login():
    url = f"{ODOO_URL}/web/session/authenticate"
    payload = {
        "jsonrpc": "2.0",
        "method": "call",
        "params": {
            "db": ODOO_DB,
            "login": ODOO_USERNAME,
            "password": ODOO_PASSWORD
        },
        "id": 1
    }
    resp = session.post(url, data=json.dumps(payload))
    resp.raise_for_status()
    return resp.json()['result']['uid']

# --------- Fetch PI Issue Bank-Wise Data ---------
def fetch_pi_bank_data(uid, company_id, batch_size=1000):
    all_records, offset = [], 0
    
    # Get current date for the domain filter
    local_tz = pytz.timezone("Asia/Dhaka")
    current_date = datetime.now(local_tz).strftime("%Y-%m-%d")
    
    domain = [
        "&", ["state", "=", "sale"],
        "&", ["sales_type", "=", "sale"],
        "&", ["pi_date", ">=", "2025-08-01"],
        ["pi_date", "<=", current_date]
    ]
    
    specification = {
        "pi_date": {},
        "bank": {"fields": {"display_name": {}}},
        "amount_total": {}
    }

    while True:
        payload = {
            "jsonrpc": "2.0",
            "method": "call",
            "params": {
                "model": "sale.order",
                "method": "web_search_read",
                "args": [],
                "kwargs": {
                    "domain": domain,
                    "specification": specification,
                    "offset": offset,
                    "limit": batch_size,
                    "context": {
                        "lang": "en_US",
                        "tz": "Asia/Dhaka",
                        "uid": uid,
                        "allowed_company_ids": [company_id],
                        "bin_size": True,
                        "current_company_id": company_id
                    },
                    "count_limit": 10001
                }
            },
            "id": 2
        }
        resp = session.post(f"{ODOO_URL}/web/dataset/call_kw/sale.order/web_search_read", data=json.dumps(payload))
        resp.raise_for_status()
        result = resp.json()['result']
        records = result['records']
        all_records.extend(records)
        print(f"[Company {company_id}] PI Bank Data: Fetched {len(records)} records, total so far: {len(all_records)}")
        if len(records) < batch_size:
            break
        offset += batch_size

    print(f"✅ Company {company_id} PI bank data total records fetched: {len(all_records)}")
    return all_records

# --------- Safe Getter ---------
def safe_get(obj, key, default=''):
    if isinstance(obj, dict):
        return obj.get(key, default)
    return default

# --------- Flatten PI Bank Record ---------
def flatten_pi_bank_record(rec):
    return {
        "PI Date": rec.get("pi_date", ""),
        "Bank": safe_get(rec.get("bank"), "display_name"),
        "Total": rec.get("amount_total", "")
    }

# --------- Upload to Google Sheet ---------
def paste_to_gsheet(df, sheet_name):
    worksheet = gc.open_by_key(GOOGLE_SHEET_ID).worksheet(sheet_name)
    if df.empty:
        print(f"Skip: {sheet_name} DataFrame is empty, not pasting.")
        return

    # Helper function to convert column number to letter (1=A, 27=AA, etc.)
    def col_num_to_letter(n):
        result = ""
        while n > 0:
            n -= 1
            result = chr(65 + (n % 26)) + result
            n //= 26
        return result
    
    # Group by PI Date, Bank and aggregate
    grouped_df = df.groupby(['PI Date', 'Bank']).agg({
        'Total': 'sum'
    }).reset_index()
    
    print(f"📊 Grouped {len(df)} records into {len(grouped_df)} summary rows")
    
    # Clear only range A:C instead of entire sheet
    worksheet.batch_clear(["A:C"])
    print(f"🗑️ Cleared range A:C from sheet: {sheet_name}")
    
    # Write header
    header = grouped_df.columns.tolist()
    end_col_letter = col_num_to_letter(len(header))
    worksheet.update(range_name=f"A1:{end_col_letter}1", values=[header])
    
    # Prepare data for writing (convert DataFrame to list of lists)
    values_to_write = grouped_df.values.tolist()
    
    if values_to_write:
        # Calculate required rows
        required_rows = 1 + len(values_to_write)  # Header + data rows
        current_row_count = worksheet.row_count
        
        # Expand sheet if necessary
        if required_rows > current_row_count:
            rows_to_add = required_rows - current_row_count
            worksheet.add_rows(rows_to_add)
            print(f"📊 Added {rows_to_add} rows to sheet. New total: {required_rows}")
        
        # Calculate the end column letter
        end_col = col_num_to_letter(len(grouped_df.columns))
        
        # Write data starting from row 2
        range_to_update = f"A2:{end_col}{1 + len(values_to_write)}"
        worksheet.update(range_name=range_to_update, values=values_to_write)
        
        # Update timestamp in J1
        local_tz = pytz.timezone("Asia/Dhaka")
        current_timestamp = datetime.now(local_tz).strftime("%Y-%m-%d %H:%M:%S")
        worksheet.update(range_name="J1", values=[[f"Last Updated: {current_timestamp}"]])
        
        print(f"✅ Data pasted to Google Sheet ({sheet_name}) with {len(values_to_write)} rows.")

# --------- Main ---------
if __name__ == "__main__":
    uid = odoo_login()
    
    # PI Bank data - Company ID mapping to Sheet Tab names
    pi_bank_map = [(1, "pi_bank_zp"), (3, "pi_bank_mt")]

    # Fetch PI Bank data
    print("\n========== Fetching PI Issue Bank-Wise Data ==========")
    for company_id, sheet_tab in pi_bank_map:
        records = fetch_pi_bank_data(uid, company_id)
        # Flatten records
        flat_records = [flatten_pi_bank_record(r) for r in records]
        df = pd.DataFrame(flat_records)
        paste_to_gsheet(df, sheet_tab)
    
    print("\n✅ All PI bank data fetched and uploaded successfully!")
