import os
import json
import base64
import requests
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
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
GOOGLE_SHEET_ID = "1WFalOBdShdwWopazEohOlE4mbjKCIMynlx5R2mFBqR8"

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

# --------- Fetch Manufacturing Order Data ---------
def fetch_manufacturing_order_data(uid, company_id, batch_size=1000):
    all_records, offset = [], 0
    
    # Domain filters:
    # - oa_total_balance > 0
    # - oa_id != false
    # - state not in [closed, cancel, hold]
    # - buyer_id.brand in [183784, 180989]
    domain = [
        ["oa_total_balance", ">", 0],
        ["oa_id", "!=", False],
        ["state", "not in", ["closed", "cancel", "hold"]],
        ["buyer_id.brand", "in", [183784, 180989]]
    ]
    
    specification = {
        "date_order": {},
        "oa_id": {"fields": {"display_name": {}}},
        "buyer_id": {"fields": {"brand": {"fields": {"display_name": {}}}}},
        "partner_id": {"fields": {"display_name": {}}},
        "fg_categ_type": {},
        "slidercodesfg": {},
        "lead_time": {},
        "product_uom_qty": {},
        "done_qty": {},
        "balance_qty": {},
        "oa_total_balance": {},
        "state": {},
        "final_price": {}
    }

    while True:
        payload = {
            "jsonrpc": "2.0",
            "method": "call",
            "params": {
                "model": "manufacturing.order",
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
        resp = session.post(f"{ODOO_URL}/web/dataset/call_kw/manufacturing.order/web_search_read", data=json.dumps(payload))
        resp.raise_for_status()
        response_json = resp.json()
        print("Debug - Odoo response:", response_json)
        result = response_json['result']
        records = result['records']
        
        all_records.extend(records)
        print(f"[Company {company_id}] Manufacturing Orders: Fetched {len(records)} records, total so far: {len(all_records)}")
        
        if len(records) < batch_size:
            break
            
        offset += batch_size

    print(f"Company {company_id} Manufacturing Orders total records fetched: {len(all_records)}")
    return all_records

# --------- Safe Getter ---------
def safe_get(obj, key, default=''):
    if isinstance(obj, dict):
        return obj.get(key, default)
    return default

# --------- Helper to safely get string values ---------
def get_string_value(field, subfield=None):
    """
    Safely extract a string from Odoo API fields.
    Handles:
      - dict with display_name or nested fields
      - int (ID)
      - str
      - False/None
    """
    if isinstance(field, dict):
        if subfield:
            value = field.get(subfield)
            return get_string_value(value)
        if "display_name" in field:
            return str(field["display_name"] or "")
        # fallback: join all dict values as string
        return " ".join([str(v) for v in field.values()])
    elif isinstance(field, int):
        return str(field)
    elif field in (False, None):
        return ""
    return str(field)

# --------- Flatten Manufacturing Order Record ---------
def flatten_manufacturing_order_record(rec):
    """Flatten manufacturing.order record into a single row"""
    return {
        "Order Date": rec.get("date_order", ""),
        "OA": safe_get(rec.get("oa_id"), "display_name"),
        "Buyer Name/Brand Group": get_string_value(rec.get("buyer_id"), "brand"),
        "Customer": safe_get(rec.get("partner_id"), "display_name"),
        "Item": rec.get("fg_categ_type", ""),
        "Sale Order Line/Slider Code (SFG)": rec.get("slidercodesfg", ""),
        "Lead Time": rec.get("lead_time", ""),
        "Quantity": rec.get("product_uom_qty", ""),
        "Done Qty": rec.get("done_qty", ""),
        "Balance": rec.get("balance_qty", ""),
        "Final Price": rec.get("final_price", ""),
        "OA Total Balance": rec.get("oa_total_balance", ""),
        "State": rec.get("state", "")
    }

# --------- Upload to Google Sheet ---------
def paste_to_gsheet(df, sheet_name):
    try:
        # Get the spreadsheet
        spreadsheet = gc.open_by_key(GOOGLE_SHEET_ID)
        
        # Try to get the worksheet
        worksheet = spreadsheet.worksheet(sheet_name)
    except gspread.exceptions.WorksheetNotFound:
        # Print all available worksheet names for debugging
        available_sheets = [ws.title for ws in spreadsheet.worksheets()]
        print(f"Worksheet '{sheet_name}' not found. Available worksheets: {available_sheets}")
        raise
    except Exception as e:
        print(f"Error accessing Google Sheet: {str(e)}")
        raise
    if df.empty:
        print(f"Empty DataFrame for {sheet_name}, pasting message.")
        worksheet.batch_clear(["A:M"])
        worksheet.update(range_name="A1", values=[["There is no data for this period from date to current date"]])
        # Update timestamp
        local_tz = pytz.timezone("Asia/Dhaka")
        current_timestamp = datetime.now(local_tz).strftime("%Y-%m-%d %H:%M:%S")
        worksheet.update(range_name="B1", values=[[f"Last Updated: {current_timestamp}"]])
        return

    # Helper function to convert column number to letter (1=A, 27=AA, etc.)
    def col_num_to_letter(n):
        result = ""
        while n > 0:
            n -= 1
            result = chr(65 + (n % 26)) + result
            n //= 26
        return result
    
    # Clear only range A:I instead of entire sheet
    worksheet.batch_clear(["A:M"])
    print(f"Cleared range A:M from sheet: {sheet_name}")
    
    # Write header
    header = df.columns.tolist()
    end_col_letter = col_num_to_letter(len(header))
    worksheet.update(range_name=f"A1:{end_col_letter}1", values=[header])
    
    # Prepare data for writing (convert DataFrame to list of lists)
    values_to_write = df.values.tolist()
    
    if values_to_write:
        # Calculate required rows
        required_rows = 1 + len(values_to_write)  # Header + data rows
        current_row_count = worksheet.row_count
        
        # Expand sheet if necessary
        if required_rows > current_row_count:
            rows_to_add = required_rows - current_row_count
            worksheet.add_rows(rows_to_add)
            print(f"Added {rows_to_add} rows to sheet. New total: {required_rows}")
        
        # Calculate the end column letter
        end_col = col_num_to_letter(len(df.columns))
        
        # Write data starting from row 2
        range_to_update = f"A2:{end_col}{1 + len(values_to_write)}"
        worksheet.update(range_name=range_to_update, values=values_to_write)
        
        # Update timestamp
        local_tz = pytz.timezone("Asia/Dhaka")
        current_timestamp = datetime.now(local_tz).strftime("%Y-%m-%d %H:%M:%S")
        worksheet.update(range_name=f"{col_num_to_letter(len(header) + 1)}1", values=[[f"Last Updated: {current_timestamp}"]])
        
        print(f"Data pasted to Google Sheet ({sheet_name}) with {len(values_to_write)} rows.")

# --------- Main ---------
if __name__ == "__main__":
    uid = odoo_login()
    
    # Define company to sheet name mapping
    companies = [
        {"id": 1, "sheet_name": "Pending_Orders_zip"},
        {"id": 3, "sheet_name": "Pending_Orders_MT"}
    ]

    for company in companies:
        company_id = company["id"]
        sheet_name = company["sheet_name"]
        
        print(f"\n========== Fetching Manufacturing Order Data for Company {company_id} ==========")
        
        # Fetch Manufacturing Order data
        records = fetch_manufacturing_order_data(uid, company_id)
        
        # Flatten records
        flat_records = []
        for r in records:
            flat_records.append(flatten_manufacturing_order_record(r))
        
        df = pd.DataFrame(flat_records)
        paste_to_gsheet(df, sheet_name)
        
        print(f"Data fetched and uploaded successfully to '{sheet_name}' sheet!")
    
    print("\nAll companies' manufacturing order data processed successfully!")
