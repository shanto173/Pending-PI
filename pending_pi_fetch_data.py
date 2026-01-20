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
GOOGLE_SHEET_ID = "1Qc0Y3KjhCZx20zkgfrMfHl4FuvDfS5b1vqkAutrj4KI"

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

# --------- Fetch Regular Sale Orders Data ---------
def fetch_regular_sale_data(uid, company_id, batch_size=1000):
    all_records, offset = [], 0
    domain = [
        "&", "&", "&", "&", "&",
        ["company_id", "=", company_id],
        ["sales_type", "=", "sale"],
        "|", ["oa_count", "=", False], ["oa_count", "=", 0],
        ["is_active", "=", True],
        ["pi_type", "=", "regular"],
        ["state", "!=", "cancel"]
    ]
    
    specification = {
        "name": {},
        "create_date": {},
        "partner_id": {"fields": {"display_name": {}}},
        "order_line": {
            "fields": {
                "product_template_id": {
                    "fields": {
                        "fg_categ_type": {"fields": {"display_name": {}}},
                        "display_name": {}
                    }
                },
                "order_partner_id": {"fields": {"display_name": {}}},
                "create_date": {},
                "order_id": {"fields": {"display_name": {}}},
                "price_total": {},
                "price_subtotal": {},
                "product_uom_qty": {},
                "qty_to_invoice": {}
            }
        }
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
        print(f"[Company {company_id}] Regular Sale: Fetched {len(records)} records, total so far: {len(all_records)}")
        if len(records) < batch_size:
            break
        offset += batch_size

    print(f"✅ Company {company_id} regular sale total records fetched: {len(all_records)}")
    return all_records

# --------- Safe Getter ---------
def safe_get(obj, key, default=''):
    if isinstance(obj, dict):
        return obj.get(key, default)
    return default

# --------- Flatten Regular Sale Record ---------
def flatten_regular_sale_record(rec):
    """Flatten sale order with order lines into multiple rows (one per order line)"""
    flattened_rows = []
    order_lines = rec.get("order_line", [])
    
    # Get current date
    local_tz = pytz.timezone("Asia/Dhaka")
    current_date = datetime.now(local_tz).strftime("%Y-%m-%d")
    
    # If no order lines, return single row with order info
    if not order_lines:
        return [{
            "Date": current_date,
            "FG Category": "",
            "Customer": safe_get(rec.get("partner_id"), "display_name"),
            "Created on": rec.get("create_date", ""),
            "Order Reference": rec.get("name", ""),
            "Total": "",
            "Subtotal": "",
            "Quantity": "",
            "Quantity To Invoice": ""
        }]
    
    # Create a row for each order line
    for line in order_lines:
        product_template = line.get("product_template_id", {})
        fg_categ = product_template.get("fg_categ_type", {})
        flattened_rows.append({
            "Date": current_date,
            "FG Category": safe_get(fg_categ, "display_name"),
            "Customer": safe_get(line.get("order_partner_id"), "display_name"),
            "Created on": line.get("create_date", ""),
            "Order Reference": safe_get(line.get("order_id"), "display_name"),
            "Total": line.get("price_total", ""),
            "Subtotal": line.get("price_subtotal", ""),
            "Quantity": line.get("product_uom_qty", ""),
            "Quantity To Invoice": line.get("qty_to_invoice", "")
        })
    
    return flattened_rows

# --------- Upload to Google Sheet ---------
def paste_to_gsheet(df, sheet_name):
    worksheet = gc.open_by_key(GOOGLE_SHEET_ID).worksheet(sheet_name)
    if df.empty:
        print(f"Skip: {sheet_name} DataFrame is empty, not pasting.")
        return

    # Get current date
    local_tz = pytz.timezone("Asia/Dhaka")
    current_date = datetime.now(local_tz).strftime("%Y-%m-%d")
    
    # Helper function to convert column number to letter (1=A, 27=AA, etc.)
    def col_num_to_letter(n):
        result = ""
        while n > 0:
            n -= 1
            result = chr(65 + (n % 26)) + result
            n //= 26
        return result
    
    # Group by Date, FG Category, Customer and aggregate
    grouped_df = df.groupby(['Date', 'FG Category', 'Customer']).agg({
        'Total': 'sum',
        'Subtotal': 'sum',
        'Quantity': 'sum',
        'Quantity To Invoice': 'sum'
    }).reset_index()
    
    print(f"📊 Grouped {len(df)} records into {len(grouped_df)} summary rows")
    
    # Get all existing data from sheet
    existing_data = worksheet.get_all_values()
    
    # Find the last row with data (skip header)
    last_row_with_data = 1  # Start from row 1 (header is row 1)
    
    if len(existing_data) > 1:
        # Check from bottom to find last non-empty row
        for i in range(len(existing_data) - 1, 0, -1):
            # Check if any cell in the row has data
            if any(cell.strip() for cell in existing_data[i]):
                last_row_with_data = i + 1  # +1 because sheet rows are 1-indexed
                break
    
    # Calculate starting row for new data
    start_row = last_row_with_data + 1
    
    # If sheet is empty or only has header, write header first
    if len(existing_data) <= 1 or not any(existing_data[0]):
        # Write header
        header = grouped_df.columns.tolist()
        end_col_letter = col_num_to_letter(len(header))
        worksheet.update(range_name=f"A1:{end_col_letter}1", values=[header])
        start_row = 2
    
    # Prepare data for writing (convert DataFrame to list of lists)
    values_to_write = grouped_df.values.tolist()
    
    if values_to_write:
        # Calculate required rows
        required_rows = start_row + len(values_to_write)
        current_row_count = worksheet.row_count
        
        # Expand sheet if necessary
        if required_rows > current_row_count:
            rows_to_add = required_rows - current_row_count
            worksheet.add_rows(rows_to_add)
            print(f"📊 Added {rows_to_add} rows to sheet. New total: {required_rows}")
        
        # Calculate the end column letter
        end_col = col_num_to_letter(len(grouped_df.columns))
        
        # Write data starting from the calculated row
        range_to_update = f"A{start_row}:{end_col}{start_row + len(values_to_write) - 1}"
        worksheet.update(range_name=range_to_update, values=values_to_write)
        
        print(f"✅ Data appended to Google Sheet ({sheet_name}) starting at row {start_row}.")

# --------- Main ---------
if __name__ == "__main__":
    uid = odoo_login()
    
    # Regular Sale data - Company ID mapping to Sheet Tab names
    regular_sale_map = [(1, "Pending_Pi_Zipper"), (3, "Pending_Pi_MT")]

    # Fetch Regular Sale data
    print("\n========== Fetching Regular Sale Data ==========")
    for company_id, sheet_tab in regular_sale_map:
        records = fetch_regular_sale_data(uid, company_id)
        # Flatten records (each order line becomes a row)
        flat_records = []
        for r in records:
            flat_records.extend(flatten_regular_sale_record(r))
        df = pd.DataFrame(flat_records)
        paste_to_gsheet(df, sheet_tab)
    
    print("\n✅ All regular sale data fetched and uploaded successfully!")
