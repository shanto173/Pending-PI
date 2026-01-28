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

# --------- Fetch Carter's Journey OA/BO/SA PI Data ---------
def fetch_carters_journey_data(uid, company_id, sales_types, batch_size=1000):
    all_records, offset = [], 0
    
    # Get date range: from 2025-04-01 to current date for the domain filter
    local_tz = pytz.timezone("Asia/Dhaka")
    now = datetime.now(local_tz)
    current_date = now.strftime("%Y-%m-%d %H:%M:%S")
    # Fixed start date: April 1, 2025
    start_date = "2025-04-01 00:00:00"
    
    # Base domain for all sales types (same filters for OA/BO/SA/PI)
    domain = [
        "&", ["brand_group", "in", [183784, 180989]],
        "&", "&", ["date_order", ">=", start_date],
        ["date_order", "<=", current_date],
        "&", ["state", "=", "sale"],
        ["sales_type", "in", sales_types]
    ]
    
    specification = {
        "date_order": {},
        "order_line": {
            "fields": {
                "order_id": {
                    "fields": {
                        "display_name": {},
                        "brand_group": {"fields": {"display_name": {}}},
                        "team_id": {"fields": {"display_name": {}}}
                    }
                },
                "order_partner_id": {"fields": {"display_name": {}}},
                "product_template_id": {
                    "fields": {
                        "fg_categ_type": {"fields": {"display_name": {}}}
                    }
                },
                "slidercodesfg": {},
                "product_uom_qty": {},
                "price_subtotal": {}
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
        print(f"[Company {company_id}] Carter's Journey: Fetched {len(records)} records, total so far: {len(all_records)}")
        if len(records) < batch_size:
            break
        offset += batch_size

    print(f"Company {company_id} Carter's Journey total records fetched: {len(all_records)}")
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

# --------- Flatten Carter's Journey Record ---------
def flatten_carters_journey_record(rec):
    """Flatten sale order with order lines into multiple rows (one per order line)"""
    flattened_rows = []
    order_lines = rec.get("order_line", [])
    
    # If no order lines, return empty list
    if not order_lines:
        return []
    
    # Create a row for each order line
    for line in order_lines:
        order_id = line.get("order_id", {})
        product_template = line.get("product_template_id", {})
        fg_categ = product_template.get("fg_categ_type", {})
        
        flattened_rows.append({
            "Order Date": rec.get("date_order", ""),
            "Order Lines/Order Reference": safe_get(order_id, "display_name"),
            "Order Lines/Order Reference/Brand Group": safe_get(order_id.get("brand_group"), "display_name"),
            "Order Lines/Customer": safe_get(line.get("order_partner_id"), "display_name"),
            "Order Lines/Order Reference/Sales Team": safe_get(order_id.get("team_id"), "display_name"),
            "Order Lines/Product Template/FG Category": safe_get(fg_categ, "display_name"),
            "Order Lines/Slider Code (SFG)": line.get("slidercodesfg") or "",
            "Order Lines/Quantity": line.get("product_uom_qty", ""),
            "Order Lines/Subtotal": line.get("price_subtotal", "")
        })
    
    return flattened_rows

# --------- Upload to Google Sheet ---------
def paste_to_gsheet(df, sheet_name):
    worksheet = gc.open_by_key(GOOGLE_SHEET_ID).worksheet(sheet_name)
    if df.empty:
        print(f"Empty DataFrame for {sheet_name}, pasting message.")
        worksheet.batch_clear(["A:J"])
        worksheet.update(range_name="A1", values=[["There is no data for this period from date to current date"]])
        # Update timestamp
        local_tz = pytz.timezone("Asia/Dhaka")
        current_timestamp = datetime.now(local_tz).strftime("%Y-%m-%d %H:%M:%S")
        worksheet.update(range_name="J1", values=[[f"Last Updated: {current_timestamp}"]])
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
    worksheet.batch_clear(["A:J"])
    print(f"Cleared range A:I from sheet: {sheet_name}")
    
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
            print(f"📊 Added {rows_to_add} rows to sheet. New total: {required_rows}")
        
        # Calculate the end column letter
        end_col = col_num_to_letter(len(df.columns))
        
        # Write data starting from row 2
        range_to_update = f"A2:{end_col}{1 + len(values_to_write)}"
        worksheet.update(range_name=range_to_update, values=values_to_write)
        
        # Update timestamp
        local_tz = pytz.timezone("Asia/Dhaka")
        current_timestamp = datetime.now(local_tz).strftime("%Y-%m-%d %H:%M:%S")
        worksheet.update(range_name="K1", values=[[f"Last Updated: {current_timestamp}"]])
        
        print(f"Data pasted to Google Sheet ({sheet_name}) with {len(values_to_write)} rows.")

# --------- Main ---------
if __name__ == "__main__":
    uid = odoo_login()
    
    # Carter's Journey data - Sales Types mapping to Sheet Tab names
    carters_journey_map = [
        (["oa"], "OA"),
        (["sample"], "SA"),
        (["bo"], "BO"),
        (["sale"], "PI")
    ]

    # Fetch Carter's Journey data
    print("\n========== Fetching Carter's Journey OA/BO/SA PI Data ==========")
    for sales_types, sheet_tab in carters_journey_map:
        all_flat_records = []
        
        # Fetch data from both companies
        for company_id in [1, 3]:
            records = fetch_carters_journey_data(uid, company_id, sales_types)
            # Flatten records (each order line becomes a row)
            for r in records:
                flat_rows = flatten_carters_journey_record(r)
                # Add Company Type column to each row
                for row in flat_rows:
                    row["Company"] = "Zipper" if company_id == 1 else "Metal Trims"
                all_flat_records.extend(flat_rows)
        
        # Create DataFrame
        df = pd.DataFrame(all_flat_records)
        print(f"[{sheet_tab}] Total records after flattening: {len(df)}")
        
        if not df.empty:
            # Calculate subtotal before any processing
            subtotal_before = df["Order Lines/Subtotal"].sum()
            print(f"[{sheet_tab}] Total Subtotal (before grouping): {subtotal_before}")
            
            # Remove timestamp from Order Date column (keep only date part)
            df["Order Date"] = df["Order Date"].apply(lambda x: str(x).split()[0] if x else "")
            
            # Group by Order Reference and sum Quantity and Subtotal
            # Keep first occurrence of other columns
            df_grouped = df.groupby("Order Lines/Order Reference", as_index=False).agg({
                "Order Date": "first",
                "Order Lines/Order Reference/Brand Group": "first",
                "Order Lines/Customer": "first",
                "Order Lines/Order Reference/Sales Team": "first",
                "Order Lines/Product Template/FG Category": "first",
                "Order Lines/Slider Code (SFG)": "first",
                "Order Lines/Quantity": "sum",
                "Order Lines/Subtotal": "sum",
                "Company": "first"
            })
            
            # Sort by Order Date
            df_grouped = df_grouped.sort_values(by="Order Date").reset_index(drop=True)
            
            print(f"[{sheet_tab}] Records after grouping by Order Reference: {len(df_grouped)}")
            
            # Calculate subtotal after grouping
            subtotal_after = df_grouped["Order Lines/Subtotal"].sum()
            print(f"[{sheet_tab}] Total Subtotal (after grouping): {subtotal_after}")
            print(f"[{sheet_tab}] Subtotal difference: {subtotal_before - subtotal_after}")
            
            df = df_grouped
        
        paste_to_gsheet(df, sheet_tab)
    
    print("\nAll Carter's Journey OA/BO/SA PI data fetched and uploaded successfully!")
