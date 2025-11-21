##### BTD_update.py – DYNAMIC COLUMN VERSION (2025) #####
# Stock_Analysis → Google Sheet
# Finds columns by header name → survives column inserts/moves
# ===============================================================

import time
import pandas as pd
import yfinance as yf
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
import pytz
import gspread.utils  # For A1 notation helpers

# -------------------------------------------------
# 1. Google Sheets authentication
# -------------------------------------------------
SCOPE = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
CREDS_FILE = "/home/neo/PycharmProjects/PythonProject/aerobic-arcade-377707-80bfc207c8b4.json"

creds = ServiceAccountCredentials.from_json_keyfile_name(CREDS_FILE, SCOPE)
client = gspread.authorize(creds)
workbook = client.open("Xiang Stock Analysis")
sheet = workbook.worksheet("Stock Summary USD")
hist_sheet = workbook.worksheet("Historical_BTD_Metric")

# -------------------------------------------------
# 2. Current SG Time
# -------------------------------------------------
sg_tz = pytz.timezone("Asia/Singapore")
now_sg_dt = datetime.now(sg_tz)
last_updated_str = now_sg_dt.strftime("%b %d, %Y")  # Nov 21, 2025
now_sg_str = now_sg_dt.strftime("%Y-%m-%d %H:%M:%S %Z")
run_date_str = now_sg_dt.strftime("%Y-%m-%d")  # For historical sheet

print(f"Script started at: {now_sg_str}", flush=True)


# -------------------------------------------------
# 3. Helper: Find columns by header name
# -------------------------------------------------
def build_column_map(worksheet, required_headers, header_row=1):
    """
    Returns dict: {"Next Earnings Date": "AF", "Enterprise Value": "AG", ...}
    Raises clear error if any header is missing.
    """
    headers = worksheet.row_values(header_row)
    col_map = {}

    for header in required_headers:
        clean = header.strip().lower()
        found = False
        for idx, cell in enumerate(headers, 1):
            if cell.strip().lower() == clean:
                col_letter = gspread.utils.rowcol_to_a1(header_row, idx).replace(str(header_row), "")
                col_map[header] = col_letter
                found = True
                break
        if not found:
            raise ValueError(f"ERROR: Header '{header}' not found in {worksheet.title}! "
                             f"Check row {header_row} spelling/case.")
    return col_map


# -------------------------------------------------
# 4. Read tickers + current BTD (Col E)
# -------------------------------------------------
def get_tickers_and_btd():
    col_a = sheet.col_values(1)  # Tickers
    col_e = sheet.col_values(5)  # BTD (still hard-coded E — safe, never moves)
    pairs = []
    for i in range(1, min(len(col_a), len(col_e)) + 1):
        ticker = col_a[i - 1].strip().upper() if i <= len(col_a) else ""
        btd = col_e[i - 1].strip() if i <= len(col_e) else ""
        if ticker:
            pairs.append((ticker, btd))
    return pairs


ticker_btd_pairs = get_tickers_and_btd()
if not ticker_btd_pairs:
    print("No tickers found. Exiting.", flush=True)
    raise SystemExit

tickers = [p[0] for p in ticker_btd_pairs]
print(f"Found {len(tickers)} tickers", flush=True)

# -------------------------------------------------
# 5. Fetch data from yfinance (with retry on earnings_dates)
# -------------------------------------------------
records = []

for ticker in tickers:
    print(f"  → {ticker}", end="", flush=True)
    row = {}

    try:
        t = yf.Ticker(ticker)
        info = t.info

        # ---- Next Earnings Date (robust retry) ----
        earnings_date = "N/A"
        max_retries = 3
        base_delay = 2

        for attempt in range(max_retries):
            try:
                df = t.earnings_dates
            if df is None or df.empty:
                raise ValueError("Empty earnings_dates")

            df.index = pd.to_datetime(df.index)
            today = pd.Timestamp.now(tz='UTC').normalize()

            reported_col = next((c for c in df.columns if "reported" in c.lower()), None)
            future = df[df[reported_col].isna()] if reported_col and reported_col in df.columns else df[
                df.index > today]

            if not future.empty:
                next_date = future.index.min()
                earnings_date = next_date.strftime("%b %d, %Y")
            else:
                earnings_date = "No upcoming"
            break

        except Exception as e:
        if attempt < max_retries - 1:
            delay = base_delay * (2 ** attempt)
            print(f" [Retry {attempt + 2}/{max_retries} in {delay}s]", end="", flush=True)
            time.sleep(delay)
        else:
            earnings_date = "Error"
            print(f" [FAILED: {e}]", end="", flush=True)

row["Next_Earnings_Date"] = earnings_date
row["enterpriseValue"] = info.get("enterpriseValue", "")
row["totalRevenue"] = info.get("totalRevenue", "")
row["enterpriseToEbitda"] = info.get("enterpriseToEbitda", "")
row["revenueGrowth"] = info.get("revenueGrowth", "")
row["grossMargins"] = info.get("grossMargins", "")
row["No. of FTE"] = info.get("fullTimeEmployees", "")
row["Last_Updated"] = last_updated_str

print(" OK", flush=True)
except Exception as e:
print(f" [FATAL: {e}]", flush=True)
for k in ["Next_Earnings_Date", "enterpriseValue", "totalRevenue",
          "enterpriseToEbitda", "revenueGrowth", "grossMargins",
          "No. of FTE", "Last_Updated"]:
    row[k] = "ERROR"

records.append(row)
time.sleep(0.6)

# -------------------------------------------------
# 6. DYNAMIC COLUMN MAPPING & WRITE TO MAIN SHEET
# -------------------------------------------------
desired_headers = [
    "Next Earnings Date",
    "Enterprise Value",
    "Total Revenue",
    "EV/EBITDA",
    "Revenue Growth",
    "Gross Margin",
    "No. of FTE",
    "Last Updated"
]

print("Locating columns by header name...", flush=True)
col_map = build_column_map(sheet, desired_headers, header_row=1)

start_col = col_map[desired_headers[0]]
end_col = col_map[desired_headers[-1]]
print(f"Target block: {start_col} → {end_col}", flush=True)

# Build DataFrame in exact header order
df_output = pd.DataFrame(records)[[
    "Next_Earnings_Date", "enterpriseValue", "totalRevenue",
    "enterpriseToEbitda", "revenueGrowth", "grossMargins",
    "No. of FTE", "Last_Updated"
]]
df_output.columns = desired_headers  # Match exact header text

# Write header + data
header_range = f"{start_col}1:{end_col}1"
data_range = f"{start_col}2:{end_col}{len(df_output) + 1}"

sheet.update(range_name=header_range, values=[desired_headers], value_input_option="USER_ENTERED")
sheet.update(range_name=data_range, values=df_output.astype(str).values.tolist(), value_input_option="USER_ENTERED")

print(f"Main sheet updated successfully ({start_col}→{end_col}) at {now_sg_str}", flush=True)

# -------------------------------------------------
# 7. APPEND TO Historical_BTD_Metric (deduplicated)
# -------------------------------------------------
# Read existing (Date, Ticker) pairs
existing_pairs = set()
try:
    all_vals = hist_sheet.get_all_values()
    if len(all_vals) > 1:
        for row in all_vals[1:]:
            if len(row) >= 2:
                date = row[0].strip()
                ticker = row[1].strip().upper()
                if date and ticker:
                    existing_pairs.add((date, ticker))
except Exception as e:
    print(f"[WARN] Could not read historical sheet: {e}", flush=True)

# Build new rows (skip duplicates)
hist_rows = []
for ticker, btd in ticker_btd_pairs:
    ticker = ticker.strip().upper()
    key = (run_date_str, ticker)
    if key not in existing_pairs:
        hist_rows.append([run_date_str, ticker, btd])

added = len(hist_rows)

# Ensure header exists
if not hist_sheet.row_values(1):
    hist_sheet.update('A1:C1', [["Date (SG)", "Ticker", "BTD (Col E)"]], value_input_option="USER_ENTERED")
    print("[INFO] Header added to Historical_BTD_Metric", flush=True)

# Write new rows using update() – 100% reliable
if hist_rows:
    start_row = len(hist_sheet.get_all_values()) + 1
    end_row = start_row + len(hist_rows) - 1
    hist_sheet.update(
        range_name=f"A{start_row}:C{end_row}",
        values=hist_rows,
        value_input_option="USER_ENTERED"
    )
    print(f"[SUCCESS] Added {added} new BTD record(s) to Historical_BTD_Metric (rows {start_row}–{end_row})",
          flush=True)
else:
    print("[INFO] No new rows (today's data already logged).", flush=True)

print(f"\nALL DONE! Finished at {datetime.now(sg_tz).strftime('%Y-%m-%d %H:%M:%S %Z')}", flush=True)