#####TEST CHANGES
# ==============================
#  BTD_update.py
#  Stock_Analysis → Google Sheet
#  AE = Next Earnings Date
#  AL = Last Updated (SG run time)
#  All columns shifted right by 1
# ==============================

import time
import pandas as pd
import yfinance as yf
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
import pytz

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
# 2. Current SG Time (Nov 11, 2025 10:43 PM +08)
# -------------------------------------------------
sg_tz = pytz.timezone("Asia/Singapore")
now_sg_dt = datetime.now(sg_tz)  # Use current real time
last_updated_str = now_sg_dt.strftime("%b %d, %Y")  # e.g., Nov 11, 2025
now_sg_str = now_sg_dt.strftime("%Y-%m-%d %H:%M:%S %Z")

print(f"Script started at: {now_sg_str}", flush=True)


# -------------------------------------------------
# 3. Read tickers + current BTD (Col E) ← KEEP THIS FUNCTION
# -------------------------------------------------
def get_tickers_and_btd():
    col_a = sheet.col_values(1)  # Tickers
    col_e = sheet.col_values(5)  # BTD (Column E)
    pairs = []
    for i in range(1, min(len(col_a), len(col_e))):
        ticker = col_a[i].strip().upper() if i < len(col_a) else ""
        btd    = col_e[i].strip() if i < len(col_e) else ""
        if ticker:
            pairs.append((ticker, btd))
    return pairs

# ← THIS LINE MUST BE HERE
ticker_btd_pairs = get_tickers_and_btd()
if not ticker_btd_pairs:
    print("No tickers found. Exiting.", flush=True)
    raise SystemExit

tickers = [p[0] for p in ticker_btd_pairs]
print(f"Found {len(tickers)} tickers", flush=True)

# -------------------------------------------------
# 4. Fetch data from yfinance (ROBUST + RETRY on earnings_dates)
# -------------------------------------------------
records = []

for ticker in tickers:
    print(f"  → {ticker}", end="", flush=True)
    row = {}

    try:
        t = yf.Ticker(ticker)
        info = t.info

        # ---- Next Earnings Date (AE): First future row only + RETRY ----
        earnings_date = "N/A"
        max_retries = 3
        base_delay = 2  # seconds

        for attempt in range(max_retries):
            try:
                df = t.earnings_dates

                if df is None or df.empty:
                    raise ValueError("Empty or None earnings_dates")

                df.index = pd.to_datetime(df.index)

                # Find reported EPS column
                reported_col = next((c for c in df.columns if "reported" in c.lower()), None)

                # Determine future rows
                today = pd.Timestamp.now(tz='UTC').normalize()
                if reported_col and reported_col in df.columns:
                    future = df[df[reported_col].isna()]
                    print(f" [Reported col: {reported_col}]", end="", flush=True)
                else:
                    future = df[df.index > today]
                    print(" [Using date filter]", end="", flush=True)

                if not future.empty:
                    next_date = future.index.min()
                    earnings_date = next_date.strftime("%b %d, %Y")
                    print(f" [Next: {earnings_date}]", end="", flush=True)
                else:
                    earnings_date = "No upcoming"
                    print(" [No future]", end="", flush=True)

                # Success: exit retry loop
                break

            except Exception as e:
                if attempt < max_retries - 1:
                    delay = base_delay * (2 ** attempt)  # 2s, 4s, 8s
                    print(f" [Retry {attempt+1}/{max_retries} in {delay}s: {e}]", end="", flush=True)
                    time.sleep(delay)
                else:
                    earnings_date = "Error"
                    print(f" [FAILED after {max_retries} tries: {e}]", end="", flush=True)

        row["Next_Earnings_Date"] = earnings_date

        # ---- Financial Metrics ----
        row["enterpriseValue"] = info.get("enterpriseValue", "")
        row["totalRevenue"] = info.get("totalRevenue", "")
        row["enterpriseToEbitda"] = info.get("enterpriseToEbitda", "")
        row["revenueGrowth"] = info.get("revenueGrowth", "")
        row["grossMargins"] = info.get("grossMargins", "")
        row["No. of FTE"] = info.get("fullTimeEmployees", "")
        row["Last_Updated"] = last_updated_str

        print(" OK", flush=True)
    except Exception as e:
        print(f" [FATAL ERROR: {e}]", flush=True)
        for key in ["Next_Earnings_Date", "enterpriseValue", "totalRevenue",
                    "enterpriseToEbitda", "revenueGrowth", "grossMargins",
                    "No. of FTE", "Last_Updated"]:
            row[key] = "ERROR"

    records.append(row)
    time.sleep(0.6)  # Respect Yahoo Finance

# -------------------------------------------------
# 5. Build DataFrame (AF → AM)
# -------------------------------------------------
cols = [
    "Next_Earnings_Date",  # AF
    "enterpriseValue",  # AG
    "totalRevenue",  # AH
    "enterpriseToEbitda",  # AI
    "revenueGrowth",  # AJ
    "grossMargins",  # AK
    "No. of FTE",  # AL
    "Last_Updated"  # AM
]
df = pd.DataFrame(records)[cols]

# -------------------------------------------------
# 6. Write to Google Sheet – NEW ARG ORDER (no deprecation warning)
# -------------------------------------------------
header = [
    "Next Earnings Date", "Enterprise Value", "Total Revenue",
    "EV/EBITDA", "Revenue Growth", "Gross Margin", "No. of FTE", "Last Updated"
]

# Header row
sheet.update(
    values=[header],
    range_name="AF1:AM1",
    value_input_option="USER_ENTERED"
)

# Data rows
data_end_row = len(df) + 1
sheet.update(
    values=df.astype(str).values.tolist(),
    range_name=f"AF2:AM{data_end_row}",
    value_input_option="USER_ENTERED"
)

print(f"\nSUCCESS! Updated AF→AM at {now_sg_str}", flush=True)

# -------------------------------------------------
# 7. APPEND ONLY: Date, Ticker, BTD → Historical_BTD_Metric
#     • Skip only if (Date + Ticker) already exists
#     • Use update() instead of append_rows() → 100% reliable
# -------------------------------------------------

# ----- 7.1 Read existing (Date, Ticker) pairs -----
existing_pairs = set()
try:
    all_values = hist_sheet.get_all_values()
    if len(all_values) > 1:  # Has header + data
        for row in all_values[1:]:
            if len(row) >= 2:
                date_val = row[0].strip()
                ticker_val = row[1].strip().upper()
                if date_val and ticker_val:
                    existing_pairs.add((date_val, ticker_val))
except Exception as e:
    print(f"[WARN] Could not read existing pairs: {e}", flush=True)

# ----- 7.2 Build new rows (skip duplicates) -----
hist_rows = []
run_date_str = now_sg_dt.strftime("%Y-%m-%d")

for idx in range(len(tickers)):
    ticker = tickers[idx].strip().upper()
    btd = ticker_btd_pairs[idx][1]
    pair_key = (run_date_str, ticker)

    if pair_key in existing_pairs:
        continue

    hist_rows.append([run_date_str, ticker, btd])

added = len(hist_rows)
skipped = len(tickers) - added

# ----- 7.3 Write header if missing -----
if not hist_sheet.row_values(1):
    header = ["Date (SG)", "Ticker", "BTD (Col E)"]
    hist_sheet.update('A1:C1', [header], value_input_option="USER_ENTERED")
    print("[INFO] Header written to Historical_BTD_Metric", flush=True)
    start_row = 2
else:
    start_row = len(hist_sheet.get_all_values()) + 1  # Next empty row

# ----- 7.4 WRITE using update() → NEVER fails silently -----
if hist_rows:
    end_row = start_row + len(hist_rows) - 1
    range_name = f"A{start_row}:C{end_row}"

    try:
        hist_sheet.update(
            range_name=range_name,
            values=hist_rows,
            value_input_option="USER_ENTERED"
        )
        print(f"[SUCCESS] {added} new BTD row(s) appended to Historical_BTD_Metric (rows {start_row}–{end_row})", flush=True)
    except Exception as e:
        print(f"[ERROR] Failed to write to Historical_BTD_Metric: {e}", flush=True)
        raise
else:
    print("[INFO] No new rows to append (all tickers for today already logged).", flush=True)