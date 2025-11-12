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
CREDS_FILE = "/aerobic-arcade-377707-80bfc207c8b4.json"

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
# 5. Build DataFrame (AE → AL)
# -------------------------------------------------
cols = [
    "Next_Earnings_Date",  # AE
    "enterpriseValue",  # AF
    "totalRevenue",  # AG
    "enterpriseToEbitda",  # AH
    "revenueGrowth",  # AI
    "grossMargins",  # AJ
    "No. of FTE",  # AK
    "Last_Updated"  # AL
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
    range_name="AE1:AL1",
    value_input_option="USER_ENTERED"
)

# Data rows
data_end_row = len(df) + 1
sheet.update(
    values=df.astype(str).values.tolist(),
    range_name=f"AF2:AM{data_end_row}",
    value_input_option="USER_ENTERED"
)

print(f"\nSUCCESS! Updated AE→AL at {now_sg_str}", flush=True)

# -------------------------------------------------
# 7. APPEND ONLY: Date, Ticker, BTD → Historical_BTD_Metric
#     • Skip rows whose DATE already exists
# -------------------------------------------------
import logging

# ----- 7.1 Read existing dates (column A) -----
existing_dates = set()
try:
    # Pull only column A – fastest possible read
    date_col = hist_sheet.col_values(1)          # 1-based index → column A
    # Skip header row (index 0)
    existing_dates = {d.strip() for d in date_col[1:] if d.strip()}
except Exception as e:
    print(f"[WARN] Could not read existing dates: {e}", flush=True)

# ----- 7.2 Build rows that are NOT already present -----
hist_rows = []
skipped = 0
added   = 0

run_date_str = now_sg_dt.strftime("%Y-%m-%d")   # ← date part only

for idx in range(len(tickers)):
    ticker = tickers[idx]
    btd    = ticker_btd_pairs[idx][1]

    # Build the candidate row
    candidate = [run_date_str, ticker, btd]

    # ----- CHECK DUPLICATE -----
    if run_date_str in existing_dates:
        # Date already logged today → skip this *entire* day
        if skipped == 0:   # print only once per run
            print(f"[INFO] Date {run_date_str} already exists in Historical_BTD_Metric → skipping all rows for today.", flush=True)
        skipped += 1
        continue

    # New date → add the row
    hist_rows.append(candidate)
    added += 1

# ----- 7.3 Write header if sheet is empty -----
if hist_sheet.row_count == 0 or not hist_sheet.row_values(1):
    hist_header = ["Date (SG)", "Ticker", "BTD (Col E)"]
    hist_sheet.append_row(hist_header, value_input_option="USER_ENTERED")
    print("[INFO] Header written to Historical_BTD_Metric", flush=True)

# ----- 7.4 Append only the NEW rows (if any) -----
if hist_rows:
    hist_sheet.append_rows(hist_rows, value_input_option="USER_ENTERED")
    print(f"[SUCCESS] {added} new BTD row(s) appended to Historical_BTD_Metric", flush=True)
else:
    print("[INFO] No new rows to append (date already present).", flush=True)

# ----- 7.5 Final summary -----
print(f"\n=== RUN SUMMARY ==="
      f"\nMain sheet updated (E-M) | "
      f"{len(tickers)} ticker(s) processed | "
      f"{added} added, {skipped} skipped (date already logged)"
      f"\nTimestamp: {now_sg_str}\n", flush=True)