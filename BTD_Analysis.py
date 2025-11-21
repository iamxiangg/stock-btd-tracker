##### BTD_Analysis.py – FINAL CLEAN VERSION (No warnings, no errors) #####
import time
import pandas as pd
import yfinance as yf
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
import pytz
import gspread.utils

# -------------------------------------------------
# 1. Google Sheets Setup
# -------------------------------------------------
SCOPE = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
CREDS_FILE = "/home/neo/PycharmProjects/PythonProject/aerobic-arcade-377707-80bfc207c8b4.json"

creds = ServiceAccountCredentials.from_json_keyfile_name(CREDS_FILE, SCOPE)
client = gspread.authorize(creds)
workbook = client.open("Xiang Stock Analysis")
sheet = workbook.worksheet("Stock Summary USD")
hist_sheet = workbook.worksheet("Historical_BTD_Metric")

# -------------------------------------------------
# 2. Singapore Time
# -------------------------------------------------
sg_tz = pytz.timezone("Asia/Singapore")
now_sg_dt = datetime.now(sg_tz)
last_updated_str = now_sg_dt.strftime("%b %d, %Y")
now_sg_str = now_sg_dt.strftime("%Y-%m-%d %H:%M:%S %Z")
run_date_str = now_sg_dt.strftime("%Y-%m-%d")

print(f"Script started at: {now_sg_str}", flush=True)

# -------------------------------------------------
# 3. Helper: Map headers → column letters
# -------------------------------------------------
def build_column_map(worksheet, required_headers, header_row=1):
    headers = worksheet.row_values(header_row)
    col_map = {}
    for header in required_headers:
        clean = header.strip().lower()
        for idx, cell in enumerate(headers, 1):
            if cell.strip().lower() == clean:
                col_letter = gspread.utils.rowcol_to_a1(header_row, idx).replace(str(header_row), "")
                col_map[header] = col_letter
                break
        else:
            raise ValueError(f"Header not found: '{header}' in {worksheet.title}")
    return col_map

# -------------------------------------------------
# 4. Get Tickers + BTD – SKIP HEADER / PLACEHOLDERS
# -------------------------------------------------
col_a = sheet.col_values(1)
col_e = sheet.col_values(5)

ticker_btd_pairs = []
for i in range(1, len(col_a)):
    raw_ticker = col_a[i].strip().upper()
    if not raw_ticker or raw_ticker in ["TICKER", "SYMBOL", "STOCK"]:
        continue
    btd = col_e[i].strip() if i < len(col_e) else ""
    ticker_btd_pairs.append((raw_ticker, btd))

if not ticker_btd_pairs:
    print("No valid tickers found. Exiting.")
    raise SystemExit

tickers = [p[0] for p in ticker_btd_pairs]
print(f"Found {len(tickers)} valid tickers", flush=True)

# -------------------------------------------------
# 5. Fetch Data
# -------------------------------------------------
records = []
for ticker in tickers:
    print(f"  → {ticker}", end="", flush=True)
    row = {}

    try:
        t = yf.Ticker(ticker)
        info = t.info

        # Next Earnings Date
        earnings_date = "N/A"
        for attempt in range(3):
            try:
                df = t.earnings_dates
                if df is None or df.empty:
                    raise ValueError("No data")
                df.index = pd.to_datetime(df.index)
                today = pd.Timestamp.now(tz="UTC").normalize()
                reported_col = next((c for c in df.columns if "reported" in c.lower()), None)
                future = df[df[reported_col].isna()] if reported_col and reported_col in df.columns else df[df.index > today]
                earnings_date = future.index.min().strftime("%b %d, %Y") if not future.empty else "No upcoming"
                print(f" [Next: {earnings_date}]", end="", flush=True)
                break
            except:
                if attempt < 2:
                    time.sleep(2 ** attempt)
                else:
                    earnings_date = "Error"
                    print(" [FAILED]", end="", flush=True)

        row.update({
            "Next_Earnings_Date": earnings_date,
            "enterpriseValue": info.get("enterpriseValue", ""),
            "totalRevenue": info.get("totalRevenue", ""),
            "ebitdaMargins": info.get("ebitdaMargins", ""),
            "revenueGrowth": info.get("revenueGrowth", ""),
            "grossMargins": info.get("grossMargins", ""),
            "No. of FTE": info.get("fullTimeEmployees", ""),
            "Last_Updated": last_updated_str
        })
        print(" OK", flush=True)
    except:
        print(" [ERROR]", flush=True)
        row = {k: "ERROR" for k in [
            "Next_Earnings_Date","enterpriseValue","totalRevenue",
            "ebitdaMargins","revenueGrowth","grossMargins",
            "No. of FTE","Last_Updated"
        ]}

    records.append(row)
    time.sleep(0.6)

# -------------------------------------------------
# 6. DYNAMIC WRITE – NO DEPRECATION WARNINGS
# -------------------------------------------------
desired_headers = [
    "Next Earnings Date", "Enterprise Value", "Total Revenue",
    "EV/EBITDA", "Revenue Growth", "Gross Margin", "No. of FTE", "Last Updated"
]

print("Locating target columns...", flush=True)
col_map = build_column_map(sheet, desired_headers)
start_col = col_map[desired_headers[0]]
end_col   = col_map[desired_headers[-1]]
print(f"Writing to columns {start_col} → {end_col}", flush=True)

df_output = pd.DataFrame(records)[[
    "Next_Earnings_Date","enterpriseValue","totalRevenue",
    "ebitdaMargins","revenueGrowth","grossMargins",
    "No. of FTE","Last_Updated"
]]
df_output.columns = desired_headers

# FIXED: values first, then range_name=
sheet.update(values=[desired_headers],
             range_name=f"{start_col}1:{end_col}1",
             value_input_option="USER_ENTERED")

sheet.update(values=df_output.astype(str).values.tolist(),
             range_name=f"{start_col}2:{end_col}{len(df_output)+1}",
             value_input_option="USER_ENTERED")

print("Main sheet updated successfully!", flush=True)

# -------------------------------------------------
# 7. Historical_BTD_Metric (deduplicated)
# -------------------------------------------------
existing = set()
try:
    for r in hist_sheet.get_all_values()[1:]:
        if len(r) >= 2:
            existing.add((r[0].strip(), r[1].strip().upper()))
except: pass

new_rows = [[run_date_str, t.strip().upper(), b]
            for t, b in ticker_btd_pairs
            if (run_date_str, t.strip().upper()) not in existing]

if new_rows:
    if not hist_sheet.row_values(1):
        hist_sheet.update(values=[["Date (SG)","Ticker","BTD (Col E)"]], range_name="A1:C1")
    start = len(hist_sheet.get_all_values()) + 1
    hist_sheet.update(values=new_rows,
                      range_name=f"A{start}:C{start+len(new_rows)-1}",
                      value_input_option="USER_ENTERED")
    print(f"Appended {len(new_rows)} new historical rows", flush=True)
else:
    print("No new historical rows today", flush=True)

print(f"\nALL DONE! Finished at {datetime.now(sg_tz).strftime('%H:%M:%S')}")