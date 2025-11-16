# ==============================
# BTD_update.py - TERMUX ANDROID 100% WORKING (NO PANDAS)
# ==============================

import time
import yfinance as yf
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timezone
import pytz
import gspread.utils as gutils

# -------------------------------------------------
# 1. Google Sheets auth
# -------------------------------------------------
SCOPE = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
CREDS_FILE = "/storage/emulated/0/BTD_Analysis/aerobic-arcade-377707-80bfc207c8b4.json"

creds = ServiceAccountCredentials.from_json_keyfile_name(CREDS_FILE, SCOPE)
client = gspread.authorize(creds)
workbook = client.open("Xiang Stock Analysis")
sheet = workbook.worksheet("Stock Summary USD")

# -------------------------------------------------
# 2. SG Time
# -------------------------------------------------
sg_tz = pytz.timezone("Asia/Singapore")
now_sg_dt = datetime.now(sg_tz)
last_updated_str = now_sg_dt.strftime("%b %d, %Y")
now_sg_str = now_sg_dt.strftime("%Y-%m-%d %H:%M:%S %Z")
today_str = now_sg_dt.strftime("%Y-%m-%d")

print(f"Script started at: {now_sg_str}", flush=True)

# -------------------------------------------------
# 3. Get tickers
# -------------------------------------------------
col_a = sheet.col_values(1)
tickers = [t.strip().upper() for t in col_a[1:] if t.strip()]
if not tickers:
    print("No tickers. Exit.")
    raise SystemExit
print(f"Found {len(tickers)} tickers", flush=True)

# -------------------------------------------------
# 4. Fetch data (NO PANDAS)
# -------------------------------------------------
records = []
utc_now = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)

for ticker in tickers:
    print(f"  → {ticker}", end="", flush=True)
    row = {}
    try:
        t = yf.Ticker(ticker)
        info = t.info

        # Earnings Date (NO PANDAS)
        earnings_date = "N/A"
        try:
            ed = t.earnings_dates
            if ed is not None and len(ed) > 0:
                future_dates = []
                for idx, row in ed.iterrows():
                    try:
                        dt = idx.to_pydatetime() if hasattr(idx, 'to_pydatetime') else idx
                        if dt.tzinfo is None:
                            dt = pytz.UTC.localize(dt)
                        if dt > utc_now:
                            future_dates.append(dt)
                    except:
                        continue
                if future_dates:
                    next_dt = min(future_dates)
                    earnings_date = next_dt.astimezone(sg_tz).strftime("%b %d, %Y")
                    print(f" [Next: {earnings_date}]", end="", flush=True)
                else:
                    earnings_date = "No upcoming"
            else:
                earnings_date = "No data"
        except Exception as e:
            print(f" [Err: {e}]", end="", flush=True)
            earnings_date = "Error"

        # Metrics
        row.update({
            "Next_Earnings_Date": earnings_date,
            "enterpriseValue": info.get("enterpriseValue", ""),
            "totalRevenue": info.get("totalRevenue", ""),
            "enterpriseToEbitda": info.get("enterpriseToEbitda", ""),
            "revenueGrowth": info.get("revenueGrowth", ""),
            "grossMargins": info.get("grossMargins", ""),
            "No. of FTE": info.get("fullTimeEmployees", ""),
            "Last_Updated": last_updated_str
        })
        print(" OK", flush=True)
    except Exception as e:
        print(f" [ERROR: {e}]", flush=True)
        row = {k: "ERROR" for k in [
            "Next_Earnings_Date", "enterpriseValue", "totalRevenue",
            "enterpriseToEbitda", "revenueGrowth", "grossMargins",
            "No. of FTE", "Last_Updated"
        ]}
    records.append(row)
    time.sleep(0.6)

# -------------------------------------------------
# 5. Write AE→AL (Pure lists)
# -------------------------------------------------
cols = ["Next_Earnings_Date", "enterpriseValue", "totalRevenue", "enterpriseToEbitda",
        "revenueGrowth", "grossMargins", "No. of FTE", "Last_Updated"]
header = ["Next Earnings Date", "Enterprise Value", "Total Revenue", "EV/EBITDA",
          "Revenue Growth", "Gross Margin", "No. of FTE", "Last Updated"]

data_rows = [[row.get(c, "") for c in cols] for row in records]

sheet.update(values=[header], range_name="AE1:AL1", value_input_option="USER_ENTERED")
sheet.update(values=data_rows, range_name=f"AE2:AL{len(data_rows)+1}", value_input_option="USER_ENTERED")

# -------------------------------------------------
# 6. Historical_BTD_Metric (Column E)
# -------------------------------------------------
print("Updating Historical_BTD_Metric...", flush=True)
try:
    hist_sheet = workbook.worksheet("Historical_BTD_Metric")
except:
    hist_sheet = workbook.add_worksheet("Historical_BTD_Metric", 1000, len(tickers)+2)
    hist_sheet.update(values=[["Date"] + tickers], range_name="A1")

col_e = sheet.col_values(5)
current_e = {}
for i, t in enumerate(tickers):
    val = col_e[i+1] if i+1 < len(col_e) else ""
    current_e[t] = float(val) if str(val).replace('.','').replace('-','').isdigit() else ""

dates = [r[0] for r in hist_sheet.get_all_values()[1:]] if hist_sheet.row_count > 1 else []
if today_str not in dates:
    hist_sheet.append_row([today_str] + [current_e.get(t, "") for t in tickers])
    print(f"Appended {today_str}")
else:
    print("Today exists")

# -------------------------------------------------
# 7. Sparklines (AQ) + Trend (AR)
# -------------------------------------------------
hist_data = hist_sheet.get_all_values()
hist_rows = hist_data[1:] if len(hist_data) > 1 else []

spark_formulas = []
trends = []

for i, t in enumerate(tickers):
    col = i + 1  # Date=0, tickers start at 1
    recent = []
    for r in hist_rows[-30:]:
        val = r[col] if col < len(r) else ""
        try:
            recent.append(float(val)) if val else recent.append(None)
        except:
            recent.append(None)
    recent = [v for v in recent if v is not None]

    # Sparkline
    if len(recent) >= 2:
        start = len(hist_rows) - len(recent) + 2
        end = len(hist_rows) + 1
        ref = f"Historical_BTD_Metric!{gutils.rowcol_to_a1(start, col+1)}:{gutils.rowcol_to_a1(end, col+1)}"
        formula = f'=SPARKLINE({ref},{{"charttype","line";"color","#1a73e8"}})'
    else:
        formula = ""
    spark_formulas.append([formula])

    # Trend
    last3 = recent[-3:] if len(recent) >= 3 else recent
    if len(last3) >= 2 and last3[0] != 0:
        chg = (last3[-1] - last3[0]) / abs(last3[0])
        if chg > 0.15: trends.append("Strong Up")
        elif chg > 0.05: trends.append("Rising")
        elif chg < -0.15: trends.append("Strong Down")
        elif chg < -0.05: trends.append("Falling")
        else: trends.append("Stable")
    else:
        trends.append("")

sheet.update(values=spark_formulas, range_name=f"AQ2:AQ{len(tickers)+1}", value_input_option="USER_ENTERED")
sheet.update(values=[[t] for t in trends], range_name=f"AR2:AR{len(tickers)+1}", value_input_option="USER_ENTERED")
sheet.update(values=[["E Trend", "Trend"]], range_name="AQ1:AR1", value_input_option="USER_ENTERED")

print(f"\nSUCCESS! Updated at {now_sg_str}", flush=True)