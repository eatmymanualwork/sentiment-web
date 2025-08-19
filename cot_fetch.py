import os
import requests
import pandas as pd

BASE_URL = "https://publicreporting.cftc.gov/resource/yw9f-hn96.json"
OUT = "cot_history.csv"

MAP = {
    "EURO FX": "EURUSD",
    "BRITISH POUND": "GBPUSD",
    "AUSTRALIAN DOLLAR": "AUDUSD",
    "S&P 500": "US500",
    "RUSSELL 2000": "US2000",
}

def get_latest_net(fragment, symbol):
    params = {
        "$select": "report_date_as_yyyy_mm_dd,lev_money_positions_long,lev_money_positions_short",
        "$where": f"upper(market_and_exchange_names) like '%{fragment.upper()}%'",
        "$order": "report_date_as_yyyy_mm_dd DESC",
        "$limit": 1
    }
    r = requests.get(BASE_URL, params=params, headers={"User-Agent": "Mozilla/5.0"}, timeout=60)
    r.raise_for_status()
    data = r.json()
    if not data:
        return None
    rec = data[0]
    net = int(rec["lev_money_positions_long"]) - int(rec["lev_money_positions_short"])
    return {"date": rec["report_date_as_yyyy_mm_dd"], "symbol": symbol, "lev_funds_net": net}

def main():
    records = []
    for frag, sym in MAP.items():
        row = get_latest_net(frag, sym)
        if row:
            records.append(row)
    df_new = pd.DataFrame(records)
    if os.path.exists(OUT):
        df_old = pd.read_csv(OUT)
    else:
        df_old = pd.DataFrame(columns=["date","symbol","lev_funds_net"])
    df_all = pd.concat([df_old, df_new], ignore_index=True)
    df_all["date"] = pd.to_datetime(df_all["date"]).dt.date
    df_all = df_all.drop_duplicates(subset=["date","symbol"]).sort_values(["symbol","date"])
    df_all.to_csv(OUT, index=False)
    print(f"Updated history: {len(df_all)} rows")

if __name__ == "__main__":
    main()
