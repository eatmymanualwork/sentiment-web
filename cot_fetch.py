import os
import requests
import pandas as pd
from datetime import datetime

BASE = "https://publicreporting.cftc.gov/resource/yw9f-hn96.json"
OUT = "cot_history.csv"

# mapowanie nazw rynków na symbole
MAP = {
    "EURO FX": "EURUSD",
    "BRITISH POUND": "GBPUSD",
    "AUSTRALIAN DOLLAR": "AUDUSD",
    "E-MINI S&P 500": "US500",
    "RUSSELL E-MINI": "US2000",
}

HEADERS = {"User-Agent": "Mozilla/5.0"}

def get_latest_net(frag, symbol):
    params = {
        "$select": "report_date_as_yyyy_mm_dd,"
                   "(lev_money_positions_long_all - lev_money_positions_short_all) as net",
        "$where": f"upper(market_and_exchange_names) like '%{frag.upper()}%'",
        "$order": "report_date_as_yyyy_mm_dd DESC",
        "$limit": 1,
    }
    r = requests.get(BASE, params=params, headers=HEADERS, timeout=40)
    r.raise_for_status()
    data = r.json()
    if not data:
        return None
    row = data[0]
    date_str = row["report_date_as_yyyy_mm_dd"][:10]  # YYYY-MM-DD
    net_val = int(float(row["net"]))
    return {"date": date_str, "symbol": symbol, "lev_funds_net": net_val}

def main():
    rows = []
    for key, sym in MAP.items():
        record = get_latest_net(key, sym)
        if record:
            rows.append(record)

    new_df = pd.DataFrame(rows, columns=["date", "symbol", "lev_funds_net"])

    if os.path.exists(OUT):
        try:
            old_df = pd.read_csv(OUT)
        except Exception:
            old_df = pd.DataFrame(columns=["date", "symbol", "lev_funds_net"])
    else:
        old_df = pd.DataFrame(columns=["date", "symbol", "lev_funds_net"])

    df = pd.concat([old_df, new_df], ignore_index=True)
    df = df.drop_duplicates(subset=["date", "symbol"]).sort_values(["symbol", "date"])
    df.to_csv(OUT, index=False)
    print(f"Zapisano {len(df)} wierszy do {OUT}")

if __name__ == "__main__":
    main()
