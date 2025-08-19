# cot_fetch.py
import os
import requests
import pandas as pd
from requests.exceptions import HTTPError

BASE = "https://publicreporting.cftc.gov/resource/yw9f-hn96.json"
OUT = "cot_history.csv"

# CFTC -> nasz symbol
MAP = {
    "EURO FX": "EURUSD",
    "BRITISH POUND": "GBPUSD",
    "AUSTRALIAN DOLLAR": "AUDUSD",
    "E-MINI S&P 500": "US500",
    "RUSSELL E-MINI": "US2000",
}

HEADERS = {"User-Agent": "Mozilla/5.0"}

def normalize_date(value) -> str:
    s = "" if value is None else str(value)
    return s[:10]  # 'YYYY-MM-DD' z 'YYYY-MM-DDT00:00:00.000'

def soql_params(api_names: bool, market_like: str):
    """
    Buduje parametry SoQL. Gdy api_names=True używa nazw z podkreśleniami,
    w przeciwnym razie nazw 'ze spacjami' w backtickach (wymagane na części instancji Socraty).
    """
    if api_names:
        date_col = "report_date_as_yyyy_mm_dd"
        long_col = "lev_money_positions_long_all"
        short_col = "lev_money_positions_short_all"
        where_col = "market_and_exchange_names"
    else:
        # wersja jak w Twoim logu (ze spacjami) – wszystko w backtickach
        date_col = "`report date as yyyy mm dd`"
        long_col = "`lev money positions long all`"
        short_col = "`lev money positions short all`"
        where_col = "`market and exchange names`"

    return {
        "$select": f"{date_col}, ({long_col} - {short_col}) as net",
        "$where": f"{where_col} like '{market_like}%'",
        "$order": f"{date_col} DESC",
        "$limit": 1,
    }

def get_latest_net(market_like: str):
    # 1. próba – nazwy API (z podkreśleniami)
    params = soql_params(True, market_like)
    try:
        r = requests.get(BASE, params=params, timeout=40, headers=HEADERS)
        r.raise_for_status()
        data = r.json()
    except HTTPError as e:
        # jeśli 400 – retry z nazwami 'spaced' + backticks
        if e.response is not None and e.response.status_code == 400:
            params2 = soql_params(False, market_like)
            r2 = requests.get(BASE, params=params2, timeout=40, headers=HEADERS)
            r2.raise_for_status()
            data = r2.json()
        else:
            raise
    if not data:
        return None

    row = data[0]
    # net może być stringiem/floatem
    net_raw = row.get("net")
    try:
        net = int(float(net_raw))
    except Exception:
        # awaryjne liczenie po stronie klienta (obsługa obu wariantów nazw)
        long_ = int(row.get("lev_money_positions_long_all") or
                    row.get("lev money positions long all") or 0)
        short_ = int(row.get("lev_money_positions_short_all") or
                     row.get("lev money positions short all") or 0)
        net = long_ - short_

    # obsłuż oba warianty nazwy kolumny daty
    date_val = (row.get("report_date_as_yyyy_mm_dd") or
                row.get("report date as yyyy mm dd"))
    date = normalize_date(date_val)
    return {"date": date, "lev_funds_net": net}

def main():
    rows = []
    for cftc_name, symbol in MAP.items():
        item = get_latest_net(cftc_name)
        if item:
            rows.append({"date": item["date"], "symbol": symbol, "lev_funds_net": item["lev_funds_net"]})

    df_new = pd.DataFrame(rows, columns=["date", "symbol", "lev_funds_net"])

    # wczytaj historię (jeśli istnieje)
    if os.path.exists(OUT):
        try:
            df_old = pd.read_csv(OUT)
        except Exception:
            df_old = pd.DataFrame(columns=["date", "symbol", "lev_funds_net"])
    else:
        df_old = pd.DataFrame(columns=["date", "symbol", "lev_funds_net"])

    # złącz, znormalizuj daty, deduplikuj i posortuj
    df = pd.concat([df_old, df_new], ignore_index=True)
    if not df.empty:
        df["date"] = df["date"].astype(str).str.slice(0, 10)
        df = df.drop_duplicates(subset=["date", "symbol"]).sort_values(["symbol", "date"]).reset_index(drop=True)

    df.to_csv(OUT, index=False)
    print(f"Saved {len(df)} rows to {OUT}")

if __name__ == "__main__":
    main()
