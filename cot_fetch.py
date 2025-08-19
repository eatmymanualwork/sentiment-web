# cot_fetch.py
import os
import requests
import pandas as pd

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

def normalize_date(value) -> str:
    """Socrata zwraca 'YYYY-MM-DD' albo 'YYYY-MM-DDT00:00:00.000'.
    Zwracamy zawsze 'YYYY-MM-DD' jako string."""
    if value is None:
        return ""
    s = str(value)
    return s[:10]  # utnij do 'YYYY-MM-DD'

def get_latest_net(market_like: str):
    """Pobierz najnowszy wiersz dla rynku (LIKE '...%') i policz net = long - short."""
    params = {
        # wybieramy datę oraz różnicę (Socrata policzy to po stronie serwera)
        "$select": "report_date_as_yyyy_mm_dd, "
                   "(lev_money_positions_long_all - lev_money_positions_short_all) as net",
        "$where": f"market_and_exchange_names like '{market_like}%'",
        "$order": "report_date_as_yyyy_mm_dd DESC",
        "$limit": 1,
    }
    r = requests.get(BASE, params=params, timeout=40, headers={"User-Agent": "Mozilla/5.0"})
    r.raise_for_status()
    data = r.json()
    if not data:
        return None

    row = data[0]
    # czasem 'net' przychodzi jako string z kropką; zrzucamy do int bezpiecznie
    net_raw = row.get("net")
    try:
        net = int(float(net_raw))
    except Exception:
        # awaryjnie wylicz po stronie klienta
        long_ = int(row.get("lev_money_positions_long_all", 0))
        short_ = int(row.get("lev_money_positions_short_all", 0))
        net = long_ - short_

    date = normalize_date(row.get("report_date_as_yyyy_mm_dd"))
    return {"date": date, "lev_funds_net": net}

def main():
    rows = []
    for cftc_name, symbol in MAP.items():
        item = get_latest_net(cftc_name)
        if item:
            rows.append({"date": item["date"], "symbol": symbol, "lev_funds_net": item["lev_funds_net"]})

    df_new = pd.DataFrame(rows, columns=["date", "symbol", "lev_funds_net"])

    # wczytaj starą historię, jeśli istnieje
    if os.path.exists(OUT):
        try:
            df_old = pd.read_csv(OUT)
        except Exception:
            df_old = pd.DataFrame(columns=["date", "symbol", "lev_funds_net"])
    else:
        df_old = pd.DataFrame(columns=["date", "symbol", "lev_funds_net"])

    # złącz, znormalizuj datę do 'YYYY-MM-DD', deduplikuj, posortuj
    df_all = pd.concat([df_old, df_new], ignore_index=True)
    if not df_all.empty:
        df_all["date"] = df_all["date"].astype(str).str.slice(0, 10)
        df_all = (
            df_all.drop_duplicates(subset=["date", "symbol"])
                  .sort_values(["symbol", "date"])
                  .reset_index(drop=True)
        )

    df_all.to_csv(OUT, index=False)
    print(f"Saved {len(df_all)} rows to {OUT}")

if __name__ == "__main__":
    main()
