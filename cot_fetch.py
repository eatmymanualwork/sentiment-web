import os
import io
import requests
import pandas as pd

# Zbiór TFF Combined w Socrata – zwraca JSON/CSV
BASE_URL = "https://publicreporting.cftc.gov/resource/yw9f-hn96.json"
OUT = "cot_history.csv"

# mapowanie fragmentów nazw rynku -> symbol w dashboardzie
MAP = {
    "EURO FX": "EURUSD",
    "BRITISH POUND": "GBPUSD",
    "AUSTRALIAN DOLLAR": "AUDUSD",
    "S&P 500": "US500",
    "RUSSELL 2000": "US2000",
}

def get_latest_net(symbol_fragment, symbol_code):
    """
    Pobiera najnowszy rekord dla podanego rynku i oblicza pozycję netto leveraged funds.
    Zwraca dict lub None, jeśli nie znaleziono.
    """
    params = {
        "$select": "report_date_as_yyyy_mm_dd, lev_money_positions_long_all, lev_money_positions_short_all",
        "$where": f"market_and_exchange_names like '%{symbol_fragment}%'",
        "$order": "report_date_as_yyyy_mm_dd DESC",
        "$limit": 1
    }
    resp = requests.get(BASE_URL, params=params, headers={"User-Agent": "Mozilla/5.0"}, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    if not data:
        return None
    rec = data[0]
    net = int(rec["lev_money_positions_long_all"]) - int(rec["lev_money_positions_short_all"])
    return {
        "date": rec["report_date_as_yyyy_mm_dd"],
        "symbol": symbol_code,
        "lev_funds_net": net
    }

def main():
    # dla każdego rynku oblicz pozycję netto
    records = []
    for frag, sym in MAP.items():
        item = get_latest_net(frag, sym)
        if item:
            records.append(item)

    if not records:
        print("Brak rekordów – sprawdź mapę i parametry API")
        return

    df_new = pd.DataFrame(records)

    # wczytaj dotychczasową historię
    if os.path.exists(OUT):
        try:
            df_old = pd.read_csv(OUT)
        except Exception:
            df_old = pd.DataFrame(columns=["date","symbol","lev_funds_net"])
    else:
        df_old = pd.DataFrame(columns=["date","symbol","lev_funds_net"])

    # połącz, usuń duplikaty i sortuj
    df_all = pd.concat([df_old, df_new], ignore_index=True)
    df_all["date"] = pd.to_datetime(df_all["date"]).dt.date
    df_all = df_all.drop_duplicates(subset=["date","symbol"]).sort_values(["symbol","date"])
    df_all.to_csv(OUT, index=False)
    print(f"Zaktualizowano {OUT}, wierszy: {len(df_all)}")

if __name__ == "__main__":
    main()
