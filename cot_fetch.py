import os
import io
import requests
import pandas as pd
from datetime import datetime, timezone

# URL pobierający aktualny raport TFF w formacie CSV.
# Serwis publicreporting.cftc.gov udostępnia gotowe tabele w formacie CSV.
CSV_URL = (
    "https://publicreporting.cftc.gov/api/v1/aggregation/tff?"
    "format=csv&commodity=all"
)

# plik wyjściowy
OUT = "cot_history.csv"

# mapowanie nazw rynków (fragment nazwy rynku w raporcie) na symbol instrumentu
MAP = {
    "EURO FX": "EURUSD",
    "BRITISH POUND": "GBPUSD",
    "AUSTRALIAN DOLLAR": "AUDUSD",
    "S&P 500": "US500",
    "RUSSELL 2000": "US2000",
}

def fetch_tff_csv() -> pd.DataFrame:
    """
    Pobierz CSV z API CFTC i zwróć ramkę danych.
    W nagłówku zapytania ustawiamy User-Agent, żeby uniknąć 403.
    """
    resp = requests.get(CSV_URL, headers={"User-Agent": "Mozilla/5.0"}, timeout=60)
    resp.raise_for_status()
    # Użyj io.StringIO, żeby przekazać tekst CSV do pandas
    return pd.read_csv(io.StringIO(resp.text))

def extract_records(df: pd.DataFrame) -> pd.DataFrame:
    """
    Z oryginalnego CSV wybierz rynki z MAP i oblicz lev_funds_net = long − short.
    Załóż, że kolumny nazywają się:
      - Report_Date – data raportu (format YYYY-MM-DD)
      - Market_and_Exchange_Name – nazwa rynku
      - Leveraged_Money_Long_All – pozycje długie leveraged funds
      - Leveraged_Money_Short_All – pozycje krótkie leveraged funds
    Jeśli w Twoim pliku kolumny nazywają się inaczej, zmień nazwy tutaj.
    """
    records = []
    for key_fragment, symbol in MAP.items():
        # filtrowanie wierszy, w których nazwa rynku zawiera dany fragment
        mdf = df[df["Market_and_Exchange_Name"].str.contains(key_fragment, case=False, na=False)]
        if mdf.empty:
            continue
        # weź najnowszą datę (pliki zawierają historię wielu raportów)
        latest_row = mdf.sort_values("Report_Date").iloc[-1]
        date_str = latest_row["Report_Date"]
        # oblicz netto
        long_pos = latest_row["Leveraged_Money_Long_All"]
        short_pos = latest_row["Leveraged_Money_Short_All"]
        net = long_pos - short_pos
        records.append({"date": date_str, "symbol": symbol, "lev_funds_net": int(net)})
    return pd.DataFrame(records, columns=["date", "symbol", "lev_funds_net"])

def main():
    try:
        df_csv = fetch_tff_csv()
    except Exception as e:
        print(f"Unable to fetch CSV: {e}")
        return
    records_df = extract_records(df_csv)
    if records_df.empty:
        print("No records extracted from CSV. Please check column names and MAP.")
        return
    # wczytaj istniejącą historię, jeżeli plik istnieje
    if os.path.exists(OUT):
        try:
            df_old = pd.read_csv(OUT)
        except Exception:
            df_old = pd.DataFrame(columns=["date", "symbol", "lev_funds_net"])
    else:
        df_old = pd.DataFrame(columns=["date", "symbol", "lev_funds_net"])
    df_all = pd.concat([df_old, records_df], ignore_index=True)
    # deduplikacja i sortowanie
    df_all["date"] = pd.to_datetime(df_all["date"]).dt.date
    df_all = df_all.drop_duplicates(subset=["date", "symbol"]).sort_values(["symbol", "date"])
    df_all.to_csv(OUT, index=False)
    print(f"Saved {OUT} rows: {len(df_all)}")

if __name__ == "__main__":
    main()
