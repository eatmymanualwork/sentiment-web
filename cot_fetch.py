import os
import re
import datetime as dt
import requests
import pandas as pd

# Adres tygodniowego raportu CFTC
URL = "https://www.cftc.gov/dea/newcot/FinFutWk.txt"
# Nazwa pliku wyjściowego z historią
OUT = "cot_history.csv"

# Mapowanie nazw rynków CFTC -> symbol instrumentu
MAP = {
    "EURO FX": "EURUSD",
    "BRITISH POUND": "GBPUSD",
    "AUSTRALIAN DOLLAR": "AUDUSD",
    "S&P 500": "US500",
    "RUSSELL 2000": "US2000",
}

def parse_weekly(txt: str) -> pd.DataFrame:
    """
    Parsuje treść raportu FinFutWk.txt i zwraca DataFrame z kolumnami
    ['date', 'symbol', 'lev_funds_net'].
    """
    lines = txt.splitlines()
    # Określ datę raportu (np. 08/06/24) z pierwszych kilkunastu linii
    week_date = None
    for L in lines[:20]:
        m = re.search(r"(\d{2})/(\d{2})/(\d{2})", L)
        if m:
            mm, dd, yy = m.groups()
            week_date = f"20{yy}-{mm}-{dd}"
            break
    if not week_date:
        # Fallback: użyj ostatniego wtorku
        today = dt.date.today()
        week_date = (today - dt.timedelta(days=(today.weekday() - 1) % 7)).isoformat()

    current_market = None
    records = []
    for raw in lines:
        up = raw.strip().upper()
        # Wykryj początek bloku rynku
        for mk, sym in MAP.items():
            if mk in up:  # zamiast up.startswith(mk)
                current_market = sym
        # Szukaj wiersza z danymi "Leveraged Funds" (lub skrótu "Lev Funds")
        if current_market and ("LEVERAGED FUNDS" in up or "LEV FUNDS" in up):
            # Wyciągnij wszystkie liczby (mogą mieć przecinki i minusy)
            nums = [int(n.replace(",", "")) for n in re.findall(r"-?\d[\d,]*", up)]
            # Netto = long – short, jeśli mamy ≥2 liczby; w przeciwnym razie weź ostatnią liczbę
            net = None
            if len(nums) >= 2:
                net = nums[0] - nums[1]
            elif nums:
                net = nums[-1]
            if net is not None:
                records.append({
                    "date": week_date,
                    "symbol": current_market,
                    "lev_funds_net": int(net)
                })
            current_market = None  # Zamknij blok rynku
    # Zwróć DataFrame z kolumnami nawet gdy brak wierszy
    return pd.DataFrame(records, columns=["date", "symbol", "lev_funds_net"])

def main():
    # Pobierz treść pliku z nagłówkiem User-Agent (inaczej może być 403)
    resp = requests.get(URL, headers={"User-Agent": "Mozilla/5.0"}, timeout=40)
    resp.raise_for_status()
    df_new = parse_weekly(resp.text)
    # Wczytaj istniejący plik historii (jeśli istnieje)
    if os.path.exists(OUT):
        try:
            df_old = pd.read_csv(OUT)
        except Exception:
            df_old = pd.DataFrame(columns=["date", "symbol", "lev_funds_net"])
    else:
        df_old = pd.DataFrame(columns=["date", "symbol", "lev_funds_net"])
    # Sklej starą i nową historię, usuń duplikaty, posortuj
    df = pd.concat([df_old, df_new], ignore_index=True)
    if not df.empty:
        df["date"] = pd.to_datetime(df["date"]).dt.date
        df = df.drop_duplicates(subset=["date", "symbol"]).sort_values(["symbol", "date"])
    df.to_csv(OUT, index=False)
    print(f"Saved {OUT} rows: {len(df)}")

if __name__ == "__main__":
    main()
