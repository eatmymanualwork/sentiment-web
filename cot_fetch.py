import os, re, datetime as dt
import requests
import pandas as pd

URL = "https://www.cftc.gov/dea/newcot/FinFutWk.txt"
OUT = "cot_history.csv"

# mapowanie CFTC -> nasz symbol
MAP = {
    "EURO FX": "EURUSD",
    "BRITISH POUND STERLING": "GBPUSD",
    "AUSTRALIAN DOLLAR": "AUDUSD",
    "E-MINI S&P 500": "US500",
    "RUSSELL 2000 MINI": "US2000",
}

def parse_weekly(txt: str) -> pd.DataFrame:
    """Zwraca DF z kolumnami ['date','symbol','lev_funds_net'] (nawet gdy pusty)."""
    lines = txt.splitlines()
    # spróbuj wyczytać datę tygodnia z nagłówka
    week_date = None
    date_re = re.compile(r"(\d{2}/\d{2}/\d{2})")
    for L in lines[:15]:
        m = date_re.search(L)
        if m:
            mm, dd, yy = m.group(1).split("/")
            week_date = f"20{yy}-{mm}-{dd}"
            break
    if week_date is None:
      # fallback: ostatni wtorek (stan raportu)
        today = dt.date.today()
        week_date = (today - dt.timedelta(days=(today.weekday() - 1) % 7)).isoformat()

    current = None
    out = []
    for line in lines:
        up = line.strip().upper()
        for mk, sym in MAP.items():
            if up.startswith(mk):
                current = sym
        if current and "LEV FUNDS" in up and "NET" in up:
            parts = [p for p in up.replace(",", " ").split() if p.replace("-", "").isdigit()]
            if parts:
                net = int(parts[-1])
                out.append({"date": week_date, "symbol": current, "lev_funds_net": net})
                current = None

    # ważne: nawet gdy brak danych – zwróć DF z kolumnami
    return pd.DataFrame(out, columns=["date", "symbol", "lev_funds_net"])

def main():
    r = requests.get(URL, timeout=40)
    r.raise_for_status()
    df_new = parse_weekly(r.text)

    if os.path.exists(OUT):
        df_old = pd.read_csv(OUT)  # może być puste, ale z kolumnami
    else:
        # utwórz pusty DF z poprawnymi kolumnami
        df_old = pd.DataFrame(columns=["date", "symbol", "lev_funds_net"])

    # sklej i odfiltruj duplikaty; jeśli oba puste – dalej będzie pusty, ale z kolumnami
    df = pd.concat([df_old, df_new], ignore_index=True)
    if not df.empty:
        df["date"] = pd.to_datetime(df["date"]).dt.date
        df = df.drop_duplicates(subset=["date", "symbol"]).sort_values(["symbol", "date"])

    df.to_csv(OUT, index=False)
    print(f"Saved {OUT} rows: {len(df)}")

if __name__ == "__main__":
    main()
