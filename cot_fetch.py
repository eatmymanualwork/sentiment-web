import requests, pandas as pd, io, datetime as dt, re, os

# Proste źródło tygodniowe: FinFutWk.txt (Traders in Financial Futures, Futures Only)
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

def parse_weekly(txt: str):
    lines = txt.splitlines()
    current = None
    out = []
    # Spróbuj odczytać datę tygodnia z nagłówka (często jest np. "Commitments of Traders - Futures Only, ... 08/06/24")
    # Jeśli się nie uda, użyj dzisiejszego piątku.
    week_date = None
    date_re = re.compile(r"(\d{2}/\d{2}/\d{2})")
    for L in lines[:10]:
        m = date_re.search(L)
        if m:
            # mm/dd/yy -> YYYY-MM-DD
            mm, dd, yy = m.group(1).split("/")
            week_date = f"20{yy}-{mm}-{dd}"
            break
    if week_date is None:
        # fallback: ostatni wtorek (stan raportu)
        today = dt.date.today()
        tuesday = today - dt.timedelta(days=(today.weekday()-1) % 7)
        week_date = tuesday.isoformat()

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
    return pd.DataFrame(out)

def main():
    r = requests.get(URL, timeout=30); r.raise_for_status()
    df_new = parse_weekly(r.text)
    if os.path.exists(OUT):
        df_old = pd.read_csv(OUT)
        df = pd.concat([df_old, df_new], ignore_index=True)
        df = df.drop_duplicates(subset=["date","symbol"]).sort_values(["symbol","date"])
    else:
        df = df_new.sort_values(["symbol","date"])
    df.to_csv(OUT, index=False)
    print("Saved", OUT, "rows:", len(df))

if __name__ == "__main__":
    main()
