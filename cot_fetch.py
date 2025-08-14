import os, re, datetime as dt
import requests
import pandas as pd

URL = "https://www.cftc.gov/dea/newcot/FinFutWk.txt"
OUT = "cot_history.csv"

# CFTC market -> nasz symbol
MAP = {
    "EURO FX": "EURUSD",
    "BRITISH POUND STERLING": "GBPUSD",
    "AUSTRALIAN DOLLAR": "AUDUSD",
    "E-MINI S&P 500": "US500",
    "RUSSELL 2000 MINI": "US2000",
}

def parse_weekly(txt: str) -> pd.DataFrame:
    """
    Zwraca DF z kolumnami ['date','symbol','lev_funds_net'].
    Parser szuka bloków rynku oraz wierszy z 'Leveraged Funds'.
    Wyciąga liczby z tej linii i liczy net = long - short.
    """
    lines = txt.splitlines()

    # Data tygodnia (spróbuj z nagłówka; fallback: ostatni wtorek)
    week_date = None
    for L in lines[:20]:
        m = re.search(r"(\d{2})/(\d{2})/(\d{2})", L)
        if m:
            mm, dd, yy = m.groups()
            week_date = f"20{yy}-{mm}-{dd}"
            break
    if not week_date:
        today = dt.date.today()
        week_date = (today - dt.timedelta(days=(today.weekday() - 1) % 7)).isoformat()

    current = None
    out = []

    for raw in lines:
        up = raw.strip().upper()

        # wykryj początek bloku rynku
        for mk, sym in MAP.items():
            if up.startswith(mk):
                current = sym

        # jeśli jesteśmy w bloku i trafimy wiersz Leveraged Funds – spróbuj wyciągnąć long/short
        if current and ("LEVERAGED FUNDS" in up or "LEV FUNDS" in up):
            # wszystkie liczby całkowite (z ewentualnymi przecinkami i minusami)
            nums = re.findall(r"-?\d[\d,]*", up)
            nums = [int(n.replace(",", "")) for n in nums]
            # typowy układ: Long, Short, Spreading, ..., Net ; ale bywa różnie.
            # Dlatego liczmy net = long - short gdy są min. 2 liczby.
            net = None
            if len(nums) >= 2:
                net = nums[0] - nums[1]
            # jeśli jest sporo liczb, ostatnia bywa "Net" – możemy użyć jako fallback
            if net is None and len(nums) >= 1:
                net = nums[-1]

            if net is not None:
                out.append({"date": week_date, "symbol": current, "lev_funds_net": int(net)})

            current = None  # zamykamy blok rynku

    # zwróć DF z kolumnami nawet, gdy pusto
    return pd.DataFrame(out, columns=["date", "symbol", "lev_funds_net"])

def main():
    r = requests.get(URL, timeout=40)
    r.raise_for_status()
    df_new = parse_weekly(r.text)

    if os.path.exists(OUT):
        try:
            df_old = pd.read_csv(OUT)
        except Exception:
            df_old = pd.DataFrame(columns=["date", "symbol", "lev_funds_net"])
    else:
        df_old = pd.DataFrame(columns=["date", "symbol", "lev_funds_net"])

    df = pd.concat([df_old, df_new], ignore_index=True)
    if not df.empty:
        df["date"] = pd.to_datetime(df["date"]).dt.date
        df = df.drop_duplicates(subset=["date", "symbol"]).sort_values(["symbol", "date"])

    df.to_csv(OUT, index=False)
    print(f"Saved {OUT} rows: {len(df)}")

if __name__ == "__main__":
    main()

