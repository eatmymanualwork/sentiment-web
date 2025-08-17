import os, re, datetime as dt
import requests
import pandas as pd

URL = "https://www.cftc.gov/dea/newcot/FinFutWk.txt"
OUT = "cot_history.csv"

# CFTC market -> symbol
MAP = {
    "EURO FX": "EURUSD",
    "BRITISH POUND STERLING": "GBPUSD",
    "AUSTRALIAN DOLLAR": "AUDUSD",
    "E-MINI S&P 500": "US500",
    "RUSSELL 2000 MINI": "US2000",
}

def parse_weekly(txt: str) -> pd.DataFrame:
    lines = txt.splitlines()
    # Ustal datę z nagłówka mm/dd/yy; fallback: ostatni wtorek
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
        # rozpoznaj rynek
        for mk, sym in MAP.items():
            if up.startswith(mk):
                current = sym
        # znajdź wiersz Leveraged Funds
        if current and ("LEVERAGED FUNDS" in up or "LEV FUNDS" in up):
            nums = [int(n.replace(",", "")) for n in re.findall(r"-?\d[\d,]*", up)]
            # net = long - short (pierwsze dwie liczby); fallback: ostatnia liczba w linii
            net = nums[0] - nums[1] if len(nums) >= 2 else nums[-1] if nums else None
            if net is not None:
                out.append({"date": week_date, "symbol": current, "lev_funds_net": int(net)})
            current = None
    return pd.DataFrame(out, columns=["date", "symbol", "lev_funds_net"])

def main():
    resp = requests.get(URL, timeout=40)
    resp.raise_for_status()
    df_new = parse_weekly(resp.text)

    df_old = pd.DataFrame(columns=["date","symbol","lev_funds_net"])
    if os.path.exists(OUT):
        try:
            df_old = pd.read_csv(OUT)
        except Exception:
            pass

    df = pd.concat([df_old, df_new], ignore_index=True)
    if not df.empty:
        df["date"] = pd.to_datetime(df["date"]).dt.date
        df = df.drop_duplicates(subset=["date","symbol"]).sort_values(["symbol","date"])
    df.to_csv(OUT, index=False)
    print(f"Saved {OUT} rows: {len(df)}")

if __name__ == "__main__":
    main()



