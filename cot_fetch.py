import os, re, datetime as dt
import requests
import pandas as pd

URL = "https://www.cftc.gov/dea/newcot/FinFutWk.txt"
OUT = "cot_history.csv"

# CFTC market -> Twój symbol
MAP = {
    "EURO FX": "EURUSD",
    "BRITISH POUND STERLING": "GBPUSD",
    "AUSTRALIAN DOLLAR": "AUDUSD",
    "E-MINI S&P 500": "US500",
    "RUSSELL 2000 MINI": "US2000",
}

def parse_weekly(txt: str) -> pd.DataFrame:
    lines = txt.splitlines()
    # ustal datę tygodnia z nagłówka (mm/dd/yy)
    week_date = None
    for L in lines[:20]:
        m = re.search(r"(\d{2})/(\d{2})/(\d{2})", L)
        if m:
            mm, dd, yy = m.groups()
            week_date = f"20{yy}-{mm}-{dd}"
            break
    # fallback – ostatni wtorek, jeżeli nie znajdziesz daty
    if not week_date:
        today = dt.date.today()
        week_date = (today - dt.timedelta(days=(today.weekday() - 1) % 7)).isoformat()

    current_market = None
    records = []
    for raw in lines:
        up = raw.strip().upper()
        # wykryj początek bloku rynku
        for market, sym in MAP.items():
            if up.startswith(market):
                current_market = sym
        # w wierszu z Leverage Funds znajdź long/short i licz net
        if current_market and ("LEVERAGED FUNDS" in up or "LEV FUNDS" in up):
            nums = re.findall(r"-?\d[\d,]*", up)
            nums = [int(n.replace(",", "")) for n in nums]
            net = None
            if len(nums) >= 2:
                net = nums[0] - nums[1]  # long - short
            if net is None and nums:
                net = nums[-1]  # fallback – ostatnia liczba w linii
            if net is not None:
                records.append({"date": week_date, "symbol": current_market, "lev_funds_net": int(net)})
            current_market = None
    # zwróć ramkę nawet, jeśli lista records jest pusta
    return pd.DataFrame(records, columns=["date", "symbol", "lev_funds_net"])

def main():
    r = requests.get(URL, timeout=40)
    r.raise_for_status()
    df_new = parse_weekly(r.text)

    # czytaj istniejący CSV albo utwórz pusty z kolumnami, jeśli nie istnieje
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


