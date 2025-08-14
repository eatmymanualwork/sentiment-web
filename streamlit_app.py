import os, io, math, time
from datetime import datetime, timezone
import requests
import pandas as pd
import streamlit as st
import plotly.express as px

# -------------------- KONFIG --------------------
SYMBOLS = ["EURUSD", "GBPUSD", "AUDUSD", "US500", "US2000", "GER40"]
DEFAULT_WEIGHTS = {"retail": 0.6, "institutional": 0.4}
THRESHOLDS = {"long": 30, "short": -30}
RETAIL_REFRESH_SECONDS = 300  # 5 min
# ------------------------------------------------

st.set_page_config(page_title="Sentiment Dashboard", layout="wide")
st.title("📊 Sentiment Dashboard — Retail vs Institutions")
st.caption("Retail: Myfxbook (kontrariańsko). Institutions: COT (Leveraged Funds). NetScore = w_retail*Retail + (1-w_retail)*Institutions.")

# -------------------- FUNKCJE NARZĘDZIOWE --------------------
def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))

def retail_score_from_long(avg_long: float | None) -> float:
    """Kontrariańsko: więcej longów => bardziej ujemny wynik. Skala [-100, 100]."""
    if avg_long is None:
        return 0.0
    score = -2.0 * (avg_long - 50.0)
    return clamp(score, -100.0, 100.0)

@st.cache_data(show_spinner=False, ttl=RETAIL_REFRESH_SECONDS)
def get_myfxbook_outlook(email: str, password: str) -> pd.DataFrame | None:
    """Pobiera Community Outlook z Myfxbook -> DataFrame [symbol, long_pct, short_pct]."""
    BASE = "https://www.myfxbook.com/api"
    try:
        s = requests.Session()
        r = s.get(f"{BASE}/login.json", params={"email": email, "password": password}, timeout=15)
        r.raise_for_status()
        data = r.json()
        if data.get("error"):
            st.error("Myfxbook login error: " + str(data.get("message")))
            return None
        token = data.get("session")

        r = s.get(f"{BASE}/get-community-outlook.json", params={"session": token}, timeout=20)
        r.raise_for_status()
        payload = r.json()
        s.get(f"{BASE}/logout.json", params={"session": token}, timeout=10)

        items = payload.get("symbols", []) or payload.get("data", [])
        rows = []
        for it in items:
            name = str(it.get("name") or it.get("symbol") or "").replace(".", "").upper()
            long_pct = float(it.get("longPercentage") or it.get("long") or 0)
            short_pct = float(it.get("shortPercentage") or it.get("short") or 0)
            rows.append({"symbol": name, "long_pct": long_pct, "short_pct": short_pct})
        df = pd.DataFrame(rows).drop_duplicates(subset=["symbol"])
        return df
    except Exception as e:
        st.error(f"Myfxbook fetch exception: {e}")
        return None

@st.cache_data(show_spinner=False, ttl=60*60)
def get_cot_last_week() -> pd.DataFrame:
    """
    Pobiera tygodniowy plik FinFutWk.txt z CFTC i wyciąga surowy 'Leveraged Funds Net' jako proxy.
    Używane tylko jako fallback, jeśli nie podasz COT_HISTORY_URL.
    """
    url = "https://www.cftc.gov/dea/newcot/FinFutWk.txt"
    markets = {
        "EURO FX": "EURUSD",
        "BRITISH POUND STERLING": "GBPUSD",
        "AUSTRALIAN DOLLAR": "AUDUSD",
        "E-MINI S&P 500": "US500",
        "RUSSELL 2000 MINI": "US2000",
    }
    try:
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        lines = r.text.splitlines()
        current = None
        records = []
        for ln in lines:
            up = ln.strip().upper()
            for mk, sym in markets.items():
                if up.startswith(mk):
                    current = sym
            if current and "LEV FUNDS" in up and "NET" in up:
                parts = [p for p in up.replace(",", " ").split() if p.replace("-", "").isdigit()]
                if parts:
                    net = int(parts[-1])
                    records.append({"symbol": current, "lev_funds_net": net})
                    current = None
        return pd.DataFrame(records)
    except Exception:
        return pd.DataFrame(columns=["symbol", "lev_funds_net"])

@st.cache_data(show_spinner=False, ttl=60*10)
def get_cot_history_from_csv(url: str) -> pd.DataFrame:
    """Wczytuje historię COT z CSV (kolumny: date,symbol,lev_funds_net)."""
    try:
        r = requests.get(url, timeout=20)
        r.raise_for_status()
        df = pd.read_csv(io.StringIO(r.text), parse_dates=["date"])
        df["symbol"] = df["symbol"].astype(str).str.replace(".", "", regex=False).str.upper()
        return df.sort_values(["symbol", "date"]).reset_index(drop=True)
    except Exception as e:
        st.warning(f"Nie udało się pobrać historii COT z {url}: {e}")
        return pd.DataFrame(columns=["date","symbol","lev_funds_net"])

def institutional_score_from_history(df_hist: pd.DataFrame, sym: str) -> float:
    """Z-score tygodniowej ZMIANY lev_funds_net, zmapowany logistycznie do [-100, 100]."""
    ser = df_hist.loc[df_hist["symbol"] == sym, "lev_funds_net"]
    if len(ser) < 6:
        return 0.0
    delta = ser.diff()
    mu = delta.rolling(13).mean().iloc[-1] if len(delta) >= 13 else delta.mean()
    sigma = delta.rolling(13).std().iloc[-1] if len(delta) >= 13 else delta.std(ddof=0)
    if sigma == 0 or pd.isna(sigma):
        return 0.0
    z = float((delta.iloc[-1] - mu) / sigma)
    scaled = 100.0 * (2 / (1 + math.exp(-z)) - 1)
    return clamp(scaled, -100.0, 100.0)

# -------------------- UI (SIDEBAR) --------------------
with st.sidebar:
    st.header("Ustawienia")
    w_retail = st.slider("Waga Retail", 0.0, 1.0, DEFAULT_WEIGHTS["retail"], 0.1)
    w_inst = 1.0 - w_retail
    long_thr = st.slider("Próg LONG (Net ≥)", 10, 60, THRESHOLDS["long"], 5)
    short_thr_abs = st.slider("Próg SHORT (Net ≤ -X)", 10, 60, abs(THRESHOLDS["short"]), 5)
    short_thr = -short_thr_abs
    selected_symbols = st.multiselect("Symbole", SYMBOLS, default=SYMBOLS)

    st.markdown("---")
    st.caption("Sekrety (Streamlit Cloud → Settings → Secrets):")
    st.code(
        'MYFXBOOK_EMAIL="..." \nMYFXBOOK_PASSWORD="..." \nCOT_HISTORY_URL="https://raw.githubusercontent.com/<user>/sentiment-web/main/cot_history.csv"'
    )

# -------------------- DANE (SECRETS) --------------------
email = st.secrets.get("MYFXBOOK_EMAIL") or os.getenv("MYFXBOOK_EMAIL", "")
password = st.secrets.get("MYFXBOOK_PASSWORD") or os.getenv("MYFXBOOK_PASSWORD", "")
cot_hist_url = st.secrets.get("COT_HISTORY_URL", "") or os.getenv("COT_HISTORY_URL", "")

# Retail
retail_df = None
if email and password:
    retail_df = get_myfxbook_outlook(email, password)
else:
    st.warning("Ustaw MYFXBOOK_EMAIL i MYFXBOOK_PASSWORD w sekcji Secrets.")

# Institutions
cot_hist = get_cot_history_from_csv(cot_hist_url) if cot_hist_url else pd.DataFrame(columns=["date","symbol","lev_funds_net"])
cot_last = get_cot_last_week()  # fallback podgląd

# -------------------- LICZENIA I WIDOK --------------------
now = datetime.now(timezone.utc).isoformat(timespec="seconds")
rows_hist = []
cards = []

for sym in selected_symbols:
    # Retail
    retail_long = None
    if retail_df is not None:
        r = retail_df.loc[retail_df["symbol"] == sym]
        if not r.empty:
            retail_long = float(r["long_pct"].iloc[0])
    r_score = retail_score_from_long(retail_long)

    # Institutions
    inst_score = 0.0
    inst_proxy = None
    if not cot_hist.empty:
        inst_score = institutional_score_from_history(cot_hist, sym)
        last = cot_hist.loc[cot_hist["symbol"] == sym].tail(1)
        if not last.empty:
            inst_proxy = float(last["lev_funds_net"].iloc[0])
    else:
        # fallback: tylko ostatni tydzień (bez z-score)
        if not cot_last.empty:
            rr = cot_last.loc[cot_last["symbol"] == sym]
            if not rr.empty:
                inst_proxy = float(rr["lev_funds_net"].iloc[0])

    net = w_retail * r_score + w_inst * inst_score
    direction = "LONG" if net >= long_thr else "SHORT" if net <= short_thr else "FLAT"

    cards.append((sym, retail_long, r_score, inst_proxy, inst_score, net, direction))
    rows_hist.append({
        "timestamp": now, "symbol": sym,
        "retail_long_pct": retail_long,
        "retail_score": round(r_score, 1),
        "institutional_score": round(inst_score, 1),
        "net_score": round(net, 1)
    })

df_cards = pd.DataFrame(
    cards,
    columns=["Symbol","Retail % long","RetailScore","Inst proxy (lev funds net)","InstitutionalScore","NetScore","Kierunek"]
)
st.dataframe(df_cards.set_index("Symbol"), use_container_width=True)

tab1, tab2, tab3 = st.tabs(["Retail (intraday)", "Institutions (weekly)", "NetScore & sygnał"])

with tab1:
    st.subheader("Retail: Myfxbook")
    sym = st.selectbox("Symbol", selected_symbols, key="sym1")
    df_sym = pd.DataFrame([r for r in rows_hist if r["symbol"] == sym])
    if not df_sym.empty:
        c1, c2 = st.columns(2)
        with c1:
            fig = px.line(df_sym, x="timestamp", y="retail_long_pct", title=f"{sym} — % long (Myfxbook)")
            st.plotly_chart(fig, use_container_width=True)
        with c2:
            fig2 = px.line(df_sym, x="timestamp", y="retail_score", title=f"{sym} — RetailScore (kontrariańsko)")
            st.plotly_chart(fig2, use_container_width=True)
    else:
        st.info("Brak danych w tej sesji — odczekaj chwilę lub odśwież.")

with tab2:
    st.subheader("Institutions: COT")
    if not cot_hist.empty:
        st.caption("InstitutionalScore = z‑score tygodniowej zmiany (na podstawie historii z COT_HISTORY_URL).")
        st.dataframe(cot_hist.tail(20), use_container_width=True)
    else:
        st.caption("Proxy z ostatniego tygodnia (bez z‑score). Aby mieć InstitutionalScore, dodaj COT_HISTORY_URL do Secrets.")
        st.dataframe(cot_last, use_container_width=True)

with tab3:
    st.subheader("NetScore (agregat) i kierunek")
    st.caption(f"Reguły: LONG jeśli Net ≥ {long_thr}; SHORT jeśli Net ≤ {short_thr}; w innym wypadku FLAT.")
    fig3 = px.bar(df_cards, x="Symbol", y="NetScore", color="Kierunek", title="NetScore")
    st.plotly_chart(fig3, use_container_width=True)
