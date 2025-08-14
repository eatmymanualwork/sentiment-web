# Sentiment Web (Streamlit)

Dashboard webowy łączący sentyment detaliczny (Myfxbook) z instytucjonalnym (COT proxy) i podający kierunek (NetScore).

## Szybki start (Streamlit Cloud, bez instalacji)

1. Utwórz nowe repo na GitHub, np. `sentiment-web`.
2. Wgraj tam pliki: `streamlit_app.py`, `requirements.txt` oraz folder `.streamlit/secrets.toml.sample` (dla podglądu).
3. Wejdź na https://share.streamlit.io → **New app** → wskaż repo/branch/plik `streamlit_app.py`.
4. W panelu **Secrets** dodaj:
   ```
   MYFXBOOK_EMAIL=twoj_mail
   MYFXBOOK_PASSWORD=twoje_haslo
   ```
5. Deploy. Otrzymasz link do dashboardu.

### Opcjonalnie: z-score dla instytucji (COT)
- Ten projekt ma prosty parser tygodniowego pliku CFTC `FinFutWk.txt` i pokazuje **net** dla Leveraged Funds (proxy).
- Aby mieć **InstitutionalScore** (z-score zmiany tydzień/tydzień), dodaj w sekrecie adres CSV z historią, np.:
  ```
  COT_HISTORY_URL=https://raw.githubusercontent.com/<user>/<repo>/main/cot_history.csv
  ```
  Struktura CSV (przykład):
  ```
  date,symbol,lev_funds_net
  2025-06-06,EURUSD,12345
  2025-06-13,EURUSD,11890
  ...
  ```
  Wtedy aplikacja policzy z-score delty i pokaże `InstitutionalScore` w skali [-100,100].

## Lokalnie (opcjonalnie)
1. Zainstaluj zależności: `pip install -r requirements.txt`
2. Skopiuj `.streamlit/secrets.toml.sample` do `.streamlit/secrets.toml` i uzupełnij dane Myfxbook.
3. `streamlit run streamlit_app.py`

## Uwaga
- Myfxbook Community Outlook odświeża się zwykle co ~5 min.
- COT publikowany jest w piątki (stan z wtorku, opóźnienie ~3 dni).
- GER40 nie ma COT w CFTC — dla niego InstitutionalScore pozostanie 0 (proxy można dodać później).
