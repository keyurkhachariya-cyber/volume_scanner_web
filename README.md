# F&O Relative Volume Scanner (Web)

Compares **today's cumulative volume from 09:15 IST till now** with the **previous trading day's cumulative volume till the same clock time**.

Built with Streamlit + yfinance.

## Features
- Complete official NSE F&O equity universe (218 stocks, with one-click live NSE sync)
- 1-minute (or 5-minute) bars
- Customizable minimum ratio (default 1.5×)
- Minimum volume filter
- Parallel batched fetching (`yf.download` in multi-ticker chunks)
- Previous trading day caching (in-memory & disk cache in `.cache/`, cuts API calls by over 99%)
- Auto-refresh with seamless `@st.fragment` background updates (no full-page reload)
- CSV download
- IST-aware + basic holiday handling

## Quick Start

```bash
# 1. Go to the project folder
cd volume_scanner_web

# 2. Create virtual environment (recommended)
python -m venv venv
source venv/bin/activate          # Linux / macOS
# or
venv\Scripts\activate             # Windows

# 3. Install dependencies
pip install -r requirements.txt

# 4. Run the app
streamlit run app.py
```

The app will open in your browser (usually http://localhost:8501).

## How to use
1. Set **Minimum Ratio** (e.g. 1.5, 2.0, 3.0) in the sidebar.
2. Optionally set a minimum volume filter.
3. Choose 1m or 5m interval.
4. Click **Scan Now**.
5. Results are sorted by Ratio (highest first).

## Notes & Limitations
- yfinance 1-minute data is only available for roughly the last 7 days and can have occasional gaps.
- Batched fetching and caching reduces scan time to ~3–8 seconds across 150+ stocks and prevents rate-limiting.
- Previous trading day logic skips weekends + a static list of major holidays. Update `holidays.py` for better accuracy.
- This tool is for research/educational use only.

## Expanding the universe
Edit `universe.py` and add more `.NS` symbols from the official NSE F&O list.

## Next possible upgrades
- Switch data source to Upstox / Kotak historical + WebSocket for live cumulative volume
- Add sector filter / watchlist save
- Deploy to Streamlit Community Cloud
