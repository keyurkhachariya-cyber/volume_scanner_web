# F&O Relative Volume Scanner (Web)

Compares **today's cumulative volume from 09:15 IST till now** with the **previous trading day's cumulative volume till the same clock time**, combined with **NSE F&O Open Interest buildup** and intraday momentum quality metrics.

Built with Streamlit + `curl_cffi` HTTP/2 Yahoo Chart API (with `yfinance` fallback) + `orjson` + NumPy baseline caching.

## Features
- Official NSE F&O equity universe (~218 stocks) with one-click live NSE sync
- 1-minute (or 5-minute) bars via multiplexed HTTP/2 `AsyncSession` fetching & compact `.cache/` v2 NumPy baselines
- Intraday momentum metrics: **15m Vol Burst**, **From Open %**, **VWAP Dist %**, and **Range Pos %**
- Live NSE aggregate F&O Open Interest: **OI**, **OI Chg (Day)**, **OI Chg %**, and **OI Buildup** (`Long Buildup`, `Short Buildup`, `Short Covering`, `Long Unwinding`)
- **🟢 Bullish / 🔴 Bearish** row highlighting, pinned **Signal** & **Symbol** columns, and 1-click header filter pills (`Ratio > 1.0×`, `OI Chg > +10%`, `|% Chg| > 1.5%`)
- Client-side filtering (**Momentum Quality Filters**, **Min OI Chg %**, **Gap filter**, **Custom sorting**) without extra network scans
- Fragment-isolated auto-refresh during market hours (`09:15–15:30 IST`), CSV export, and live NSE holiday + `^NSEI` session detection

## Column & Signal Guide (How to Use)

| Column | Meaning | How to Use for Intraday Momentum |
| :--- | :--- | :--- |
| **Signal** | Bullish / Bearish combo highlight | **🟢 Bullish:** `Ratio > 1.0×`, `OI Chg > +10%`, `% Chg > +1.5%`<br>**🔴 Bearish:** `Ratio > 1.0×`, `OI Chg > +10%`, `% Chg < -1.5%` |
| **Ratio** | `Vol Today ÷ Vol Prev (same time)` | `≥ 1.5×` = strong activity vs yesterday; `> 2.5×` = unusual institutional volume |
| **15m Vol Burst** | `Trailing 15m active-bar vol ÷ Today avg` | `> 1.5×` = accelerating *right now*; `< 0.5×` = morning spike that has already cooled off |
| **% Chg** | Change from Previous Close | Full daily move including opening gap |
| **From Open %** | `(LTP - Day Open) / Day Open` | Confirms real intraday candle direction (helps avoid chasing gap-ups that are selling off) |
| **VWAP Dist %** | `(LTP - VWAP) / VWAP` | Bullish setups hold **above VWAP (`> 0%`)**; Bearish hold **below VWAP (`< 0%`)** |
| **Range Pos %** | `(LTP - Day Low) / (Day High - Day Low)` | `70–100%` = holding strong near Day High (Bullish); `0–30%` = weak near Day Low (Bearish) |
| **OI Chg (Day) / %** | Today's aggregate F&O OI change vs yesterday close | `> +10%` confirms heavy fresh positioning behind the price/volume move |
| **OI Buildup** | Price Δ vs Day OI Δ | Long Buildup (`+P, +OI`), Short Buildup (`-P, +OI`), Short Covering (`+P, -OI`), Long Unwinding (`-P, -OI`) |

### Quick Trading Workflow
1. Choose **Minimum Ratio** (default `1.5×`), **Minimum Volume**, and `1m` or `5m` bars in the sidebar, then click **Scan Now** (or enable **Auto-refresh** during market hours `09:15–15:30 IST`).
2. Use the **🟢 Bullish / 🔴 Bearish** header pills or **Sort results by** (`Ratio`, `Signal (Bullish / Bearish first)`, `15m Vol Burst`, `OI Chg %`, etc.) to isolate setups in memory.
3. Turn on **Momentum Quality Filters** in the sidebar to filter out stocks trading against **VWAP**, red-candle gap-ups (**From Open %**), weak closes (**Range Pos %**), or stale morning spikes (**15m Vol Burst**).

## Quick Start

```bash
# 1. Go to the project folder
cd volume_scanner_web

# 2. Create and activate a virtual environment (recommended)
python -m venv venv
source venv/bin/activate          # Linux / macOS
# or
venv\Scripts\activate             # Windows

# 3. Install dependencies
pip install -r requirements.txt

# 4. Run the app
streamlit run app.py
```

The app will open in your browser (usually `http://localhost:8501`).

### Running Tests

```bash
python -m unittest discover -s tests -v
```

## Project Structure
- `app.py` — Streamlit UI layout, sidebar controls, and auto-refresh fragment
- `table_config.py` — Column schemas, labels, sort options, pinned columns, and column guide
- `screener_filters.py` — Pure filtering, sorting, row styling, and auto-refresh schedule logic
- `scanner.py` — Intraday bar summarization (`VWAP`, `Vol_Burst_15m`) and scan orchestration
- `baseline_cache.py` — Vectorized v2 NumPy previous-day volume matrix and `.cache/` persistence
- `yahoo_client.py` — Async HTTP/2 Yahoo Chart API client and `yfinance` thread-pool fallback
- `oi.py` — Rate-limited NSE aggregate F&O Open Interest fetcher and Signal/Buildup classifiers
- `holidays.py` — Live NSE holiday sync (`.cache/nse_holidays.json`) and empirical `^NSEI` session detection
- `universe.py` — NSE F&O underlying symbol list with live sync and built-in fallback
- `utils.py` — Shared IST timezone, `orjson` JSON decoding, date parsing, and NSE session helpers
- `tests/` — Unit and Streamlit `AppTest` UI tests

## Notes & Limitations
- Yahoo Finance 1-minute data covers roughly the last 7 days and can occasionally contain placeholder `NaN` bars near market close; `15m Vol Burst` evaluates trailing active bars to stay robust across minor feed gaps.
- Previous trading day resolution combines NSE's live official holiday calendar with empirical `^NSEI` trading-day detection to handle unscheduled closures and special weekend sessions.
- NSE F&O OI fetches at most once every 45 seconds when the market is open (every 5 minutes when closed) with automatic cooldown backoff on HTTP `403`/`429`.
- This tool is for research and educational use only — not investment advice.
