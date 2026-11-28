# F&O Relative Volume Scanner (Web)

Compares **today's cumulative volume from 09:15 IST till now** with the **previous trading day's cumulative volume till the same clock time**, combined with **NSE F&O Open Interest buildup** and intraday momentum quality metrics.

Built with Streamlit + pluggable market data providers (**Yahoo Finance** HTTP/2 + `yfinance` fallback, **Upstox API v3** REST + live `MarketDataStreamerV3` WebSocket, or **5paisa Xstream** V2 REST + live `MarketFeedV3` WebSocket) + `orjson` + NumPy baseline caching.

## Features
- Pluggable data sources in the sidebar: **Yahoo Finance** (zero login), **Upstox v3** (OAuth daily token + **Live WSS**), or **5paisa Xstream** (TOTP/OAuth daily token + **Live WSS**)
- Official NSE F&O equity universe (~218 stocks) with one-click live NSE sync and cached Upstox `NSE.json.gz` / 5paisa ScripMaster equity key mappings
- 1-minute (or 5-minute) bars via multiplexed HTTP/2 `AsyncSession` fetching & isolated per-provider `.cache/` v2 NumPy baselines
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

### Using Upstox v3 or 5paisa Xstream (Optional Live WSS)
- **Upstox v3:** Switch **Data Source** to **Upstox v3**, paste today's **Access Token** (or use **Daily Login Helper (OAuth Code)** / `st.secrets["upstox"]`), and leave **Stream Live WSS (`MarketDataStreamerV3`)** checked.
- **5paisa Xstream:** Switch **Data Source** to **5paisa Xstream**, paste today's JWT **Access Token** + **Client Code** (or use **Daily Login Helper (TOTP / OAuth)** for one-click TOTP+PIN headless login / `st.secrets["fivepaisa"]`), and leave **Stream Live WSS (`MarketFeedV3`)** checked.

## Project Structure
- `app.py` — Streamlit web entrypoint (`streamlit run app.py`)
- `core/` — Core scanner engine (`scanner.py`, `baseline_cache.py`, `holidays.py`, `oi.py`, `universe.py`, `utils.py`)
- `ui/` — UI column schemas, filters, sorting, and row styling (`table_config.py`, `screener_filters.py`)
- `providers/` — Pluggable `MarketDataProvider` adapters organized by vendor:
  - `base.py` — `MarketDataProvider` protocol & shared type aliases
  - `common/` — Shared persistent HTTP/2 runtime (`http_runtime.py`) & live stream state (`stream_base.py`)
  - `yahoo/` — HTTP/2 Yahoo Chart client (`client.py`) & zero-login `YahooProvider` (`provider.py`)
  - `upstox/` — `UpstoxProvider` v3 REST/Batch Quote (`provider.py`), `MarketDataStreamerV3` (`stream.py`), & `NSE.json.gz` resolver (`instruments.py`)
  - `fivepaisa/` — `FivePaisaProvider` TOTP/OAuth & V2 REST (`provider.py`), `MarketFeedV3` (`stream.py`), & ScripMaster resolver (`instruments.py`)
- `docs/` — Deployment & cloud hosting guides (`ORACLE_CLOUD_SETUP.md`)
- `tests/` — Unit and Streamlit `AppTest` UI tests

## Notes & Limitations
- Yahoo Finance 1-minute data covers roughly the last 7 days and can occasionally contain placeholder `NaN` bars near market close; `15m Vol Burst` evaluates trailing active bars to stay robust across minor feed gaps.
- Previous trading day resolution combines NSE's live official holiday calendar with empirical `^NSEI` trading-day detection to handle unscheduled closures and special weekend sessions.
- NSE F&O OI fetches at most once every 45 seconds when the market is open (every 5 minutes when closed) with automatic cooldown backoff on HTTP `403`/`429`.
- This tool is for research and educational use only — not investment advice.
