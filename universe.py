"""
NSE F&O Equity Underlyings (official NSE F&O equity list, Yahoo Finance .NS format).
Includes optional live sync from NSE India with local cache and built-in fallback.
"""

from datetime import datetime
import json
from pathlib import Path
from typing import List, Optional, Tuple

from holidays import IST
from utils import fetch_nse_json

CACHE_FILE = Path(__file__).parent / ".cache" / "fno_universe.json"

FNO_STOCKS = [
    "360ONE.NS", "ABB.NS", "ABCAPITAL.NS", "ACUTAAS.NS", "ADANIENSOL.NS",
    "ADANIENT.NS", "ADANIGREEN.NS", "ADANIPORTS.NS", "ADANIPOWER.NS", "ALKEM.NS",
    "AMBER.NS", "AMBUJACEM.NS", "ANANDRATHI.NS", "ANGELONE.NS", "APARINDS.NS",
    "APLAPOLLO.NS", "APOLLOHOSP.NS", "ASHOKLEY.NS", "ASIANPAINT.NS", "ASTRAL.NS",
    "ATHERENERG.NS", "AUBANK.NS", "AUROPHARMA.NS", "AXISBANK.NS", "BAJAJ-AUTO.NS",
    "BAJAJFINSV.NS", "BAJFINANCE.NS", "BANDHANBNK.NS", "BANKBARODA.NS", "BANKINDIA.NS",
    "BDL.NS", "BEL.NS", "BHARATFORG.NS", "BHARTIARTL.NS", "BHEL.NS",
    "BIOCON.NS", "BLUESTARCO.NS", "BOSCHLTD.NS", "BPCL.NS", "BRITANNIA.NS",
    "BSE.NS", "CAMS.NS", "CANBK.NS", "CDSL.NS", "CGPOWER.NS",
    "CHOLAFIN.NS", "CIPLA.NS", "COALINDIA.NS", "COCHINSHIP.NS", "COFORGE.NS",
    "COLPAL.NS", "CONCOR.NS", "COROMANDEL.NS", "CROMPTON.NS", "CUB.NS",
    "CUMMINSIND.NS", "DABUR.NS", "DATAPATTNS.NS", "DELHIVERY.NS", "DIVISLAB.NS",
    "DIXON.NS", "DLF.NS", "DMART.NS", "DRREDDY.NS", "EICHERMOT.NS",
    "ENRIN.NS", "ETERNAL.NS", "FEDERALBNK.NS", "FORCEMOT.NS", "FORTIS.NS",
    "GAIL.NS", "GLENMARK.NS", "GMRAIRPORT.NS", "GODFRYPHLP.NS", "GODREJCP.NS",
    "GODREJPROP.NS", "GRASIM.NS", "GVT&D.NS", "HAL.NS", "HAVELLS.NS",
    "HCLTECH.NS", "HDFCAMC.NS", "HDFCBANK.NS", "HDFCLIFE.NS", "HEROMOTOCO.NS",
    "HINDALCO.NS", "HINDPETRO.NS", "HINDUNILVR.NS", "HINDZINC.NS", "HYUNDAI.NS",
    "ICICIBANK.NS", "ICICIGI.NS", "ICICILIFE.NS", "IDEA.NS", "IDFCFIRSTB.NS",
    "INDHOTEL.NS", "INDIANB.NS", "INDIGO.NS", "INDUSINDBK.NS", "INDUSTOWER.NS",
    "INFY.NS", "INOXWIND.NS", "IOC.NS", "IRFC.NS", "ITC.NS",
    "JINDALSTEL.NS", "JIOFIN.NS", "JSWENERGY.NS", "JSWSTEEL.NS", "JUBLFOOD.NS",
    "KALYANKJIL.NS", "KARURVYSYA.NS", "KAYNES.NS", "KEI.NS", "KFINTECH.NS",
    "KOTAKBANK.NS", "KPITTECH.NS", "LAURUSLABS.NS", "LICHSGFIN.NS", "LICI.NS",
    "LODHA.NS", "LT.NS", "LTF.NS", "LTM.NS", "LUPIN.NS",
    "M&M.NS", "MAHABANK.NS", "MANAPPURAM.NS", "MANKIND.NS", "MARICO.NS",
    "MARUTI.NS", "MAXHEALTH.NS", "MAZDOCK.NS", "MCX.NS", "MFSL.NS",
    "MOTHERSON.NS", "MOTILALOFS.NS", "MPHASIS.NS", "MUTHOOTFIN.NS", "NAM-INDIA.NS",
    "NATIONALUM.NS", "NAUKRI.NS", "NAVINFLUOR.NS", "NBCC.NS", "NESTLEIND.NS",
    "NHPC.NS", "NMDC.NS", "NTPC.NS", "NYKAA.NS", "OBEROIRLTY.NS",
    "OFSS.NS", "OIL.NS", "ONGC.NS", "PAGEIND.NS", "PATANJALI.NS",
    "PAYTM.NS", "PERSISTENT.NS", "PETRONET.NS", "PFC.NS", "PGEL.NS",
    "PHOENIXLTD.NS", "PIDILITIND.NS", "PIIND.NS", "PNB.NS", "PNBHOUSING.NS",
    "POLICYBZR.NS", "POLYCAB.NS", "POWERGRID.NS", "POWERINDIA.NS", "PREMIERENE.NS",
    "PRESTIGE.NS", "RADICO.NS", "RBLBANK.NS", "RECLTD.NS", "RELIANCE.NS",
    "RVNL.NS", "SAGILITY.NS", "SAIL.NS", "SBICARD.NS", "SBILIFE.NS",
    "SBIN.NS", "SHREECEM.NS", "SHRIRAMFIN.NS", "SIEMENS.NS", "SOLARINDS.NS",
    "SONACOMS.NS", "SRF.NS", "SUNPHARMA.NS", "SUPREMEIND.NS", "SUZLON.NS",
    "SWIGGY.NS", "SYRMA.NS", "TATACONSUM.NS", "TATAELXSI.NS", "TATAPOWER.NS",
    "TATASTEEL.NS", "TCS.NS", "TECHM.NS", "TIINDIA.NS", "TITAN.NS",
    "TMPV.NS", "TORNTPHARM.NS", "TRENT.NS", "TVSMOTOR.NS", "UJJIVANSFB.NS",
    "ULTRACEMCO.NS", "UNIONBANK.NS", "UNITDSPR.NS", "UNOMINDA.NS", "UPL.NS",
    "VBL.NS", "VEDL.NS", "VMM.NS", "VOLTAS.NS", "WAAREEENER.NS",
    "WIPRO.NS", "YESBANK.NS", "ZYDUSLIFE.NS",
]

# Clean unique list
FNO_STOCKS = sorted(list(set(FNO_STOCKS)))


def fetch_nse_fno_universe(timeout: int = 10) -> List[str]:
    """Fetch live NSE F&O equity underlyings directly from NSE India."""
    payload = fetch_nse_json(
        warmup_url="https://www.nseindia.com/market-data/equity-derivatives-watch",
        api_url="https://www.nseindia.com/api/underlying-information",
        timeout=timeout,
    )
    items = (payload or {}).get("data", {}).get("UnderlyingList", [])

    symbols = sorted({f"{item['symbol']}.NS" for item in items if item.get("symbol")})
    if not symbols:
        raise ValueError("Empty UnderlyingList returned from NSE")
    return symbols


def _load_cached_universe() -> Optional[Tuple[List[str], str]]:
    if not CACHE_FILE.exists():
        return None
    try:
        payload = json.loads(CACHE_FILE.read_text(encoding="utf-8"))
        symbols = payload.get("symbols") or []
        synced_date = payload.get("date", "cached")
        if symbols:
            return sorted(set(symbols)), synced_date
    except Exception:
        pass
    return None


def get_fno_universe(force_refresh: bool = False) -> Tuple[List[str], str]:
    """
    Return (symbols, source_label).
    Uses cached live NSE sync if available; falls back cleanly to built-in FNO_STOCKS.
    """
    today_str = datetime.now(IST).date().isoformat()

    if not force_refresh:
        cached = _load_cached_universe()
        if cached is not None:
            symbols, synced_date = cached
            return symbols, f"NSE Live (synced {synced_date})"

    if force_refresh:
        try:
            symbols = fetch_nse_fno_universe()
            CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
            CACHE_FILE.write_text(
                json.dumps({"date": today_str, "symbols": symbols}),
                encoding="utf-8",
            )
            return symbols, f"NSE Live (synced {today_str})"
        except Exception:
            cached = _load_cached_universe()
            if cached is not None:
                symbols, synced_date = cached
                return symbols, f"NSE Live (cached {synced_date}; refresh failed)"
            return FNO_STOCKS, "Built-in list (NSE sync failed)"

    return FNO_STOCKS, "Built-in NSE F&O list"
