import os
import sys
import types
import unittest
from datetime import date, datetime, timedelta, timezone
from unittest.mock import patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Stub pytz using stdlib zoneinfo/fixed IST offset if pytz isn't installed in sandbox Python
if "pytz" not in sys.modules:
    pytz_stub = types.ModuleType("pytz")
    IST_TZ = timezone(timedelta(hours=5, minutes=30), name="Asia/Kolkata")
    pytz_stub.timezone = lambda name: IST_TZ
    sys.modules["pytz"] = pytz_stub

import core.holidays as h


class HolidaysFixesTest(unittest.TestCase):
    def setUp(self):
        with h._STATE_LOCK:
            h._HOLIDAYS_MEMORY = None
            h._NSE_HOLIDAY_COOLDOWN_UNTIL = 0.0
            h._HOLIDAY_REFRESH_INFLIGHT = False
            h._NSEI_CACHE.clear()

    def test_stale_disk_cache_preserves_sync_date_and_retries_after_cooldown(self):
        stale_date = datetime.now(h.IST).date() - timedelta(days=10)
        stale_set = {date(stale_date.year, 1, 26), date(stale_date.year, 8, 15)}
        live_set = {date(stale_date.year, 1, 26), date(stale_date.year, 11, 24)}

        mono_now = [1000.0]

        with (
            patch.object(h.pytime, "monotonic", side_effect=lambda: mono_now[0]),
            patch.object(h, "_load_disk_holidays", return_value=(stale_date, stale_set)) as mock_load,
            patch.object(h, "_save_disk_holidays") as mock_save,
            patch.object(h, "fetch_nse_holidays", side_effect=[RuntimeError("NSE 403"), live_set]) as mock_fetch,
        ):
            # 1. Stale disk cache + NSE failure -> returns stale disk_set, preserves stale_date
            res1 = h.get_holidays()
            self.assertEqual(res1, stale_set)
            self.assertEqual(mock_fetch.call_count, 1)
            self.assertEqual(h._HOLIDAYS_MEMORY[0], stale_date)

            # Mutating returned set must not affect internal cache
            res1.add(date(2099, 1, 1))
            self.assertNotIn(date(2099, 1, 1), h._HOLIDAYS_MEMORY[1])

            # 2. Within 15-min cooldown -> does NOT retry fetch or reload disk
            mono_now[0] += 300.0
            res2 = h.get_holidays()
            self.assertEqual(res2, stale_set)
            self.assertEqual(mock_fetch.call_count, 1)
            self.assertEqual(mock_load.call_count, 1)

            # 3. After 15-min cooldown expires -> retries fetch and succeeds!
            mono_now[0] += 700.0
            res3 = h.get_holidays()
            self.assertIn(date(stale_date.year, 11, 24), res3)
            self.assertEqual(mock_fetch.call_count, 2)
            self.assertEqual(mock_save.call_count, 1)
            self.assertEqual(h._HOLIDAYS_MEMORY[0], datetime.now(h.IST).date())

    def test_force_refresh_failure_falls_back_to_disk_cache(self):
        today = datetime.now(h.IST).date()
        disk_set = {date(today.year, 1, 26), date(today.year, 11, 10)}
        with (
            patch.object(h, "_load_disk_holidays", return_value=(today, disk_set)),
            patch.object(h, "fetch_nse_holidays", side_effect=RuntimeError("timeout")),
        ):
            res = h.get_holidays(force_refresh=True)
            self.assertEqual(res, disk_set)

    def test_previous_trading_day_rescues_1d_lag_via_hourly(self):
        today = datetime.now(h.IST).date()
        with patch.object(h, "get_holidays", return_value=set()):
            # Compute calendar prev_day and day before it
            cal_prev = h.get_previous_trading_day(today, use_market_data=False)
            earlier = h.get_previous_trading_day(cal_prev, use_market_data=False)

            calls = []

            def fake_fetch(period="15d", interval="1d", timeout=3):
                calls.append(interval)
                if interval == "1d":
                    return {earlier}  # 1d missed cal_prev
                return {earlier, cal_prev}  # 1h proves cal_prev traded

            with patch.object(h, "_fetch_nsei_trading_dates", side_effect=fake_fetch):
                res1 = h.get_previous_trading_day(today, use_market_data=True)
                res2 = h.get_previous_trading_day(today, use_market_data=True)
                self.assertEqual(res1, cal_prev)
                self.assertEqual(res2, cal_prev)
                # Second call uses cached merged 1d dates without re-fetching 1h
                self.assertEqual(calls, ["1d", "1h"])

    def test_previous_trading_day_confirms_unscheduled_closure_via_hourly(self):
        today = datetime.now(h.IST).date()
        with patch.object(h, "get_holidays", return_value=set()):
            cal_prev = h.get_previous_trading_day(today, use_market_data=False)
            earlier = h.get_previous_trading_day(cal_prev, use_market_data=False)

            calls = []

            def fake_fetch(period="15d", interval="1d", timeout=3):
                calls.append(interval)
                if interval == "1d":
                    return {earlier}
                return {earlier, today}  # 1h confirms earlier traded and cal_prev had no session

            with patch.object(h, "_fetch_nsei_trading_dates", side_effect=fake_fetch):
                res1 = h.get_previous_trading_day(today, use_market_data=True)
                res2 = h.get_previous_trading_day(today, use_market_data=True)
                self.assertEqual(res1, earlier)
                self.assertEqual(res2, earlier)
                # 1h cache prevents repeated hourly fetches within TTL
                self.assertEqual(calls, ["1d", "1h"])

    def test_previous_trading_day_falls_back_to_cal_prev_when_hourly_unavailable(self):
        today = datetime.now(h.IST).date()
        with patch.object(h, "get_holidays", return_value=set()):
            cal_prev = h.get_previous_trading_day(today, use_market_data=False)
            earlier = h.get_previous_trading_day(cal_prev, use_market_data=False)

            def fake_fetch(period="15d", interval="1d", timeout=3):
                return {earlier} if interval == "1d" else None

            with patch.object(h, "_fetch_nsei_trading_dates", side_effect=fake_fetch):
                self.assertEqual(h.get_previous_trading_day(today, use_market_data=True), cal_prev)

    def test_previous_trading_day_special_weekend_session(self):
        today = datetime.now(h.IST).date()
        cal_prev = h.get_previous_trading_day(today, use_market_data=False)
        special_day = today - timedelta(days=1)
        if special_day <= cal_prev:
            # Find a ref_date where yesterday is a weekend/holiday
            test_monday = date(2026, 11, 23)
            with patch.object(h, "get_holidays", return_value=set()):
                # Override datetime.now in holidays module so ref_date == today_ist
                fake_now = datetime(2026, 11, 23, 10, 0, tzinfo=h.IST)
                with (
                    patch.object(h, "datetime", wraps=datetime) as mock_dt,
                    patch.object(h, "_fetch_nsei_trading_dates", return_value={date(2026, 11, 20), date(2026, 11, 22)}),
                ):
                    mock_dt.now.return_value = fake_now
                    self.assertEqual(h.get_previous_trading_day(test_monday, use_market_data=True), date(2026, 11, 22))
        else:
            with patch.object(h, "_fetch_nsei_trading_dates", return_value={cal_prev, special_day}):
                self.assertEqual(h.get_previous_trading_day(today, use_market_data=True), special_day)

    def test_nsei_cache_invalidates_across_midnight_ist(self):
        day1 = date(2026, 11, 25)
        day2 = date(2026, 11, 26)
        with (
            patch.object(h, "_fetch_nsei_trading_dates", side_effect=[{date(2026, 11, 23)}, {date(2026, 11, 23), day1}]) as mock_fetch,
        ):
            r1 = h._get_recent_nsei_trading_dates(day1, interval="1d")
            r2 = h._get_recent_nsei_trading_dates(day2, interval="1d")
            self.assertEqual(r1, {date(2026, 11, 23)})
            self.assertEqual(r2, {date(2026, 11, 23), day1})
            self.assertEqual(mock_fetch.call_count, 2)

    def test_background_holiday_refresh_returns_immediately_without_blocking(self):
        import threading
        import time as real_time

        today = datetime.now(h.IST).date()
        started = threading.Event()
        release = threading.Event()
        live_set = {date(today.year, 1, 26), date(today.year, 11, 24)}

        def slow_nse():
            started.set()
            release.wait(timeout=2.0)
            return live_set

        with (
            patch.object(h, "_load_disk_holidays", return_value=None),
            patch.object(h, "_save_disk_holidays") as mock_save,
            patch.object(h, "fetch_nse_holidays", side_effect=slow_nse),
        ):
            t0 = real_time.perf_counter()
            immediate = h.get_holidays(background_refresh=True)
            elapsed = real_time.perf_counter() - t0
            self.assertLess(elapsed, 0.2)
            self.assertEqual(immediate, set(h.HOLIDAYS))
            self.assertTrue(started.wait(timeout=1.0))
            release.set()
            # Wait briefly for worker thread to commit memory & clear inflight flag
            deadline = real_time.monotonic() + 1.0
            while h._HOLIDAY_REFRESH_INFLIGHT and real_time.monotonic() < deadline:
                real_time.sleep(0.01)
            refreshed = h.get_holidays()
            self.assertEqual(refreshed, live_set | {d for d in h.HOLIDAYS if d.year != today.year})
            mock_save.assert_called_once()

    def test_fetch_nsei_dates_direct_parses_chart_payload_and_skips_null_close(self):
        ep1 = int(datetime(2026, 11, 24, 9, 15, tzinfo=h.IST).timestamp())
        ep2 = int(datetime(2026, 11, 25, 9, 15, tzinfo=h.IST).timestamp())
        ep_null = int(datetime(2026, 11, 26, 9, 15, tzinfo=h.IST).timestamp())
        chart_json = {
            "chart": {
                "result": [
                    {
                        "timestamp": [ep1, ep2, ep_null],
                        "indicators": {"quote": [{"close": [24100.0, 24250.0, None]}]},
                    }
                ]
            }
        }

        class FakeResp:
            status_code = 200
            content = None

            def json(self):
                return chart_json

        class FakeSess:
            def get(self, url, params=None, timeout=None):
                return FakeResp()

            def close(self):
                pass

        cffi_mod = types.ModuleType("curl_cffi")
        cffi_req_mod = types.ModuleType("curl_cffi.requests")
        cffi_req_mod.Session = lambda *a, **kw: FakeSess()
        cffi_mod.requests = cffi_req_mod
        h._NSEI_SESSION = None

        with patch.dict(sys.modules, {"curl_cffi": cffi_mod, "curl_cffi.requests": cffi_req_mod}):
            dates = h._fetch_nsei_dates_direct(period="15d", interval="1d")
        self.assertEqual(dates, {date(2026, 11, 24), date(2026, 11, 25)})
        h._NSEI_SESSION = None


if __name__ == "__main__":
    unittest.main(verbosity=2)
