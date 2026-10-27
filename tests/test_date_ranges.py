"""
Tests for date ranges (date_start/date_end) across every range endpoint.

The fixture spans New Year 2024 → 2025: 2024 is a leap year, so its last day
is DOY 366, followed by 2025 DOY 1 and 2. AbsolTEC and TEC-suite parquet files
are written to a temporary data root and read through the real HTTP routes.
"""

import datetime

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from app.db.columns import ABSOLTEC_COLS, I_V, TEC_COLS_RAW, UT
from app.day_range import DayRange
from app.main import app

client = TestClient(app)

UTS = [0.0, 0.5]
# AbsolTEC I_v per (station, year, doy); 0.0 is a missing sample.
ABSOLTEC = {
    ("aaaa", 2024, 366): [10.0, 0.0],
    ("aaaa", 2025, 1): [12.0, 20.0],
    ("aaaa", 2025, 2): [14.0, 22.0],
    ("bbbb", 2025, 1): [30.0, 31.0],
}
# TEC-suite satellites per (station, year, doy).
TECSUITE = {
    ("tttt", 2024, 366): ["G01", "R05"],
    ("tttt", 2025, 1): ["G01"],
    ("uuuu", 2025, 1): ["E11"],
}
NEW_YEAR = {"date_start": "2024-12-31", "date_end": "2025-01-01"}


def _write_absoltec(root, station, year, doy, tec):
    folder = root / str(year) / f"{doy:03d}" / f"{station}{doy:03d}0"
    folder.mkdir(parents=True, exist_ok=True)
    rows = {col: [1.0] * len(tec) for col in ABSOLTEC_COLS}
    rows[UT] = UTS
    rows[I_V] = tec
    pd.DataFrame(rows).to_parquet(folder / f"{station}_{doy:03d}_{year}.parquet")


def _write_tecsuite(root, station, year, doy, satellite):
    folder = root / str(year) / f"{doy:03d}" / f"{station}{doy:03d}0"
    folder.mkdir(parents=True, exist_ok=True)
    rows = dict(zip(TEC_COLS_RAW, [[0, 1], [0.0, 1 / 120], [30.0, 31.0], [90.0, 91.0],
                                   [5.0, 6.0], [7.0, -1.0], [0, 0]]))
    pd.DataFrame(rows).to_parquet(folder / f"{station}_{satellite}_{doy:03d}_{str(year)[-2:]}.parquet")


@pytest.fixture
def data_root(tmp_path):
    for (station, year, doy), tec in ABSOLTEC.items():
        _write_absoltec(tmp_path, station, year, doy, tec)
    for (station, year, doy), satellites in TECSUITE.items():
        for satellite in satellites:
            _write_tecsuite(tmp_path, station, year, doy, satellite)
    return str(tmp_path)


def _get(path, data_root, params):
    response = client.get(path, params={"data_root": data_root, **params} if isinstance(params, dict)
                          else [("data_root", data_root), *params])
    assert response.status_code == 200, response.text
    return response.json()


# ── DayRange ──────────────────────────────────────────────────────────────────

class TestDayRange:

    def test_days_cross_new_year_in_a_leap_year(self):
        r = DayRange(datetime.date(2024, 12, 30), datetime.date(2025, 1, 2))
        assert r.days() == [(2024, 365), (2024, 366), (2025, 1), (2025, 2)]
        assert r.n_days == 4

    def test_concat_hours_continue_across_new_year(self):
        r = DayRange(datetime.date(2024, 12, 31), datetime.date(2025, 1, 1))
        assert r.concat_hours(2024, 366, 0.5) == 0.5
        assert r.concat_hours(2025, 1, 0.5) == 24.5

    def test_doy_form_matches_original_concat_formula(self):
        r = DayRange.from_doys(2025, 100, 104)
        assert r.concat_hours(2025, 102, 3.0) == (102 - 100) * 24 + 3.0
        assert (r.year, r.doy_start, r.doy_end) == (2025, 100, 104)

    def test_missing_leap_day_is_rejected(self):
        with pytest.raises(ValueError):
            DayRange.from_doys(2025, 1, 366)

    def test_label(self):
        assert DayRange.from_doys(2025, 5, 9).label() == "2025_005_009"
        assert DayRange(datetime.date(2024, 12, 31), datetime.date(2025, 1, 1)).label() == "2024-12-31_2025-01-01"


# ── Parameter validation ─────────────────────────────────────────────────────

class TestRangeValidation:

    @pytest.mark.parametrize("params, message", [
        ({"year": 2025, "doy_start": 1, "doy_end": 2, **NEW_YEAR}, "not both"),
        ({"date_start": "2025-01-01"}, "together"),
        ({"date_start": "2025-01-02", "date_end": "2025-01-01"}, "date_start must be"),
        ({"year": 2025, "doy_start": 5, "doy_end": 1}, "doy_start must be"),
        ({"year": 2025, "doy_start": 360, "doy_end": 366}, "no day 366"),
        ({"date_start": "2024-01-01", "date_end": "2025-12-31"}, "the limit is"),
        ({}, "Provide year"),
    ])
    def test_bad_ranges_are_422(self, params, message):
        response = client.get("/absoltec/statistics", params={"station": "aaaa", **params})
        assert response.status_code == 422
        assert message in response.text

    def test_malformed_date_is_422(self):
        response = client.get("/absoltec/statistics", params={
            "station": "aaaa", "date_start": "31.12.2024", "date_end": "2025-01-01",
        })
        assert response.status_code == 422


# ── AbsolTEC and CB ───────────────────────────────────────────────────────────

class TestAbsoltecAndCbRanges:

    def test_raw_range_across_new_year(self, data_root):
        rows = _get("/absoltec/raw/range", data_root, {"station": "aaaa", **NEW_YEAR})
        assert [(r["year"], r["doy"], r["concat_ut"], r["tec"]) for r in rows] == [
            (2024, 366, 0.0, 10.0), (2024, 366, 0.5, None),
            (2025, 1, 24.0, 12.0), (2025, 1, 24.5, 20.0),
        ]

    def test_doy_form_still_works(self, data_root):
        rows = _get("/absoltec/raw/range", data_root, {"station": "aaaa", "year": 2025, "doy_start": 1, "doy_end": 2})
        assert [(r["doy"], r["concat_ut"]) for r in rows] == [(1, 0.0), (1, 0.5), (2, 24.0), (2, 24.5)]

    def test_statistics_across_new_year(self, data_root):
        body = _get("/absoltec/statistics", data_root, {"station": "aaaa", **NEW_YEAR})
        assert (body["date_start"], body["date_end"]) == ("2024-12-31", "2025-01-01")
        assert (body["year"], body["doy_start"], body["doy_end"]) == (2024, 366, 1)
        assert body["total_days"] == 2
        points = {p["ut"]: p for p in body["points"]}
        assert points[0.0]["n"] == 2 and points[0.0]["mean_tec"] == pytest.approx(11.0)
        assert points[0.5]["n"] == 1 and points[0.5]["student_ci"] is None

    def test_per_station_day_across_new_year(self, data_root):
        body = _get("/absoltec/statistics/per-station-day", data_root,
                    [("stations", "aaaa"), ("stations", "bbbb"), *NEW_YEAR.items()])
        assert [(d["year"], d["doy"], d["stations_found"]) for d in body] == [
            (2024, 366, ["aaaa"]), (2025, 1, ["aaaa", "bbbb"]),
        ]

    def test_cb_range_endpoints(self, data_root):
        rows = _get("/cb/raw/range", data_root, {"station": "aaaa", **NEW_YEAR})
        assert [r["cb"] is None for r in rows] == [False, True, False, False]
        stats = _get("/cb/statistics", data_root, {"station": "aaaa", **NEW_YEAR})
        assert stats["total_days"] == 2 and stats["date_end"] == "2025-01-01"
        per_day = _get("/cb/statistics/per-station-day", data_root, [("stations", "aaaa"), *NEW_YEAR.items()])
        assert [(d["year"], d["doy"]) for d in per_day] == [(2024, 366), (2025, 1)]

    def test_csv_download_name_uses_dates(self, data_root):
        response = client.get("/absoltec/raw/range", params={
            "station": "aaaa", "format": "csv", "data_root": data_root, **NEW_YEAR,
        })
        assert response.status_code == 200
        assert "absoltec_raw_range_2024-12-31_2025-01-01_aaaa.csv" in response.headers["content-disposition"]


# ── TEC-suite and propagation ────────────────────────────────────────────────

class TestTecAndPropagationRanges:

    def test_tec_range_across_new_year(self, data_root):
        rows = _get("/tec/raw/range", data_root, {"station": "tttt", **NEW_YEAR})
        assert sorted({(r["year"], r["doy"], r["satellite"]) for r in rows}) == [
            (2024, 366, "G01"), (2024, 366, "R05"), (2025, 1, "G01"),
        ]
        g01_next_year = [r for r in rows if r["satellite"] == "G01" and r["year"] == 2025]
        assert g01_next_year[0]["concat_hour"] == pytest.approx(24.0)

    def test_tec_satellite_filter_is_case_insensitive(self, data_root):
        rows = _get("/tec/raw/range", data_root, {"station": "tttt", "satellite": "r05", **NEW_YEAR})
        assert {r["satellite"] for r in rows} == {"R05"}

    def test_tec_multiple_satellites(self, data_root):
        rows = _get("/tec/raw/range", data_root,
                    [("station", "tttt"), ("satellites", "G01"), ("satellites", "R05"), *NEW_YEAR.items()])
        assert {r["satellite"] for r in rows} == {"G01", "R05"}

    def test_propagation_absoltec_raw_range(self, data_root):
        rows = _get("/propagation/absoltec/raw/range", data_root,
                    {"station": "aaaa", "signal_band": "GPS_L1", **NEW_YEAR})
        assert len(rows) == 4
        assert rows[0]["b_k"] > 0 and rows[0]["signal_band"] == "GPS_L1"
        assert rows[1]["tec"] is None and rows[1]["b_k"] is None
        assert rows[2]["concat_ut"] == 24.0

    def test_propagation_tec_raw_range_uses_selected_observable(self, data_root):
        rows = _get("/propagation/tec/raw/range", data_root, {
            "station": "tttt", "satellite": "G01", "observable": "tec_c1p2", "f_hz": 1.5e9, **NEW_YEAR,
        })
        assert len(rows) == 4
        assert all(r["observable"] == "tec_c1p2" and "tec_l1l2" not in r and "tec_c1p2" not in r for r in rows)
        assert [r["tec"] for r in rows[:2]] == [7.0, -1.0]
        # A negative relative TEC has no physical B_k.
        assert rows[0]["b_k"] > 0 and rows[1]["b_k"] is None

    def test_propagation_statistics_across_new_year(self, data_root):
        body = _get("/propagation/absoltec/statistics", data_root,
                    {"station": "aaaa", "signal_band": "GPS_L1", **NEW_YEAR})
        assert body["total_days"] == 2 and body["date_start"] == "2024-12-31"

    def test_propagation_range_needs_a_station(self, data_root):
        response = client.get("/propagation/absoltec/raw/range", params={"signal_band": "GPS_L1", **NEW_YEAR})
        assert response.status_code == 422


# ── Availability over a range ────────────────────────────────────────────────

class TestAvailabilityRanges:

    def test_stations_available_range(self, data_root):
        body = _get("/stations/available/range", data_root,
                    {"date_start": "2024-12-31", "date_end": "2025-01-02"})
        assert body["days_in_range"] == 3
        assert body["absoltec_stations"] == ["aaaa", "bbbb"]
        assert body["tec_stations"] == ["tttt", "uuuu"]
        days = {d["station"]: d for d in body["station_days"]}
        assert (days["aaaa"]["absoltec_days"], days["aaaa"]["tec_days"]) == (3, 0)
        assert (days["tttt"]["absoltec_days"], days["tttt"]["tec_days"]) == (0, 2)

    def test_single_source_leaves_other_counts_null(self, data_root):
        body = _get("/stations/available/range", data_root, {"source": "absoltec", **NEW_YEAR})
        assert "tec_stations" not in body or body["tec_stations"] is None
        assert all(d["tec_days"] is None for d in body["station_days"])

    def test_satellites_range(self, data_root):
        body = _get("/tec/satellites/range", data_root, {"station": "tttt", **NEW_YEAR})
        assert body["satellites"] == ["G01", "R05"]
        assert {s["satellite"]: s["days"] for s in body["satellite_days"]} == {"G01": 2, "R05": 1}

    def test_station_map_range(self, data_root):
        body = _get("/stations/map/range", data_root, NEW_YEAR)
        assert {s["station"]: s["days"] for s in body["stations"]} == {"tttt": 2, "uuuu": 1}

    def test_availability_csv(self, data_root):
        response = client.get("/stations/available/range", params={"format": "csv", "data_root": data_root, **NEW_YEAR})
        assert response.status_code == 200
        header, *rows = response.text.lstrip("﻿").splitlines()
        assert header.endswith("station,absoltec_days,tec_days")
        counts = {
            cols[-3]: (float(cols[-2]), float(cols[-1]))
            for cols in (row.split(",") for row in rows)
            if cols[4] == "station_days"
        }
        assert counts == {"aaaa": (2, 0), "bbbb": (1, 0), "tttt": (0, 2), "uuuu": (0, 1)}
