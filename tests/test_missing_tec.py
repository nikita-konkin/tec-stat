"""
End-to-end tests for missing AbsolTEC samples and sample-variance statistics.

TayAbsTEC writes I_v = 0.000 for half-hours it could not solve. These tests
write small parquet files with such placeholders (and a negative value) into
a temporary data root, then run the real DuckDB queries, so the SQL filters
and aggregates are covered — not just the DataFrame post-processing.
"""

import math
import statistics

import pandas as pd
import pytest
from fastapi.testclient import TestClient
from scipy.stats import t as student_t

from app.db.columns import ABSOLTEC_COLS, I_V, UT
from app.main import app
from app.plotting.absoltec_plots import _smooth_valid
from app.services.absoltec import (
    compute_statistics,
    compute_statistics_per_station_day,
    get_raw_data,
    get_raw_data_range,
)
from app.services.cb import (
    _calculate_cb,
    compute_statistics_cb,
    compute_statistics_per_station_day_cb,
    get_raw_data_cb,
)
from app.services.propagation import (
    calculate_b_k,
    compute_statistics_propagation_absoltec,
    get_raw_data_propagation_absoltec,
)

YEAR = 2025
UTS = [0.0, 0.5, 1.0]
F_HZ = 1575.42e6

# I_v per (station, doy) over UTS. 0.0 and negatives are missing samples.
SERIES = {
    ("aaaa", 1): [10.0, 0.0, 20.0],
    ("aaaa", 2): [12.0, 0.0, 22.0],
    ("aaaa", 3): [0.0, 0.0, 0.0],      # TayAbsTEC failed for the whole day
    ("aaaa", 4): [11.0, 0.0, -1.0],
    ("bbbb", 1): [14.0, 0.0, 0.0],
    ("bbbb", 3): [5.0, 6.0, 7.0],
}


def _write_day(root, station: str, doy: int, tec: list[float]) -> None:
    folder = root / str(YEAR) / f"{doy:03d}" / f"{station}{doy:03d}0"
    folder.mkdir(parents=True)
    # Placeholder rows carry zero gradients too, as TayAbsTEC writes them.
    rows = {col: [0.0 if v <= 0 else 1.0 for v in tec] for col in ABSOLTEC_COLS}
    rows[UT] = UTS
    rows[I_V] = tec
    pd.DataFrame(rows).to_parquet(folder / f"{station}_{doy:03d}_{YEAR}.parquet")


@pytest.fixture
def data_root(tmp_path):
    for (station, doy), tec in SERIES.items():
        _write_day(tmp_path, station, doy, tec)
    return str(tmp_path)


def _by_ut(points):
    return {p.ut: p for p in points}


def _ci(values, alpha=0.05):
    n = len(values)
    return student_t.ppf(1 - alpha / 2, df=n - 1) * statistics.stdev(values) / math.sqrt(n)


# ── Raw reads ─────────────────────────────────────────────────────────────────

class TestRawMissingSamples:

    def test_placeholder_becomes_none_and_grid_is_kept(self, data_root):
        points = get_raw_data(YEAR, 1, "aaaa", data_root)
        assert [p.ut for p in points] == UTS
        assert [p.tec for p in points] == [10.0, None, 20.0]

    def test_negative_tec_is_missing(self, data_root):
        points = get_raw_data(YEAR, 4, "aaaa", data_root)
        assert [p.tec for p in points] == [11.0, None, None]

    def test_range_passes_none_through(self, data_root):
        rows = get_raw_data_range(YEAR, 3, 3, ["aaaa"], data_root)
        assert [r["tec"] for r in rows] == [None, None, None]

    def test_cb_raw_is_none_for_missing_tec(self, data_root):
        points = get_raw_data_cb(YEAR, 1, "aaaa", data_root)
        assert [p.cb is None for p in points] == [False, True, False]
        assert points[0].cb == pytest.approx(_calculate_cb(10.0))

    def test_propagation_raw_is_none_for_missing_tec(self, data_root):
        points = get_raw_data_propagation_absoltec(YEAR, 1, "aaaa", F_HZ, data_root=data_root)
        assert points[1].tec is None
        assert points[1].nt is None and points[1].b_k is None and points[1].gdd is None
        assert points[0].b_k is not None


class TestSmoothValid:

    def test_missing_samples_stay_missing(self):
        tec = [1.0, None, 2.0, 3.0, None, 4.0, 5.0, 6.0]
        smoothed = _smooth_valid(tec, 2)
        assert [v is None for v in smoothed] == [v is None for v in tec]
        assert all(math.isfinite(v) for v in smoothed if v is not None)

    def test_too_few_valid_samples(self):
        assert _smooth_valid([1.0, None, 2.0], 2) is None


# ── Single-station statistics ─────────────────────────────────────────────────

class TestStatisticsSkipMissing:

    def test_absoltec_statistics(self, data_root):
        result = compute_statistics(YEAR, 1, 4, "aaaa", data_root=data_root)
        # Day 3 holds only placeholders, so it is not a day with data.
        assert result.total_days == 3
        pts = _by_ut(result.points)
        # No valid sample at 00:30 on any day: the slot is omitted, not averaged as 0.
        assert set(pts) == {0.0, 1.0}

        p0 = pts[0.0]
        assert p0.n == 3
        assert p0.mean_tec == pytest.approx(11.0)
        assert p0.variance == pytest.approx(statistics.variance([10.0, 12.0, 11.0]))
        assert p0.std_dev == pytest.approx(statistics.stdev([10.0, 12.0, 11.0]), abs=1e-5)
        assert p0.student_ci == pytest.approx(_ci([10.0, 12.0, 11.0]), abs=1e-5)
        # Gradients of placeholder rows do not dilute the SIP mean either.
        assert p0.mean_g_lon == pytest.approx(1.0)

        p1 = pts[1.0]
        assert p1.n == 2
        assert p1.mean_tec == pytest.approx(21.0)
        assert p1.variance == pytest.approx(2.0)

    def test_cb_statistics(self, data_root):
        result = compute_statistics_cb(YEAR, 1, 4, "aaaa", data_root=data_root)
        assert result.total_days == 3
        pts = _by_ut(result.points)
        assert set(pts) == {0.0, 1.0}
        cbs = [_calculate_cb(v) for v in (10.0, 12.0, 11.0)]
        assert pts[0.0].n == 3
        assert pts[0.0].mean_cb == pytest.approx(statistics.mean(cbs))
        assert pts[0.0].variance == pytest.approx(statistics.variance(cbs))
        assert pts[0.0].student_ci == pytest.approx(_ci(cbs))

    def test_propagation_statistics(self, data_root):
        result = compute_statistics_propagation_absoltec(YEAR, 1, 4, "aaaa", F_HZ, data_root=data_root)
        assert result.total_days == 3
        pts = _by_ut(result.points)
        assert set(pts) == {0.0, 1.0}
        b_ks = [calculate_b_k(v * 1e16, F_HZ) for v in (10.0, 12.0, 11.0)]
        p0 = pts[0.0]
        assert p0.n == 3
        assert p0.mean_tec == pytest.approx(11.0)
        assert p0.variance_b_k == pytest.approx(statistics.variance(b_ks))
        assert p0.student_ci_b_k == pytest.approx(_ci(b_ks))

    def test_absoltec_and_propagation_agree_on_mean_tec(self, data_root):
        stats = _by_ut(compute_statistics(YEAR, 1, 4, "aaaa", data_root=data_root).points)
        prop = _by_ut(compute_statistics_propagation_absoltec(YEAR, 1, 4, "aaaa", F_HZ, data_root=data_root).points)
        assert set(stats) == set(prop)
        for ut, p in stats.items():
            assert prop[ut].n == p.n
            assert prop[ut].mean_tec == pytest.approx(p.mean_tec, abs=1e-3)

    def test_all_missing_range_has_no_points(self, data_root):
        result = compute_statistics(YEAR, 3, 3, "aaaa", data_root=data_root)
        assert result.total_days == 0
        assert result.points == []


# ── Per-station-day statistics ────────────────────────────────────────────────

class TestPerStationDaySkipMissing:

    def test_station_with_only_placeholders_is_not_found(self, data_root):
        results = compute_statistics_per_station_day(YEAR, 3, 3, ["aaaa", "bbbb"], data_root=data_root)
        assert len(results) == 1
        assert results[0].stations_found == ["bbbb"]
        # One station left: means are defined, spread is not.
        p0 = _by_ut(results[0].points)[0.0]
        assert p0.n == 1 and p0.mean_tec == pytest.approx(5.0)
        assert p0.variance is None and p0.std_dev is None and p0.student_ci is None

    def test_pools_valid_samples_across_stations(self, data_root):
        results = compute_statistics_per_station_day(YEAR, 1, 1, ["aaaa", "bbbb"], data_root=data_root)
        pts = _by_ut(results[0].points)
        assert set(pts) == {0.0, 1.0}
        assert pts[0.0].n == 2
        assert pts[0.0].variance == pytest.approx(statistics.variance([10.0, 14.0]))
        assert pts[1.0].n == 1 and pts[1.0].student_ci is None

    def test_cb_pools_valid_samples_across_stations(self, data_root):
        results = compute_statistics_per_station_day_cb(YEAR, 1, 1, ["aaaa", "bbbb"], data_root=data_root)
        assert results[0].stations_found == ["aaaa", "bbbb"]
        pts = _by_ut(results[0].points)
        cbs = [_calculate_cb(10.0), _calculate_cb(14.0)]
        assert pts[0.0].n == 2
        assert pts[0.0].mean_cb == pytest.approx(statistics.mean(cbs))
        assert pts[0.0].variance == pytest.approx(statistics.variance(cbs))
        assert pts[1.0].n == 1 and pts[1.0].variance is None

    def test_cb_station_with_only_placeholders_is_not_found(self, data_root):
        results = compute_statistics_per_station_day_cb(YEAR, 3, 3, ["aaaa", "bbbb"], data_root=data_root)
        assert results[0].stations_found == ["bbbb"]


# ── HTTP layer: nulls serialise, plots render ────────────────────────────────

class TestEndpointsWithMissingSamples:

    @pytest.fixture
    def client(self):
        return TestClient(app)

    def test_raw_json_has_null_tec(self, client, data_root):
        r = client.get("/absoltec/raw", params={"year": YEAR, "doy": 1, "station": "aaaa", "data_root": data_root})
        assert r.status_code == 200
        assert [row["tec"] for row in r.json()] == [10.0, None, 20.0]

    def test_statistics_json_has_null_spread_for_single_sample(self, client, data_root):
        r = client.get("/absoltec/statistics", params={
            "year": YEAR, "doy_start": 3, "doy_end": 3, "station": "bbbb", "data_root": data_root,
        })
        assert r.status_code == 200
        point = r.json()["points"][0]
        assert point["n"] == 1
        assert point["variance"] is None and point["student_ci"] is None

    @pytest.mark.parametrize("path, params", [
        ("/plots/absoltec/average", {"doy_start": 1, "doy_end": 4, "station": "aaaa", "show_var": True}),
        ("/plots/absoltec/day", {"doy": 1, "station": "aaaa", "smooth": True, "poly": 2}),
        ("/plots/absoltec/per-station-averages/1", {"doy_start": 1, "doy_end": 1, "stations": ["aaaa", "bbbb"]}),
        ("/plots/cb/average", {"doy_start": 1, "doy_end": 4, "station": "aaaa", "show_var": True}),
        ("/plots/cb/per-station-averages/1", {"doy_start": 1, "doy_end": 1, "stations": ["aaaa", "bbbb"]}),
        ("/plots/cb/multi-station", {"doy_start": 1, "doy_end": 4, "station": "aaaa"}),
        ("/plots/propagation/absoltec/average", {"doy_start": 1, "doy_end": 4, "station": "aaaa", "signal_band": "GPS_L1"}),
    ])
    def test_plots_render_with_missing_samples(self, client, data_root, path, params):
        query = {"year": YEAR, "data_root": data_root, **params}
        png = client.get(path, params=query)
        assert png.status_code == 200, png.text
        assert png.content[:8] == b"\x89PNG\r\n\x1a\n"
        script = client.get(path, params={**query, "format": "script"})
        assert script.status_code == 200, script.text
        compile(script.text, path, "exec")

    def test_average_script_runs_with_null_ci(self, client, data_root, tmp_path, monkeypatch):
        """The generated script must cope with null CI/variance, not just compile."""
        script = client.get("/plots/absoltec/average", params={
            "year": YEAR, "data_root": data_root, "doy_start": 1, "doy_end": 4,
            "station": "aaaa", "show_var": True, "format": "script",
        }).text
        monkeypatch.chdir(tmp_path)
        import matplotlib
        matplotlib.use("Agg")
        monkeypatch.setattr("matplotlib.pyplot.show", lambda *a, **k: None)
        exec(compile(script, "generated_average.py", "exec"), {"__name__": "__main__"})
        assert (tmp_path / "tec_plot.png").exists()
