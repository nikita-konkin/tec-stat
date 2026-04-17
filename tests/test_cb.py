"""
Unit tests for CB (Coherence Band) computation.

Tests the CB calculation formula and statistics.
"""

import math
import pandas as pd
import pytest
from scipy.stats import t as student_t

from app.db.columns import UT
from app.services.cb import _calculate_cb, _build_stats_points_cb, get_raw_data_range_cb
from app.models.schemas import TimeSeriesPointCB


# ── CB calculation tests ──────────────────────────────────────────────────────

class TestCalculateCB:

    def test_cb_calculation_formula(self):
        """Test the CB calculation against the formula."""
        tec = 2.9
        expected_numerator = math.sqrt(4 * 3 * (10 ** 8) * (1 ** 3) * (10 ** 27))
        expected_denominator = math.sqrt(80.5 * math.pi * tec * (10 ** 16))
        expected_cb = expected_numerator / expected_denominator

        assert _calculate_cb(tec) == pytest.approx(expected_cb)

    def test_cb_zero_for_zero_tec(self):
        """CB should be 0 when TEC is 0."""
        assert _calculate_cb(0) == 0.0

    def test_cb_zero_for_negative_tec(self):
        """CB should be 0 when TEC is negative."""
        assert _calculate_cb(-1.0) == 0.0

    def test_cb_positive_for_positive_tec(self):
        """CB should be positive for positive TEC."""
        assert _calculate_cb(1.0) > 0
        assert _calculate_cb(10.0) > 0


# ── Helpers for stats tests ───────────────────────────────────────────────────

def _make_agg_df_cb(
    means: list,
    variances: list,
    std_devs: list,
    ns: list,
    g_lons: list | None = None,
    g_lats: list | None = None,
    uts: list | None = None,
) -> pd.DataFrame:
    """
    Build a DataFrame that looks like the output of the DuckDB GROUP BY query
    for CB statistics.
    """
    n = len(means)
    if uts is None:
        uts = [i * 0.5 for i in range(n)]
    if g_lons is None:
        g_lons = [None] * n
    if g_lats is None:
        g_lats = [None] * n

    return pd.DataFrame({
        UT:           uts,
        "mean_cb":    means,
        "variance":   variances,
        "std_dev":    std_devs,
        "n":          ns,
        "mean_g_lon": g_lons,
        "mean_g_lat": g_lats,
    })


# ── Core formula tests for CB ────────────────────────────────────────────────

class TestBuildStatsPointsCB:

    def test_output_length_matches_input(self):
        df = _make_agg_df_cb([5.0, 6.0], [0.1, 0.2], [0.316, 0.447], [10, 10])
        pts = _build_stats_points_cb(df, alpha=0.05)
        assert len(pts) == 2

    def test_ut_values_preserved(self):
        df = _make_agg_df_cb([5.0, 6.0], [0.1, 0.2], [0.316, 0.447], [10, 10],
                             uts=[3.0, 3.5])
        pts = _build_stats_points_cb(df, alpha=0.05)
        assert pts[0].ut == pytest.approx(3.0)
        assert pts[1].ut == pytest.approx(3.5)

    def test_mean_cb_rounded_to_3dp(self):
        df = _make_agg_df_cb([7.123456], [0.5], [0.707], [8])
        pts = _build_stats_points_cb(df, alpha=0.05)
        assert pts[0].mean_cb == pytest.approx(7.123, abs=1e-3)

    def test_variance_rounded_to_5dp(self):
        df = _make_agg_df_cb([5.0], [3.141592], [1.772], [10])
        pts = _build_stats_points_cb(df, alpha=0.05)
        assert pts[0].variance == pytest.approx(3.14159, abs=1e-5)

    def test_student_ci_formula_exact(self):
        """
        CI = t_ppf(1 - alpha/2, df=N-1) * std_dev / sqrt(N)
        """
        n, std_dev, alpha = 15, 2.5, 0.05
        expected_t  = student_t.ppf(1.0 - alpha / 2.0, df=n - 1)
        expected_ci = expected_t * std_dev / math.sqrt(n)

        df = _make_agg_df_cb([10.0], [std_dev ** 2], [std_dev], [n])
        pts = _build_stats_points_cb(df, alpha=alpha)
        assert pts[0].student_ci == pytest.approx(expected_ci, rel=1e-4)

    def test_no_ci_when_n_less_than_2(self):
        """Student CI should be 0 when n < 2."""
        df = _make_agg_df_cb([5.0], [1.0], [1.0], [1])
        pts = _build_stats_points_cb(df, alpha=0.05)
        assert pts[0].student_ci == 0.0


# ── Integration tests ────────────────────────────────────────────────────────

class TestCBIntegration:

    def test_cb_calculation_consistency(self):
        """Test that CB calculation is consistent."""
        tec_values = [1.0, 2.0, 5.0, 10.0]
        cb_values = [_calculate_cb(tec) for tec in tec_values]

        # CB should decrease as TEC increases
        assert cb_values[0] > cb_values[1] > cb_values[2] > cb_values[3]

        # All should be positive
        assert all(cb > 0 for cb in cb_values)

    def test_raw_range_concatenates_days_and_skips_missing_station(self, monkeypatch):
        def fake_get_raw_data_cb(year, doy, station, data_root=None):
            if station == "alex" and doy in (1, 2):
                return [
                    TimeSeriesPointCB(
                        ut=0.5,
                        tec=10.0 + doy,
                        cb=20.0 + doy,
                        g_lon=50.0,
                        g_lat=60.0,
                        g_q_lon=None,
                        g_q_lat=None,
                        g_t=None,
                        g_q_t=None,
                    )
                ]
            return []

        monkeypatch.setattr("app.services.cb.get_raw_data_cb", fake_get_raw_data_cb)

        rows = get_raw_data_range_cb(2026, 1, 2, ["alex", "alme"], "ignored")

        assert len(rows) == 2
        assert rows[0]["station"] == "alex"
        assert rows[0]["doy"] == 1
        assert rows[0]["concat_ut"] == pytest.approx(0.5)
        assert rows[1]["doy"] == 2
        assert rows[1]["concat_ut"] == pytest.approx(24.5)
