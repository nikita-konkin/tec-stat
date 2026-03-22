"""
Unit tests for the AbsolTEC statistics computation.

These tests are filesystem-free — they call the internal _build_stats_points()
and _safe_float() / _opt_float() helpers directly using synthetic DataFrames
that mimic the DuckDB aggregate output.

The statistics formulas are verified against scipy.stats.t and numpy so that
any accidental regression in the math is caught immediately.
"""

import math

import pandas as pd
import pytest
from scipy.stats import t as student_t

from app.db.columns import UT  # the real column name used as the DataFrame key
from app.services.absoltec import _build_stats_points, _safe_float, _opt_float


# ── Helpers ───────────────────────────────────────────────────────────────────

def _make_agg_df(
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
    in compute_statistics() / compute_statistics_per_station_day().

    Column name for universal time is the real parquet column name 'UT'
    (uppercase) — this matches what DuckDB returns when the column is
    referenced as "UT" in the SELECT.
    """
    n = len(means)
    if uts is None:
        uts = [i * 0.5 for i in range(n)]
    if g_lons is None:
        g_lons = [None] * n
    if g_lats is None:
        g_lats = [None] * n

    return pd.DataFrame({
        UT:           uts,          # note: uppercase key — matches real column
        "mean_tec":   means,
        "variance":   variances,
        "std_dev":    std_devs,
        "n":          ns,
        "mean_g_lon": g_lons,
        "mean_g_lat": g_lats,
    })


# ── Core formula tests ────────────────────────────────────────────────────────

class TestBuildStatsPoints:

    def test_output_length_matches_input(self):
        df = _make_agg_df([5.0, 6.0], [0.1, 0.2], [0.316, 0.447], [10, 10])
        pts = _build_stats_points(df, alpha=0.05)
        assert len(pts) == 2

    def test_ut_values_preserved(self):
        df = _make_agg_df([5.0, 6.0], [0.1, 0.2], [0.316, 0.447], [10, 10],
                          uts=[3.0, 3.5])
        pts = _build_stats_points(df, alpha=0.05)
        assert pts[0].ut == pytest.approx(3.0)
        assert pts[1].ut == pytest.approx(3.5)

    def test_mean_tec_rounded_to_3dp(self):
        df = _make_agg_df([7.123456], [0.5], [0.707], [8])
        pts = _build_stats_points(df, alpha=0.05)
        # rounded to 3 decimal places
        assert pts[0].mean_tec == pytest.approx(7.123, abs=1e-3)

    def test_variance_rounded_to_5dp(self):
        df = _make_agg_df([5.0], [3.141592], [1.772], [10])
        pts = _build_stats_points(df, alpha=0.05)
        assert pts[0].variance == pytest.approx(3.14159, abs=1e-5)

    def test_student_ci_formula_exact(self):
        """
        CI = t_ppf(1 - alpha/2, df=N-1) * std_dev / sqrt(N)

        This is the exact formula from the original Count_statistics().
        Verify against scipy.stats.t directly.
        """
        n, std_dev, alpha = 15, 2.5, 0.05
        expected_t  = student_t.ppf(1.0 - alpha / 2.0, df=n - 1)
        expected_ci = expected_t * std_dev / math.sqrt(n)

        df = _make_agg_df([10.0], [std_dev ** 2], [std_dev], [n])
        pts = _build_stats_points(df, alpha=alpha)
        assert pts[0].student_ci == pytest.approx(expected_ci, rel=1e-4)

    def test_single_day_ci_is_zero(self):
        """With N=1 the t-distribution is undefined — service returns 0.0."""
        df = _make_agg_df([5.0], [0.0], [0.0], [1])
        pts = _build_stats_points(df, alpha=0.05)
        assert pts[0].student_ci == 0.0

    def test_ci_is_positive(self):
        df = _make_agg_df([5.0], [1.0], [1.0], [20])
        pts = _build_stats_points(df, alpha=0.05)
        assert pts[0].student_ci > 0

    def test_ci_narrows_with_more_days(self):
        """More days → narrower CI (law of large numbers)."""
        df_small = _make_agg_df([5.0], [1.0], [1.0], [5])
        df_large = _make_agg_df([5.0], [1.0], [1.0], [50])
        ci_small = _build_stats_points(df_small, alpha=0.05)[0].student_ci
        ci_large = _build_stats_points(df_large, alpha=0.05)[0].student_ci
        assert ci_small > ci_large

    def test_ci_narrows_with_larger_alpha(self):
        """Larger alpha (lower confidence) → narrower CI."""
        df = _make_agg_df([5.0], [1.0], [1.0], [20])
        ci_95 = _build_stats_points(df, alpha=0.05)[0].student_ci  # 95 %
        ci_80 = _build_stats_points(df, alpha=0.20)[0].student_ci  # 80 %
        assert ci_95 > ci_80

    def test_n_stored_in_output(self):
        df = _make_agg_df([5.0], [1.0], [1.0], [42])
        pts = _build_stats_points(df, alpha=0.05)
        assert pts[0].n == 42

    def test_g_lon_lat_preserved_when_present(self):
        df = _make_agg_df([5.0], [1.0], [1.0], [10],
                          g_lons=[54.837], g_lats=[50.841])
        pts = _build_stats_points(df, alpha=0.05)
        assert pts[0].mean_g_lon == pytest.approx(54.837, rel=1e-3)
        assert pts[0].mean_g_lat == pytest.approx(50.841, rel=1e-3)

    def test_g_lon_lat_none_when_absent(self):
        df = _make_agg_df([5.0], [1.0], [1.0], [10])
        pts = _build_stats_points(df, alpha=0.05)
        assert pts[0].mean_g_lon is None
        assert pts[0].mean_g_lat is None

    def test_48_slots_cover_full_day(self):
        """End-to-end: 48 slots spanning 0.0 to 23.5 hours."""
        uts = [i * 0.5 for i in range(48)]
        df  = _make_agg_df(
            means=[5.0] * 48, variances=[0.5] * 48, std_devs=[0.7] * 48,
            ns=[10] * 48, uts=uts,
        )
        pts = _build_stats_points(df, alpha=0.05)
        assert len(pts) == 48
        assert pts[0].ut  == 0.0
        assert pts[-1].ut == 23.5


# ── None / NaN handling ───────────────────────────────────────────────────────

class TestNullHandling:
    """DuckDB returns None for aggregates over empty groups — must not crash."""

    def test_none_mean_defaults_to_zero(self):
        df = pd.DataFrame({
            UT: [0.0], "mean_tec": [None], "variance": [None],
            "std_dev": [None], "n": [0],
            "mean_g_lon": [None], "mean_g_lat": [None],
        })
        pts = _build_stats_points(df, alpha=0.05)
        assert pts[0].mean_tec == 0.0
        assert pts[0].student_ci == 0.0

    def test_nan_mean_defaults_to_zero(self):

        df = pd.DataFrame({
            UT: [0.0], "mean_tec": [float("nan")], "variance": [float("nan")],
            "std_dev": [float("nan")], "n": [0],
            "mean_g_lon": [None], "mean_g_lat": [None],
        })
        pts = _build_stats_points(df, alpha=0.05)
        assert pts[0].mean_tec == 0.0


# ── _safe_float / _opt_float helpers ─────────────────────────────────────────

class TestFloatHelpers:

    def test_safe_float_normal_value(self):
        row = pd.Series({"key": 3.14})
        assert _safe_float(row, "key") == pytest.approx(3.14)

    def test_safe_float_none_returns_default(self):
        row = pd.Series({"key": None})
        assert _safe_float(row, "key") == 0.0
        assert _safe_float(row, "key", default=99.0) == 99.0

    def test_safe_float_nan_returns_default(self):
        row = pd.Series({"key": float("nan")})
        assert _safe_float(row, "key") == 0.0

    def test_safe_float_missing_key_returns_default(self):
        row = pd.Series({"other": 1.0})
        assert _safe_float(row, "key") == 0.0

    def test_opt_float_normal(self):
        assert _opt_float(3.14) == pytest.approx(3.14)

    def test_opt_float_none_returns_none(self):
        assert _opt_float(None) is None

    def test_opt_float_nan_returns_none(self):
        assert _opt_float(float("nan")) is None

    def test_opt_float_zero_is_not_none(self):
        assert _opt_float(0.0) == 0.0


# ── Regression: original Count_statistics() exact reproduction ────────────────

class TestOriginalAlgorithmRegression:
    """
    Regression tests that pin the statistics to values computed manually
    from the original Count_statistics() formula.

    Scenario: 3 days, 1 time slot (ut=0.0), TEC values [10.0, 12.0, 11.0]
    """

    TEC = [10.0, 12.0, 11.0]
    N   = 3
    ALPHA = 0.05

    @property
    def _mean(self):
        return sum(self.TEC) / self.N   # 11.0

    @property
    def _var_pop(self):
        # population variance (denominator = N, not N-1)
        return sum((t - self._mean) ** 2 for t in self.TEC) / self.N  # 2/3

    @property
    def _std_pop(self):
        return self._var_pop ** 0.5

    @property
    def _student_ci(self):
        t_crit = student_t.ppf(1.0 - self.ALPHA / 2.0, df=self.N - 1)
        return t_crit * self._std_pop / math.sqrt(self.N)

    def _make_df(self) -> pd.DataFrame:
        return pd.DataFrame({
            UT:           [0.0],
            "mean_tec":   [self._mean],
            "variance":   [self._var_pop],
            "std_dev":    [self._std_pop],
            "n":          [self.N],
            "mean_g_lon": [None],
            "mean_g_lat": [None],
        })

    def test_mean_correct(self):
        pts = _build_stats_points(self._make_df(), alpha=self.ALPHA)
        assert pts[0].mean_tec == pytest.approx(self._mean, rel=1e-3)

    def test_variance_correct(self):
        pts = _build_stats_points(self._make_df(), alpha=self.ALPHA)
        assert pts[0].variance == pytest.approx(self._var_pop, rel=1e-4)

    def test_std_dev_correct(self):
        pts = _build_stats_points(self._make_df(), alpha=self.ALPHA)
        assert pts[0].std_dev == pytest.approx(self._std_pop, rel=1e-4)

    def test_student_ci_correct(self):
        pts = _build_stats_points(self._make_df(), alpha=self.ALPHA)
        assert pts[0].student_ci == pytest.approx(self._student_ci, rel=1e-4)

    def test_n_correct(self):
        pts = _build_stats_points(self._make_df(), alpha=self.ALPHA)
        assert pts[0].n == self.N
