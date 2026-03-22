"""
Tests for the standalone Python script generator.

The generated script must:
  1. Be syntactically valid Python.
  2. Contain the original data embedded as literals.
  3. Import only standard scientific libraries.
  4. Include a plt.show() and plt.savefig() call.
  5. Produce the correct plot type code for each plot_type string.

No matplotlib rendering is needed in these tests — we only inspect the
generated source code as a string.
"""

import ast
import re

import pytest

from app.plotting.script_generator import generate_script


# ── Test data fixtures ────────────────────────────────────────────────────────

def _absoltec_avg_data():
    """Minimal data dict matching what plot_average() produces."""
    return {
        "plot_type":    "absoltec_average",
        "title":        "AKSU — 01.01.2026 to 10.01.2026 (10 days)",
        "xlabel":       "Time, UT [h]",
        "ylabel":       "TEC, TECU",
        "figure_width":  12.0,
        "figure_height": 6.0,
        "dpi":           100,
        "metadata": {
            "year": 2026, "doy_start": 1, "doy_end": 10,
            "station": "aksu", "alpha": 0.05, "total_days": 10,
        },
        "series": {
            "ut":         [i * 0.5 for i in range(48)],
            "mean_tec":   [10.0 + i * 0.1 for i in range(48)],
            "student_ci": [0.5] * 48,
            "variance":   [0.25] * 48,
            "std_dev":    [0.5] * 48,
            "n":          [10] * 48,
            "mean_g_lon": [54.8] * 48,
            "mean_g_lat": [50.8] * 48,
        },
        "plot_options": {"show_student_ci": True, "show_variance": False},
    }


def _tec_satellite_data():
    return {
        "plot_type":    "tec_satellite",
        "title":        "AKSU · E07 · DOY 001/2026",
        "xlabel":       "Time, UT [h]",
        "ylabel":       "TEC, TECU",
        "figure_width":  12.0,
        "figure_height": 6.0,
        "dpi":           100,
        "metadata": {
            "year": 2026, "doy": 1, "station": "aksu",
            "satellite": "E07", "column": "tec_l1l2", "valid_only": True,
        },
        "series": {
            "hour":     [i * 0.5 for i in range(48)],
            "tec_l1l2": [12.0 + i * 0.05 for i in range(48)],
            "el":       [45.0] * 48,
            "az":       [180.0] * 48,
        },
        "plot_options": {"column": "tec_l1l2", "valid_only": True},
    }


def _tec_sky_track_data():
    return {
        "plot_type":    "tec_sky_track",
        "title":        "AKSU · E07 · DOY 001/2026",
        "xlabel":       "Azimuth",
        "ylabel":       "Elevation",
        "figure_width":  6.0,
        "figure_height": 6.0,
        "dpi":           100,
        "metadata": {"year": 2026, "doy": 1, "station": "aksu",
                     "satellite": "E07", "valid_only": True},
        "series": {
            "hour":     list(range(10)),
            "az_deg":   [i * 36.0 for i in range(10)],
            "el_deg":   [30.0 + i for i in range(10)],
            "tec_l1l2": [10.0 + i * 0.1 for i in range(10)],
        },
        "plot_options": {"color_by_tec": True, "valid_only": True},
    }


def _multi_station_data():
    return {
        "plot_type":    "absoltec_multi_station",
        "title":        "Multi-station — 01.01.2026",
        "xlabel":       "Time, UT [h]",
        "ylabel":       "TEC, TECU",
        "figure_width":  12.0,
        "figure_height": 6.0,
        "dpi":           100,
        "metadata": {"year": 2026, "doy": 1, "stations": ["aksu", "armv"]},
        "series": {
            "aksu": {"ut": [0.0, 0.5], "tec": [10.0, 10.5]},
            "armv": {"ut": [0.0, 0.5], "tec": [11.0, 11.5]},
        },
        "plot_options": {"smooth": False, "polynomial_order": 3},
    }


# ── Syntax validity ────────────────────────────────────────────────────────────

class TestSyntaxValidity:
    """Every generated script must parse as valid Python 3."""

    @pytest.mark.parametrize("data_fn", [
        _absoltec_avg_data,
        _tec_satellite_data,
        _tec_sky_track_data,
        _multi_station_data,
    ])
    def test_parses_as_valid_python(self, data_fn):
        script = generate_script(data_fn())
        try:
            ast.parse(script)
        except SyntaxError as e:
            pytest.fail(f"Generated script has a syntax error: {e}\n\n{script[:500]}")


# ── Data embedding ─────────────────────────────────────────────────────────────

class TestDataEmbedding:
    """The generated script must contain the original data as Python literals."""

    def test_metadata_year_present(self):
        script = generate_script(_absoltec_avg_data())
        assert "2026" in script

    def test_station_name_present(self):
        script = generate_script(_absoltec_avg_data())
        assert "aksu" in script

    def test_title_present(self):
        data = _absoltec_avg_data()
        script = generate_script(data)
        # Title appears in the TITLE = '...' line
        assert data["title"] in script

    def test_series_dict_keyword_present(self):
        script = generate_script(_absoltec_avg_data())
        assert "series" in script

    def test_mean_tec_key_present_for_average(self):
        script = generate_script(_absoltec_avg_data())
        assert "mean_tec" in script

    def test_satellite_series_key_present(self):
        script = generate_script(_tec_satellite_data())
        assert "tec_l1l2" in script

    def test_az_deg_present_in_sky_track(self):
        script = generate_script(_tec_sky_track_data())
        assert "az_deg" in script

    def test_multi_station_names_in_series(self):
        script = generate_script(_multi_station_data())
        assert "aksu" in script
        assert "armv" in script


# ── Required matplotlib calls ──────────────────────────────────────────────────

class TestRequiredCalls:

    def test_plt_show_present(self):
        script = generate_script(_absoltec_avg_data())
        assert "plt.show()" in script

    def test_plt_savefig_present(self):
        script = generate_script(_absoltec_avg_data())
        assert "plt.savefig(" in script

    def test_import_matplotlib_present(self):
        script = generate_script(_absoltec_avg_data())
        assert "import matplotlib" in script

    def test_import_numpy_present(self):
        script = generate_script(_tec_sky_track_data())
        # Sky track uses numpy for radians conversion
        assert "import numpy" in script

    def test_no_import_duckdb(self):
        """Generated scripts must be self-contained — no backend dependencies."""
        script = generate_script(_absoltec_avg_data())
        assert "import duckdb" not in script
        assert "import fastapi" not in script
        assert "from app" not in script


# ── Plot-type-specific code generation ────────────────────────────────────────

class TestPlotTypeDispatch:

    def test_average_plots_errorbar_when_ci_enabled(self):
        data = _absoltec_avg_data()
        data["plot_options"]["show_student_ci"] = True
        script = generate_script(data)
        assert "errorbar" in script
        assert "student_ci" in script

    def test_average_skips_errorbar_when_ci_disabled(self):
        data = _absoltec_avg_data()
        data["plot_options"]["show_student_ci"] = False
        data["plot_options"]["show_variance"]   = False
        script = generate_script(data)
        assert "errorbar" not in script

    def test_average_includes_variance_when_enabled(self):
        data = _absoltec_avg_data()
        data["plot_options"]["show_variance"] = True
        script = generate_script(data)
        assert "variance" in script
        # Two errorbar calls expected: one for CI, one for variance
        assert script.count("errorbar") == 2

    def test_single_day_smooth_uses_savgol(self):
        data = {
            "plot_type": "absoltec_single_day",
            "title": "t", "xlabel": "x", "ylabel": "y",
            "figure_width": 12.0, "figure_height": 6.0, "dpi": 100,
            "metadata": {}, "series": {"ut": [], "tec": []},
            "plot_options": {"smooth": True, "polynomial_order": 3},
        }
        script = generate_script(data)
        assert "savgol_filter" in script

    def test_sky_track_uses_polar_subplot(self):
        script = generate_script(_tec_sky_track_data())
        assert "polar=True" in script

    def test_sky_track_sets_north_at_top(self):
        script = generate_script(_tec_sky_track_data())
        assert "set_theta_zero_location" in script
        assert "'N'" in script

    def test_multi_satellite_loops_over_series(self):
        data = {
            "plot_type": "tec_multi_satellite",
            "title": "t", "xlabel": "x", "ylabel": "y",
            "figure_width": 12.0, "figure_height": 6.0, "dpi": 100,
            "metadata": {}, "series": {
                "E07": {"hour": [0.0], "tec_l1l2": [10.0]},
            },
            "plot_options": {"column": "tec_l1l2", "valid_only": True},
        }
        script = generate_script(data)
        # The multi-satellite code must iterate over the series dict
        assert "for sat" in script or "for s" in script

    def test_unknown_plot_type_generates_generic(self):
        data = {
            "plot_type": "completely_unknown_type",
            "title": "t", "xlabel": "x", "ylabel": "y",
            "figure_width": 12.0, "figure_height": 6.0, "dpi": 100,
            "metadata": {},
            "series": {"ut": [0.0, 0.5], "tec": [10.0, 10.5]},
            "plot_options": {},
        }
        script = generate_script(data)
        # Must not crash and must be valid Python
        ast.parse(script)


# ── Settings section ──────────────────────────────────────────────────────────

class TestSettingsSection:

    def test_figure_width_settable(self):
        script = generate_script(_absoltec_avg_data())
        assert "FIGURE_WIDTH" in script

    def test_figure_height_settable(self):
        script = generate_script(_absoltec_avg_data())
        assert "FIGURE_HEIGHT" in script

    def test_dpi_settable(self):
        script = generate_script(_absoltec_avg_data())
        assert "DPI" in script

    def test_title_settable(self):
        script = generate_script(_absoltec_avg_data())
        assert "TITLE" in script

    def test_xlabel_settable(self):
        script = generate_script(_absoltec_avg_data())
        assert "XLABEL" in script

    def test_ylabel_settable(self):
        script = generate_script(_absoltec_avg_data())
        assert "YLABEL" in script
