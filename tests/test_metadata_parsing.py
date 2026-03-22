"""
Tests for TEC-suite station metadata parsing.

The parser must handle:
  1. The TEC-suite L/B/H convention (L = longitude, B = latitude — Russian geodetic).
  2. ECEF XYZ coordinates.
  3. The Site field.
  4. Missing / malformed lines (must not crash, return None for that field).
  5. Whitespace variations around colons and commas.
  6. The parse_header_text() function is public — called here directly so
     tests don't need real parquet files on disk.
"""

import pytest

from app.services.tec import parse_header_text, parse_station_metadata


# ── Reference header (copied from the actual TEC-suite .dat file format) ──────

REAL_HEADER = """\
# Created on 2026-03-20 18:38:40
# Sources: /data/rinex/01/aksu001/aksu0010.26o, /data/rinex/01/aksu001/aksu0010.26l
# Satellite: E07
# Interval: 30.0
# Sampling interval: 0.0 (not used).
# Site: aksu
# Position (L, B, H): 50.84156264475084, 54.837857328545816, 126.22089951578528
# Position (X, Y, Z): 2324706.1103, 2854596.6353, 5191112.72
# datetime format: %Y-%m-%dT%H:%M:%S
# Columns: tsn, hour, el, az, tec.l1l2, tec.c1p2, validity
"""


# ── Tests using the real header format ────────────────────────────────────────

class TestRealHeaderParsing:

    def setup_method(self):
        self.meta = parse_header_text(REAL_HEADER, "aksu")

    def test_station_code_preserved(self):
        assert self.meta.station == "aksu"

    def test_site_field_parsed(self):
        assert self.meta.site == "aksu"

    def test_longitude_correct(self):
        """L = longitude in TEC-suite convention."""
        assert self.meta.lon == pytest.approx(50.84156264475084, rel=1e-9)

    def test_latitude_correct(self):
        """B = latitude in TEC-suite convention."""
        assert self.meta.lat == pytest.approx(54.837857328545816, rel=1e-9)

    def test_height_correct(self):
        assert self.meta.height == pytest.approx(126.22089951578528, rel=1e-6)

    def test_ecef_x_correct(self):
        assert self.meta.x == pytest.approx(2324706.1103, rel=1e-6)

    def test_ecef_y_correct(self):
        assert self.meta.y == pytest.approx(2854596.6353, rel=1e-6)

    def test_ecef_z_correct(self):
        assert self.meta.z == pytest.approx(5191112.72, rel=1e-6)

    def test_has_data_true(self):
        assert self.meta.has_data is True


# ── L/B convention tests ───────────────────────────────────────────────────────

class TestLBConvention:
    """
    The TEC-suite header uses the Russian geodetic convention:
      L = longitude (first value)
      B = latitude  (second value)
    This is the OPPOSITE of the ISO lat/lon order and must NOT be swapped.
    """

    def test_l_maps_to_lon(self):
        header = "# Position (L, B, H): 55.0, 45.0, 100.0"
        meta = parse_header_text(header, "test")
        assert meta.lon == pytest.approx(55.0)  # L is first → lon
        assert meta.lat == pytest.approx(45.0)  # B is second → lat

    def test_negative_longitude(self):
        header = "# Position (L, B, H): -10.5, 51.5, 20.0"
        meta = parse_header_text(header, "test")
        assert meta.lon == pytest.approx(-10.5)
        assert meta.lat == pytest.approx(51.5)

    def test_southern_hemisphere_negative_latitude(self):
        header = "# Position (L, B, H): 25.0, -33.9, 1650.0"
        meta = parse_header_text(header, "test")
        assert meta.lat == pytest.approx(-33.9)

    def test_lon_not_swapped_with_lat(self):
        """Regression guard: ensure L→lon, B→lat order is never reversed."""
        header = "# Position (L, B, H): 100.0, 60.0, 500.0"
        meta = parse_header_text(header, "test")
        # lon must be 100, lat must be 60 — not the other way round
        assert meta.lon == pytest.approx(100.0)
        assert meta.lat == pytest.approx(60.0)
        assert meta.lon != meta.lat


# ── Missing / partial header tests ────────────────────────────────────────────

class TestPartialHeaders:

    def test_no_position_line_returns_none_coords(self):
        header = "# Site: test\n# Satellite: E01\n"
        meta = parse_header_text(header, "test")
        assert meta.lat    is None
        assert meta.lon    is None
        assert meta.height is None

    def test_no_xyz_line_returns_none_ecef(self):
        header = "# Position (L, B, H): 50.0, 45.0, 200.0\n"
        meta = parse_header_text(header, "test")
        assert meta.x is None
        assert meta.y is None
        assert meta.z is None

    def test_no_site_line_uses_station_code(self):
        header = "# Position (L, B, H): 50.0, 45.0, 200.0\n"
        meta = parse_header_text(header, "mystation")
        assert meta.site == "mystation"

    def test_empty_header_returns_all_none(self):
        meta = parse_header_text("", "test")
        assert meta.lat    is None
        assert meta.lon    is None
        assert meta.height is None
        assert meta.x      is None
        assert meta.y      is None
        assert meta.z      is None

    def test_empty_header_does_not_crash(self):
        meta = parse_header_text("", "test")
        assert meta is not None
        assert meta.station == "test"


# ── Whitespace tolerance ──────────────────────────────────────────────────────

class TestWhitespaceTolerance:

    def test_extra_spaces_around_colon(self):
        header = "# Position (L, B, H)  :  50.0, 45.0, 200.0"
        meta = parse_header_text(header, "test")
        assert meta.lon == pytest.approx(50.0)

    def test_extra_spaces_around_commas(self):
        header = "# Position (L, B, H): 50.0 ,  45.0  ,  200.0"
        meta = parse_header_text(header, "test")
        assert meta.height == pytest.approx(200.0)

    def test_site_with_trailing_whitespace(self):
        header = "# Site:   aksu  \n"
        meta = parse_header_text(header, "aksu")
        # \S+ regex stops at whitespace so trailing spaces are ignored
        assert meta.site == "aksu"

    def test_case_insensitive_position_keyword(self):
        header = "# POSITION (L, B, H): 50.0, 45.0, 200.0"
        meta = parse_header_text(header, "test")
        assert meta.lon == pytest.approx(50.0)


# ── High-precision coordinate preservation ────────────────────────────────────

class TestPrecision:

    def test_full_precision_lon(self):
        """Coordinates must not be truncated — full float64 precision required."""
        header = "# Position (L, B, H): 50.84156264475084, 54.837857328545816, 126.22"
        meta = parse_header_text(header, "test")
        # Compare to 10 significant figures
        assert meta.lon == pytest.approx(50.84156264475084, rel=1e-10)

    def test_full_precision_lat(self):
        header = "# Position (L, B, H): 50.0, 54.837857328545816, 100.0"
        meta = parse_header_text(header, "test")
        assert meta.lat == pytest.approx(54.837857328545816, rel=1e-10)

    def test_large_ecef_values(self):
        header = "# Position (X, Y, Z): 2324706.1103, 2854596.6353, 5191112.72"
        meta = parse_header_text(header, "test")
        assert meta.z == pytest.approx(5191112.72, rel=1e-8)
