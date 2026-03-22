"""
Unit tests for the file-path helper functions in app/db/engine.py.

These tests don't touch the filesystem — they only verify that the
path-building logic generates the correct strings.
"""

import pytest
from app.db.engine import (
    absoltec_file_path,
    tec_file_path,
    _absoltec_station_folder,
)


class TestAbsoltecPaths:

    def test_station_folder_format(self):
        """Station folder should be 8 chars: 4-char code + '0010'."""
        folder = _absoltec_station_folder("aksu")
        assert folder == "aksu0010"

    def test_station_folder_uppercased_input(self):
        folder = _absoltec_station_folder("AKSU")
        assert folder == "aksu0010"

    def test_station_folder_short_code_padded(self):
        """Codes shorter than 4 chars should be padded with '0'."""
        folder = _absoltec_station_folder("arm")
        assert folder == "arm00010"

    def test_absoltec_file_path_structure(self):
        path = absoltec_file_path("/data", 2026, 1, "aksu")
        assert path == "/data/2026_parq/001/aksu0010/aksu_001_2026.parquet"

    def test_absoltec_doy_zero_padded(self):
        path = absoltec_file_path("/data", 2026, 7, "aksu")
        assert "007" in path

    def test_absoltec_doy_three_digits(self):
        path = absoltec_file_path("/data", 2026, 365, "aksu")
        assert "365" in path

    def test_absoltec_year_in_filename(self):
        path = absoltec_file_path("/data", 2026, 1, "aksu")
        assert "2026" in path.split("/")[-1]


class TestTecPaths:

    def test_tec_file_path_structure(self):
        path = tec_file_path("/data", 2026, 1, "aksu", "E07")
        assert path == "/data/2026_parq/001/aksu0010/aksu_E07_001_26.parquet"

    def test_tec_year_two_digit(self):
        """TEC-suite filenames use the 2-digit year."""
        path = tec_file_path("/data", 2026, 1, "aksu", "G03")
        assert "_26.parquet" in path

    def test_tec_satellite_in_filename(self):
        path = tec_file_path("/data", 2026, 100, "armv", "R22")
        assert "_R22_" in path

    def test_tec_doy_zero_padded(self):
        path = tec_file_path("/data", 2026, 5, "aksu", "E01")
        assert "/005/" in path
