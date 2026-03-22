"""
Tests for the folder-agnostic station discovery logic in app.db.engine.

The key behaviour under test: station folders can have any suffix after the
station code (aksu0010, armv001g33, armv001k00 …), and the service must find
the parquet file regardless of which suffix was used.

All tests use temporary directories built by the test themselves — no real
parquet data files are needed, only zero-byte placeholder files. The path
helpers only test whether files exist (os.path.isfile / glob.glob); they
never read the parquet content.
"""

import os
import pathlib
import tempfile

import pytest

from app.db.engine import (
    find_absoltec_file,
    absoltec_glob_files,
    absoltec_discover_stations,
    absoltec_discover_days,
    find_tec_file,
    tec_glob_satellites,
    tec_discover_stations,
)


# ── Fixtures ──────────────────────────────────────────────────────────────────

def _touch(root: str, *rel_parts: str) -> str:
    """Create an empty file (and all parent directories) and return its path."""
    path = os.path.join(root, *rel_parts)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    pathlib.Path(path).touch()
    return path


# ── AbsolTEC folder detection ─────────────────────────────────────────────────

class TestFindAbsoltecFile:

    def test_finds_standard_folder(self):
        with tempfile.TemporaryDirectory() as root:
            _touch(root, "2026_parq/001/aksu0010/aksu_001_2026.parquet")
            path = find_absoltec_file(root, 2026, 1, "aksu")
            assert path is not None
            assert "aksu_001_2026.parquet" in path

    def test_finds_variable_folder_suffix_g33(self):
        """armv001g33 is a real-world folder name — must still be found."""
        with tempfile.TemporaryDirectory() as root:
            _touch(root, "2026_parq/001/armv001g33/armv_001_2026.parquet")
            path = find_absoltec_file(root, 2026, 1, "armv")
            assert path is not None
            assert "armv_001_2026.parquet" in path

    def test_finds_variable_folder_suffix_k00(self):
        """armv001k00 is another real-world folder name variant."""
        with tempfile.TemporaryDirectory() as root:
            _touch(root, "2026_parq/001/armv001k00/armv_001_2026.parquet")
            path = find_absoltec_file(root, 2026, 1, "armv")
            assert path is not None

    def test_missing_file_returns_none(self):
        with tempfile.TemporaryDirectory() as root:
            path = find_absoltec_file(root, 2026, 1, "aksu")
            assert path is None

    def test_case_insensitive_station_input(self):
        """The station argument should be case-normalised to lowercase."""
        with tempfile.TemporaryDirectory() as root:
            _touch(root, "2026_parq/001/aksu0010/aksu_001_2026.parquet")
            # Both 'AKSU' and 'Aksu' should find the file
            assert find_absoltec_file(root, 2026, 1, "AKSU") is not None
            assert find_absoltec_file(root, 2026, 1, "Aksu") is not None

    def test_doy_zero_padded_in_path(self):
        with tempfile.TemporaryDirectory() as root:
            _touch(root, "2026_parq/007/aksu0010/aksu_007_2026.parquet")
            path = find_absoltec_file(root, 2026, 7, "aksu")
            assert path is not None
            assert "007" in path

    def test_multiple_folders_returns_first_sorted(self):
        """When multiple matching folders exist, the result should be deterministic."""
        with tempfile.TemporaryDirectory() as root:
            _touch(root, "2026_parq/001/armv001g33/armv_001_2026.parquet")
            _touch(root, "2026_parq/001/armv001k00/armv_001_2026.parquet")
            path = find_absoltec_file(root, 2026, 1, "armv")
            assert path is not None
            # Result is deterministic (sorted) — 'g' < 'k'
            assert "armv001g33" in path

    def test_wrong_station_code_returns_none(self):
        with tempfile.TemporaryDirectory() as root:
            _touch(root, "2026_parq/001/aksu0010/aksu_001_2026.parquet")
            assert find_absoltec_file(root, 2026, 1, "armv") is None

    def test_does_not_match_tec_suite_file(self):
        """TEC-suite files like aksu_E07_001_26.parquet must NOT be returned."""
        with tempfile.TemporaryDirectory() as root:
            # Only a TEC-suite file exists for this station/doy
            _touch(root, "2026_parq/001/aksu0010/aksu_E07_001_26.parquet")
            # AbsolTEC pattern requires {station}_{doy}_{year}.parquet
            path = find_absoltec_file(root, 2026, 1, "aksu")
            assert path is None


class TestAbsoltecGlobFiles:

    def test_returns_only_existing_days(self):
        with tempfile.TemporaryDirectory() as root:
            _touch(root, "2026_parq/001/aksu0010/aksu_001_2026.parquet")
            _touch(root, "2026_parq/003/aksu0010/aksu_003_2026.parquet")
            # doy 002 is intentionally missing
            files = absoltec_glob_files(root, 2026, 1, 3, "aksu")
            assert len(files) == 2
            doys = {int(f.split("/")[-1].split("_")[1]) for f in files}
            assert doys == {1, 3}

    def test_empty_result_when_no_data(self):
        with tempfile.TemporaryDirectory() as root:
            files = absoltec_glob_files(root, 2026, 1, 5, "aksu")
            assert files == []


class TestAbsoltecDiscoverStations:

    def test_discovers_multiple_stations(self):
        with tempfile.TemporaryDirectory() as root:
            _touch(root, "2026_parq/001/aksu0010/aksu_001_2026.parquet")
            _touch(root, "2026_parq/001/armv001g33/armv_001_2026.parquet")
            stations = absoltec_discover_stations(root, 2026, 1)
            assert "aksu" in stations
            assert "armv" in stations

    def test_ignores_tec_suite_files(self):
        """Four-segment filenames (satellite files) must not be counted."""
        with tempfile.TemporaryDirectory() as root:
            _touch(root, "2026_parq/001/aksu0010/aksu_E07_001_26.parquet")  # TEC-suite
            stations = absoltec_discover_stations(root, 2026, 1)
            assert stations == []

    def test_returns_sorted_list(self):
        with tempfile.TemporaryDirectory() as root:
            _touch(root, "2026_parq/001/zzzz0010/zzzz_001_2026.parquet")
            _touch(root, "2026_parq/001/aaaa0010/aaaa_001_2026.parquet")
            stations = absoltec_discover_stations(root, 2026, 1)
            assert stations == sorted(stations)


class TestAbsoltecDiscoverDays:

    def test_finds_all_days_for_station(self):
        with tempfile.TemporaryDirectory() as root:
            for doy in [1, 10, 100, 365]:
                _touch(root,
                       f"2026_parq/{doy:03d}/aksu0010/aksu_{doy:03d}_2026.parquet")
            days = absoltec_discover_days(root, 2026, "aksu")
            assert set(days) == {1, 10, 100, 365}

    def test_returns_sorted_list(self):
        with tempfile.TemporaryDirectory() as root:
            for doy in [50, 1, 200]:
                _touch(root,
                       f"2026_parq/{doy:03d}/aksu0010/aksu_{doy:03d}_2026.parquet")
            days = absoltec_discover_days(root, 2026, "aksu")
            assert days == sorted(days)

    def test_different_folder_suffix_still_found(self):
        with tempfile.TemporaryDirectory() as root:
            _touch(root, "2026_parq/042/armv001g33/armv_042_2026.parquet")
            days = absoltec_discover_days(root, 2026, "armv")
            assert 42 in days


# ── TEC-suite folder detection ────────────────────────────────────────────────

class TestFindTecFile:

    def test_finds_standard_satellite_file(self):
        with tempfile.TemporaryDirectory() as root:
            _touch(root, "2026_parq/001/aksu0010/aksu_E07_001_26.parquet")
            path = find_tec_file(root, 2026, 1, "aksu", "E07")
            assert path is not None
            assert "aksu_E07_001_26.parquet" in path

    def test_variable_folder_suffix(self):
        with tempfile.TemporaryDirectory() as root:
            _touch(root, "2026_parq/001/armv001g33/armv_G12_001_26.parquet")
            path = find_tec_file(root, 2026, 1, "armv", "G12")
            assert path is not None

    def test_missing_returns_none(self):
        with tempfile.TemporaryDirectory() as root:
            assert find_tec_file(root, 2026, 1, "aksu", "E07") is None

    def test_year_two_digit_in_filename(self):
        with tempfile.TemporaryDirectory() as root:
            _touch(root, "2026_parq/001/aksu0010/aksu_R22_001_26.parquet")
            path = find_tec_file(root, 2026, 1, "aksu", "R22")
            assert path is not None
            assert "_26.parquet" in path

    def test_does_not_match_absoltec_file(self):
        with tempfile.TemporaryDirectory() as root:
            _touch(root, "2026_parq/001/aksu0010/aksu_001_2026.parquet")  # AbsolTEC
            # Satellite code would make the filename 'aksu_E07_001_26.parquet'
            # which does NOT exist — should return None
            path = find_tec_file(root, 2026, 1, "aksu", "E07")
            assert path is None


class TestTecGlobSatellites:

    def test_lists_all_satellites(self):
        with tempfile.TemporaryDirectory() as root:
            for sat in ["E01", "E07", "G03", "R22"]:
                _touch(root, f"2026_parq/001/aksu0010/aksu_{sat}_001_26.parquet")
            sats = tec_glob_satellites(root, 2026, 1, "aksu")
            assert set(sats) == {"E01", "E07", "G03", "R22"}

    def test_returns_sorted_list(self):
        with tempfile.TemporaryDirectory() as root:
            for sat in ["G03", "E01", "R22"]:
                _touch(root, f"2026_parq/001/aksu0010/aksu_{sat}_001_26.parquet")
            sats = tec_glob_satellites(root, 2026, 1, "aksu")
            assert sats == sorted(sats)

    def test_does_not_include_absoltec_files(self):
        with tempfile.TemporaryDirectory() as root:
            _touch(root, "2026_parq/001/aksu0010/aksu_001_2026.parquet")  # AbsolTEC
            sats = tec_glob_satellites(root, 2026, 1, "aksu")
            assert sats == []


class TestTecDiscoverStations:

    def test_discovers_multiple_stations(self):
        with tempfile.TemporaryDirectory() as root:
            _touch(root, "2026_parq/001/aksu0010/aksu_E07_001_26.parquet")
            _touch(root, "2026_parq/001/armv001g33/armv_E07_001_26.parquet")
            stations = tec_discover_stations(root, 2026, 1)
            assert "aksu" in stations
            assert "armv" in stations

    def test_ignores_absoltec_files(self):
        with tempfile.TemporaryDirectory() as root:
            _touch(root, "2026_parq/001/aksu0010/aksu_001_2026.parquet")  # AbsolTEC
            stations = tec_discover_stations(root, 2026, 1)
            assert stations == []
