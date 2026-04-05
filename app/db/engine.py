"""
DuckDB engine and file-path helpers.

The key design change from the original PostgreSQL version is that station
folders can have variable suffixes (e.g. aksu0010, armv001g33, armv001k00).
Rather than constructing a fixed folder name, every find_* function globs
for '{station}*' subdirectories and locates the file by its known name.

File naming rules (confirmed from sample data):
  AbsolTEC  → {year}/{doy:03d}/{station}*/  {station}_{doy:03d}_{year}.parquet
  TEC-suite → {year}/{doy:03d}/{station}*/  {station}_{sat}_{doy:03d}_{year2d}.parquet

The folder suffix is intentionally ignored in all path construction.
"""

import glob
import os
import threading
from pathlib import Path

import duckdb

from app.config import settings

# ── Per-thread DuckDB connection ──────────────────────────────────────────────
_local = threading.local()


def get_connection() -> duckdb.DuckDBPyConnection:
    """Return a lazily-created, per-thread DuckDB in-memory connection."""
    if not hasattr(_local, "conn") or _local.conn is None:
        _local.conn = duckdb.connect(database=":memory:")
    return _local.conn


# ──────────────────────────────────────────────────────────────────────────────
# AbsolTEC helpers
# ──────────────────────────────────────────────────────────────────────────────
def find_absoltec_file(
    data_root: str, year: int, doy: int, station: str
) -> "str | None":
    """
    Locate an AbsolTEC parquet file without knowing the folder suffix.

    The folder name can be anything starting with the station code:
      aksu0010, armv001g33, armv001k00, …

    The file name inside is always predictable:
      {station}_{doy:03d}_{year}.parquet

    sorted() makes results deterministic when multiple matching folders exist.
    """
    doy_dir = os.path.join(data_root, f"{year}", f"{doy:03d}")
    station_lower = station.lower()
    filename = f"{station_lower}_{doy:03d}_{year}.parquet"

    pattern = os.path.join(doy_dir, f"{station_lower}*", filename)
    matches = sorted(glob.glob(pattern))
    return matches[0] if matches else None


def absoltec_glob_files(
    data_root: str, year: int, doy_start: int, doy_end: int, station: str
) -> list:
    """
    Return existing AbsolTEC parquet paths over [doy_start, doy_end].
    Days with no file are silently skipped.
    """
    paths = []
    for doy in range(doy_start, doy_end + 1):
        p = find_absoltec_file(data_root, year, doy, station)
        if p is not None:
            paths.append(p)
    return paths


def absoltec_discover_stations(data_root: str, year: int, doy: int) -> list:
    """
    Scan the doy directory and return station codes that have AbsolTEC files.

    AbsolTEC files have exactly 3 '_'-split stem segments (station_doy_year),
    which distinguishes them from TEC-suite files (4 segments).
    """
    pattern = os.path.join(
        data_root, f"{year}", f"{doy:03d}", "*",
        f"*_{doy:03d}_{year}.parquet"
    )
    stations: set = set()
    for path in glob.glob(pattern):
        parts = Path(path).stem.split("_")
        if len(parts) == 3:
            stations.add(parts[0].lower())
    return sorted(stations)


def absoltec_discover_days(data_root: str, year: int, station: str) -> list:
    """List all days-of-year for which a station has AbsolTEC data."""
    station_lower = station.lower()
    pattern = os.path.join(
        data_root, f"{year}", "*",
        f"{station_lower}*",
        f"{station_lower}_*_{year}.parquet"
    )
    days: set = set()
    for path in glob.glob(pattern):
        parts = Path(path).stem.split("_")
        if len(parts) == 3:
            try:
                days.add(int(parts[1]))
            except ValueError:
                pass
    return sorted(days)


# ──────────────────────────────────────────────────────────────────────────────
# TEC-suite helpers
# ──────────────────────────────────────────────────────────────────────────────

def _tec_station_folder_prefix(station: str) -> str:
    """
    Return folder prefix used by TEC-suite day folders.

    Some converters write filenames with extended station codes (e.g. arskm39,
    aksui14), while day folder names are based on the 4-letter site root plus
    extra parts (e.g. arsk001m39, aksu001i14). For folder lookup, use only the
    first 4 letters when the station code is longer than 4.
    """
    station_lower = station.lower()
    if len(station_lower) > 4:
        return station_lower[:4]
    return station_lower

def find_tec_file(
    data_root: str, year: int, doy: int, station: str, satellite: str
) -> "str | None":
    """
    Locate a TEC-suite satellite parquet file using the same folder-agnostic
    approach as find_absoltec_file.

    File name: {station_prefix}_{satellite}_{doy:03d}_{year2d}.parquet
    Example:   arsk_G01_001_16.parquet (inside folder arsk0010/)
    """
    doy_dir = os.path.join(data_root, f"{year}", f"{doy:03d}")
    year2d = str(year)[-2:]
    folder_prefix = _tec_station_folder_prefix(station)
    filename = f"{folder_prefix}_{satellite}_{doy:03d}_{year2d}.parquet"
    pattern = os.path.join(doy_dir, f"{folder_prefix}*", filename)
    matches = sorted(glob.glob(pattern))
    return matches[0] if matches else None


def tec_glob_satellites(
    data_root: str, year: int, doy: int, station: str
) -> list:
    """
    Return sorted satellite codes available for a station on a given day.

    TEC files have 4 '_'-split stem segments: station_satellite_doy_year2d.
    Satellite code is segment index 1.
    """
    year2d = str(year)[-2:]
    folder_prefix = _tec_station_folder_prefix(station)
    pattern = os.path.join(
        data_root, f"{year}", f"{doy:03d}",
        f"{folder_prefix}*",
        f"{folder_prefix}_*_{doy:03d}_{year2d}.parquet"
    )
    satellites: set = set()
    for path in glob.glob(pattern):
        parts = Path(path).stem.split("_")
        if len(parts) == 4:
            satellites.add(parts[1])
    return sorted(satellites)


def tec_discover_stations(data_root: str, year: int, doy: int) -> list:
    """Return station codes that have TEC-suite files for a given year/doy."""
    year2d = str(year)[-2:]
    pattern = os.path.join(
        data_root, f"{year}", f"{doy:03d}", "*",
        f"*_*_{doy:03d}_{year2d}.parquet"
    )
    stations: set = set()
    for path in glob.glob(pattern):
        parts = Path(path).stem.split("_")
        if len(parts) == 4:
            stations.add(parts[0].lower())
    return sorted(stations)
