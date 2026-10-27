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
import re
import threading
from pathlib import Path

import duckdb

from app.config import settings

# File stems, matched from the end: a station code may itself end in "_"
# (ufa_ → ufa__001_2025), which splitting on "_" miscounts.
_ABSOLTEC_STEM_RE = re.compile(r"^(?P<station>.+?)_(?P<doy>\d{3})_(?P<year>\d{4})$")
_TEC_STEM_RE = re.compile(r"^(?P<station>.+?)_(?P<satellite>[^_]+)_(?P<doy>\d{3})_(?P<year2d>\d{2})$")

# tec-suite writes each session (hourly or re-uploaded) archive of a station
# into the same day folder; a file whose name is already taken becomes
# {stem}__dup{N}.parquet, holding epochs the base file lacks.
_TEC_SHARD_RE = re.compile(r"__dup(\d+)\.parquet$")

# ── Per-thread DuckDB connection ──────────────────────────────────────────────
_local = threading.local()

def _candidate_year_dirs(data_root: str, year: int) -> list[str]:
    """
    Return candidate year directory names.

    Some datasets are stored as:
      {root}/{year}/{doy:03d}/...
    while others use:
      {root}/{year}_parq/{doy:03d}/...
    Support both to keep the API compatible across converter outputs.
    """
    return [
        os.path.join(data_root, f"{year}"),
        os.path.join(data_root, f"{year}_parq"),
    ]


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
    station_lower = station.lower()
    filename = f"{station_lower}_{doy:03d}_{year}.parquet"

    matches: list[str] = []
    for year_dir in _candidate_year_dirs(data_root, year):
        pattern = os.path.join(year_dir, f"{doy:03d}", f"{station_lower}*", filename)
        matches.extend(glob.glob(pattern))
    matches = sorted(set(matches))
    return matches[0] if matches else None


def absoltec_glob_files(
    data_root: str, year: int, doy_start: int, doy_end: int, station: str
) -> list:
    """
    Return existing AbsolTEC parquet paths over [doy_start, doy_end].
    Days with no file are silently skipped.
    """
    return absoltec_files_for_days(
        data_root, [(year, doy) for doy in range(doy_start, doy_end + 1)], station
    )


def absoltec_files_for_days(data_root: str, days: list, station: str) -> list:
    """
    Return existing AbsolTEC parquet paths for (year, doy) days, in day order.
    Days with no file are silently skipped.
    """
    paths = []
    for year, doy in days:
        p = find_absoltec_file(data_root, year, doy, station)
        if p is not None:
            paths.append(p)
    return paths


def absoltec_discover_stations(data_root: str, year: int, doy: int) -> list:
    """
    Scan the doy directory and return station codes that have AbsolTEC files.

    AbsolTEC stems are station_doy_year (4-digit year), which distinguishes
    them from TEC-suite stems station_satellite_doy_year2d.
    """
    patterns = [
        os.path.join(
            year_dir, f"{doy:03d}", "*", f"*_{doy:03d}_{year}.parquet"
        )
        for year_dir in _candidate_year_dirs(data_root, year)
    ]
    stations: set = set()
    for pattern in patterns:
        for path in glob.glob(pattern):
            match = _ABSOLTEC_STEM_RE.match(Path(path).stem)
            if match:
                stations.add(match["station"].lower())
    return sorted(stations)


def absoltec_discover_days(data_root: str, year: int, station: str) -> list:
    """List all days-of-year for which a station has AbsolTEC data."""
    station_lower = station.lower()
    patterns = [
        os.path.join(
            year_dir, "*", f"{station_lower}*", f"{station_lower}_*_{year}.parquet"
        )
        for year_dir in _candidate_year_dirs(data_root, year)
    ]
    days: set = set()
    for pattern in patterns:
        for path in glob.glob(pattern):
            match = _ABSOLTEC_STEM_RE.match(Path(path).stem)
            # The glob's "*" also matches longer codes sharing the prefix.
            if match and match["station"].lower() == station_lower:
                days.add(int(match["doy"]))
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
    year2d = str(year)[-2:]
    station_lower = station.lower()
    folder_prefix = _tec_station_folder_prefix(station)
    filename_prefixes = {folder_prefix, station_lower}
    matches: list[str] = []
    for year_dir in _candidate_year_dirs(data_root, year):
        for filename_prefix in filename_prefixes:
            filename = f"{filename_prefix}_{satellite}_{doy:03d}_{year2d}.parquet"
            pattern = os.path.join(year_dir, f"{doy:03d}", f"{folder_prefix}*", filename)
            matches.extend(glob.glob(pattern))
    matches = sorted(set(matches))
    return matches[0] if matches else None


def find_tec_files(
    data_root: str, year: int, doy: int, station: str, satellite: str
) -> list:
    """
    The satellite's TEC-suite file for the day plus its __dupN session shards,
    base file first. Reading only the base file keeps one session: for a
    station delivered as hourly archives that is about one hour of the day.
    """
    base = find_tec_file(data_root, year, doy, station, satellite)
    if base is None:
        return []
    shard_pattern = glob.escape(base[: -len(".parquet")]) + "__dup*.parquet"
    shards = [path for path in glob.glob(shard_pattern) if _TEC_SHARD_RE.search(path)]
    shards.sort(key=lambda path: int(_TEC_SHARD_RE.search(path).group(1)))
    return [base] + shards


def tec_glob_satellites(
    data_root: str, year: int, doy: int, station: str
) -> list:
    """
    Return sorted satellite codes available for a station on a given day.

    TEC file stems are station_satellite_doy_year2d.
    """
    year2d = str(year)[-2:]
    folder_prefix = _tec_station_folder_prefix(station)
    station_lower = station.lower()
    filename_prefixes = {folder_prefix, station_lower}
    patterns: list[str] = []
    for year_dir in _candidate_year_dirs(data_root, year):
        for filename_prefix in filename_prefixes:
            patterns.append(
                os.path.join(
                    year_dir,
                    f"{doy:03d}",
                    f"{folder_prefix}*",
                    f"{filename_prefix}_*_{doy:03d}_{year2d}.parquet",
                )
            )
    satellites: set = set()
    for pattern in patterns:
        for path in glob.glob(pattern):
            match = _TEC_STEM_RE.match(Path(path).stem)
            # "ufa_*" also matches files of a station named "ufa_" when asking for "ufa".
            if match and match["station"].lower() in filename_prefixes:
                satellites.add(match["satellite"])
    return sorted(satellites)


def tec_discover_stations(data_root: str, year: int, doy: int) -> list:
    """Return station codes that have TEC-suite files for a given year/doy."""
    year2d = str(year)[-2:]
    patterns = [
        os.path.join(
            year_dir, f"{doy:03d}", "*", f"*_*_{doy:03d}_{year2d}.parquet"
        )
        for year_dir in _candidate_year_dirs(data_root, year)
    ]
    stations: set = set()
    for pattern in patterns:
        for path in glob.glob(pattern):
            match = _TEC_STEM_RE.match(Path(path).stem)
            if match:
                stations.add(match["station"].lower())
    return sorted(stations)
