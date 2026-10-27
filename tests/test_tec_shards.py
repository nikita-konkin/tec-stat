"""
TEC-suite session shards and station codes that end in "_".

tec-suite writes every session archive of a station (hourly files, or a
re-upload of the day) into one day folder; a file whose name is taken
becomes {stem}__dup{N}. A station delivered as 24 hourly archives therefore
has, per satellite, one base file and up to 23 shards of one hour each.
"""

import os

import pandas as pd

from app.db.engine import (
    absoltec_discover_days,
    absoltec_discover_stations,
    find_tec_files,
    tec_discover_stations,
    tec_glob_satellites,
)
from app.services.tec import get_tec_data


def _tec_parquet(path, hours):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    pd.DataFrame(
        {
            "tsn": [int(h * 120) for h in hours],
            "hour": [float(h) for h in hours],
            "el": [30.0] * len(hours),
            "az": [90.0] * len(hours),
            "tec.l1l2": [10.0 + h for h in hours],
            "tec.c1p2": [20.0 + h for h in hours],
            "validity": [0] * len(hours),
        }
    ).to_parquet(path)


def _touch(path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    open(path, "wb").close()


def test_find_tec_files_returns_base_then_shards_in_numeric_order(tmp_path):
    day = tmp_path / "2025" / "001" / "moks"
    for name in ("moks_G05_001_25.parquet", "moks_G05_001_25__dup2.parquet",
                 "moks_G05_001_25__dup10.parquet", "moks_G05_001_25__dup1.parquet",
                 "moks_G13_001_25__dup1.parquet", "moks_G13_001_25.parquet"):
        _touch(day / name)

    names = [os.path.basename(p) for p in find_tec_files(str(tmp_path), 2025, 1, "moks", "G05")]

    assert names == [
        "moks_G05_001_25.parquet",
        "moks_G05_001_25__dup1.parquet",
        "moks_G05_001_25__dup2.parquet",
        "moks_G05_001_25__dup10.parquet",
    ]
    assert find_tec_files(str(tmp_path), 2025, 1, "moks", "R01") == []


def test_get_tec_data_merges_session_shards(tmp_path):
    """Each hourly session lands in its own shard; all of them are the day's data."""
    day = tmp_path / "2025" / "001" / "moks"
    _tec_parquet(str(day / "moks_G05_001_25.parquet"), [5.0, 5.5])
    _tec_parquet(str(day / "moks_G05_001_25__dup1.parquet"), [1.0, 1.5])
    # A re-uploaded session repeats rows the base file already has.
    _tec_parquet(str(day / "moks_G05_001_25__dup2.parquet"), [5.5, 9.0])

    result = get_tec_data(2025, 1, "moks", "G05", str(tmp_path))

    assert [p.hour for p in result.points] == [1.0, 1.5, 5.0, 5.5, 9.0]
    assert [p.tec_l1l2 for p in result.points] == [11.0, 11.5, 15.0, 15.5, 19.0]


def test_station_code_ending_in_underscore_is_discovered(tmp_path):
    """'ufa_' files split into an extra empty part on '_'; they were dropped."""
    tec_day = tmp_path / "tec" / "2025" / "001"
    _touch(tec_day / "ufa_" / "ufa__G05_001_25.parquet")
    _touch(tec_day / "ufa_" / "ufa__R10_001_25.parquet")
    _touch(tec_day / "aksu" / "aksu_G05_001_25.parquet")
    abs_day = tmp_path / "abs" / "2025" / "001"
    _touch(abs_day / "ufa_" / "ufa__001_2025.parquet")
    _touch(abs_day / "aksu0010" / "aksu_001_2025.parquet")

    assert tec_discover_stations(str(tmp_path / "tec"), 2025, 1) == ["aksu", "ufa_"]
    assert tec_glob_satellites(str(tmp_path / "tec"), 2025, 1, "ufa_") == ["G05", "R10"]
    assert absoltec_discover_stations(str(tmp_path / "abs"), 2025, 1) == ["aksu", "ufa_"]
    assert absoltec_discover_days(str(tmp_path / "abs"), 2025, "ufa_") == [1]


def test_prefix_station_does_not_borrow_files_of_a_longer_code(tmp_path):
    """Asking for 'ufa' must not return the satellites or days of 'ufa_'."""
    _touch(tmp_path / "tec" / "2025" / "001" / "ufa_" / "ufa__G05_001_25.parquet")
    _touch(tmp_path / "abs" / "2025" / "001" / "ufa_" / "ufa__001_2025.parquet")

    assert tec_glob_satellites(str(tmp_path / "tec"), 2025, 1, "ufa") == []
    assert absoltec_discover_days(str(tmp_path / "abs"), 2025, "ufa") == []


def test_shard_files_are_not_counted_as_extra_stations_or_satellites(tmp_path):
    day = tmp_path / "2025" / "001" / "moks"
    _touch(day / "moks_G05_001_25.parquet")
    _touch(day / "moks_G05_001_25__dup1.parquet")

    assert tec_discover_stations(str(tmp_path), 2025, 1) == ["moks"]
    assert tec_glob_satellites(str(tmp_path), 2025, 1, "moks") == ["G05"]
