"""
AbsolTEC data service.

SQL column references use the exact casing from the parquet schema — DuckDB
is case-sensitive for column names inside read_parquet(), so 'UT' and 'ut'
are different columns. All column names come from app.db.columns to avoid
typos and ensure a single source of truth.

Statistics formulas (identical to the original Count_statistics()):
  mean     = AVG("I_v")
  variance = VAR_POP("I_v")    — population variance, denominator N
  std_dev  = STDDEV_POP("I_v")
  student_ci = t_ppf(1-α/2, df=N-1) × σ / √N

We also aggregate mean SIP coordinates (G_lon, G_lat) alongside the TEC
statistics so callers can draw spatial plots of where measurements were made.
"""

import math
import os
from typing import Optional

import pandas as pd
from scipy.stats import t as student_t

from app.config import settings
from app.db.columns import UT, I_V, G_LON, G_LAT, G_Q_LON, G_Q_LAT, G_T, G_Q_T
from app.db.engine import (
    get_connection,
    find_absoltec_file,
    absoltec_glob_files,
)
from app.models.schemas import (
    TimeSeriesPoint,
    StatisticsPoint,
    StatisticsResponse,
    PerStationStatisticsResponse,
)


# ──────────────────────────────────────────────────────────────────────────────
# Raw data access
# ──────────────────────────────────────────────────────────────────────────────

def get_raw_data(
    year: int,
    doy: int,
    station: str,
    data_root: Optional[str] = None,
) -> list[TimeSeriesPoint]:
    """
    Return the full time series from one AbsolTEC parquet file.

    All 8 columns are fetched. Optional fields (G_lon etc.) will be None
    if they contain NaN in the parquet — Pydantic handles this gracefully.
    Returns an empty list when the file does not exist.
    """
    root = data_root or settings.data_root
    path = find_absoltec_file(root, year, doy, station)
    if path is None:
        return []

    conn = get_connection()
    # Quoted column names are mandatory here: "I_v" ≠ "i_v" in DuckDB
    df: pd.DataFrame = conn.execute(f"""
        SELECT
            "{UT}",
            "{I_V}",
            "{G_LON}",
            "{G_LAT}",
            "{G_Q_LON}",
            "{G_Q_LAT}",
            "{G_T}",
            "{G_Q_T}"
        FROM read_parquet('{path}')
        ORDER BY "{UT}"
    """).df()

    points = []
    for _, row in df.iterrows():
        points.append(TimeSeriesPoint(
            ut=float(row[UT]),
            tec=float(row[I_V]),
            g_lon=_opt_float(row.get(G_LON)),
            g_lat=_opt_float(row.get(G_LAT)),
            g_q_lon=_opt_float(row.get(G_Q_LON)),
            g_q_lat=_opt_float(row.get(G_Q_LAT)),
            g_t=_opt_float(row.get(G_T)),
            g_q_t=_opt_float(row.get(G_Q_T)),
        ))
    return points


# ──────────────────────────────────────────────────────────────────────────────
# Statistics — single station, range of days
# ──────────────────────────────────────────────────────────────────────────────

def compute_statistics(
    year: int,
    doy_start: int,
    doy_end: int,
    station: str,
    alpha: float = settings.default_alpha,
    data_root: Optional[str] = None,
) -> StatisticsResponse:
    """
    Compute per-slot statistics for a station over a day range.

    DuckDB reads all matching parquet files in a single query by accepting
    a list literal in read_parquet([file1, file2, ...]). This replaces the
    day-by-day loop in the original Count_statistics() with a single GROUP BY.
    """
    root = data_root or settings.data_root
    files = absoltec_glob_files(root, year, doy_start, doy_end, station)

    if not files:
        return StatisticsResponse(
            year=year, doy_start=doy_start, doy_end=doy_end,
            station=station, alpha=alpha, total_days=0, points=[],
        )

    file_list_sql = "[" + ", ".join(f"'{f}'" for f in files) + "]"
    conn = get_connection()
    df: pd.DataFrame = conn.execute(f"""
        SELECT
            "{UT}",
            AVG("{I_V}")         AS mean_tec,
            VAR_POP("{I_V}")     AS variance,
            STDDEV_POP("{I_V}")  AS std_dev,
            COUNT(*)             AS n,
            AVG("{G_LON}")       AS mean_g_lon,
            AVG("{G_LAT}")       AS mean_g_lat
        FROM read_parquet({file_list_sql})
        GROUP BY "{UT}"
        ORDER BY "{UT}"
    """).df()

    return StatisticsResponse(
        year=year, doy_start=doy_start, doy_end=doy_end,
        station=station, alpha=alpha, total_days=len(files),
        points=_build_stats_points(df, alpha),
    )


# ──────────────────────────────────────────────────────────────────────────────
# Statistics — multiple stations averaged per day
# ──────────────────────────────────────────────────────────────────────────────

def compute_statistics_per_station_day(
    year: int,
    doy_start: int,
    doy_end: int,
    stations: list,
    alpha: float = settings.default_alpha,
    data_root: Optional[str] = None,
) -> list[PerStationStatisticsResponse]:
    """
    For each day in the range, average TEC across all stations with data
    on that day.  One result per day that has at least one station.
    """
    root = data_root or settings.data_root
    results = []

    for doy in range(doy_start, doy_end + 1):
        day_files, day_stations = [], []
        for station in stations:
            p = find_absoltec_file(root, year, doy, station)
            if p is not None:
                day_files.append(p)
                day_stations.append(station)

        if not day_files:
            continue

        file_list_sql = "[" + ", ".join(f"'{f}'" for f in day_files) + "]"
        conn = get_connection()
        df: pd.DataFrame = conn.execute(f"""
            SELECT
                "{UT}",
                AVG("{I_V}")         AS mean_tec,
                VAR_POP("{I_V}")     AS variance,
                STDDEV_POP("{I_V}")  AS std_dev,
                COUNT(*)             AS n,
                AVG("{G_LON}")       AS mean_g_lon,
                AVG("{G_LAT}")       AS mean_g_lat
            FROM read_parquet({file_list_sql})
            GROUP BY "{UT}"
            ORDER BY "{UT}"
        """).df()

        results.append(PerStationStatisticsResponse(
            year=year, doy=doy, stations_found=day_stations, alpha=alpha,
            points=_build_stats_points(df, alpha),
        ))

    return results


# ──────────────────────────────────────────────────────────────────────────────
# Internal helpers
# ──────────────────────────────────────────────────────────────────────────────

def _build_stats_points(df: pd.DataFrame, alpha: float) -> list[StatisticsPoint]:
    """
    Convert a DuckDB aggregate result into a list of StatisticsPoint objects,
    adding the Student-t confidence interval.

    The DataFrame column for UT is named 'UT' (the actual parquet column name),
    while the aggregated columns have lowercase aliases assigned in the SQL.
    """
    points = []
    for _, row in df.iterrows():
        n = int(row["n"])
        std_dev  = _safe_float(row, "std_dev")
        mean_tec = _safe_float(row, "mean_tec")
        variance = _safe_float(row, "variance")

        if n > 1:
            t_critical = student_t.ppf(1.0 - alpha / 2.0, df=n - 1)
            ci = t_critical * std_dev / math.sqrt(n)
        else:
            ci = 0.0

        points.append(StatisticsPoint(
            ut=float(row[UT]),
            mean_tec=round(mean_tec, 3),
            variance=round(variance, 5),
            std_dev=round(std_dev, 5),
            student_ci=round(ci, 5),
            n=n,
            mean_g_lon=_opt_float(row.get("mean_g_lon")),
            mean_g_lat=_opt_float(row.get("mean_g_lat")),
        ))
    return points


def _safe_float(row, key: str, default: float = 0.0) -> float:
    """Extract a float from a DataFrame row, returning default for None/NaN."""
    v = row.get(key)
    if v is None:
        return default
    try:
        f = float(v)
        return f if not math.isnan(f) else default
    except (TypeError, ValueError):
        return default


def _opt_float(v) -> Optional[float]:
    """Return float or None, treating NaN as None."""
    if v is None:
        return None
    try:
        f = float(v)
        return None if math.isnan(f) else f
    except (TypeError, ValueError):
        return None
