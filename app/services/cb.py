"""
CB (Coherence Band) data service.

CB is calculated from AbsolTEC data using the formula:
cb = sqrt(4*3*10^8 * 1^3 * 10^27) / sqrt(80.5 * π * abs_tec * 10^16)

CB is undefined for a missing AbsolTEC sample (I_v <= 0, see
app.services.absoltec): raw reads return cb=None and statistics skip it.

Statistics formulas (identical to AbsolTEC), over valid samples only:
  mean     = AVG(cb)
  variance = VAR_SAMP(cb)    — sample variance, denominator N-1
  std_dev  = STDDEV_SAMP(cb)
  student_ci = t_ppf(1-α/2, df=N-1) × s / √N
variance, std_dev and student_ci are None when N < 2.

We also aggregate mean SIP coordinates (G_lon, G_lat) alongside the CB
statistics so callers can draw spatial plots of where measurements were made.
"""

import logging
import math
import os
from typing import Optional

import pandas as pd
from scipy.stats import t as student_t

logger = logging.getLogger(__name__)

from app.config import settings
from app.db.columns import UT, I_V, G_LON, G_LAT, G_Q_LON, G_Q_LAT, G_T, G_Q_T
from app.db.engine import (
    get_connection,
    find_absoltec_file,
    absoltec_glob_files,
)
from app.services.absoltec import VALID_TEC_SQL, files_with_valid_tec
from app.services.absoltec import get_raw_data as get_absoltec_raw_data
from app.services.absoltec import get_raw_data_range as get_absoltec_raw_data_range
from app.models.schemas import (
    TimeSeriesPointCB,
    StatisticsPointCB,
    StatisticsResponseCB,
    PerStationStatisticsResponseCB,
)


# SQL form of _calculate_cb(); only meaningful on rows matching VALID_TEC_SQL.
_CB_SQL = f'SQRT(4*3*POWER(10, 8)*POWER(1, 3)*POWER(10, 27)) / SQRT(80.5*PI()*"{I_V}"*POWER(10, 16))'


def _calculate_cb(tec: Optional[float]) -> Optional[float]:
    """Calculate Coherence Band from TEC value; None for a missing sample (None or <= 0)."""
    if tec is None or tec <= 0:
        return None
    numerator = math.sqrt(4 * 3 * (10 ** 8) * (1 ** 3) * (10 ** 27))
    denominator = math.sqrt(80.5 * math.pi * tec * (10 ** 16))
    return numerator / denominator


def calculate_cb(tec: Optional[float]) -> Optional[float]:
    """Public CB helper used by routers/plotting to keep CB formula consistent."""
    return _calculate_cb(tec)


def _opt_float(value) -> Optional[float]:
    """Convert pandas value to float or None if NaN."""
    if pd.isna(value):
        return None
    return float(value)


def _build_stats_points_cb(df: pd.DataFrame, alpha: float) -> list[StatisticsPointCB]:
    """Build StatisticsPointCB list from aggregated DataFrame; spread is None when n < 2."""
    points = []
    for _, row in df.iterrows():
        n = int(row["n"])
        variance = _opt_float(row.get("variance")) if n > 1 else None
        std_dev = _opt_float(row.get("std_dev")) if n > 1 else None
        if std_dev is None:
            # Cannot compute CI with < 2 samples
            student_ci = None
        else:
            # Student-t critical value for the two-tailed CI at alpha
            t_crit = student_t.ppf(1 - alpha / 2, df=n - 1)
            student_ci = t_crit * std_dev / math.sqrt(n)

        points.append(StatisticsPointCB(
            ut=float(row[UT]),
            mean_cb=float(row["mean_cb"]),
            variance=variance,
            std_dev=std_dev,
            student_ci=student_ci,
            n=n,
            mean_g_lon=_opt_float(row.get("mean_g_lon")),
            mean_g_lat=_opt_float(row.get("mean_g_lat")),
        ))
    return points


# ──────────────────────────────────────────────────────────────────────────────
# Raw data access
# ──────────────────────────────────────────────────────────────────────────────

def get_raw_data_cb(
    year: int,
    doy: int,
    station: str,
    data_root: Optional[str] = None,
) -> list[TimeSeriesPointCB]:
    """
    Return the full time series with CB calculated from one AbsolTEC parquet file.

    All 8 columns are fetched, CB is calculated from I_v. Optional fields will be None
    if they contain NaN in the parquet — Pydantic handles this gracefully.
    Returns an empty list when the file does not exist.
    """
    root = data_root or settings.data_root
    abs_points = get_absoltec_raw_data(year, doy, station, root)
    if not abs_points:
        return []

    # Quoted column names are mandatory here: "I_v" ≠ "i_v" in DuckDB
    return [
        TimeSeriesPointCB(
            ut=float(p.ut),
            tec=p.tec,
            cb=calculate_cb(p.tec),
            g_lon=_opt_float(getattr(p, "g_lon", None)),
            g_lat=_opt_float(getattr(p, "g_lat", None)),
            g_q_lon=_opt_float(getattr(p, "g_q_lon", None)),
            g_q_lat=_opt_float(getattr(p, "g_q_lat", None)),
            g_t=_opt_float(getattr(p, "g_t", None)),
            g_q_t=_opt_float(getattr(p, "g_q_t", None)),
        )
        for p in abs_points
    ]


def get_raw_data_range_cb(
    year: int,
    doy_start: int,
    doy_end: int,
    stations: list[str],
    data_root: Optional[str] = None,
) -> list[dict]:
    """
    Raw CB rows concatenated day-by-day for one or more stations.

    Time continuity across days is represented by `concat_ut`:
      concat_ut = (doy - doy_start) * 24 + ut
    """
    # Reuse AbsolTEC raw range extraction and derive CB from its `tec` values.
    # This guarantees that if AbsolTEC raw exists, CB raw exists for the same
    # filters, and keeps the file-discovery/path logic in one place.
    base_rows = get_absoltec_raw_data_range(
        year=year,
        doy_start=doy_start,
        doy_end=doy_end,
        stations=stations,
        data_root=data_root,
    )
    for row in base_rows:
        row["cb"] = calculate_cb(row.get("tec"))
    base_rows.sort(key=lambda r: (r.get("station", ""), r.get("concat_ut", 0.0)))
    return base_rows


# ──────────────────────────────────────────────────────────────────────────────
# Statistics
# ──────────────────────────────────────────────────────────────────────────────

def compute_statistics_cb(
    year: int,
    doy_start: int,
    doy_end: int,
    station: str,
    alpha: float = settings.default_alpha,
    data_root: Optional[str] = None,
) -> StatisticsResponseCB:
    """
    Compute per-slot CB statistics for a station over a day range.

    DuckDB reads all matching parquet files in a single query by accepting
    a list literal in read_parquet([file1, file2, ...]). This replaces the
    day-by-day loop with a single GROUP BY on CB values.
    """
    root = data_root or settings.data_root
    files = absoltec_glob_files(root, year, doy_start, doy_end, station)

    if not files:
        logger.warning(
            "cb stats: no AbsolTEC files for %s %d doy %d-%d under %s",
            station, year, doy_start, doy_end, root,
        )
        return StatisticsResponseCB(
            year=year, doy_start=doy_start, doy_end=doy_end,
            station=station, alpha=alpha, total_days=0, points=[],
        )

    logger.info(
        "cb stats: %s %d doy %d-%d — querying %d file(s)",
        station, year, doy_start, doy_end, len(files),
    )
    df = _aggregate_cb_by_ut(files)

    return StatisticsResponseCB(
        year=year, doy_start=doy_start, doy_end=doy_end,
        station=station, alpha=alpha, total_days=len(files_with_valid_tec(files)),
        points=_build_stats_points_cb(df, alpha),
    )


def _aggregate_cb_by_ut(files: list[str]) -> pd.DataFrame:
    """Per-UT aggregate of CB over valid I_v samples across files; slots without one are omitted."""
    file_list_sql = "[" + ", ".join(f"'{f}'" for f in files) + "]"
    conn = get_connection()
    return conn.execute(f"""
        SELECT
            "{UT}",
            AVG({_CB_SQL})          AS mean_cb,
            VAR_SAMP({_CB_SQL})     AS variance,
            STDDEV_SAMP({_CB_SQL})  AS std_dev,
            COUNT(*)                AS n,
            AVG("{G_LON}")          AS mean_g_lon,
            AVG("{G_LAT}")          AS mean_g_lat
        FROM read_parquet({file_list_sql})
        WHERE {VALID_TEC_SQL}
        GROUP BY "{UT}"
        ORDER BY "{UT}"
    """).df()


# ──────────────────────────────────────────────────────────────────────────────
# Statistics — multiple stations averaged per day
# ──────────────────────────────────────────────────────────────────────────────

def compute_statistics_per_station_day_cb(
    year: int,
    doy_start: int,
    doy_end: int,
    stations: list[str],
    alpha: float = settings.default_alpha,
    data_root: Optional[str] = None,
) -> list[PerStationStatisticsResponseCB]:
    """
    Per-day CB statistics averaged across a list of stations.

    For each day in the range, compute CB statistics across all stations
    that have valid data for that day, pooling their samples per UT slot the
    same way compute_statistics_per_station_day() does for TEC.
    """
    root = data_root or settings.data_root
    responses = []

    for doy in range(doy_start, doy_end + 1):
        day_files, day_stations = [], []
        for station in stations:
            path = find_absoltec_file(root, year, doy, station)
            if path is not None:
                day_files.append(path)
                day_stations.append(station)

        # A station whose file holds only placeholders has no data that day.
        valid_files = files_with_valid_tec(day_files)
        stations_found = [s for s, f in zip(day_stations, day_files) if f in valid_files]
        day_files = [f for f in day_files if f in valid_files]
        if not day_files:
            continue

        responses.append(PerStationStatisticsResponseCB(
            year=year,
            doy=doy,
            stations_found=stations_found,
            alpha=alpha,
            points=_build_stats_points_cb(_aggregate_cb_by_ut(day_files), alpha),
        ))

    return responses
