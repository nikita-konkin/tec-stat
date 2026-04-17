"""
CB (Coherence Band) data service.

CB is calculated from AbsolTEC data using the formula:
cb = sqrt(4*3*10^8 * 1^3 * 10^27) / sqrt(80.5 * π * abs_tec * 10^16)

Statistics formulas (identical to AbsolTEC):
  mean     = AVG(cb)
  variance = VAR_POP(cb)    — population variance, denominator N
  std_dev  = STDDEV_POP(cb)
  student_ci = t_ppf(1-α/2, df=N-1) × σ / √N

We also aggregate mean SIP coordinates (G_lon, G_lat) alongside the CB
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
    TimeSeriesPointCB,
    StatisticsPointCB,
    StatisticsResponseCB,
    PerStationStatisticsResponseCB,
)


def _calculate_cb(tec: float) -> float:
    """Calculate Coherence Band from TEC value."""
    if tec <= 0:
        return 0.0
    numerator = math.sqrt(4 * 3 * (10 ** 8) * (1 ** 3) * (10 ** 27))
    denominator = math.sqrt(80.5 * math.pi * tec * (10 ** 16))
    return numerator / denominator


def _opt_float(value) -> Optional[float]:
    """Convert pandas value to float or None if NaN."""
    if pd.isna(value):
        return None
    return float(value)


def _build_stats_points_cb(df: pd.DataFrame, alpha: float) -> list[StatisticsPointCB]:
    """Build StatisticsPointCB list from aggregated DataFrame."""
    points = []
    for _, row in df.iterrows():
        n = int(row["n"])
        if n < 2:
            # Cannot compute CI with < 2 samples
            student_ci = 0.0
        else:
            # Student-t critical value for 95% CI (two-tailed)
            t_crit = student_t.ppf(1 - alpha / 2, df=n - 1)
            student_ci = t_crit * float(row["std_dev"]) / math.sqrt(n)

        points.append(StatisticsPointCB(
            ut=float(row[UT]),
            mean_cb=float(row["mean_cb"]),
            variance=float(row["variance"]),
            std_dev=float(row["std_dev"]),
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
        tec = float(row[I_V])
        cb = _calculate_cb(tec)
        points.append(TimeSeriesPointCB(
            ut=float(row[UT]),
            tec=tec,
            cb=cb,
            g_lon=_opt_float(row.get(G_LON)),
            g_lat=_opt_float(row.get(G_LAT)),
            g_q_lon=_opt_float(row.get(G_Q_LON)),
            g_q_lat=_opt_float(row.get(G_Q_LAT)),
            g_t=_opt_float(row.get(G_T)),
            g_q_t=_opt_float(row.get(G_Q_T)),
        ))
    return points


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
    root = data_root or settings.data_root
    all_rows = []

    for station in stations:
        station_lower = station.lower()
        for doy in range(doy_start, doy_end + 1):
            points = get_raw_data_cb(year, doy, station_lower, root)
            for point in points:
                all_rows.append({
                    "year": year,
                    "station": station_lower,
                    "doy": doy,
                    "ut": point.ut,
                    "concat_ut": float((doy - doy_start) * 24 + point.ut),
                    "tec": point.tec,
                    "cb": point.cb,
                    "g_lon": point.g_lon,
                    "g_lat": point.g_lat,
                    "g_q_lon": point.g_q_lon,
                    "g_q_lat": point.g_q_lat,
                    "g_t": point.g_t,
                    "g_q_t": point.g_q_t,
                })

    return sorted(all_rows, key=lambda r: (r["station"], r["concat_ut"]))


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
        return StatisticsResponseCB(
            year=year, doy_start=doy_start, doy_end=doy_end,
            station=station, alpha=alpha, total_days=0, points=[],
        )

    file_list_sql = "[" + ", ".join(f"'{f}'" for f in files) + "]"
    conn = get_connection()
    df: pd.DataFrame = conn.execute(f"""
        SELECT
            "{UT}",
            AVG(CASE WHEN "{I_V}" > 0 THEN
                SQRT(4*3*POWER(10, 8)*POWER(1, 3)*POWER(10, 27)) /
                SQRT(80.5*PI()*"{I_V}"*POWER(10, 16))
            ELSE 0 END) AS mean_cb,
            VAR_POP(CASE WHEN "{I_V}" > 0 THEN
                SQRT(4*3*POWER(10, 8)*POWER(1, 3)*POWER(10, 27)) /
                SQRT(80.5*PI()*"{I_V}"*POWER(10, 16))
            ELSE 0 END) AS variance,
            STDDEV_POP(CASE WHEN "{I_V}" > 0 THEN
                SQRT(4*3*POWER(10, 8)*POWER(1, 3)*POWER(10, 27)) /
                SQRT(80.5*PI()*"{I_V}"*POWER(10, 16))
            ELSE 0 END) AS std_dev,
            COUNT(*) AS n,
            AVG("{G_LON}") AS mean_g_lon,
            AVG("{G_LAT}") AS mean_g_lat
        FROM read_parquet({file_list_sql})
        GROUP BY "{UT}"
        ORDER BY "{UT}"
    """).df()

    return StatisticsResponseCB(
        year=year, doy_start=doy_start, doy_end=doy_end,
        station=station, alpha=alpha, total_days=len(files),
        points=_build_stats_points_cb(df, alpha),
    )


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

    For each day in the range, compute the average CB across all stations
    that have data for that day.
    """
    root = data_root or settings.data_root
    responses = []

    for doy in range(doy_start, doy_end + 1):
        day_points = []
        stations_found = []

        for station in stations:
            path = find_absoltec_file(root, year, doy, station)
            if path is None:
                continue

            stations_found.append(station)
            conn = get_connection()
            df: pd.DataFrame = conn.execute(f"""
                SELECT
                    "{UT}",
                    CASE WHEN "{I_V}" > 0 THEN
                        SQRT(4*3*POWER(10, 8)*POWER(1, 3)*POWER(10, 27)) /
                        SQRT(80.5*PI()*"{I_V}"*POWER(10, 16))
                    ELSE 0 END AS cb,
                    "{G_LON}",
                    "{G_LAT}"
                FROM read_parquet('{path}')
                ORDER BY "{UT}"
            """).df()

            # Aggregate by UT
            grouped = df.groupby(UT).agg({
                'cb': ['mean', 'var', 'std', 'count'],
                G_LON: 'mean',
                G_LAT: 'mean'
            }).reset_index()

            for _, row in grouped.iterrows():
                n = int(row[('cb', 'count')])
                if n < 2:
                    student_ci = 0.0
                else:
                    t_crit = student_t.ppf(1 - alpha / 2, df=n - 1)
                    student_ci = t_crit * float(row[('cb', 'std')]) / math.sqrt(n)

                day_points.append(StatisticsPointCB(
                    ut=float(row[UT]),
                    mean_cb=float(row[('cb', 'mean')]),
                    variance=float(row[('cb', 'var')]),
                    std_dev=float(row[('cb', 'std')]),
                    student_ci=student_ci,
                    n=n,
                    mean_g_lon=_opt_float(row[(G_LON, 'mean')]),
                    mean_g_lat=_opt_float(row[(G_LAT, 'mean')]),
                ))

        if day_points:
            # Average across stations for each UT
            ut_groups = {}
            for point in day_points:
                if point.ut not in ut_groups:
                    ut_groups[point.ut] = []
                ut_groups[point.ut].append(point)

            averaged_points = []
            for ut, points in ut_groups.items():
                mean_cb = sum(p.mean_cb for p in points) / len(points)
                variance = sum(p.variance for p in points) / len(points)
                std_dev = sum(p.std_dev for p in points) / len(points)
                student_ci = sum(p.student_ci for p in points) / len(points)
                n = sum(p.n for p in points)
                mean_g_lon = sum(p.mean_g_lon or 0 for p in points) / len(points) if points[0].mean_g_lon else None
                mean_g_lat = sum(p.mean_g_lat or 0 for p in points) / len(points) if points[0].mean_g_lat else None

                averaged_points.append(StatisticsPointCB(
                    ut=ut,
                    mean_cb=mean_cb,
                    variance=variance,
                    std_dev=std_dev,
                    student_ci=student_ci,
                    n=n,
                    mean_g_lon=mean_g_lon,
                    mean_g_lat=mean_g_lat,
                ))

            responses.append(PerStationStatisticsResponseCB(
                year=year,
                doy=doy,
                stations_found=stations_found,
                alpha=alpha,
                points=sorted(averaged_points, key=lambda p: p.ut),
            ))

    return responses
