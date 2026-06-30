"""
Propagation-parameter service.

This module derives physical propagation parameters from TEC and working
frequency using:

  N_t = TEC * 10^16
  B_k = sqrt((3*10^8 * f^3) / (80.5 * pi * N_t))
  GDD = -(3 * 80.5 * N_t) / (2 * 3*10^8 * pi * f^3)

`f` is expected in Hz. TEC inputs are expected in TECU.

For raw dataset endpoints, non-positive TEC values are preserved in the source
field but treated as invalid for derived propagation parameters, so `b_k` and
`gdd` are returned as None. For statistics, only strictly positive TEC samples
participate in the aggregate.
"""

import math
from typing import Literal, Optional

import pandas as pd
from scipy.stats import t as student_t

from app.config import settings
from app.db.columns import UT, I_V, G_LON, G_LAT
from app.db.engine import absoltec_glob_files
from app.models.schemas import (
    PropagationDirectResponse,
    PropagationPointAbsoltec,
    PropagationStatisticsPoint,
    PropagationStatisticsResponse,
    PropagationTecPoint,
    PropagationTecResponse,
)
from app.services.absoltec import get_raw_data as get_absoltec_raw_data
from app.services.tec import get_tec_data
from app.db.engine import get_connection

LIGHT_SPEED = 3.0 * (10 ** 8)
TEC_TO_NT = 10.0 ** 16
PROPAGATION_COEFF = 80.5

TecObservable = Literal["tec_l1l2", "tec_c1p2"]
SignalBandName = Literal[
    "GPS_L1",
    "GPS_L2",
    "GPS_L5",
    "GAL_E1",
    "GAL_E5A",
    "GAL_E5B",
    "GAL_E5",
    "BDS_B1I",
    "BDS_B1C",
    "BDS_B2A",
    "BDS_B2I",
]

SIGNAL_BAND_FREQUENCIES_HZ: dict[SignalBandName, float] = {
    "GPS_L1": 1575.42e6,
    "GPS_L2": 1227.60e6,
    "GPS_L5": 1176.45e6,
    "GAL_E1": 1575.42e6,
    "GAL_E5A": 1176.45e6,
    "GAL_E5B": 1207.14e6,
    "GAL_E5": 1191.795e6,
    "BDS_B1I": 1561.098e6,
    "BDS_B1C": 1575.42e6,
    "BDS_B2A": 1176.45e6,
    "BDS_B2I": 1207.14e6,
}
SUPPORTED_SIGNAL_BANDS = tuple(SIGNAL_BAND_FREQUENCIES_HZ.keys())


def normalize_signal_band(signal_band: Optional[str]) -> Optional[SignalBandName]:
    """Normalize a user-provided signal-band preset name."""
    if not signal_band:
        return None
    normalized = signal_band.strip().upper().replace("-", "_").replace(" ", "_")
    if normalized in SIGNAL_BAND_FREQUENCIES_HZ:
        return normalized  # type: ignore[return-value]
    return None


def resolve_frequency(
    f_hz: Optional[float],
    signal_band: Optional[str],
) -> tuple[float, Optional[SignalBandName]]:
    """
    Resolve the effective working frequency.

    Explicit `f_hz` takes priority over a preset because the caller may need a
    non-standard value. In that case, signal_band is not echoed in responses
    because frequency lookup did not use the preset.
    """
    if f_hz is not None:
        if f_hz <= 0:
            raise ValueError("f_hz must be positive")
        return float(f_hz), None

    normalized_band = normalize_signal_band(signal_band)
    if normalized_band is None:
        supported = ", ".join(SUPPORTED_SIGNAL_BANDS)
        raise ValueError(
            "Provide either a positive f_hz or a supported signal_band "
            f"({supported})"
        )

    return float(SIGNAL_BAND_FREQUENCIES_HZ[normalized_band]), normalized_band


def tec_to_nt(tec: float) -> Optional[float]:
    """Convert TECU to absolute total electron content N_t."""
    if tec < 0:
        return None
    return tec * TEC_TO_NT


def calculate_b_k(nt: Optional[float], f_hz: float) -> Optional[float]:
    """Calculate coherence bandwidth B_k."""
    if nt is None or nt <= 0 or f_hz <= 0:
        return None
    return math.sqrt((LIGHT_SPEED * (f_hz ** 3)) / (PROPAGATION_COEFF * math.pi * nt))


def calculate_gdd(nt: Optional[float], f_hz: float) -> Optional[float]:
    """Calculate group delay dispersion GDD."""
    if nt is None or nt <= 0 or f_hz <= 0:
        return None
    numerator = -3.0 * PROPAGATION_COEFF * nt
    denominator = 2.0 * LIGHT_SPEED * math.pi * (f_hz ** 3)
    return numerator / denominator


def calculate_propagation(
    tec: float,
    f_hz: float,
    signal_band: Optional[SignalBandName] = None,
) -> PropagationDirectResponse:
    """Direct calculation helper for the simple calculator endpoint."""
    nt = tec_to_nt(tec)
    b_k = calculate_b_k(nt, f_hz)
    gdd = calculate_gdd(nt, f_hz)
    if nt is None or b_k is None or gdd is None:
        raise ValueError("TEC and frequency must be positive to calculate propagation parameters")
    return PropagationDirectResponse(
        tec=float(tec),
        nt=float(nt),
        f_hz=float(f_hz),
        signal_band=signal_band,
        b_k=float(b_k),
        gdd=float(gdd),
    )


def get_raw_data_propagation_absoltec(
    year: int,
    doy: int,
    station: str,
    f_hz: float,
    signal_band: Optional[SignalBandName] = None,
    data_root: Optional[str] = None,
) -> list[PropagationPointAbsoltec]:
    """Derive propagation parameters from one AbsolTEC file."""
    root = data_root or settings.data_root
    abs_points = get_absoltec_raw_data(year, doy, station, root)
    if not abs_points:
        return []

    points: list[PropagationPointAbsoltec] = []
    for p in abs_points:
        nt = tec_to_nt(float(p.tec))
        points.append(
            PropagationPointAbsoltec(
                ut=float(p.ut),
                tec=float(p.tec),
                nt=_opt_float(nt),
                f_hz=float(f_hz),
                signal_band=signal_band,
                b_k=_opt_float(calculate_b_k(nt, f_hz)),
                gdd=_opt_float(calculate_gdd(nt, f_hz)),
                g_lon=_opt_float(getattr(p, "g_lon", None)),
                g_lat=_opt_float(getattr(p, "g_lat", None)),
                g_q_lon=_opt_float(getattr(p, "g_q_lon", None)),
                g_q_lat=_opt_float(getattr(p, "g_q_lat", None)),
                g_t=_opt_float(getattr(p, "g_t", None)),
                g_q_t=_opt_float(getattr(p, "g_q_t", None)),
            )
        )
    return points


def get_raw_data_propagation_tec(
    year: int,
    doy: int,
    station: str,
    satellite: str,
    observable: TecObservable,
    f_hz: float,
    signal_band: Optional[SignalBandName] = None,
    data_root: Optional[str] = None,
) -> PropagationTecResponse:
    """Derive propagation parameters from one TEC-suite satellite file."""
    root = data_root or settings.data_root
    tec_data = get_tec_data(year, doy, station, satellite, root)

    points: list[PropagationTecPoint] = []
    for p in tec_data.points:
        tec_value = float(getattr(p, observable))
        nt = tec_to_nt(tec_value)
        points.append(
            PropagationTecPoint(
                tsn=int(p.tsn),
                hour=float(p.hour),
                el=float(p.el),
                az=float(p.az),
                observable=observable,
                tec=tec_value,
                nt=_opt_float(nt),
                f_hz=float(f_hz),
                signal_band=signal_band,
                b_k=_opt_float(calculate_b_k(nt, f_hz)),
                gdd=_opt_float(calculate_gdd(nt, f_hz)),
                validity=int(p.validity),
            )
        )

    return PropagationTecResponse(
        year=year,
        doy=doy,
        station=station,
        satellite=satellite,
        observable=observable,
        f_hz=float(f_hz),
        signal_band=signal_band,
        points=points,
    )


def compute_statistics_propagation_absoltec(
    year: int,
    doy_start: int,
    doy_end: int,
    station: str,
    f_hz: float,
    signal_band: Optional[SignalBandName] = None,
    alpha: float = settings.default_alpha,
    data_root: Optional[str] = None,
) -> PropagationStatisticsResponse:
    """
    Compute per-slot propagation statistics over an AbsolTEC day range.

    Only strictly positive TEC values participate in the propagation aggregate.
    This avoids non-physical infinities for B_k at TEC=0 and rejects negative
    source values without mutating the raw dataset.
    """
    root = data_root or settings.data_root
    files = absoltec_glob_files(root, year, doy_start, doy_end, station)

    if not files:
        return PropagationStatisticsResponse(
            year=year,
            doy_start=doy_start,
            doy_end=doy_end,
            station=station,
            alpha=alpha,
            f_hz=f_hz,
            signal_band=signal_band,
            total_days=0,
            points=[],
        )

    file_list_sql = "[" + ", ".join(f"'{f}'" for f in files) + "]"
    conn = get_connection()
    df: pd.DataFrame = conn.execute(f"""
        SELECT
            "{UT}",
            AVG(CASE WHEN "{I_V}" > 0 THEN "{I_V}" END) AS mean_tec,
            AVG(CASE WHEN "{I_V}" > 0 THEN "{I_V}" * POWER(10, 16) END) AS mean_nt,
            AVG(CASE WHEN "{I_V}" > 0 THEN
                SQRT(({LIGHT_SPEED} * POWER({f_hz}, 3)) / ({PROPAGATION_COEFF} * PI() * ("{I_V}" * POWER(10, 16))))
            END) AS mean_b_k,
            VAR_POP(CASE WHEN "{I_V}" > 0 THEN
                SQRT(({LIGHT_SPEED} * POWER({f_hz}, 3)) / ({PROPAGATION_COEFF} * PI() * ("{I_V}" * POWER(10, 16))))
            END) AS variance_b_k,
            STDDEV_POP(CASE WHEN "{I_V}" > 0 THEN
                SQRT(({LIGHT_SPEED} * POWER({f_hz}, 3)) / ({PROPAGATION_COEFF} * PI() * ("{I_V}" * POWER(10, 16))))
            END) AS std_dev_b_k,
            AVG(CASE WHEN "{I_V}" > 0 THEN
                (-3 * {PROPAGATION_COEFF} * ("{I_V}" * POWER(10, 16))) / (2 * {LIGHT_SPEED} * PI() * POWER({f_hz}, 3))
            END) AS mean_gdd,
            VAR_POP(CASE WHEN "{I_V}" > 0 THEN
                (-3 * {PROPAGATION_COEFF} * ("{I_V}" * POWER(10, 16))) / (2 * {LIGHT_SPEED} * PI() * POWER({f_hz}, 3))
            END) AS variance_gdd,
            STDDEV_POP(CASE WHEN "{I_V}" > 0 THEN
                (-3 * {PROPAGATION_COEFF} * ("{I_V}" * POWER(10, 16))) / (2 * {LIGHT_SPEED} * PI() * POWER({f_hz}, 3))
            END) AS std_dev_gdd,
            SUM(CASE WHEN "{I_V}" > 0 THEN 1 ELSE 0 END) AS n,
            AVG(CASE WHEN "{I_V}" > 0 THEN "{G_LON}" END) AS mean_g_lon,
            AVG(CASE WHEN "{I_V}" > 0 THEN "{G_LAT}" END) AS mean_g_lat
        FROM read_parquet({file_list_sql})
        GROUP BY "{UT}"
        HAVING SUM(CASE WHEN "{I_V}" > 0 THEN 1 ELSE 0 END) > 0
        ORDER BY "{UT}"
    """).df()

    return PropagationStatisticsResponse(
        year=year,
        doy_start=doy_start,
        doy_end=doy_end,
        station=station,
        alpha=alpha,
        f_hz=float(f_hz),
        signal_band=signal_band,
        total_days=len(files),
        points=_build_stats_points_propagation(df, alpha),
    )


def _build_stats_points_propagation(
    df: pd.DataFrame,
    alpha: float,
) -> list[PropagationStatisticsPoint]:
    """Convert the aggregate DataFrame into API models."""
    points: list[PropagationStatisticsPoint] = []
    for _, row in df.iterrows():
        n = int(row["n"])
        std_dev_b_k = _safe_float(row.get("std_dev_b_k"))
        std_dev_gdd = _safe_float(row.get("std_dev_gdd"))

        if n > 1:
            t_critical = student_t.ppf(1.0 - alpha / 2.0, df=n - 1)
            ci_b_k = t_critical * std_dev_b_k / math.sqrt(n)
            ci_gdd = t_critical * std_dev_gdd / math.sqrt(n)
        else:
            ci_b_k = 0.0
            ci_gdd = 0.0

        points.append(
            PropagationStatisticsPoint(
                ut=float(row[UT]),
                mean_tec=_safe_float(row.get("mean_tec")),
                mean_nt=_safe_float(row.get("mean_nt")),
                mean_b_k=_safe_float(row.get("mean_b_k")),
                variance_b_k=_safe_float(row.get("variance_b_k")),
                std_dev_b_k=std_dev_b_k,
                student_ci_b_k=ci_b_k,
                mean_gdd=_safe_float(row.get("mean_gdd")),
                variance_gdd=_safe_float(row.get("variance_gdd")),
                std_dev_gdd=std_dev_gdd,
                student_ci_gdd=ci_gdd,
                n=n,
                mean_g_lon=_opt_float(row.get("mean_g_lon")),
                mean_g_lat=_opt_float(row.get("mean_g_lat")),
            )
        )
    return points


def _safe_float(value, default: float = 0.0) -> float:
    """Return float or a default for None / NaN."""
    if value is None:
        return default
    try:
        result = float(value)
        return result if not math.isnan(result) else default
    except (TypeError, ValueError):
        return default


def _opt_float(value) -> Optional[float]:
    """Return float or None, treating NaN as None."""
    if value is None:
        return None
    try:
        result = float(value)
        return None if math.isnan(result) else result
    except (TypeError, ValueError):
        return None
