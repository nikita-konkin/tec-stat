"""
Propagation-parameter service.

This module derives physical propagation parameters from TEC and working
frequency using:

  N_t = TEC * 10^16
  B_k = sqrt((3*10^8 * f^3) / (80.5 * pi * N_t))
  GDD = -(3 * 80.5 * N_t) / (2 * 3*10^8 * pi * f^3)

`f` is expected in Hz. TEC inputs are expected in TECU.

For raw dataset endpoints, non-positive TEC values are treated as invalid for
derived propagation parameters, so `b_k` and `gdd` are returned as None.
TEC-suite values are preserved in the source field; AbsolTEC ones arrive as
tec=None already, because I_v <= 0 marks a missing sample there (see
app.services.absoltec). For statistics, only strictly positive TEC samples
participate in the aggregate, and variances are sample variances
(denominator N-1), None together with the CI when N < 2.
"""

import math
from typing import Literal, Optional

import pandas as pd
from scipy.stats import t as student_t

from app.config import settings
from app.db.columns import UT, I_V, G_LON, G_LAT
from app.day_range import DayRange
from app.db.engine import absoltec_files_for_days
from app.models.schemas import (
    PropagationDirectResponse,
    PropagationPointAbsoltec,
    PropagationStatisticsPoint,
    PropagationStatisticsResponse,
    PropagationTecPoint,
    PropagationTecResponse,
)
from app.services.absoltec import VALID_TEC_SQL, files_with_valid_tec
from app.services.absoltec import get_raw_data as get_absoltec_raw_data
from app.services.absoltec import get_raw_data_range as get_absoltec_raw_data_range
from app.services.tec import get_tec_data, get_tec_data_range
from app.db.engine import get_connection

import logging

logger = logging.getLogger(__name__)

LIGHT_SPEED = 3.0 * (10 ** 8)
TEC_TO_NT = 10.0 ** 16
PROPAGATION_COEFF = 80.5

TecObservable = Literal["tec_l1l2", "tec_c1p2"]
SignalBandName = Literal[
    "GPS_L1",
    "GPS_L2",
    "GPS_L5",
    "GLO_L1",
    "GLO_L2",
    "GLO_L3",
    "GAL_E1",
    "GAL_E5A",
    "GAL_E5B",
    "GAL_E5",
    "BDS_B1I",
    "BDS_B1C",
    "BDS_B2A",
    "BDS_B2I",
]

# GLONASS L1/L2 are FDMA (f = f0 + k·Δf, k = −7…+6); the table uses the k=0
# centre frequencies (1602.0 / 1246.0 MHz). L3 is CDMA with a fixed carrier.
SIGNAL_BAND_FREQUENCIES_HZ: dict[SignalBandName, float] = {
    "GPS_L1": 1575.42e6,
    "GPS_L2": 1227.60e6,
    "GPS_L5": 1176.45e6,
    "GLO_L1": 1602.0e6,
    "GLO_L2": 1246.0e6,
    "GLO_L3": 1202.025e6,
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
    if normalized.startswith("GLONASS_"):
        normalized = "GLO_" + normalized[len("GLONASS_"):]
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


def tec_to_nt(tec: Optional[float]) -> Optional[float]:
    """Convert TECU to absolute total electron content N_t."""
    if tec is None or tec < 0:
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
        nt = tec_to_nt(p.tec)
        points.append(
            PropagationPointAbsoltec(
                ut=float(p.ut),
                tec=p.tec,
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


def get_raw_data_range_propagation_absoltec(
    day_range: DayRange,
    stations: list[str],
    f_hz: float,
    signal_band: Optional[SignalBandName] = None,
    data_root: Optional[str] = None,
) -> list[dict]:
    """AbsolTEC raw range rows (see absoltec.get_raw_data_range) with derived parameters."""
    rows = get_absoltec_raw_data_range(day_range, stations, data_root or settings.data_root)
    for row in rows:
        nt = tec_to_nt(row["tec"])
        row.update(
            nt=_opt_float(nt),
            f_hz=float(f_hz),
            signal_band=signal_band,
            b_k=_opt_float(calculate_b_k(nt, f_hz)),
            gdd=_opt_float(calculate_gdd(nt, f_hz)),
        )
    return rows


def get_raw_data_range_propagation_tec(
    day_range: DayRange,
    stations: list[str],
    observable: TecObservable,
    f_hz: float,
    signal_band: Optional[SignalBandName] = None,
    data_root: Optional[str] = None,
    satellites: Optional[list[str]] = None,
) -> list[dict]:
    """
    TEC-suite raw range rows (see tec.get_tec_data_range) with derived parameters.

    `tec` holds the selected observable; the other observable column is
    dropped so each row carries one TEC value, matching /propagation/tec/raw.
    """
    rows = get_tec_data_range(day_range, stations, data_root or settings.data_root, satellites)
    out = []
    for row in rows:
        tec_value = row.pop(observable)
        row.pop("tec_c1p2" if observable == "tec_l1l2" else "tec_l1l2", None)
        nt = tec_to_nt(tec_value)
        row.update(
            observable=observable,
            tec=tec_value,
            nt=_opt_float(nt),
            f_hz=float(f_hz),
            signal_band=signal_band,
            b_k=_opt_float(calculate_b_k(nt, f_hz)),
            gdd=_opt_float(calculate_gdd(nt, f_hz)),
        )
        out.append(row)
    return out


def compute_statistics_propagation_absoltec(
    day_range: DayRange,
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
    files = absoltec_files_for_days(root, day_range.days(), station)

    if not files:
        logger.warning(
            "propagation stats: no AbsolTEC files for %s %s..%s under %s",
            station, day_range.start, day_range.end, root,
        )
        return PropagationStatisticsResponse(
            **day_range.response_fields(),
            station=station,
            alpha=alpha,
            f_hz=f_hz,
            signal_band=signal_band,
            total_days=0,
            points=[],
        )

    logger.info(
        "propagation stats: %s %s..%s f=%.4g Hz — querying %d file(s)",
        station, day_range.start, day_range.end, f_hz, len(files),
    )
    file_list_sql = "[" + ", ".join(f"'{f}'" for f in files) + "]"
    b_k_sql = f'SQRT(({LIGHT_SPEED} * POWER({f_hz}, 3)) / ({PROPAGATION_COEFF} * PI() * ("{I_V}" * POWER(10, 16))))'
    gdd_sql = f'(-3 * {PROPAGATION_COEFF} * ("{I_V}" * POWER(10, 16))) / (2 * {LIGHT_SPEED} * PI() * POWER({f_hz}, 3))'
    conn = get_connection()
    df: pd.DataFrame = conn.execute(f"""
        SELECT
            "{UT}",
            AVG("{I_V}")                  AS mean_tec,
            AVG("{I_V}" * POWER(10, 16))  AS mean_nt,
            AVG({b_k_sql})                AS mean_b_k,
            VAR_SAMP({b_k_sql})           AS variance_b_k,
            STDDEV_SAMP({b_k_sql})        AS std_dev_b_k,
            AVG({gdd_sql})                AS mean_gdd,
            VAR_SAMP({gdd_sql})           AS variance_gdd,
            STDDEV_SAMP({gdd_sql})        AS std_dev_gdd,
            COUNT(*)                      AS n,
            AVG("{G_LON}")                AS mean_g_lon,
            AVG("{G_LAT}")                AS mean_g_lat
        FROM read_parquet({file_list_sql})
        WHERE {VALID_TEC_SQL}
        GROUP BY "{UT}"
        ORDER BY "{UT}"
    """).df()

    return PropagationStatisticsResponse(
        **day_range.response_fields(),
        station=station,
        alpha=alpha,
        f_hz=float(f_hz),
        signal_band=signal_band,
        total_days=len(files_with_valid_tec(files)),
        points=_build_stats_points_propagation(df, alpha),
    )


def _build_stats_points_propagation(
    df: pd.DataFrame,
    alpha: float,
) -> list[PropagationStatisticsPoint]:
    """Convert the aggregate DataFrame into API models; spread is None when n < 2."""
    points: list[PropagationStatisticsPoint] = []
    for _, row in df.iterrows():
        n = int(row["n"])
        spread = n > 1
        std_dev_b_k = _opt_float(row.get("std_dev_b_k")) if spread else None
        std_dev_gdd = _opt_float(row.get("std_dev_gdd")) if spread else None

        ci_b_k = ci_gdd = None
        if spread:
            t_critical = student_t.ppf(1.0 - alpha / 2.0, df=n - 1)
            if std_dev_b_k is not None:
                ci_b_k = t_critical * std_dev_b_k / math.sqrt(n)
            if std_dev_gdd is not None:
                ci_gdd = t_critical * std_dev_gdd / math.sqrt(n)

        points.append(
            PropagationStatisticsPoint(
                ut=float(row[UT]),
                mean_tec=_safe_float(row.get("mean_tec")),
                mean_nt=_safe_float(row.get("mean_nt")),
                mean_b_k=_safe_float(row.get("mean_b_k")),
                variance_b_k=_opt_float(row.get("variance_b_k")) if spread else None,
                std_dev_b_k=std_dev_b_k,
                student_ci_b_k=ci_b_k,
                mean_gdd=_safe_float(row.get("mean_gdd")),
                variance_gdd=_opt_float(row.get("variance_gdd")) if spread else None,
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
