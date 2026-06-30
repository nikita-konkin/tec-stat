"""
Propagation-parameter HTTP router.

This router exposes derived physical parameters based on existing AbsolTEC and
TEC-suite datasets. The caller may provide either an explicit working
frequency (`f_hz`) or a supported GNSS signal-band preset (`signal_band`).
"""

from typing import Literal, Optional

from fastapi import APIRouter, HTTPException, Query

from app.config import settings
from app.models.schemas import (
    PropagationDirectResponse,
    PropagationPointAbsoltec,
    PropagationStatisticsResponse,
    PropagationTecResponse,
)
from app.routers.export import ExportFormat, format_payload
from app.services.propagation import (
    SUPPORTED_SIGNAL_BANDS,
    calculate_propagation,
    compute_statistics_propagation_absoltec,
    get_raw_data_propagation_absoltec,
    get_raw_data_propagation_tec,
    resolve_frequency,
)

router = APIRouter(prefix="/propagation", tags=["Propagation", "Data Analysis"])
SIGNAL_BAND_DESCRIPTION = (
    "Optional GNSS signal-band preset. Supported: "
    + ", ".join(SUPPORTED_SIGNAL_BANDS)
)


def _resolve_frequency_or_422(
    f_hz: Optional[float],
    signal_band: Optional[str],
) -> tuple[float, Optional[str]]:
    try:
        return resolve_frequency(f_hz, signal_band)
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from exc


@router.get("/calc", response_model=PropagationDirectResponse)
def direct_calc(
    tec: float = Query(..., gt=0, description="TEC in TECU"),
    f_hz: Optional[float] = Query(None, gt=0, description="Working frequency in Hz"),
    signal_band: Optional[str] = Query(None, description=SIGNAL_BAND_DESCRIPTION),
    format: ExportFormat = Query("json", description="Response format: json, csv, xlsx"),
):
    """Direct B_k / GDD calculation from a TEC value and working frequency."""
    resolved_f_hz, resolved_signal_band = _resolve_frequency_or_422(f_hz, signal_band)
    payload = calculate_propagation(tec, resolved_f_hz, resolved_signal_band)
    return format_payload(payload, format, "propagation_calc")


@router.get("/absoltec/raw", response_model=list[PropagationPointAbsoltec])
def absoltec_raw(
    year: int = Query(..., ge=2000, le=2100),
    doy: int = Query(..., ge=1, le=366),
    station: str = Query(..., min_length=2, max_length=9),
    f_hz: Optional[float] = Query(None, gt=0, description="Working frequency in Hz"),
    signal_band: Optional[str] = Query(None, description=SIGNAL_BAND_DESCRIPTION),
    data_root: Optional[str] = Query(None, description="Override ABSOLTEC_DATA_ROOT"),
    format: ExportFormat = Query("json", description="Response format: json, csv, xlsx"),
):
    """Raw AbsolTEC series with derived propagation parameters."""
    resolved_f_hz, resolved_signal_band = _resolve_frequency_or_422(f_hz, signal_band)
    payload = get_raw_data_propagation_absoltec(
        year=year,
        doy=doy,
        station=station,
        f_hz=resolved_f_hz,
        signal_band=resolved_signal_band,
        data_root=settings.get_absoltec_root(data_root),
    )
    return format_payload(
        payload,
        format,
        f"propagation_absoltec_raw_{year}_{doy:03d}_{station.lower()}",
    )


@router.get("/absoltec/statistics", response_model=PropagationStatisticsResponse)
def absoltec_statistics(
    year: int = Query(..., ge=2000, le=2100),
    doy_start: int = Query(..., ge=1, le=366),
    doy_end: int = Query(..., ge=1, le=366),
    station: str = Query(..., min_length=2, max_length=9),
    f_hz: Optional[float] = Query(None, gt=0, description="Working frequency in Hz"),
    signal_band: Optional[str] = Query(None, description=SIGNAL_BAND_DESCRIPTION),
    alpha: float = Query(settings.default_alpha, ge=0.001, le=0.5),
    data_root: Optional[str] = Query(None, description="Override ABSOLTEC_DATA_ROOT"),
    format: ExportFormat = Query("json", description="Response format: json, csv, xlsx"),
):
    """Mean plus Student-CI for derived propagation parameters over an AbsolTEC day range."""
    if doy_start > doy_end:
        raise HTTPException(422, "doy_start must be <= doy_end")

    resolved_f_hz, resolved_signal_band = _resolve_frequency_or_422(f_hz, signal_band)
    payload = compute_statistics_propagation_absoltec(
        year=year,
        doy_start=doy_start,
        doy_end=doy_end,
        station=station,
        f_hz=resolved_f_hz,
        signal_band=resolved_signal_band,
        alpha=alpha,
        data_root=settings.get_absoltec_root(data_root),
    )
    return format_payload(
        payload,
        format,
        f"propagation_absoltec_stats_{year}_{doy_start:03d}_{doy_end:03d}_{station.lower()}",
    )


@router.get("/tec/raw", response_model=PropagationTecResponse)
def tec_raw(
    year: int = Query(..., ge=2000, le=2100),
    doy: int = Query(..., ge=1, le=366),
    station: str = Query(..., min_length=2, max_length=9),
    satellite: str = Query(..., min_length=2, max_length=4),
    observable: Literal["tec_l1l2", "tec_c1p2"] = Query("tec_l1l2"),
    f_hz: Optional[float] = Query(None, gt=0, description="Working frequency in Hz"),
    signal_band: Optional[str] = Query(None, description=SIGNAL_BAND_DESCRIPTION),
    data_root: Optional[str] = Query(None, description="Override TECSUITE_DATA_ROOT"),
    format: ExportFormat = Query("json", description="Response format: json, csv, xlsx"),
):
    """Raw TEC-suite satellite series with derived propagation parameters."""
    resolved_f_hz, resolved_signal_band = _resolve_frequency_or_422(f_hz, signal_band)
    payload = get_raw_data_propagation_tec(
        year=year,
        doy=doy,
        station=station,
        satellite=satellite,
        observable=observable,
        f_hz=resolved_f_hz,
        signal_band=resolved_signal_band,
        data_root=settings.get_tec_root(data_root),
    )
    return format_payload(
        payload,
        format,
        f"propagation_tec_raw_{year}_{doy:03d}_{station.lower()}_{satellite.lower()}",
    )
