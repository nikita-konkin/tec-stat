"""
AbsolTEC HTTP router.

Uses settings.get_absoltec_root() so the data path is resolved with the
correct priority: query-param override → ABSOLTEC_DATA_ROOT → DATA_ROOT.
"""

from fastapi import APIRouter, Query, HTTPException
from typing import Optional

from app.config import settings
from app.db.engine import absoltec_discover_stations, absoltec_discover_days
from app.services.absoltec import (
    get_raw_data,
    compute_statistics,
    compute_statistics_per_station_day,
)
from app.models.schemas import AvailabilityResponse, StatisticsResponse, TimeSeriesPoint

router = APIRouter(prefix="/absoltec", tags=["AbsolTEC"])


@router.get("/stations", response_model=AvailabilityResponse)
def list_stations(
    year: int = Query(..., ge=2000, le=2100),
    doy:  int = Query(..., ge=1,    le=366),
    data_root: Optional[str] = Query(None, description="Override ABSOLTEC_DATA_ROOT"),
):
    """Discover which stations have AbsolTEC data for a given year/day."""
    root = settings.get_absoltec_root(data_root)
    return AvailabilityResponse(year=year, doy=doy,
                                stations=absoltec_discover_stations(root, year, doy))


@router.get("/days", response_model=AvailabilityResponse)
def list_days(
    year:    int = Query(..., ge=2000, le=2100),
    station: str = Query(..., min_length=2, max_length=9),
    data_root: Optional[str] = Query(None),
):
    """List all days-of-year for which a station has data in a given year."""
    root = settings.get_absoltec_root(data_root)
    return AvailabilityResponse(year=year, station=station,
                                days=absoltec_discover_days(root, year, station))


@router.get("/raw", response_model=list[TimeSeriesPoint])
def raw_data(
    year:    int = Query(..., ge=2000, le=2100),
    doy:     int = Query(..., ge=1,    le=366),
    station: str = Query(..., min_length=2, max_length=9),
    data_root: Optional[str] = Query(None),
):
    """Raw 48-point TEC time series for one station/day (all 8 columns)."""
    return get_raw_data(year, doy, station, settings.get_absoltec_root(data_root))


@router.get("/statistics", response_model=StatisticsResponse)
def statistics(
    year:      int   = Query(..., ge=2000, le=2100),
    doy_start: int   = Query(..., ge=1,    le=366),
    doy_end:   int   = Query(..., ge=1,    le=366),
    station:   str   = Query(..., min_length=2, max_length=9),
    alpha:     float = Query(settings.default_alpha, ge=0.001, le=0.5),
    data_root: Optional[str] = Query(None),
):
    """Mean ± Student-CI for a station over a day range."""
    if doy_start > doy_end:
        raise HTTPException(422, "doy_start must be ≤ doy_end")
    return compute_statistics(year, doy_start, doy_end, station, alpha,
                              settings.get_absoltec_root(data_root))


@router.get("/statistics/per-station-day", response_model=list)
def statistics_per_station_day(
    year:      int  = Query(..., ge=2000, le=2100),
    doy_start: int  = Query(..., ge=1,    le=366),
    doy_end:   int  = Query(..., ge=1,    le=366),
    stations:  list[str] = Query(...),
    alpha:     float = Query(settings.default_alpha, ge=0.001, le=0.5),
    data_root: Optional[str] = Query(None),
):
    """Per-day statistics averaged across a list of stations."""
    if doy_start > doy_end:
        raise HTTPException(422, "doy_start must be ≤ doy_end")
    return compute_statistics_per_station_day(
        year, doy_start, doy_end, stations, alpha,
        settings.get_absoltec_root(data_root))
