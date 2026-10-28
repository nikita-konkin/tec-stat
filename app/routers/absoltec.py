"""
AbsolTEC HTTP router.

Uses settings.get_absoltec_root() so the data path is resolved with the
correct priority: query-param override → ABSOLTEC_DATA_ROOT → DATA_ROOT.
"""

from fastapi import APIRouter, Depends, Query
from typing import Optional

from app.config import settings
from app.day_range import DayRange, day_range_query
from app.db.engine import absoltec_discover_stations, absoltec_discover_days
from app.routers.export import ExportFormat, format_payload
from app.station_selection import station_selection
from app.services.absoltec import (
    get_raw_data,
    get_raw_data_range,
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
    format: ExportFormat = Query("json", description="Response format: json, csv, xlsx"),
):
    """Discover which stations have AbsolTEC data for a given year/day."""
    root = settings.get_absoltec_root(data_root)
    payload = AvailabilityResponse(year=year, doy=doy,
                                   stations=absoltec_discover_stations(root, year, doy))
    return format_payload(payload, format, f"absoltec_stations_{year}_{doy:03d}")


@router.get("/days", response_model=AvailabilityResponse)
def list_days(
    year:    int = Query(..., ge=2000, le=2100),
    station: str = Query(..., min_length=2, max_length=9),
    data_root: Optional[str] = Query(None),
    format: ExportFormat = Query("json", description="Response format: json, csv, xlsx"),
):
    """List all days-of-year for which a station has data in a given year."""
    root = settings.get_absoltec_root(data_root)
    payload = AvailabilityResponse(year=year, station=station,
                                   days=absoltec_discover_days(root, year, station))
    return format_payload(payload, format, f"absoltec_days_{year}_{station.lower()}")


@router.get("/raw", response_model=list[TimeSeriesPoint])
def raw_data(
    year:    int = Query(..., ge=2000, le=2100),
    doy:     int = Query(..., ge=1,    le=366),
    station: str = Query(..., min_length=2, max_length=9),
    data_root: Optional[str] = Query(None),
    format: ExportFormat = Query("json", description="Response format: json, csv, xlsx"),
):
    """Raw 48-point TEC time series for one station/day (all 8 columns)."""
    payload = get_raw_data(year, doy, station, settings.get_absoltec_root(data_root))
    return format_payload(payload, format, f"absoltec_raw_{year}_{doy:03d}_{station.lower()}")


@router.get("/raw/range", response_model=list[dict])
def raw_data_range(
    day_range: DayRange = Depends(day_range_query),
    station_list: list[str] = Depends(station_selection("absoltec")),
    data_root: Optional[str] = Query(None),
    format: ExportFormat = Query("json", description="Response format: json, csv, xlsx"),
):
    """
    Raw AbsolTEC rows concatenated day-by-day for one or more stations.

    Give the range as year + doy_start + doy_end, or as date_start + date_end
    (which may cross New Year). Time continuity across days is represented by
    `concat_ut`:
      concat_ut = (date - date_start).days * 24 + ut

    Stations are listed (station, stations) or chosen by area (near +
    radius_km, or bbox); an area keeps stations with AbsolTEC data.
    """
    payload = get_raw_data_range(
        day_range,
        station_list,
        settings.get_absoltec_root(data_root),
    )
    filename_stations = "-".join(station_list[:3])
    if len(station_list) > 3:
        filename_stations += f"-plus{len(station_list)-3}"
    return format_payload(
        payload,
        format,
        f"absoltec_raw_range_{day_range.label()}_{filename_stations}",
    )


@router.get("/statistics", response_model=StatisticsResponse)
def statistics(
    day_range: DayRange = Depends(day_range_query),
    station:   str   = Query(..., min_length=2, max_length=9),
    alpha:     float = Query(settings.default_alpha, ge=0.001, le=0.5),
    data_root: Optional[str] = Query(None),
    format: ExportFormat = Query("json", description="Response format: json, csv, xlsx"),
):
    """Mean ± Student-CI for a station over a day or date range."""
    payload = compute_statistics(day_range, station, alpha,
                                 settings.get_absoltec_root(data_root))
    return format_payload(
        payload,
        format,
        f"absoltec_stats_{day_range.label()}_{station.lower()}",
    )


@router.get("/statistics/per-station-day", response_model=list)
def statistics_per_station_day(
    day_range: DayRange = Depends(day_range_query),
    stations:  list[str] = Depends(station_selection("absoltec")),
    alpha:     float = Query(settings.default_alpha, ge=0.001, le=0.5),
    data_root: Optional[str] = Query(None),
    format: ExportFormat = Query("json", description="Response format: json, csv, xlsx"),
):
    """Per-day statistics averaged across a list of stations, over a day or date range."""
    payload = compute_statistics_per_station_day(
        day_range, stations, alpha,
        settings.get_absoltec_root(data_root))
    return format_payload(
        payload,
        format,
        f"absoltec_stats_per_station_day_{day_range.label()}",
    )
