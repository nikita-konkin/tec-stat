"""
TEC-suite HTTP router.

Uses settings.get_tec_root() for path resolution.
"""

from fastapi import APIRouter, Depends, Query, HTTPException
from typing import Optional

from app.config import settings
from app.day_range import DayRange, day_range_query
from app.routers.export import ExportFormat, format_payload
from app.services.tec import (
    get_tec_data,
    get_tec_data_range,
    list_satellites,
    list_satellites_range,
    list_stations_with_meta,
)
from app.models.schemas import (
    TecDataResponse,
    SatelliteListResponse,
    SatelliteRangeResponse,
    StationMapResponse,
)

router = APIRouter(prefix="/tec", tags=["TEC-suite"])


@router.get("/stations", response_model=StationMapResponse)
def stations_with_meta(
    year: int = Query(..., ge=2000, le=2100),
    doy:  int = Query(..., ge=1,    le=366),
    data_root: Optional[str] = Query(None, description="Override TEC_DATA_ROOT"),
    format: ExportFormat = Query("json", description="Response format: json, csv, xlsx"),
):
    """Stations with geodetic coordinates for the world-map feature."""
    payload = list_stations_with_meta(year, doy, settings.get_tec_root(data_root))
    return format_payload(payload, format, f"tec_stations_{year}_{doy:03d}")


@router.get("/satellites", response_model=SatelliteListResponse)
def satellites(
    year:    int = Query(..., ge=2000, le=2100),
    doy:     int = Query(..., ge=1,    le=366),
    station: str = Query(..., min_length=2, max_length=9),
    data_root: Optional[str] = Query(None),
    format: ExportFormat = Query("json", description="Response format: json, csv, xlsx"),
):
    """List satellites observed at a station on a given day."""
    payload = list_satellites(year, doy, station, settings.get_tec_root(data_root))
    return format_payload(payload, format, f"tec_satellites_{year}_{doy:03d}_{station.lower()}")


@router.get("/satellites/range", response_model=SatelliteRangeResponse)
def satellites_range(
    day_range: DayRange = Depends(day_range_query),
    station: str = Query(..., min_length=2, max_length=9),
    data_root: Optional[str] = Query(None),
    format: ExportFormat = Query("json", description="Response format: json, csv, xlsx"),
):
    """Satellites observed at a station on any day of a day or date range, with day counts."""
    payload = list_satellites_range(day_range, station, settings.get_tec_root(data_root))
    return format_payload(payload, format, f"tec_satellites_range_{day_range.label()}_{station.lower()}")


@router.get("/data", response_model=TecDataResponse)
def satellite_data(
    year:      int = Query(..., ge=2000, le=2100),
    doy:       int = Query(..., ge=1,    le=366),
    station:   str = Query(..., min_length=2, max_length=9),
    satellite: str = Query(..., min_length=2, max_length=4),
    data_root: Optional[str] = Query(None),
    format: ExportFormat = Query("json", description="Response format: json, csv, xlsx"),
):
    """Full observation time series for one satellite pass."""
    payload = get_tec_data(year, doy, station, satellite, settings.get_tec_root(data_root))
    return format_payload(
        payload,
        format,
        f"tec_data_{year}_{doy:03d}_{station.lower()}_{satellite.lower()}",
    )


@router.get("/raw/range", response_model=list[dict])
def satellite_data_range(
    day_range: DayRange = Depends(day_range_query),
    station:   Optional[str] = Query(None, min_length=2, max_length=9),
    stations:  Optional[list[str]] = Query(None, description="Alternative: repeated ?stations=... query values"),
    satellite: Optional[str] = Query(None, min_length=2, max_length=4, description="Only this satellite, e.g. G01"),
    satellites: Optional[list[str]] = Query(None, description="Alternative: repeated ?satellites=... query values"),
    data_root: Optional[str] = Query(None),
    format: ExportFormat = Query("json", description="Response format: json, csv, xlsx"),
):
    """
    Raw TEC-suite rows concatenated day-by-day for one or more stations.

    Returns rows for all available satellites, or only the ones named by
    satellite/satellites, and provides a continuous timeline:
      concat_hour = (date - date_start).days * 24 + hour
    Give the range as year + doy_start + doy_end, or as date_start + date_end
    (which may cross New Year).
    """
    station_list: list[str] = []
    if station:
        station_list.append(station)
    if stations:
        station_list.extend(stations)
    station_list = sorted({s.lower() for s in station_list if s})
    if not station_list:
        raise HTTPException(422, "Provide either station or stations")

    satellite_list = sorted({s.upper() for s in [satellite, *(satellites or [])] if s}) or None

    payload = get_tec_data_range(
        day_range,
        station_list,
        settings.get_tec_root(data_root),
        satellite_list,
    )
    filename_stations = "-".join(station_list[:3])
    if len(station_list) > 3:
        filename_stations += f"-plus{len(station_list)-3}"
    if satellite_list:
        filename_stations += "_" + "-".join(satellite_list[:3]).lower()
    return format_payload(
        payload,
        format,
        f"tec_raw_range_{day_range.label()}_{filename_stations}",
    )
