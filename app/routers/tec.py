"""
TEC-suite HTTP router.

Uses settings.get_tec_root() for path resolution.
"""

from fastapi import APIRouter, Query
from typing import Optional

from app.config import settings
from app.routers.export import ExportFormat, format_payload
from app.services.tec import get_tec_data, list_satellites, list_stations_with_meta
from app.models.schemas import TecDataResponse, SatelliteListResponse, StationMapResponse

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
