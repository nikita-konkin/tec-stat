"""
Stations router — combined discovery endpoint for both data types.

Provides a unified view of which stations are active on a given day,
merging information from both the AbsolTEC and TEC-suite trees.
This is the feed for the future world-map visualisation.

Endpoints:
  GET /stations/available   → stations present in AbsolTEC and/or TEC-suite
  GET /stations/map         → station list with coordinates (TEC-suite metadata)
"""

from fastapi import APIRouter, Query
from typing import Optional, Literal

from app.config import settings
from app.db.engine import absoltec_discover_stations, tec_discover_stations
from app.routers.export import ExportFormat, format_payload
from app.services.tec import list_stations_with_meta

router = APIRouter(prefix="/stations", tags=["Stations"])


@router.get("/available")
def available_stations(
    year: int   = Query(..., ge=2000, le=2100),
    doy: int    = Query(..., ge=1, le=366),
    source: Literal["absoltec", "tec", "both"] = Query(
        "both",
        description=(
            "'absoltec' — only AbsolTEC files, "
            "'tec' — only TEC-suite files, "
            "'both' — union of both (default)"
        ),
    ),
    data_root: Optional[str] = Query(None),
    format: ExportFormat = Query("json", description="Response format: json, csv, xlsx"),
):
    """
    Return station codes that have data for the given year/day.

    When source='both', the response contains two lists so the caller can
    tell which data types are available per station.
    """
    absoltec_root = settings.get_absoltec_root(data_root)
    tec_root      = settings.get_tec_root(data_root)

    result: dict = {"year": year, "doy": doy}

    if source in ("absoltec", "both"):
        result["absoltec_stations"] = absoltec_discover_stations(absoltec_root, year, doy)

    if source in ("tec", "both"):
        result["tec_stations"] = tec_discover_stations(tec_root, year, doy)

    if source == "both":
        combined = sorted(
            set(result["absoltec_stations"]) | set(result["tec_stations"])
        )
        result["all_stations"] = combined

    return format_payload(result, format, f"stations_available_{year}_{doy:03d}_{source}")


@router.get("/map")
def station_map(
    year: int = Query(..., ge=2000, le=2100),
    doy: int  = Query(..., ge=1, le=366),
    data_root: Optional[str] = Query(None),
    format: ExportFormat = Query("json", description="Response format: json, csv, xlsx"),
):
    """
    Return station metadata (lat, lon, height, ECEF XYZ) for all stations
    that have TEC-suite data on the given day.

    This is the primary feed for the world-map visualisation. Stations
    without extractable coordinates will have null lat/lon fields — the
    frontend should gracefully skip those markers.
    """
    root = settings.get_tec_root(data_root)
    payload = list_stations_with_meta(year, doy, root)
    return format_payload(payload, format, f"stations_map_{year}_{doy:03d}")