"""
Stations router — combined discovery endpoint for both data types.

Provides a unified view of which stations are active on a given day,
merging information from both the AbsolTEC and TEC-suite trees.
This is the feed for the future world-map visualisation.

Endpoints:
  GET /stations/available         → stations present in AbsolTEC and/or TEC-suite
  GET /stations/available/range   → same, over a day or date range, with day counts
  GET /stations/map               → station list with coordinates (TEC-suite metadata)
  GET /stations/map/range         → same, over a day or date range, with day counts
"""

from fastapi import APIRouter, Depends, Query
from typing import Optional, Literal

from app.config import settings
from app.day_range import DayRange, day_range_query, map_days
from app.db.engine import absoltec_discover_stations, tec_discover_stations
from app.models.schemas import StationAvailabilityRangeResponse, StationDays, StationMapRangeResponse
from app.routers.export import ExportFormat, format_payload
from app.services.tec import list_stations_with_meta, list_stations_with_meta_range

SourceName = Literal["absoltec", "tec", "both"]
SOURCE_DESCRIPTION = (
    "'absoltec' — only AbsolTEC files, "
    "'tec' — only TEC-suite files, "
    "'both' — union of both (default)"
)

router = APIRouter(prefix="/stations", tags=["Stations"])


@router.get("/available")
def available_stations(
    year: int   = Query(..., ge=2000, le=2100),
    doy: int    = Query(..., ge=1, le=366),
    source: SourceName = Query("both", description=SOURCE_DESCRIPTION),
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


@router.get("/available/range", response_model=StationAvailabilityRangeResponse)
def available_stations_range(
    day_range: DayRange = Depends(day_range_query),
    source: SourceName = Query("both", description=SOURCE_DESCRIPTION),
    data_root: Optional[str] = Query(None),
    format: ExportFormat = Query("json", description="Response format: json, csv, xlsx"),
):
    """
    Station codes with data on any day of a day or date range.

    station_days says on how many days of the range each station has files,
    per data type, so callers can pick well-covered stations.
    """
    days = day_range.days()
    counts: dict[str, dict[str, int]] = {}
    result: dict = {
        "date_start": day_range.start, "date_end": day_range.end,
        "source": source, "days_in_range": len(days),
    }

    sources = {
        "absoltec": (absoltec_discover_stations, settings.get_absoltec_root(data_root)),
        "tec": (tec_discover_stations, settings.get_tec_root(data_root)),
    }
    requested = [name for name in sources if source in (name, "both")]
    for name in requested:
        discover, root = sources[name]
        per_day = map_days(lambda year, doy: discover(root, year, doy), days)
        for stations in per_day:
            for station in stations:
                entry = counts.setdefault(station, {})
                entry[name] = entry.get(name, 0) + 1
        result[f"{name}_stations"] = sorted(s for s, c in counts.items() if name in c)

    if source == "both":
        result["all_stations"] = sorted(counts)
    # 0 for a requested type the station lacks; None for a type not requested.
    result["station_days"] = [
        StationDays(
            station=s,
            absoltec_days=c.get("absoltec", 0) if "absoltec" in requested else None,
            tec_days=c.get("tec", 0) if "tec" in requested else None,
        )
        for s, c in sorted(counts.items())
    ]

    payload = StationAvailabilityRangeResponse(**result)
    return format_payload(payload, format, f"stations_available_range_{day_range.label()}_{source}")


@router.get("/map/range", response_model=StationMapRangeResponse)
def station_map_range(
    day_range: DayRange = Depends(day_range_query),
    data_root: Optional[str] = Query(None),
    format: ExportFormat = Query("json", description="Response format: json, csv, xlsx"),
):
    """
    Station metadata for every station with TEC-suite data on any day of a
    day or date range, plus how many days each has. Coordinates come from the
    station's latest day in the range.
    """
    payload = list_stations_with_meta_range(day_range, settings.get_tec_root(data_root))
    return format_payload(payload, format, f"stations_map_range_{day_range.label()}")


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