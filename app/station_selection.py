"""
Which stations a range request covers, shared by every range endpoint.

Stations can be listed (station=, repeated stations=) or chosen by area:
  near=lat,lon&radius_km=R      — within R km of a point
  near=kazn&radius_km=R         — within R km of a station
  bbox=lon_min,lat_min,lon_max,lat_max
An area keeps only stations with data of the endpoint's type in the range.
Listed stations and an area together select both.
"""

from __future__ import annotations

from typing import Callable, Optional

from fastapi import Depends, HTTPException, Query

from app.config import settings
from app.day_range import DayRange, day_range_query
from app.services.area import Source, has_source_data, resolve_area, stations_in_area

AREA_HELP = "near + radius_km, or bbox"


def station_selection(source: Source) -> Callable[..., list[str]]:
    """Dependency resolving the request's stations; `source` is the data type an area must have."""

    def dependency(
        day_range: DayRange = Depends(day_range_query),
        station: Optional[str] = Query(None, min_length=2, max_length=9),
        stations: Optional[list[str]] = Query(None, description="Alternative: repeated ?stations=... query values"),
        near: Optional[str] = Query(
            None, description="Area centre as 'lat,lon' or a station code; use with radius_km"
        ),
        radius_km: Optional[float] = Query(None, gt=0, le=5000, description="Area radius around `near` (km)"),
        bbox: Optional[str] = Query(None, description="Area box as lon_min,lat_min,lon_max,lat_max"),
        data_root: Optional[str] = Query(None),
    ) -> list[str]:
        selected = {s.lower() for s in [station, *(stations or [])] if s}

        if near is not None or radius_km is not None or bbox is not None:
            tec_root = settings.get_tec_root(data_root)
            try:
                area = resolve_area(day_range, tec_root, near=near, radius_km=radius_km, bbox=bbox)
            except ValueError as exc:
                raise HTTPException(422, str(exc)) from exc
            except LookupError as exc:
                raise HTTPException(404, str(exc)) from exc
            found = [
                s.station
                for s in stations_in_area(
                    day_range, area, tec_root=tec_root, absoltec_root=settings.get_absoltec_root(data_root)
                )
                if has_source_data(s, source)
            ]
            if not found and not selected:
                raise HTTPException(404, f"No stations with {source} data within {area.describe()} in this range")
            if len(found) > settings.max_area_stations:
                raise HTTPException(
                    422,
                    f"{len(found)} stations within {area.describe()}; the limit is "
                    f"{settings.max_area_stations}. Use a smaller area.",
                )
            selected.update(found)

        if not selected:
            raise HTTPException(422, f"Provide station, stations, or an area ({AREA_HELP})")
        return sorted(selected)

    return dependency
