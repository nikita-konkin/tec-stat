"""
Station selection by area: within a radius of a point or station, or inside
a longitude/latitude box.

Positions come from the TEC-suite parquet headers (the same source as
/stations/map). Looking them up for a range reads one header per station, so
a range's positions are cached for settings.station_positions_ttl_sec, and
every position seen is remembered so a station can be used as a centre even
on days it has no data of its own.
"""

from __future__ import annotations

import math
import threading
import time
from dataclasses import dataclass
from typing import Literal, Optional

from app.config import settings
from app.day_range import DayRange, map_days
from app.db.engine import absoltec_discover_stations
from app.services.tec import list_stations_with_meta_range

EARTH_RADIUS_KM = 6371.0088  # mean Earth radius (IUGG)

Source = Literal["absoltec", "tec", "any"]


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in km; within 0.5% of the ellipsoid at these scales."""
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = p2 - p1, math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * EARTH_RADIUS_KM * math.asin(min(1.0, math.sqrt(a)))


@dataclass(frozen=True)
class Area:
    """A circle (centre + radius) or a box; centre_station names the centre when it is a station."""

    lat: Optional[float] = None
    lon: Optional[float] = None
    radius_km: Optional[float] = None
    centre_station: Optional[str] = None
    bbox: Optional[tuple[float, float, float, float]] = None  # lon_min, lat_min, lon_max, lat_max

    def contains(self, lat: float, lon: float) -> bool:
        if self.bbox is not None:
            lon_min, lat_min, lon_max, lat_max = self.bbox
            in_lon = lon_min <= lon <= lon_max if lon_min <= lon_max else (lon >= lon_min or lon <= lon_max)
            return lat_min <= lat <= lat_max and in_lon
        return haversine_km(self.lat, self.lon, lat, lon) <= self.radius_km

    def distance_km(self, lat: float, lon: float) -> Optional[float]:
        if self.lat is None or self.lon is None:
            return None
        return round(haversine_km(self.lat, self.lon, lat, lon), 1)

    def describe(self) -> str:
        if self.bbox is not None:
            return "bbox " + ",".join(f"{v:g}" for v in self.bbox)
        centre = self.centre_station or f"{self.lat:.4f},{self.lon:.4f}"
        return f"{self.radius_km:g} km of {centre}"


def parse_bbox(text: str) -> tuple[float, float, float, float]:
    """'lon_min,lat_min,lon_max,lat_max'; lon_min > lon_max crosses the antimeridian."""
    try:
        lon_min, lat_min, lon_max, lat_max = (float(v) for v in text.split(","))
    except ValueError:
        raise ValueError("bbox must be lon_min,lat_min,lon_max,lat_max") from None
    if not (-180 <= lon_min <= 180 and -180 <= lon_max <= 180 and -90 <= lat_min <= lat_max <= 90):
        raise ValueError("bbox needs longitudes in -180..180 and -90 <= lat_min <= lat_max <= 90")
    return lon_min, lat_min, lon_max, lat_max


def parse_near(text: str) -> tuple[Optional[float], Optional[float], Optional[str]]:
    """'lat,lon' gives a point; anything else is a station code to centre on."""
    parts = [p.strip() for p in text.split(",")]
    if len(parts) == 2:
        try:
            lat, lon = float(parts[0]), float(parts[1])
        except ValueError:
            raise ValueError("near must be 'lat,lon' or a station code") from None
        if not (-90 <= lat <= 90 and -180 <= lon <= 180):
            raise ValueError("near needs -90 <= lat <= 90 and -180 <= lon <= 180")
        return lat, lon, None
    if len(parts) == 1 and parts[0]:
        return None, None, parts[0].lower()
    raise ValueError("near must be 'lat,lon' or a station code")


# ── Position registry ────────────────────────────────────────────────────────

@dataclass(frozen=True)
class StationPosition:
    station: str
    lat: float
    lon: float
    tec_days: int


_lock = threading.Lock()
_range_cache: dict[tuple[str, object, object], tuple[float, dict[str, StationPosition]]] = {}
_last_known: dict[tuple[str, str], tuple[float, float]] = {}


def clear_position_cache() -> None:
    with _lock:
        _range_cache.clear()
        _last_known.clear()


def station_positions(day_range: DayRange, tec_root: str) -> dict[str, StationPosition]:
    """Stations with TEC-suite data in the range and known coordinates, by code."""
    key = (tec_root, day_range.start, day_range.end)
    now = time.monotonic()
    with _lock:
        cached = _range_cache.get(key)
        if cached is not None and now - cached[0] < settings.station_positions_ttl_sec:
            return cached[1]

    response = list_stations_with_meta_range(day_range, tec_root)
    positions = {
        meta.station: StationPosition(meta.station, float(meta.lat), float(meta.lon), meta.days)
        for meta in response.stations
        if meta.lat is not None and meta.lon is not None
    }
    with _lock:
        if len(_range_cache) > 64:
            _range_cache.clear()
        _range_cache[key] = (now, positions)
        for position in positions.values():
            _last_known[(tec_root, position.station)] = (position.lat, position.lon)
    return positions


def resolve_area(
    day_range: DayRange,
    tec_root: str,
    *,
    near: Optional[str],
    radius_km: Optional[float],
    bbox: Optional[str],
) -> Area:
    """Build the Area a request asks for; ValueError explains a bad combination."""
    if bbox is not None:
        if near is not None or radius_km is not None:
            raise ValueError("Give either bbox or near + radius_km, not both")
        return Area(bbox=parse_bbox(bbox))
    if near is None or radius_km is None:
        raise ValueError("near and radius_km must be given together")
    lat, lon, centre_station = parse_near(near)
    if centre_station is not None:
        position = station_positions(day_range, tec_root).get(centre_station)
        if position is not None:
            lat, lon = position.lat, position.lon
        else:
            with _lock:
                known = _last_known.get((tec_root, centre_station))
            if known is None:
                raise LookupError(
                    f"No position known for station '{centre_station}' in this range. "
                    "Give the centre as near=lat,lon instead."
                )
            lat, lon = known
    return Area(lat=lat, lon=lon, radius_km=radius_km, centre_station=centre_station)


# ── Stations in an area ──────────────────────────────────────────────────────

@dataclass(frozen=True)
class NearbyStation:
    station: str
    lat: float
    lon: float
    distance_km: Optional[float]
    tec_days: int
    absoltec_days: int


def stations_in_area(
    day_range: DayRange,
    area: Area,
    *,
    tec_root: str,
    absoltec_root: str,
) -> list[NearbyStation]:
    """Stations inside the area with data in the range, nearest first (by code for a box)."""
    inside = [p for p in station_positions(day_range, tec_root).values() if area.contains(p.lat, p.lon)]
    absoltec_days: dict[str, int] = {}
    if inside:
        wanted = {p.station for p in inside}
        per_day = map_days(lambda year, doy: absoltec_discover_stations(absoltec_root, year, doy), day_range.days())
        for stations in per_day:
            for station in stations:
                if station in wanted:
                    absoltec_days[station] = absoltec_days.get(station, 0) + 1
    result = [
        NearbyStation(
            station=p.station,
            lat=p.lat,
            lon=p.lon,
            distance_km=area.distance_km(p.lat, p.lon),
            tec_days=p.tec_days,
            absoltec_days=absoltec_days.get(p.station, 0),
        )
        for p in inside
    ]
    result.sort(key=lambda s: (s.distance_km if s.distance_km is not None else 0.0, s.station))
    return result


def has_source_data(station: NearbyStation, source: Source) -> bool:
    if source == "absoltec":
        return station.absoltec_days > 0
    if source == "tec":
        return station.tec_days > 0
    return station.absoltec_days > 0 or station.tec_days > 0
