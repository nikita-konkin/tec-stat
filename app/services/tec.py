"""
TEC-suite data service.

SQL column references use the exact lowercase names stored in the parquet files.
The two dot-named columns (tec.l1l2, tec.c1p2) must be double-quoted in SQL;
they are aliased to underscore names (tec_l1l2, tec_c1p2) so the rest of the
Python code can use them as clean attribute names.

Station coordinate extraction
------------------------------
TEC-suite embeds the original header comment block in the parquet schema
metadata under the key b'tec_suite_meta'.  If that key is absent (older
converters), we look for a sidecar .meta text file alongside the parquet.

The header follows the TEC-suite format where L=longitude, B=latitude
(Russian geodetic convention: Долгота/Широта).

If neither source is present, the service still works — coordinate fields
will be None and the world-map feature will skip that station.
"""

import math
import os
import re
import json
from typing import Optional

import pandas as pd
import pyarrow.parquet as pq

from app.config import settings
from app.db.columns import (
    TSN, HOUR, EL, AZ,
    TEC_L1L2_RAW, TEC_C1P2_RAW,
    TEC_L1L2, TEC_C1P2,
    VALIDITY, TEC_SELECT,
)
from app.db.engine import (
    get_connection,
    find_tec_file,
    tec_glob_satellites,
    tec_discover_stations,
)
from app.models.schemas import (
    TecPoint,
    TecDataResponse,
    SatelliteListResponse,
    StationMetadata,
    StationMapResponse,
)


# ──────────────────────────────────────────────────────────────────────────────
# Raw satellite data
# ──────────────────────────────────────────────────────────────────────────────

def get_tec_data(
    year: int,
    doy: int,
    station: str,
    satellite: str,
    data_root: Optional[str] = None,
) -> TecDataResponse:
    """
    Read the full observation time series for one station/day/satellite.

    The SELECT uses TEC_SELECT from db.columns which aliases:
      "tec.l1l2" → tec_l1l2
      "tec.c1p2" → tec_c1p2
    so the returned DataFrame has clean underscore-named columns.
    """
    root = data_root or settings.data_root
    path = find_tec_file(root, year, doy, station, satellite)

    if path is None:
        return TecDataResponse(
            year=year, doy=doy, station=station, satellite=satellite, points=[]
        )

    conn = get_connection()
    df: pd.DataFrame = conn.execute(
        f"SELECT {TEC_SELECT} FROM read_parquet('{path}') ORDER BY {HOUR}"
    ).df()

    points = [
        TecPoint(
            tsn=int(row[TSN]),
            hour=float(row[HOUR]),
            el=float(row[EL]),
            az=float(row[AZ]),
            tec_l1l2=float(row[TEC_L1L2]),
            tec_c1p2=float(row[TEC_C1P2]),
            validity=int(row[VALIDITY]),
        )
        for _, row in df.iterrows()
    ]

    return TecDataResponse(
        year=year, doy=doy, station=station, satellite=satellite, points=points
    )


def get_tec_data_range(
    year: int,
    doy_start: int,
    doy_end: int,
    stations: list[str],
    data_root: Optional[str] = None,
) -> list[dict]:
    """
    Return raw TEC-suite rows across a day range and station set.

    Includes all satellites available for each station/day and exposes a
    synthetic continuous time axis:
      concat_hour = (doy - doy_start) * 24 + hour
    """
    root = data_root or settings.data_root
    rows: list[dict] = []

    for station in stations:
        station_lower = station.lower()
        for doy in range(doy_start, doy_end + 1):
            satellites = tec_glob_satellites(root, year, doy, station_lower)
            for satellite in satellites:
                payload = get_tec_data(year, doy, station_lower, satellite, root)
                for p in payload.points:
                    rows.append({
                        "year": year,
                        "doy": doy,
                        "station": station_lower,
                        "satellite": satellite,
                        "concat_hour": float((doy - doy_start) * 24 + p.hour),
                        "hour": p.hour,
                        "tsn": p.tsn,
                        "el": p.el,
                        "az": p.az,
                        "tec_l1l2": p.tec_l1l2,
                        "tec_c1p2": p.tec_c1p2,
                        "validity": p.validity,
                    })

    rows.sort(key=lambda r: (r["station"], r["satellite"], r["concat_hour"]))
    return rows


# ──────────────────────────────────────────────────────────────────────────────
# Discovery
# ──────────────────────────────────────────────────────────────────────────────

def list_satellites(
    year: int, doy: int, station: str, data_root: Optional[str] = None
) -> SatelliteListResponse:
    root = data_root or settings.data_root
    sats = tec_glob_satellites(root, year, doy, station)
    return SatelliteListResponse(year=year, doy=doy, station=station, satellites=sats)


def list_stations_with_meta(
    year: int, doy: int, data_root: Optional[str] = None
) -> StationMapResponse:
    """
    Return all TEC-suite stations for a given day, with geodetic coordinates
    extracted from the parquet metadata or sidecar file.
    """
    root = data_root or settings.data_root
    stations_codes = tec_discover_stations(root, year, doy)

    station_metas = []
    for station in stations_codes:
        sats = tec_glob_satellites(root, year, doy, station)
        if not sats:
            station_metas.append(StationMetadata(station=station, has_data=False))
            continue

        # Use the first available satellite to read metadata
        path = find_tec_file(root, year, doy, station, sats[0])
        if path is None:
            station_metas.append(StationMetadata(station=station, has_data=False))
            continue

        meta = parse_station_metadata(path, station)
        station_metas.append(meta)

    return StationMapResponse(year=year, doy=doy, stations=station_metas)


# ──────────────────────────────────────────────────────────────────────────────
# Metadata parsing (public so tests can call it directly)
# ──────────────────────────────────────────────────────────────────────────────

def parse_station_metadata(parquet_path: str, station: str) -> StationMetadata:
    """
    Extract station coordinates from a TEC-suite parquet file.

    Tries two sources in priority order:
      1. The parquet schema key-value metadata (key b'tec_suite_meta').
         Embed with: pa.schema([...], metadata={"tec_suite_meta": header_text})
      2. A sidecar .meta text file with the same base name.

    If neither is present, all coordinate fields are None — the service
    still works; the map feature will just skip that station.
    """
    header_text = _read_embedded_metadata(parquet_path)
    if header_text is None:
        header_text = _read_sidecar_metadata(parquet_path)

    if header_text:
        header_text = _normalize_header_text(header_text)
        return parse_header_text(header_text, station)

    return StationMetadata(station=station)


def _read_embedded_metadata(parquet_path: str) -> Optional[str]:
    """
    Read the 'tec_suite_meta' key from the parquet schema metadata.
    Returns None if absent or unreadable.
    """
    try:
        schema = pq.read_schema(parquet_path)
        meta = schema.metadata or {}
        # pyarrow usually stores metadata keys/values as bytes.
        # Some converters use variant key names; normalize and check broadly.
        candidate_keys = {
            "tec_suite_meta",
            "tec-suite-meta",
            "tec_meta",
            "dat_parquet_handler.header_lines",
            "header",
            "metadata",
        }

        for key, raw in meta.items():
            key_str = key.decode("utf-8", errors="ignore") if isinstance(key, bytes) else str(key)
            normalized = key_str.strip().lower().replace("\x00", "")
            if normalized in candidate_keys and raw:
                text = raw.decode("utf-8") if isinstance(raw, bytes) else str(raw)
                if "Position" in text or "Site:" in text:
                    return text

        # Last-resort fallback: scan metadata values for header-like content.
        for raw in meta.values():
            if not raw:
                continue
            text = raw.decode("utf-8") if isinstance(raw, bytes) else str(raw)
            if "Position" in text or "Site:" in text:
                return text
    except Exception:
        pass
    return None


def _normalize_header_text(text: str) -> str:
    """
    Normalize metadata text into a plain multiline header block.

    Some parquet writers store header lines as a JSON array string, e.g.
      ["# ...", "# Site: ...", "# Position (L, B, H): ..."]
    Convert that representation into newline-joined lines so regex parsing works.
    """
    stripped = text.strip()
    if stripped.startswith("[") and stripped.endswith("]"):
        try:
            parsed = json.loads(stripped)
            if isinstance(parsed, list):
                lines = [str(item).strip() for item in parsed if str(item).strip()]
                if lines:
                    return "\n".join(lines)
        except Exception:
            pass
    return text


def _read_sidecar_metadata(parquet_path: str) -> Optional[str]:
    """
    Look for a .meta text file next to the parquet file.
    This is the fallback when the parquet was written without embedded metadata.
    """
    meta_path = os.path.splitext(parquet_path)[0] + ".meta"
    if os.path.isfile(meta_path):
        try:
            with open(meta_path, "r", encoding="utf-8") as fh:
                return fh.read()
        except Exception:
            pass
    return None


# Regular expressions for the TEC-suite header comment format.
# The convention is:  L = longitude, B = latitude  (Russian geodetic standard)
# Example lines:
#   # Position (L, B, H): 50.84156264475084, 54.837857328545816, 126.22089951578528
#   # Position (X, Y, Z): 2324706.1103, 2854596.6353, 5191112.72
#   # Site: aksu
_NUM = r"([+-]?(?:\d+(?:\.\d+)?|\.\d+)(?:[eE][+-]?\d+)?)"
_RE_LBH = re.compile(
    rf"(?:^|\n)\s*#?\s*Position\s*\(\s*L\s*,\s*B\s*,\s*H\s*\)\s*:\s*"
    rf"{_NUM}\s*,\s*{_NUM}\s*,\s*{_NUM}",
    re.IGNORECASE,
)
_RE_XYZ = re.compile(
    rf"(?:^|\n)\s*#?\s*Position\s*\(\s*X\s*,\s*Y\s*,\s*Z\s*\)\s*:\s*"
    rf"{_NUM}\s*,\s*{_NUM}\s*,\s*{_NUM}",
    re.IGNORECASE,
)
_RE_SITE = re.compile(r"(?:^|\n)\s*#?\s*Site\s*:\s*(.+?)\s*(?:$|\n)", re.IGNORECASE)


def parse_header_text(text: str, station: str) -> StationMetadata:
    """
    Parse a raw TEC-suite header comment block into a StationMetadata object.

    This function is public so it can be called in unit tests without needing
    real parquet files on disk.
    """
    lat = lon = height = x = y = z = None
    site = station

    m = _RE_LBH.search(text)
    if m:
        # L = longitude, B = latitude (Russian convention — counter-intuitive!)
        lon    = float(m.group(1))  # L (first value, longitude)
        lat    = float(m.group(2))  # B (second value, latitude)
        height = float(m.group(3))

    m = _RE_XYZ.search(text)
    if m:
        x, y, z = float(m.group(1)), float(m.group(2)), float(m.group(3))

    m = _RE_SITE.search(text)
    if m:
        site = m.group(1).strip()

    return StationMetadata(
        station=station,
        site=site,
        lat=lat,
        lon=lon,
        height=height,
        x=x, y=y, z=z,
    )
