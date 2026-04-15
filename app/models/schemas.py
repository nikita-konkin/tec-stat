"""
Pydantic schemas for all API request parameters and response bodies.

Column naming note:
  AbsolTEC columns use the exact casing from the parquet files:
    UT, I_v, G_lon, G_lat, G_q_lon, G_q_lat, G_t, G_q_t
  Python model fields use snake_case aliases for clean attribute access.
"""

from typing import Optional, Any
from pydantic import BaseModel, Field


# ──────────────────────────────────────────────────────────────────────────────
# AbsolTEC schemas
# ──────────────────────────────────────────────────────────────────────────────

class TimeSeriesPoint(BaseModel):
    """
    One 30-minute sample from a single AbsolTEC parquet file.

    All eight columns from the TayAbsTEC output are included. The
    sub-ionospheric point (SIP) coordinates G_lon / G_lat tell you
    where in the ionosphere the measurement was made — useful for
    building spatial maps later.
    """
    ut:     float = Field(...,  description="Universal time in decimal hours [0, 23.5]")
    tec:    float = Field(...,  description="Absolute vertical TEC, TECU (column I_v)")
    g_lon:  Optional[float] = Field(None, description="SIP geographic longitude (°)")
    g_lat:  Optional[float] = Field(None, description="SIP geographic latitude (°)")
    g_q_lon: Optional[float] = Field(None, description="Quality flag for G_lon")
    g_q_lat: Optional[float] = Field(None, description="Quality flag for G_lat")
    g_t:    Optional[float] = Field(None, description="TayAbsTEC G_t parameter")
    g_q_t:  Optional[float] = Field(None, description="Quality flag for G_t")


class StatisticsPoint(BaseModel):
    """
    Aggregated statistics for one 30-minute time slot across N days.

    This is the core output of the service — it replaces the 4-row
    matrix_stat from the original Count_statistics() function, extended
    with the mean SIP coordinates so clients can draw spatial plots.
    """
    ut:          float = Field(..., description="Universal time in decimal hours")
    mean_tec:    float = Field(..., description="Mean TEC across N days (TECU)")
    variance:    float = Field(..., description="Population variance σ² (TECU²)")
    std_dev:     float = Field(..., description="Standard deviation σ (TECU)")
    student_ci:  float = Field(
        ..., description="Half-width of Student-t CI at the configured alpha. Plot as mean ± student_ci."
    )
    n:           int   = Field(..., description="Number of days in this slot's average")
    mean_g_lon:  Optional[float] = Field(None, description="Mean SIP longitude across N days")
    mean_g_lat:  Optional[float] = Field(None, description="Mean SIP latitude across N days")


class StatisticsResponse(BaseModel):
    year:       int
    doy_start:  int
    doy_end:    int
    station:    str
    alpha:      float
    total_days: int = Field(..., description="Days with actual data found in the range")
    points:     list[StatisticsPoint]


class PerStationStatisticsResponse(BaseModel):
    """Statistics for one specific day, averaged across multiple stations."""
    year:           int
    doy:            int
    stations_found: list[str]
    alpha:          float
    points:         list[StatisticsPoint]


class AvailabilityResponse(BaseModel):
    year:     int
    doy:      Optional[int]        = None
    station:  Optional[str]        = None
    stations: Optional[list[str]]  = None
    days:     Optional[list[int]]  = None


# ──────────────────────────────────────────────────────────────────────────────
# CB (Coherence Band) schemas — derived from AbsolTEC
# ──────────────────────────────────────────────────────────────────────────────

class TimeSeriesPointCB(BaseModel):
    """
    One 30-minute sample with CB calculated from AbsolTEC.

    CB is calculated as: sqrt(4*3*10^8 * 1^3 * 10^27) / sqrt(80.5 * π * abs_tec * 10^16)
    """
    ut:     float = Field(...,  description="Universal time in decimal hours [0, 23.5]")
    tec:    float = Field(...,  description="Absolute vertical TEC, TECU")
    cb:     float = Field(...,  description="Coherence Band value")
    g_lon:  Optional[float] = Field(None, description="SIP geographic longitude (°)")
    g_lat:  Optional[float] = Field(None, description="SIP geographic latitude (°)")
    g_q_lon: Optional[float] = Field(None, description="Quality flag for G_lon")
    g_q_lat: Optional[float] = Field(None, description="Quality flag for G_lat")
    g_t:    Optional[float] = Field(None, description="TayAbsTEC G_t parameter")
    g_q_t:  Optional[float] = Field(None, description="Quality flag for G_t")


class StatisticsPointCB(BaseModel):
    """
    Aggregated statistics for CB values in one 30-minute time slot across N days.
    """
    ut:          float = Field(..., description="Universal time in decimal hours")
    mean_cb:     float = Field(..., description="Mean CB across N days")
    variance:    float = Field(..., description="Population variance σ²")
    std_dev:     float = Field(..., description="Standard deviation σ")
    student_ci:  float = Field(
        ..., description="Half-width of Student-t CI at the configured alpha. Plot as mean ± student_ci."
    )
    n:           int   = Field(..., description="Number of days in this slot's average")
    mean_g_lon:  Optional[float] = Field(None, description="Mean SIP longitude across N days")
    mean_g_lat:  Optional[float] = Field(None, description="Mean SIP latitude across N days")


class StatisticsResponseCB(BaseModel):
    year:       int
    doy_start:  int
    doy_end:    int
    station:    str
    alpha:      float
    total_days: int = Field(..., description="Days with actual data found in the range")
    points:     list[StatisticsPointCB]


class PerStationStatisticsResponseCB(BaseModel):
    """Statistics for CB values on one specific day, averaged across multiple stations."""
    year:           int
    doy:            int
    stations_found: list[str]
    alpha:          float
    points:         list[StatisticsPointCB]


# ──────────────────────────────────────────────────────────────────────────────
# TEC-suite schemas
# ──────────────────────────────────────────────────────────────────────────────

class TecPoint(BaseModel):
    """
    One observation from a TEC-suite satellite parquet file.

    Column names follow the original TEC-suite header comment exactly
    (all lowercase). The dot-named columns (tec.l1l2, tec.c1p2) are
    aliased to underscore names at the SQL layer.
    """
    tsn:       int   = Field(..., description="Time sequence number")
    hour:      float = Field(..., description="Time in decimal hours")
    el:        float = Field(..., description="Satellite elevation (°)")
    az:        float = Field(..., description="Satellite azimuth (°)")
    tec_l1l2:  float = Field(..., description="TEC from L1/L2 carrier phase (TECU)")
    tec_c1p2:  float = Field(..., description="TEC from C1/P2 pseudorange (TECU)")
    validity:  int   = Field(..., description="Quality flag (0 = valid observation)")


class StationMetadata(BaseModel):
    """
    Station position extracted from the TEC-suite parquet metadata or sidecar.

    TEC-suite uses the Russian geodetic convention in its header:
      L = longitude, B = latitude, H = height (not the other way round).
    These are stored here as lon/lat/height for clarity.
    """
    station: str
    lat:    Optional[float] = Field(None, description="Geodetic latitude (°)")
    lon:    Optional[float] = Field(None, description="Geodetic longitude (°)")
    height: Optional[float] = Field(None, description="Ellipsoidal height (m)")
    x:      Optional[float] = Field(None, description="ECEF X (m)")
    y:      Optional[float] = Field(None, description="ECEF Y (m)")
    z:      Optional[float] = Field(None, description="ECEF Z (m)")
    site:   Optional[str]   = None
    has_data: bool = True


class TecDataResponse(BaseModel):
    year:      int
    doy:       int
    station:   str
    satellite: str
    points:    list[TecPoint]


class SatelliteListResponse(BaseModel):
    year:       int
    doy:        int
    station:    str
    satellites: list[str]


class StationMapResponse(BaseModel):
    year:     int
    doy:      int
    stations: list[StationMetadata]


# ──────────────────────────────────────────────────────────────────────────────
# Plot data / script schemas
# ──────────────────────────────────────────────────────────────────────────────

class PlotData(BaseModel):
    """
    JSON-serialisable payload returned when format=json or used to generate
    a format=script response.

    'series' is a column-oriented dict: each key is a column name, each value
    is a list of numbers. This layout is efficient for both JSON transfer and
    direct use with numpy/matplotlib.

    Example:
      {
        "plot_type": "absoltec_average",
        "title":     "...",
        "metadata":  {...},
        "series":    {"ut": [...], "mean_tec": [...], ...},
        "plot_options": {"show_student_ci": true, ...}
      }
    """
    plot_type:    str
    title:        str
    xlabel:       str
    ylabel:       str
    metadata:     dict[str, Any]
    series:       dict[str, list]
    plot_options: dict[str, Any] = {}
    figure_width:  float = 12.0
    figure_height: float = 6.0
    dpi:           int   = 100
