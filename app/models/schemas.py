"""
Pydantic schemas for all API request parameters and response bodies.

Column naming note:
  AbsolTEC columns use the exact casing from the parquet files:
    UT, I_v, G_lon, G_lat, G_q_lon, G_q_lat, G_t, G_q_t
  Python model fields use snake_case aliases for clean attribute access.
"""

import datetime
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
    tec:    Optional[float] = Field(
        ..., description="Absolute vertical TEC, TECU (column I_v); None when TayAbsTEC left the slot unsolved (I_v <= 0)"
    )
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
    mean_tec:    float = Field(..., description="Mean TEC across N valid samples (TECU)")
    variance:    Optional[float] = Field(..., description="Sample variance s² (TECU², denominator N-1); None when N < 2")
    std_dev:     Optional[float] = Field(..., description="Sample standard deviation s (TECU); None when N < 2")
    student_ci:  Optional[float] = Field(
        ..., description="Half-width of Student-t CI at the configured alpha; None when N < 2. Plot as mean ± student_ci."
    )
    n:           int   = Field(..., description="Number of valid samples (I_v > 0) in this slot's average")
    mean_g_lon:  Optional[float] = Field(None, description="Mean SIP longitude across N days")
    mean_g_lat:  Optional[float] = Field(None, description="Mean SIP latitude across N days")


class StatisticsResponse(BaseModel):
    year:       int
    doy_start:  int
    doy_end:    int = Field(..., description="DOY of the last day, in date_end's year")
    date_start: Optional[datetime.date] = Field(None, description="First day of the range")
    date_end:   Optional[datetime.date] = Field(None, description="Last day of the range")
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
    tec:    Optional[float] = Field(..., description="Absolute vertical TEC, TECU; None for a missing sample (I_v <= 0)")
    cb:     Optional[float] = Field(..., description="Coherence Band value; None when tec is None")
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
    mean_cb:     float = Field(..., description="Mean CB across N valid samples")
    variance:    Optional[float] = Field(..., description="Sample variance s² (denominator N-1); None when N < 2")
    std_dev:     Optional[float] = Field(..., description="Sample standard deviation s; None when N < 2")
    student_ci:  Optional[float] = Field(
        ..., description="Half-width of Student-t CI at the configured alpha; None when N < 2. Plot as mean ± student_ci."
    )
    n:           int   = Field(..., description="Number of valid samples (I_v > 0) in this slot's average")
    mean_g_lon:  Optional[float] = Field(None, description="Mean SIP longitude across N days")
    mean_g_lat:  Optional[float] = Field(None, description="Mean SIP latitude across N days")


class StatisticsResponseCB(BaseModel):
    year:       int
    doy_start:  int
    doy_end:    int = Field(..., description="DOY of the last day, in date_end's year")
    date_start: Optional[datetime.date] = Field(None, description="First day of the range")
    date_end:   Optional[datetime.date] = Field(None, description="Last day of the range")
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
# Propagation parameter schemas — derived from TEC / AbsolTEC
# ──────────────────────────────────────────────────────────────────────────────

class PropagationDirectResponse(BaseModel):
    """
    Direct propagation-parameter calculation from TEC and working frequency.

    Input TEC is provided in TECU. The derived N_t is returned in electrons/m^2,
    while B_k and GDD are returned exactly as the implemented physical formulas
    produce for the supplied SI frequency value.
    """
    tec:  float = Field(..., description="Input TEC in TECU")
    nt:   float = Field(..., description="Absolute total electron content N_t in electrons/m^2")
    f_hz: float = Field(..., description="Working frequency in Hz")
    signal_band: Optional[str] = Field(None, description="Resolved signal-band preset used for frequency lookup")
    b_k:  float = Field(..., description="Coherence bandwidth B_k")
    gdd:  float = Field(..., description="Group delay dispersion GDD")


class PropagationPointAbsoltec(BaseModel):
    """One AbsolTEC sample with derived propagation parameters."""
    ut:     float = Field(..., description="Universal time in decimal hours [0, 23.5]")
    tec:    Optional[float] = Field(..., description="Absolute vertical TEC, TECU (column I_v); None for a missing sample (I_v <= 0)")
    nt:     Optional[float] = Field(None, description="Absolute total electron content N_t in electrons/m^2")
    f_hz:   float = Field(..., description="Working frequency in Hz")
    signal_band: Optional[str] = Field(None, description="Resolved signal-band preset used for frequency lookup")
    b_k:    Optional[float] = Field(None, description="Coherence bandwidth B_k")
    gdd:    Optional[float] = Field(None, description="Group delay dispersion GDD")
    g_lon:  Optional[float] = Field(None, description="SIP geographic longitude (°)")
    g_lat:  Optional[float] = Field(None, description="SIP geographic latitude (°)")
    g_q_lon: Optional[float] = Field(None, description="Quality flag for G_lon")
    g_q_lat: Optional[float] = Field(None, description="Quality flag for G_lat")
    g_t:    Optional[float] = Field(None, description="TayAbsTEC G_t parameter")
    g_q_t:  Optional[float] = Field(None, description="Quality flag for G_t")


class PropagationStatisticsPoint(BaseModel):
    """Aggregated propagation statistics for one AbsolTEC time slot across N days."""
    ut:             float = Field(..., description="Universal time in decimal hours")
    mean_tec:       float = Field(..., description="Mean TEC across valid samples (TECU)")
    mean_nt:        float = Field(..., description="Mean absolute total electron content N_t")
    mean_b_k:       float = Field(..., description="Mean coherence bandwidth B_k")
    variance_b_k:   Optional[float] = Field(..., description="Sample variance of B_k (denominator N-1); None when N < 2")
    std_dev_b_k:    Optional[float] = Field(..., description="Sample standard deviation of B_k; None when N < 2")
    student_ci_b_k: Optional[float] = Field(..., description="Half-width of Student-t CI for B_k; None when N < 2")
    mean_gdd:       float = Field(..., description="Mean group delay dispersion GDD")
    variance_gdd:   Optional[float] = Field(..., description="Sample variance of GDD (denominator N-1); None when N < 2")
    std_dev_gdd:    Optional[float] = Field(..., description="Sample standard deviation of GDD; None when N < 2")
    student_ci_gdd: Optional[float] = Field(..., description="Half-width of Student-t CI for GDD; None when N < 2")
    n:              int   = Field(..., description="Number of valid samples in this slot's average")
    mean_g_lon:     Optional[float] = Field(None, description="Mean SIP longitude across valid samples")
    mean_g_lat:     Optional[float] = Field(None, description="Mean SIP latitude across valid samples")


class PropagationStatisticsResponse(BaseModel):
    year:       int
    doy_start:  int
    doy_end:    int = Field(..., description="DOY of the last day, in date_end's year")
    date_start: Optional[datetime.date] = Field(None, description="First day of the range")
    date_end:   Optional[datetime.date] = Field(None, description="Last day of the range")
    station:    str
    alpha:      float
    f_hz:       float
    signal_band: Optional[str] = None
    total_days: int = Field(..., description="Days with actual data found in the range")
    points:     list[PropagationStatisticsPoint]


class PropagationTecPoint(BaseModel):
    """One TEC-suite observation with derived propagation parameters."""
    tsn:        int   = Field(..., description="Time sequence number")
    hour:       float = Field(..., description="Time in decimal hours")
    el:         float = Field(..., description="Satellite elevation (°)")
    az:         float = Field(..., description="Satellite azimuth (°)")
    observable: str   = Field(..., description="Selected TEC observable: tec_l1l2 or tec_c1p2")
    tec:        float = Field(..., description="Selected TEC observable value in TECU")
    nt:         Optional[float] = Field(None, description="Absolute total electron content N_t in electrons/m^2")
    f_hz:       float = Field(..., description="Working frequency in Hz")
    signal_band: Optional[str] = Field(None, description="Resolved signal-band preset used for frequency lookup")
    b_k:        Optional[float] = Field(None, description="Coherence bandwidth B_k")
    gdd:        Optional[float] = Field(None, description="Group delay dispersion GDD")
    validity:   int   = Field(..., description="Quality flag (0 = valid observation)")


class PropagationTecResponse(BaseModel):
    year:       int
    doy:        int
    station:    str
    satellite:  str
    observable: str
    f_hz:       float
    signal_band: Optional[str] = None
    points:     list[PropagationTecPoint]


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
# Availability over a date range
# ──────────────────────────────────────────────────────────────────────────────

class SatelliteDays(BaseModel):
    satellite: str
    days:      int = Field(..., description="Days in the range with a file for this satellite")


class SatelliteRangeResponse(BaseModel):
    station:        str
    date_start:     datetime.date
    date_end:       datetime.date
    satellites:     list[str]
    satellite_days: list[SatelliteDays]


class StationRangeMetadata(StationMetadata):
    """Station coordinates (from its latest day in the range) plus how many days it has data."""
    days: int = Field(..., description="Days in the range with TEC-suite data for this station")


class StationMapRangeResponse(BaseModel):
    date_start: datetime.date
    date_end:   datetime.date
    stations:   list[StationRangeMetadata]


class StationDays(BaseModel):
    station:       str
    absoltec_days: Optional[int] = Field(None, description="Days with AbsolTEC files; None when not requested")
    tec_days:      Optional[int] = Field(None, description="Days with TEC-suite files; None when not requested")


class StationAvailabilityRangeResponse(BaseModel):
    date_start:        datetime.date
    date_end:          datetime.date
    source:            str
    days_in_range:     int
    absoltec_stations: Optional[list[str]] = None
    tec_stations:      Optional[list[str]] = None
    all_stations:      Optional[list[str]] = None
    station_days:      list[StationDays]


class NearbyStation(BaseModel):
    station:       str
    lat:           float
    lon:           float
    distance_km:   Optional[float] = Field(None, description="From the area centre; None for a bbox")
    absoltec_days: int = Field(..., description="Days in the range with AbsolTEC files")
    tec_days:      int = Field(..., description="Days in the range with TEC-suite files")


class AreaCentre(BaseModel):
    lat:     float
    lon:     float
    station: Optional[str] = Field(None, description="Set when the centre is a station")


class NearbyStationsResponse(BaseModel):
    date_start:    datetime.date
    date_end:      datetime.date
    source:        str
    centre:        Optional[AreaCentre] = None
    radius_km:     Optional[float] = None
    bbox:          Optional[list[float]] = Field(None, description="lon_min, lat_min, lon_max, lat_max")
    station_count: int
    stations:      list[NearbyStation] = Field(..., description="Nearest first; by code for a bbox")


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
