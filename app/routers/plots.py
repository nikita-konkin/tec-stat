"""
Plots router.

Every endpoint accepts a 'format' query parameter:
  format=png    (default) → StreamingResponse with Content-Type: image/png
  format=json             → JSONResponse with the column-oriented data dict
  format=script           → text/x-python attachment (standalone Python script)

This means a single URL can serve the rendered image to a web browser,
the raw data to a JavaScript chart library, or a reproducible script to
a scientist who wants to customise the figure in their own environment.

Helper _respond() centralises the format dispatch so each endpoint only needs
to compute a PlotResult and pass it here with a suggested filename stem.
"""

import io
from typing import Literal, Optional

from fastapi import APIRouter, Query, HTTPException
from fastapi.responses import JSONResponse, Response, StreamingResponse

from app.config import settings
from app.db.engine import tec_glob_satellites, find_tec_file
from app.services.absoltec import (
    compute_statistics,
    compute_statistics_per_station_day,
    get_raw_data,
    get_raw_data_range,
)
from app.services.cb import (
    compute_statistics_cb,
    compute_statistics_per_station_day_cb,
    get_raw_data_cb,
    get_raw_data_range_cb,
)
from app.services.tec import get_tec_data
from app.plotting import PlotResult
from app.plotting import absoltec_plots as ap
from app.plotting import tec_plots as tp
from app.plotting import cb_plots as cp

router = APIRouter(prefix="/plots", tags=["Plots"])

# Type alias for the three supported formats
PlotFormat = Literal["png", "json", "script"]
ABSOLTEC_RAW_COLUMNS = {"tec", "g_lon", "g_lat", "g_q_lon", "g_q_lat", "g_t", "g_q_t"}


# ── Format dispatcher ─────────────────────────────────────────────────────────

def _respond(result: PlotResult, fmt: str, filename_stem: str) -> Response:
    """
    Convert a PlotResult to the HTTP response matching the requested format.

    PNG   — StreamingResponse so the browser can display it inline via <img src>.
    JSON  — JSONResponse with the full column-oriented data dict.
    Script — text/x-python attachment; the client gets a runnable .py file.
    """
    if fmt == "json":
        return JSONResponse(content=result.data)

    if fmt == "script":
        from app.plotting.script_generator import generate_script
        script_text = generate_script(result.data)
        return Response(
            content=script_text,
            media_type="text/x-python",
            headers={
                "Content-Disposition": f'attachment; filename="{filename_stem}.py"'
            },
        )

    # Default: PNG
    return StreamingResponse(
        io.BytesIO(result.png),
        media_type="image/png",
        headers={"Content-Disposition": "inline"},
    )


# ── AbsolTEC plots ────────────────────────────────────────────────────────────

@router.get("/absoltec/average")
def plot_absoltec_average(
    year: int      = Query(..., ge=2000, le=2100),
    doy_start: int = Query(..., ge=1, le=366),
    doy_end: int   = Query(..., ge=1, le=366),
    station: str   = Query(..., min_length=2, max_length=9),
    alpha: float   = Query(settings.default_alpha, ge=0.001, le=0.5),
    show_ci:  bool = Query(True,  description="Show Student CI error bars"),
    show_var: bool = Query(False, description="Show variance error bars"),
    width_px: int  = Query(settings.plot_width_px, ge=400, le=4000),
    height_px: int = Query(settings.plot_height_px, ge=300, le=4000),
    dpi: int       = Query(settings.plot_dpi, ge=72, le=300),
    fmt: PlotFormat = Query("png", alias="format",
                             description="Response format: png | json | script"),
    data_root: Optional[str] = Query(None),
):
    """
    Mean TEC averaged across all days in [doy_start, doy_end] for one station,
    with optional Student CI and variance error bars.
    """
    if doy_start > doy_end:
        raise HTTPException(422, "doy_start must be ≤ doy_end")
    root   = settings.get_absoltec_root(data_root)
    result = compute_statistics(year, doy_start, doy_end, station, alpha, root)
    if not result.points:
        raise HTTPException(404, f"No data for station={station!r} "
                                  f"year={year} doy {doy_start}–{doy_end}")

    plot = ap.plot_average(
        result.points, year, doy_start, doy_end, station,
        result.total_days, show_ci, show_var, width_px, height_px, dpi,
    )
    return _respond(plot, fmt, f"absoltec_avg_{station}_{year}_d{doy_start}-d{doy_end}")


@router.get("/absoltec/day")
def plot_absoltec_day(
    year: int    = Query(..., ge=2000, le=2100),
    doy: int     = Query(..., ge=1, le=366),
    station: str = Query(..., min_length=2, max_length=9),
    smooth: bool = Query(False, description="Apply Savitzky-Golay smoothing"),
    poly: int    = Query(settings.savgol_polynomial_order, ge=2, le=6),
    width_px: int  = Query(settings.plot_width_px),
    height_px: int = Query(settings.plot_height_px),
    dpi: int       = Query(settings.plot_dpi),
    fmt: PlotFormat = Query("png", alias="format"),
    data_root: Optional[str] = Query(None),
):
    """Raw (or smoothed) TEC for a single day — all 8 columns available in JSON format."""
    root   = settings.get_absoltec_root(data_root)
    points = get_raw_data(year, doy, station, root)
    if not points:
        raise HTTPException(404, f"No data for station={station!r} year={year} doy={doy}")
    plot = ap.plot_single_day(points, year, doy, station, smooth, poly,
                               width_px, height_px, dpi)
    return _respond(plot, fmt, f"absoltec_day_{station}_{year}_d{doy:03d}")


@router.get("/absoltec/multi-station")
def plot_absoltec_multi_station(
    year: int    = Query(..., ge=2000, le=2100),
    doy: int     = Query(..., ge=1, le=366),
    stations: list[str] = Query(..., description="Station codes to overlay"),
    smooth: bool = Query(False),
    poly: int    = Query(settings.savgol_polynomial_order),
    width_px: int  = Query(settings.plot_width_px),
    height_px: int = Query(settings.plot_height_px),
    dpi: int       = Query(settings.plot_dpi),
    fmt: PlotFormat = Query("png", alias="format"),
    data_root: Optional[str] = Query(None),
):
    """Overlay TEC for multiple stations on a single day."""
    root   = settings.get_absoltec_root(data_root)
    series = {s: get_raw_data(year, doy, s, root) for s in stations}
    series = {k: v for k, v in series.items() if v}
    if not series:
        raise HTTPException(404, "No data found for any of the requested stations")
    plot = ap.plot_multi_station(series, year, doy, smooth, poly,
                                  width_px, height_px, dpi)
    return _respond(plot, fmt, f"absoltec_multi_{year}_d{doy:03d}")


@router.get("/absoltec/per-station-averages/{doy}")
def plot_absoltec_per_station_avg(
    doy: int,
    year: int      = Query(..., ge=2000, le=2100),
    doy_start: int = Query(..., ge=1, le=366),
    doy_end: int   = Query(..., ge=1, le=366),
    stations: list[str] = Query(...),
    alpha: float   = Query(settings.default_alpha),
    show_ci:  bool = Query(True),
    show_var: bool = Query(False),
    width_px: int  = Query(settings.plot_width_px),
    height_px: int = Query(settings.plot_height_px),
    dpi: int       = Query(settings.plot_dpi),
    fmt: PlotFormat = Query("png", alias="format"),
    data_root: Optional[str] = Query(None),
):
    """Average TEC per station group for a specific day (path parameter)."""
    root        = settings.get_absoltec_root(data_root)
    day_results = compute_statistics_per_station_day(
        year, doy_start, doy_end, stations, alpha, root
    )
    day_result = next((r for r in day_results if r.doy == doy), None)
    if day_result is None or not day_result.points:
        raise HTTPException(404, f"No data for doy={doy}")
    plots = ap.plot_per_station_averages(
        [day_result], year, show_ci, show_var, width_px, height_px, dpi
    )
    if not plots:
        raise HTTPException(404, "Plot generation returned empty result")
    return _respond(plots[0], fmt, f"absoltec_psa_{year}_d{doy:03d}")


@router.get("/absoltec/raw/day-by-day")
def plot_absoltec_raw_day_by_day(
    year: int      = Query(..., ge=2000, le=2100),
    doy_start: int = Query(..., ge=1, le=366),
    doy_end: int   = Query(..., ge=1, le=366),
    station: Optional[str] = Query(None, min_length=2, max_length=9),
    stations: Optional[list[str]] = Query(None, description="Alternative: repeated ?stations=... query values"),
    columns: Optional[list[str]] = Query(None, description="Columns to plot: tec,g_lon,g_lat,g_q_lon,g_q_lat,g_t,g_q_t"),
    width_px: int  = Query(settings.plot_width_px),
    height_px: int = Query(settings.plot_height_px),
    dpi: int       = Query(settings.plot_dpi),
    fmt: PlotFormat = Query("png", alias="format"),
    data_root: Optional[str] = Query(None),
):
    """
    Plot AbsolTEC raw data over a day range with a concatenated time axis.

    Supports one station (`station`) or multiple stations (`stations`) and one
    or multiple raw columns (`columns`).
    """
    if doy_start > doy_end:
        raise HTTPException(422, "doy_start must be ≤ doy_end")

    station_list: list[str] = []
    if station:
        station_list.append(station)
    if stations:
        station_list.extend(stations)
    station_list = sorted({s.lower() for s in station_list if s})
    if not station_list:
        raise HTTPException(422, "Provide either station or stations")

    selected_columns = [c.lower() for c in (columns or ["tec"])]
    invalid = sorted({c for c in selected_columns if c not in ABSOLTEC_RAW_COLUMNS})
    if invalid:
        raise HTTPException(
            422,
            f"Unsupported columns: {', '.join(invalid)}. Allowed: {', '.join(sorted(ABSOLTEC_RAW_COLUMNS))}",
        )

    rows = get_raw_data_range(
        year,
        doy_start,
        doy_end,
        station_list,
        settings.get_absoltec_root(data_root),
    )
    if not rows:
        raise HTTPException(404, "No raw data found for the requested filters")

    plot = ap.plot_day_by_day_columns(
        rows,
        year,
        doy_start,
        doy_end,
        selected_columns,
        width_px,
        height_px,
        dpi,
    )
    return _respond(plot, fmt, f"absoltec_raw_day_by_day_{year}_{doy_start:03d}_{doy_end:03d}")


# ── CB plots ──────────────────────────────────────────────────────────────────

@router.get("/cb/average")
def plot_cb_average(
    year: int      = Query(..., ge=2000, le=2100),
    doy_start: int = Query(..., ge=1, le=366),
    doy_end: int   = Query(..., ge=1, le=366),
    station: str   = Query(..., min_length=2, max_length=9),
    alpha: float   = Query(settings.default_alpha, ge=0.001, le=0.5),
    show_ci:  bool = Query(True,  description="Show Student CI error bars"),
    show_var: bool = Query(False, description="Show variance error bars"),
    width_px: int  = Query(settings.plot_width_px, ge=400, le=4000),
    height_px: int = Query(settings.plot_height_px, ge=300, le=4000),
    dpi: int       = Query(settings.plot_dpi, ge=72, le=300),
    fmt: PlotFormat = Query("png", alias="format",
                             description="Response format: png | json | script"),
    data_root: Optional[str] = Query(None),
):
    """
    Mean CB averaged across all days in [doy_start, doy_end] for one station,
    with optional Student CI and variance error bars.
    """
    if doy_start > doy_end:
        raise HTTPException(422, "doy_start must be ≤ doy_end")
    root   = settings.get_absoltec_root(data_root)
    result = compute_statistics_cb(year, doy_start, doy_end, station, alpha, root)
    if not result.points:
        raise HTTPException(404, f"No data for station={station!r} "
                                  f"year={year} doy {doy_start}–{doy_end}")

    plot = cp.plot_average_cb(
        result.points, year, doy_start, doy_end, station,
        result.total_days, show_ci, show_var, width_px, height_px, dpi,
    )
    return _respond(plot, fmt, f"cb_avg_{station}_{year}_d{doy_start}-d{doy_end}")


@router.get("/cb/day")
def plot_cb_day(
    year: int    = Query(..., ge=2000, le=2100),
    doy: int     = Query(..., ge=1, le=366),
    station: str = Query(..., min_length=2, max_length=9),
    width_px: int  = Query(settings.plot_width_px),
    height_px: int = Query(settings.plot_height_px),
    dpi: int       = Query(settings.plot_dpi),
    fmt: PlotFormat = Query("png", alias="format"),
    data_root: Optional[str] = Query(None),
):
    """Raw CB for a single day."""
    root   = settings.get_absoltec_root(data_root)
    points = get_raw_data_cb(year, doy, station, root)
    if not points:
        raise HTTPException(404, f"No data for station={station!r} year={year} doy={doy}")
    plot = cp.plot_single_day_cb(points, year, doy, station,
                                 width_px, height_px, dpi)
    return _respond(plot, fmt, f"cb_day_{station}_{year}_d{doy:03d}")


@router.get("/cb/multi-station")
def plot_cb_multi_station(
    year: int      = Query(..., ge=2000, le=2100),
    doy_start: int = Query(..., ge=1, le=366),
    doy_end: int   = Query(..., ge=1, le=366),
    station: Optional[str] = Query(None, min_length=2, max_length=9),
    stations: Optional[list[str]] = Query(None, description="Alternative: repeated ?stations=... query values"),
    width_px: int  = Query(settings.plot_width_px),
    height_px: int = Query(settings.plot_height_px),
    dpi: int       = Query(settings.plot_dpi),
    fmt: PlotFormat = Query("png", alias="format"),
    data_root: Optional[str] = Query(None),
):
    """CB time series for multiple stations over a day range."""
    if doy_start > doy_end:
        raise HTTPException(422, "doy_start must be ≤ doy_end")

    station_list: list[str] = []
    if station:
        station_list.append(station)
    if stations:
        station_list.extend(stations)
    station_list = sorted({s.lower() for s in station_list if s})
    if not station_list:
        raise HTTPException(422, "Provide either station or stations")

    rows = get_raw_data_range_cb(
        year,
        doy_start,
        doy_end,
        station_list,
        settings.get_absoltec_root(data_root),
    )
    if not rows:
        raise HTTPException(404, "No CB data found for the requested filters")

    plot = cp.plot_multi_station_cb(
        rows, year, doy_start, doy_end, station_list,
        width_px, height_px, dpi,
    )
    filename_stations = "-".join(station_list[:3])
    if len(station_list) > 3:
        filename_stations += f"-plus{len(station_list)-3}"
    return _respond(plot, fmt, f"cb_multi_{year}_{doy_start:03d}_{doy_end:03d}_{filename_stations}")


@router.get("/cb/vs-tec")
def plot_cb_vs_tec(
    year: int      = Query(..., ge=2000, le=2100),
    doy_start: int = Query(..., ge=1, le=366),
    doy_end: int   = Query(..., ge=1, le=366),
    station: str   = Query(..., min_length=2, max_length=9),
    width_px: int  = Query(settings.plot_width_px),
    height_px: int = Query(settings.plot_height_px),
    dpi: int       = Query(settings.plot_dpi),
    fmt: PlotFormat = Query("png", alias="format"),
    data_root: Optional[str] = Query(None),
):
    """Scatter plot of CB vs AbsolTEC values."""
    rows = get_raw_data_range_cb(
        year,
        doy_start,
        doy_end,
        [station],
        settings.get_absoltec_root(data_root),
    )
    if not rows:
        raise HTTPException(404, f"No data for station={station!r} "
                                  f"year={year} doy {doy_start}–{doy_end}")

    plot = cp.plot_cb_vs_tec(
        rows, year, doy_start, doy_end, station,
        width_px, height_px, dpi,
    )
    return _respond(plot, fmt, f"cb_vs_tec_{station}_{year}_d{doy_start}-d{doy_end}")


@router.get("/cb/per-station-averages/{doy}")
def plot_cb_per_station_avg(
    doy: int,
    year: int      = Query(..., ge=2000, le=2100),
    doy_start: int = Query(..., ge=1, le=366),
    doy_end: int   = Query(..., ge=1, le=366),
    stations: list[str] = Query(...),
    alpha: float   = Query(settings.default_alpha),
    show_ci:  bool = Query(True),
    show_var: bool = Query(False),
    width_px: int  = Query(settings.plot_width_px),
    height_px: int = Query(settings.plot_height_px),
    dpi: int       = Query(settings.plot_dpi),
    fmt: PlotFormat = Query("png", alias="format"),
    data_root: Optional[str] = Query(None),
):
    """Average CB per station group for a specific day (path parameter)."""
    root        = settings.get_absoltec_root(data_root)
    day_results = compute_statistics_per_station_day_cb(
        year, doy_start, doy_end, stations, alpha, root
    )
    day_result = next((r for r in day_results if r.doy == doy), None)
    if day_result is None or not day_result.points:
        raise HTTPException(404, f"No data for doy={doy}")
    plots = cp.plot_per_station_averages_cb(
        [day_result], year, doy_start, doy_end, stations,
        width_px, height_px, dpi
    )
    if not plots:
        raise HTTPException(404, "Plot generation returned empty result")
    return _respond(plots[0], fmt, f"cb_psa_{year}_d{doy:03d}")


# ── TEC-suite plots ───────────────────────────────────────────────────────────

@router.get("/tec/satellite")
def plot_tec_satellite(
    year: int      = Query(..., ge=2000, le=2100),
    doy: int       = Query(..., ge=1, le=366),
    station: str   = Query(..., min_length=2, max_length=9),
    satellite: str = Query(..., min_length=2, max_length=4),
    column: Literal["tec_l1l2", "tec_c1p2"] = Query("tec_l1l2"),
    valid_only: bool = Query(True),
    width_px: int  = Query(settings.plot_width_px),
    height_px: int = Query(settings.plot_height_px),
    dpi: int       = Query(settings.plot_dpi),
    fmt: PlotFormat = Query("png", alias="format"),
    data_root: Optional[str] = Query(None),
):
    """TEC time series for one satellite on one day."""
    root   = settings.get_tec_root(data_root)
    result = get_tec_data(year, doy, station, satellite, root)
    if not result.points:
        raise HTTPException(404, "No observations found")
    plot = tp.plot_satellite(result.points, year, doy, station, satellite,
                              column, valid_only, width_px, height_px, dpi)
    return _respond(plot, fmt, f"tec_{station}_{satellite}_{year}_d{doy:03d}")


@router.get("/tec/sky-track")
def plot_tec_sky(
    year: int      = Query(..., ge=2000, le=2100),
    doy: int       = Query(..., ge=1, le=366),
    station: str   = Query(..., min_length=2, max_length=9),
    satellite: str = Query(..., min_length=2, max_length=4),
    color_by_tec: bool = Query(True),
    valid_only: bool   = Query(True),
    size_px: int       = Query(settings.plot_height_px),
    dpi: int           = Query(settings.plot_dpi),
    fmt: PlotFormat     = Query("png", alias="format"),
    data_root: Optional[str] = Query(None),
):
    """Polar sky-track plot (el/az coloured by TEC)."""
    root   = settings.get_tec_root(data_root)
    result = get_tec_data(year, doy, station, satellite, root)
    if not result.points:
        raise HTTPException(404, "No observations found")
    plot = tp.plot_sky_track(result.points, year, doy, station, satellite,
                              color_by_tec, valid_only, size_px, size_px, dpi)
    return _respond(plot, fmt, f"tec_sky_{station}_{satellite}_{year}_d{doy:03d}")


@router.get("/tec/all-satellites")
def plot_tec_all_satellites(
    year: int    = Query(..., ge=2000, le=2100),
    doy: int     = Query(..., ge=1, le=366),
    station: str = Query(..., min_length=2, max_length=9),
    column: Literal["tec_l1l2", "tec_c1p2"] = Query("tec_l1l2"),
    valid_only: bool = Query(True),
    width_px: int  = Query(settings.plot_width_px),
    height_px: int = Query(settings.plot_height_px),
    dpi: int       = Query(settings.plot_dpi),
    fmt: PlotFormat = Query("png", alias="format"),
    data_root: Optional[str] = Query(None),
):
    """Overlay TEC for all available satellites at a station on one day."""
    root = settings.get_tec_root(data_root)
    sats = tec_glob_satellites(root, year, doy, station)
    if not sats:
        raise HTTPException(404, "No satellite files found")
    sat_data = {
        sat: get_tec_data(year, doy, station, sat, root).points
        for sat in sats
    }
    plot = tp.plot_multi_satellite(sat_data, year, doy, station,
                                    column, valid_only, width_px, height_px, dpi)
    return _respond(plot, fmt, f"tec_all_sats_{station}_{year}_d{doy:03d}")