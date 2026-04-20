"""
TEC-suite matplotlib plotting.

All functions return PlotResult(png, data) following the same contract as
absoltec_plots.py so the router layer is uniform across both data types.
"""

import io
from typing import Literal, Optional

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
from matplotlib.ticker import FuncFormatter
import numpy as np

from app.config import settings
from app.models.schemas import TecPoint
from app.plotting import PlotResult


plt.rcParams.update(
    {
        "font.family": "serif",
        "font.serif": ["Times New Roman", "Times", "DejaVu Serif"],
        "axes.titlesize": 15,
        "axes.labelsize": 14,
        "xtick.labelsize": 12,
        "ytick.labelsize": 12,
        "legend.fontsize": 12,
    }
)

_TITLE_FS = 15
_LABEL_FS = 14
_LEGEND_FS = 12


def _format_ut_hours(x: float, _pos: int) -> str:
    minutes = int(round(float(x) * 60.0))
    hh = (minutes // 60) % 24
    mm = minutes % 60
    return f"{hh:02d}:{mm:02d}"


def _apply_ut_axis(ax, x_step: float = 2.0):
    ax.xaxis.set_major_locator(ticker.MultipleLocator(x_step))
    ax.xaxis.set_major_formatter(FuncFormatter(_format_ut_hours))


# ── Helpers ───────────────────────────────────────────────────────────────────

def _new_fig(width_px: int, height_px: int, dpi: int):
    return plt.subplots(figsize=(width_px / dpi, height_px / dpi), dpi=dpi)


def _render(fig) -> bytes:
    buf = io.BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    return buf.read()


def _valid(points: list[TecPoint]) -> list[TecPoint]:
    return [p for p in points if p.validity == 0]


def _empty_result(message: str = "No data") -> PlotResult:
    """Return a minimal 'no data' plot so callers always get a valid PlotResult."""
    fig, ax = plt.subplots(figsize=(6, 3), dpi=80)
    ax.text(0.5, 0.5, message, ha="center", va="center",
            transform=ax.transAxes, fontsize=14, color="gray")
    ax.set_axis_off()
    return PlotResult(
        png=_render(fig),
        data={"plot_type": "empty", "title": message, "xlabel": "", "ylabel": "",
              "figure_width": 6.0, "figure_height": 3.0, "dpi": 80,
              "metadata": {}, "series": {}, "plot_options": {}},
    )


# ── Plot 1: Satellite TEC time series ─────────────────────────────────────────

def plot_satellite(
    points: list[TecPoint],
    year: int,
    doy: int,
    station: str,
    satellite: str,
    column: Literal["tec_l1l2", "tec_c1p2"] = "tec_l1l2",
    valid_only: bool = True,
    width_px: int = settings.plot_width_px,
    height_px: int = settings.plot_height_px,
    dpi: int = settings.plot_dpi,
) -> PlotResult:
    """TEC time series for one satellite pass (phase or code observable)."""
    data_pts = _valid(points) if valid_only else points
    if not data_pts:
        return _empty_result("No valid observations")

    hours = [p.hour         for p in data_pts]
    tec   = [getattr(p, column) for p in data_pts]
    el    = [p.el           for p in data_pts]
    az    = [p.az           for p in data_pts]

    title = f"{station.upper()} · {satellite} · DOY {doy:03d}/{year}"
    fig, ax = _new_fig(width_px, height_px, dpi)
    ax.plot(hours, tec, ".", markersize=3, label=f"{satellite} {column}")
    ax.set_xlabel("Time, UT [h]", fontsize=_LABEL_FS)
    ax.set_ylabel("TEC, TECU",    fontsize=_LABEL_FS)
    ax.set_title(title, fontsize=_TITLE_FS)
    _apply_ut_axis(ax, 2.0)
    ax.grid(True, which="major", color="#666666", linestyle="-", alpha=0.5)
    ax.minorticks_on()
    ax.grid(True, which="minor", color="#999999", linestyle="-", alpha=0.2)
    ax.legend(fontsize=_LEGEND_FS)
    png = _render(fig)

    col_label = "tec_l1l2" if column == "tec_l1l2" else "tec_c1p2"
    data = {
        "plot_type":    "tec_satellite",
        "title":        title,
        "xlabel":       "Time, UT [h]",
        "ylabel":       "TEC, TECU",
        "figure_width":  width_px / dpi,
        "figure_height": height_px / dpi,
        "dpi":           dpi,
        "metadata": {
            "year": year, "doy": doy, "station": station,
            "satellite": satellite, "column": column, "valid_only": valid_only,
        },
        "series": {
            "hour": hours, col_label: tec, "el": el, "az": az,
        },
        "plot_options": {"column": column, "valid_only": valid_only},
    }
    return PlotResult(png=png, data=data)


# ── Plot 2: Sky track (polar elevation / azimuth) ─────────────────────────────

def plot_sky_track(
    points: list[TecPoint],
    year: int,
    doy: int,
    station: str,
    satellite: str,
    color_by_tec: bool = True,
    valid_only: bool = True,
    width_px: int = settings.plot_height_px,
    height_px: int = settings.plot_height_px,
    dpi: int = settings.plot_dpi,
) -> PlotResult:
    """
    Polar sky-track plot: azimuth on the angular axis, zenith distance on the
    radial axis, points coloured by tec_l1l2 when color_by_tec=True.
    """
    data_pts = _valid(points) if valid_only else points
    if not data_pts:
        return _empty_result("No valid observations")

    az  = np.radians([p.az for p in data_pts])
    # Convert elevation to zenith distance so low-elevation points are at the rim
    el_zen = np.array([90.0 - p.el for p in data_pts])
    tec    = np.array([p.tec_l1l2  for p in data_pts])
    hours  = [p.hour for p in data_pts]

    title = f"{station.upper()} · {satellite} · DOY {doy:03d}/{year}"

    fig = plt.figure(figsize=(width_px / dpi, height_px / dpi), dpi=dpi)
    ax  = fig.add_subplot(111, polar=True)
    ax.set_theta_zero_location("N")   # North at top
    ax.set_theta_direction(-1)         # Clockwise azimuth
    ax.set_ylim(0, 90)
    ax.set_yticks([10, 30, 50, 70, 90])
    ax.set_yticklabels(["80°", "60°", "40°", "20°", "0°"], fontsize=9)

    if color_by_tec:
        sc = ax.scatter(az, el_zen, c=tec, s=6, cmap="plasma", alpha=0.85)
        cb = fig.colorbar(sc, ax=ax, pad=0.1, shrink=0.8)
        cb.set_label("TEC, TECU", fontsize=11)
    else:
        ax.scatter(az, el_zen, s=6, alpha=0.7, label=satellite)
    ax.set_title(title, pad=18, fontsize=12)
    png = _render(fig)

    data = {
        "plot_type":    "tec_sky_track",
        "title":        title,
        "xlabel":       "Azimuth",
        "ylabel":       "Elevation",
        "figure_width":  width_px / dpi,
        "figure_height": height_px / dpi,
        "dpi":           dpi,
        "metadata": {
            "year": year, "doy": doy, "station": station,
            "satellite": satellite, "valid_only": valid_only,
        },
        "series": {
            "hour": hours,
            "az_deg": [p.az for p in data_pts],
            "el_deg": [p.el for p in data_pts],
            "tec_l1l2": tec.tolist(),
        },
        "plot_options": {"color_by_tec": color_by_tec, "valid_only": valid_only},
    }
    return PlotResult(png=png, data=data)


# ── Plot 3: Multi-satellite overlay ───────────────────────────────────────────

def plot_multi_satellite(
    satellite_data: dict,
    year: int,
    doy: int,
    station: str,
    column: Literal["tec_l1l2", "tec_c1p2"] = "tec_l1l2",
    valid_only: bool = True,
    width_px: int = settings.plot_width_px,
    height_px: int = settings.plot_height_px,
    dpi: int = settings.plot_dpi,
) -> PlotResult:
    """Overlay TEC time series for all available satellites at a station."""
    title = f"{station.upper()} · all satellites · DOY {doy:03d}/{year}"
    fig, ax = _new_fig(width_px, height_px, dpi)
    series: dict = {}
    has_data = False

    for sat, pts in satellite_data.items():
        data_pts = _valid(pts) if valid_only else pts
        if not data_pts:
            continue
        hours = [p.hour             for p in data_pts]
        tec   = [getattr(p, column) for p in data_pts]
        ax.plot(hours, tec, ".", markersize=3, label=sat, alpha=0.8)
        series[sat] = {"hour": hours, column: tec}
        has_data = True

    if not has_data:
        plt.close(fig)
        return _empty_result("No valid observations")

    ax.set_xlabel("Time, UT [h]", fontsize=_LABEL_FS)
    ax.set_ylabel("TEC, TECU",    fontsize=_LABEL_FS)
    ax.set_title(title, fontsize=_TITLE_FS)
    _apply_ut_axis(ax, 2.0)
    ax.grid(True, which="major", color="#666666", linestyle="-", alpha=0.5)
    ax.minorticks_on()
    ax.grid(True, which="minor", color="#999999", linestyle="-", alpha=0.2)
    ax.legend(fontsize=10, ncol=4, loc="upper left")
    png = _render(fig)

    data = {
        "plot_type":    "tec_multi_satellite",
        "title":        title,
        "xlabel":       "Time, UT [h]",
        "ylabel":       "TEC, TECU",
        "figure_width":  width_px / dpi,
        "figure_height": height_px / dpi,
        "dpi":           dpi,
        "metadata": {
            "year": year, "doy": doy, "station": station,
            "satellites": list(satellite_data.keys()), "column": column,
        },
        "series":       series,
        "plot_options": {"column": column, "valid_only": valid_only},
    }
    return PlotResult(png=png, data=data)
