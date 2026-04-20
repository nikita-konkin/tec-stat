"""
AbsolTEC matplotlib plotting.

Every plot_* function returns a PlotResult(png, data) namedtuple so the
HTTP router can serve the client's preferred format (PNG image, JSON data,
or a standalone Python script) from a single computation.

The 'data' dict is intentionally kept column-oriented:
  {"ut": [...48 floats...], "mean_tec": [...], ...}

This layout is the most efficient for both JSON transfer and direct numpy
ingestion on the client side.
"""

import datetime
import io
from typing import Optional

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
from matplotlib.ticker import FuncFormatter
import numpy as np
from scipy.signal import savgol_filter

from app.config import settings
from app.models.schemas import StatisticsPoint, TimeSeriesPoint
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


def _apply_concat_time_axis(ax, year: int, doy_start: int):
    start = datetime.datetime.strptime(f"{doy_start}.{year}", "%j.%Y")

    def _fmt(x: float, _pos: int) -> str:
        try:
            dt = start + datetime.timedelta(hours=float(x))
        except Exception:
            return ""
        return dt.strftime("%d.%m %H:%M")

    ax.xaxis.set_major_locator(ticker.MultipleLocator(6.0))
    ax.xaxis.set_major_formatter(FuncFormatter(_fmt))
    for label in ax.get_xticklabels():
        label.set_rotation(30)
        label.set_ha("right")


# ── Figure helpers ────────────────────────────────────────────────────────────

def _new_fig(width_px: int, height_px: int, dpi: int):
    return plt.subplots(figsize=(width_px / dpi, height_px / dpi), dpi=dpi)


def _style_ax(ax, x_step: float = 2.0, y_step: Optional[float] = None):
    ax.set_xlabel("Time, UT [h]", fontsize=_LABEL_FS)
    ax.set_ylabel("TEC, TECU", fontsize=_LABEL_FS)
    ax.xaxis.set_major_locator(ticker.MultipleLocator(x_step))
    ax.xaxis.set_major_formatter(FuncFormatter(_format_ut_hours))
    if y_step:
        ax.yaxis.set_major_locator(ticker.MultipleLocator(y_step))
    ax.grid(True, which="major", color="#666666", linestyle="-", alpha=0.5)
    ax.minorticks_on()
    ax.grid(True, which="minor", color="#999999", linestyle="-", alpha=0.2)
    ax.legend(loc="upper left", fontsize=_LEGEND_FS)


def _render(fig) -> bytes:
    buf = io.BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    return buf.read()


def _doy_to_date(year: int, doy: int) -> str:
    return datetime.datetime.strptime(f"{doy}.{year}", "%j.%Y").strftime("%d.%m.%Y")


# ── Plot 1: Average over a day range (single station) ────────────────────────

def plot_average(
    points: list[StatisticsPoint],
    year: int,
    doy_start: int,
    doy_end: int,
    station: str,
    total_days: int,
    show_student_ci: bool = True,
    show_variance: bool = False,
    width_px: int = settings.plot_width_px,
    height_px: int = settings.plot_height_px,
    dpi: int = settings.plot_dpi,
) -> PlotResult:
    """
    Mean TEC vs time with optional Student-CI and variance error bars.

    Mirrors the original Plot_graph() function. The 'data' dict includes
    mean_tec, student_ci, variance and std_dev for each of the 48 time slots
    so the client can construct any alternative view from the same payload.
    """
    ut     = [p.ut          for p in points]
    mean   = [p.mean_tec    for p in points]
    ci     = [p.student_ci  for p in points]
    var    = [p.variance     for p in points]
    std    = [p.std_dev      for p in points]
    n_vals = [p.n            for p in points]
    g_lon  = [p.mean_g_lon  for p in points]
    g_lat  = [p.mean_g_lat  for p in points]

    d_start = _doy_to_date(year, doy_start)
    d_end   = _doy_to_date(year, doy_end)
    title   = f"{station.upper()} — {d_start} to {d_end} ({total_days} days)"

    # ── plot ──
    fig, ax = _new_fig(width_px, height_px, dpi)
    ax.plot(ut, mean, "--", label="average TEC")

    if show_student_ci:
        ax.errorbar(ut, mean, yerr=ci, fmt=".k", capsize=8,
                    label=f"Student CI (α={settings.default_alpha})", zorder=3)
    if show_variance:
        ax.errorbar(ut, mean, yerr=var, fmt="o", capsize=4,
                    label="variance", alpha=0.6, zorder=2)

    ax.set_title(title, fontsize=_TITLE_FS)
    _style_ax(ax)
    png = _render(fig)

    # ── data dict ──
    data = {
        "plot_type":    "absoltec_average",
        "title":        title,
        "xlabel":       "Time, UT [h]",
        "ylabel":       "TEC, TECU",
        "figure_width":  width_px / dpi,
        "figure_height": height_px / dpi,
        "dpi":           dpi,
        "metadata": {
            "year": year, "doy_start": doy_start, "doy_end": doy_end,
            "station": station, "alpha": settings.default_alpha,
            "total_days": total_days,
        },
        "series": {
            "ut": ut, "mean_tec": mean,
            "student_ci": ci, "variance": var, "std_dev": std,
            "n": n_vals, "mean_g_lon": g_lon, "mean_g_lat": g_lat,
        },
        "plot_options": {
            "show_student_ci": show_student_ci,
            "show_variance":   show_variance,
        },
    }
    return PlotResult(png=png, data=data)


# ── Plot 2: Single day raw TEC ────────────────────────────────────────────────

def plot_single_day(
    points: list[TimeSeriesPoint],
    year: int,
    doy: int,
    station: str,
    smooth: bool = False,
    polynomial_order: int = settings.savgol_polynomial_order,
    width_px: int = settings.plot_width_px,
    height_px: int = settings.plot_height_px,
    dpi: int = settings.plot_dpi,
) -> PlotResult:
    """
    Raw (or Savitzky-Golay smoothed) TEC for a single day.

    The Savitzky-Golay window is computed as floor(N/2)+1 (must be odd and
    > polynomial_order), matching the original GUI's approach.
    """
    ut  = [p.ut  for p in points]
    tec = [p.tec for p in points]
    g_lon = [p.g_lon for p in points]
    g_lat = [p.g_lat for p in points]

    title = f"{station.upper()} — {_doy_to_date(year, doy)}"
    tec_smooth = None

    fig, ax = _new_fig(width_px, height_px, dpi)
    if smooth and len(tec) > polynomial_order:
        window = max(polynomial_order + 2, len(tec) // 2 + 1)
        if window % 2 == 0:
            window += 1
        tec_smooth = savgol_filter(np.array(tec), window, polynomial_order).tolist()
        ax.plot(ut, tec, alpha=0.35, color="steelblue", label="raw TEC")
        ax.plot(ut, tec_smooth, linewidth=2, color="steelblue", label="smoothed TEC")
    else:
        ax.plot(ut, tec, label="TEC")

    ax.set_title(title, fontsize=_TITLE_FS)
    _style_ax(ax)
    png = _render(fig)

    data = {
        "plot_type":    "absoltec_single_day",
        "title":        title,
        "xlabel":       "Time, UT [h]",
        "ylabel":       "TEC, TECU",
        "figure_width":  width_px / dpi,
        "figure_height": height_px / dpi,
        "dpi":           dpi,
        "metadata": {"year": year, "doy": doy, "station": station},
        "series": {
            "ut": ut, "tec": tec,
            "tec_smooth": tec_smooth,
            "g_lon": g_lon, "g_lat": g_lat,
        },
        "plot_options": {
            "smooth": smooth, "polynomial_order": polynomial_order,
        },
    }
    return PlotResult(png=png, data=data)


# ── Plot 3: Multiple stations overlaid on one day ─────────────────────────────

def plot_multi_station(
    station_series: dict,
    year: int,
    doy: int,
    smooth: bool = False,
    polynomial_order: int = settings.savgol_polynomial_order,
    width_px: int = settings.plot_width_px,
    height_px: int = settings.plot_height_px,
    dpi: int = settings.plot_dpi,
) -> PlotResult:
    """
    Overlay TEC curves for multiple stations on one day.
    station_series: {station_code: [TimeSeriesPoint, ...]}
    """
    title = f"Multi-station — {_doy_to_date(year, doy)}"
    fig, ax = _new_fig(width_px, height_px, dpi)
    series_data = {}

    for station, points in station_series.items():
        if not points:
            continue
        ut  = [p.ut  for p in points]
        tec = [p.tec for p in points]

        if smooth and len(tec) > polynomial_order:
            window = max(polynomial_order + 2, len(tec) // 2 + 1)
            if window % 2 == 0:
                window += 1
            tec = savgol_filter(np.array(tec), window, polynomial_order).tolist()

        ax.plot(ut, tec, label=station)
        series_data[station] = {"ut": ut, "tec": tec}

    ax.set_title(f"{title} · {len(station_series)} stations", fontsize=_TITLE_FS)
    _style_ax(ax)
    png = _render(fig)

    data = {
        "plot_type":    "absoltec_multi_station",
        "title":        title,
        "xlabel":       "Time, UT [h]",
        "ylabel":       "TEC, TECU",
        "figure_width":  width_px / dpi,
        "figure_height": height_px / dpi,
        "dpi":           dpi,
        "metadata": {"year": year, "doy": doy, "stations": list(station_series.keys())},
        "series":       series_data,
        "plot_options": {"smooth": smooth, "polynomial_order": polynomial_order},
    }
    return PlotResult(png=png, data=data)


# ── Plot 4: Per-station-day averages ──────────────────────────────────────────

def plot_per_station_averages(
    day_results: list,
    year: int,
    show_student_ci: bool = True,
    show_variance: bool = False,
    width_px: int = settings.plot_width_px,
    height_px: int = settings.plot_height_px,
    dpi: int = settings.plot_dpi,
) -> list[PlotResult]:
    """
    One PlotResult per day — average across that day's station group.
    Returns a list; the router picks the requested day or streams all.
    """
    results = []
    for result in day_results:
        if not result.points:
            continue
        ut   = [p.ut          for p in result.points]
        mean = [p.mean_tec    for p in result.points]
        ci   = [p.student_ci  for p in result.points]
        var  = [p.variance     for p in result.points]
        std  = [p.std_dev      for p in result.points]

        stations_label = ", ".join(result.stations_found[:5])
        if len(result.stations_found) > 5:
            stations_label += f" +{len(result.stations_found) - 5} more"
        title = (
            f"{_doy_to_date(year, result.doy)} · "
            f"{stations_label} (N={len(result.stations_found)})"
        )

        fig, ax = _new_fig(width_px, height_px, dpi)
        ax.plot(ut, mean, "--", label="average TEC")
        if show_student_ci:
            ax.errorbar(ut, mean, yerr=ci, fmt=".k", capsize=8,
                        label="Student CI", zorder=3)
        if show_variance:
            ax.errorbar(ut, mean, yerr=var, fmt="o", capsize=4,
                        label="variance", alpha=0.6)
        ax.set_title(title, fontsize=11)
        _style_ax(ax)
        png = _render(fig)

        data = {
            "plot_type":    "absoltec_per_station_avg",
            "title":        title,
            "xlabel":       "Time, UT [h]",
            "ylabel":       "TEC, TECU",
            "figure_width":  width_px / dpi,
            "figure_height": height_px / dpi,
            "dpi":           dpi,
            "metadata": {
                "year": year, "doy": result.doy,
                "stations": result.stations_found, "alpha": result.alpha,
            },
            "series": {
                "ut": ut, "mean_tec": mean,
                "student_ci": ci, "variance": var, "std_dev": std,
            },
            "plot_options": {
                "show_student_ci": show_student_ci,
                "show_variance":   show_variance,
            },
        }
        results.append(PlotResult(png=png, data=data))

    return results


def plot_day_by_day_columns(
    rows: list[dict],
    year: int,
    doy_start: int,
    doy_end: int,
    columns: list[str],
    width_px: int = settings.plot_width_px,
    height_px: int = settings.plot_height_px,
    dpi: int = settings.plot_dpi,
) -> PlotResult:
    """
    Plot selected AbsolTEC raw columns over a concatenated multi-day timeline.

    `rows` is expected to contain `concat_ut`, `station`, and requested column
    values produced by services.absoltec.get_raw_data_range().
    """
    fig, ax = _new_fig(width_px, height_px, dpi)

    stations = sorted({str(r.get("station", "")) for r in rows if r.get("station")})
    include_station_in_label = len(stations) > 1

    series_data: dict[str, dict[str, list[float]]] = {}
    for station in stations:
        station_rows = [r for r in rows if r.get("station") == station]
        for col in columns:
            x_vals, y_vals = [], []
            for row in station_rows:
                val = row.get(col)
                if val is None:
                    continue
                try:
                    x_vals.append(float(row["concat_ut"]))
                    y_vals.append(float(val))
                except (TypeError, ValueError, KeyError):
                    continue

            if not x_vals:
                continue
            label = f"{station}:{col}" if include_station_in_label else col
            ax.plot(x_vals, y_vals, label=label)
            series_data[label] = {"x": x_vals, "y": y_vals}

    title = f"AbsolTEC raw day-by-day {year} DOY {doy_start:03d}-{doy_end:03d}"
    ax.set_title(title, fontsize=_TITLE_FS)
    ax.set_xlabel("Time (UTC)", fontsize=_LABEL_FS)
    ax.set_ylabel("Value", fontsize=_LABEL_FS)
    _apply_concat_time_axis(ax, year, doy_start)
    ax.grid(True, which="major", color="#666666", linestyle="-", alpha=0.5)
    ax.minorticks_on()
    ax.grid(True, which="minor", color="#999999", linestyle="-", alpha=0.2)
    if series_data:
        ax.legend(loc="upper left", fontsize=_LEGEND_FS)
    png = _render(fig)

    data = {
        "plot_type": "absoltec_day_by_day_raw",
        "title": title,
        "xlabel": "Time (UTC)",
        "ylabel": "Value",
        "figure_width": width_px / dpi,
        "figure_height": height_px / dpi,
        "dpi": dpi,
        "metadata": {
            "year": year,
            "doy_start": doy_start,
            "doy_end": doy_end,
            "stations": stations,
            "columns": columns,
        },
        "series": series_data,
        "plot_options": {},
    }
    return PlotResult(png=png, data=data)
