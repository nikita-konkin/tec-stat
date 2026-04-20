"""
CB (Coherence Band) matplotlib plotting.

Every plot_* function returns a PlotResult(png, data) namedtuple so the
HTTP router can serve the client's preferred format (PNG image, JSON data,
or a standalone Python script) from a single computation.

The 'data' dict is intentionally kept column-oriented:
  {"ut": [...48 floats...], "mean_cb": [...], ...}

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
from app.models.schemas import StatisticsPointCB, TimeSeriesPointCB
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
    start = datetime.date(year, 1, 1) + datetime.timedelta(days=doy_start - 1)

    def _fmt(x: float, _pos: int) -> str:
        try:
            dt = datetime.datetime.combine(start, datetime.time(0, 0)) + datetime.timedelta(hours=float(x))
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
    ax.set_ylabel("CB", fontsize=_LABEL_FS)
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
    buf.seek(0)
    png = buf.read()
    plt.close(fig)
    return png


def _doy_to_date(year: int, doy: int) -> str:
    """Convert year/doy to YYYY-MM-DD string."""
    date = datetime.date(year, 1, 1) + datetime.timedelta(days=doy - 1)
    return date.strftime("%Y-%m-%d")


# ── Plot 1: Average CB ────────────────────────────────────────────────────────

def plot_average_cb(
    points: list[StatisticsPointCB],
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
    Mean CB vs time with optional Student-CI and variance error bars.

    Mirrors the original Plot_graph() function. The 'data' dict includes
    mean_cb, student_ci, variance and std_dev for each of the 48 time slots
    so the client can construct any alternative view from the same payload.
    """
    ut     = [p.ut          for p in points]
    mean   = [p.mean_cb     for p in points]
    ci     = [p.student_ci  for p in points]
    var    = [p.variance     for p in points]
    std    = [p.std_dev      for p in points]
    n_vals = [p.n            for p in points]
    g_lon  = [p.mean_g_lon  for p in points]
    g_lat  = [p.mean_g_lat  for p in points]

    d_start = _doy_to_date(year, doy_start)
    d_end   = _doy_to_date(year, doy_end)
    title   = f"{station.upper()} — CB {d_start} to {d_end} ({total_days} days)"

    # ── plot ──
    fig, ax = _new_fig(width_px, height_px, dpi)
    ax.plot(ut, mean, "--", label="average CB")

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
        "plot_type":    "cb_average",
        "title":        title,
        "xlabel":       "Time, UT [h]",
        "ylabel":       "CB",
        "figure_width":  width_px / dpi,
        "figure_height": height_px / dpi,
        "dpi":           dpi,
        "metadata": {
            "year": year, "doy_start": doy_start, "doy_end": doy_end,
            "station": station, "alpha": settings.default_alpha,
            "total_days": total_days,
        },
        "series": {
            "ut": ut, "mean_cb": mean,
            "student_ci": ci, "variance": var, "std_dev": std,
            "n": n_vals, "mean_g_lon": g_lon, "mean_g_lat": g_lat,
        },
        "plot_options": {
            "show_student_ci": show_student_ci,
            "show_variance":   show_variance,
        },
    }
    return PlotResult(png=png, data=data)


# ── Plot 2: Single day raw CB ────────────────────────────────────────────────

def plot_single_day_cb(
    points: list[TimeSeriesPointCB],
    year: int,
    doy: int,
    station: str,
    width_px: int = settings.plot_width_px,
    height_px: int = settings.plot_height_px,
    dpi: int = settings.plot_dpi,
) -> PlotResult:
    """
    Raw CB time series for one day.

    Mirrors the original Plot_graph() function for single-day plots.
    """
    ut = [p.ut  for p in points]
    cb = [p.cb  for p in points]

    date = _doy_to_date(year, doy)
    title = f"{station.upper()} — CB {date}"

    # ── plot ──
    fig, ax = _new_fig(width_px, height_px, dpi)
    ax.plot(ut, cb, "o-", markersize=4, label="CB")

    ax.set_title(title, fontsize=_TITLE_FS)
    _style_ax(ax)
    png = _render(fig)

    # ── data dict ──
    data = {
        "plot_type":    "cb_single_day",
        "title":        title,
        "xlabel":       "Time, UT [h]",
        "ylabel":       "CB",
        "figure_width":  width_px / dpi,
        "figure_height": height_px / dpi,
        "dpi":           dpi,
        "metadata": {
            "year": year, "doy": doy, "station": station,
        },
        "series": {
            "ut": ut, "cb": cb,
        },
    }
    return PlotResult(png=png, data=data)


# ── Plot 3: Multi-station CB ─────────────────────────────────────────────────

def plot_multi_station_cb(
    data: list[dict],
    year: int,
    doy_start: int,
    doy_end: int,
    stations: list[str],
    width_px: int = settings.plot_width_px,
    height_px: int = settings.plot_height_px,
    dpi: int = settings.plot_dpi,
) -> PlotResult:
    """
    CB time series for multiple stations over a day range.

    Each station gets its own line, with time continuity across days.
    """
    # Group by station (Plotly-friendly: each trace as {x: [...], y: [...]}).
    station_data: dict[str, dict[str, list[float]]] = {}
    for row in data:
        station = str(row.get("station", ""))
        if not station:
            continue
        if station not in station_data:
            station_data[station] = {"x": [], "y": []}
        station_data[station]["x"].append(float(row["concat_ut"]))
        station_data[station]["y"].append(float(row["cb"]))

    d_start = _doy_to_date(year, doy_start)
    d_end   = _doy_to_date(year, doy_end)
    title   = f"CB {d_start} to {d_end} — {', '.join(stations).upper()}"

    # ── plot ──
    fig, ax = _new_fig(width_px, height_px, dpi)
    for station, series in station_data.items():
        ax.plot(series["x"], series["y"], "o-", markersize=2,
                label=f"{station.upper()}", alpha=0.8)

    ax.set_title(title, fontsize=_TITLE_FS)
    ax.set_xlabel("Time (UTC)", fontsize=_LABEL_FS)
    ax.set_ylabel("CB", fontsize=_LABEL_FS)
    _apply_concat_time_axis(ax, year, doy_start)
    ax.legend(loc="upper left", fontsize=_LEGEND_FS)
    ax.grid(True, which="major", color="#666666", linestyle="-", alpha=0.5)
    png = _render(fig)

    # ── data dict ──
    data_dict = {
        "plot_type":    "cb_multi_station",
        "title":        title,
        "xlabel":       "Time (UTC)",
        "ylabel":       "CB",
        "figure_width":  width_px / dpi,
        "figure_height": height_px / dpi,
        "dpi":           dpi,
        "metadata": {
            "year": year, "doy_start": doy_start, "doy_end": doy_end,
            "stations": stations,
        },
        "series": station_data,
    }
    return PlotResult(png=png, data=data_dict)


def plot_multi_station_cb_with_absoltec(
    data: list[dict],
    year: int,
    doy_start: int,
    doy_end: int,
    stations: list[str],
    width_px: int = settings.plot_width_px,
    height_px: int = settings.plot_height_px,
    dpi: int = settings.plot_dpi,
) -> PlotResult:
    """
    Plot AbsolTEC (TEC) and derived CB on the same time axis.

    Uses two y-axes (left: TEC, right: CB). Supports one or more stations over
    a day range using concatenated UT hours.
    """
    station_data: dict[str, dict[str, list[float]]] = {}
    for row in data:
        station = str(row.get("station", ""))
        if not station:
            continue
        try:
            x = float(row["concat_ut"])
            tec = float(row["tec"])
            cb = float(row["cb"])
        except (KeyError, TypeError, ValueError):
            continue
        if station not in station_data:
            station_data[station] = {"x": [], "tec": [], "cb": []}
        station_data[station]["x"].append(x)
        station_data[station]["tec"].append(tec)
        station_data[station]["cb"].append(cb)

    d_start = _doy_to_date(year, doy_start)
    d_end = _doy_to_date(year, doy_end)
    title = f"TEC + CB {d_start} to {d_end} — {', '.join(stations).upper()}"

    fig, ax1 = _new_fig(width_px, height_px, dpi)
    ax2 = ax1.twinx()

    colors = plt.rcParams.get("axes.prop_cycle", None)
    color_list = colors.by_key().get("color", []) if colors else []
    if not color_list:
        color_list = ["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd"]

    for idx, (station, series) in enumerate(sorted(station_data.items())):
        color = color_list[idx % len(color_list)]
        ax1.plot(
            series["x"],
            series["tec"],
            "-",
            linewidth=1.8,
            color=color,
            label=f"{station.upper()} TEC",
            alpha=0.85,
        )
        ax2.plot(
            series["x"],
            series["cb"],
            "--",
            linewidth=1.6,
            color=color,
            label=f"{station.upper()} CB",
            alpha=0.85,
        )

    ax1.set_title(title, fontsize=_TITLE_FS)
    ax1.set_xlabel("Time (UTC)", fontsize=_LABEL_FS)
    ax1.set_ylabel("TEC, TECU", fontsize=_LABEL_FS)
    ax2.set_ylabel("CB", fontsize=_LABEL_FS)
    _apply_concat_time_axis(ax1, year, doy_start)
    ax1.grid(True, which="major", color="#666666", linestyle="-", alpha=0.5)
    ax1.minorticks_on()
    ax1.grid(True, which="minor", color="#999999", linestyle="-", alpha=0.2)

    handles1, labels1 = ax1.get_legend_handles_labels()
    handles2, labels2 = ax2.get_legend_handles_labels()
    if handles1 or handles2:
        ax1.legend(
            handles1 + handles2,
            labels1 + labels2,
            loc="upper left",
            fontsize=_LEGEND_FS,
            ncol=2,
        )

    png = _render(fig)

    plotly_traces = []
    for station, series in sorted(station_data.items()):
        plotly_traces.append(
            {
                "type": "scatter",
                "mode": "lines",
                "name": f"{station.upper()} TEC",
                "x": series["x"],
                "y": series["tec"],
            }
        )
        plotly_traces.append(
            {
                "type": "scatter",
                "mode": "lines",
                "name": f"{station.upper()} CB",
                "x": series["x"],
                "y": series["cb"],
                "yaxis": "y2",
                "line": {"dash": "dash"},
            }
        )

    data_dict = {
        "plot_type": "tec_cb_multi_station",
        "title": title,
        "xlabel": "Time (UTC)",
        "ylabel": "TEC, TECU",
        "figure_width": width_px / dpi,
        "figure_height": height_px / dpi,
        "dpi": dpi,
        "metadata": {
            "year": year,
            "doy_start": doy_start,
            "doy_end": doy_end,
            "stations": stations,
        },
        # Plotly-first payload for JSON rendering (dual y-axes).
        "data": plotly_traces,
        "layout": {
            "yaxis": {"title": "TEC, TECU"},
            "yaxis2": {"title": "CB", "overlaying": "y", "side": "right"},
        },
        # Script generator / fallback rendering support.
        "series": {
            f"{station.upper()} TEC": {"x": series["x"], "y": series["tec"]}
            for station, series in sorted(station_data.items())
        }
        | {
            f"{station.upper()} CB": {"x": series["x"], "y": series["cb"]}
            for station, series in sorted(station_data.items())
        },
        "plot_options": {},
    }
    return PlotResult(png=png, data=data_dict)


# ── Plot 4: CB vs AbsolTEC ───────────────────────────────────────────────────

def plot_cb_vs_tec(
    data: list[dict],
    year: int,
    doy_start: int,
    doy_end: int,
    station: str,
    width_px: int = settings.plot_width_px,
    height_px: int = settings.plot_height_px,
    dpi: int = settings.plot_dpi,
) -> PlotResult:
    """
    Scatter plot of CB vs AbsolTEC values.

    Shows the relationship between TEC and calculated CB.
    """
    tec_vals = [row["tec"] for row in data]
    cb_vals  = [row["cb"]  for row in data]

    d_start = _doy_to_date(year, doy_start)
    d_end   = _doy_to_date(year, doy_end)
    title   = f"{station.upper()} — CB vs AbsolTEC {d_start} to {d_end}"

    # ── plot ──
    fig, ax = _new_fig(width_px, height_px, dpi)
    ax.scatter(tec_vals, cb_vals, alpha=0.6, s=10)
    ax.set_xlabel("AbsolTEC, TECU", fontsize=_LABEL_FS)
    ax.set_ylabel("CB", fontsize=_LABEL_FS)
    ax.set_title(title, fontsize=_TITLE_FS)
    ax.grid(True, which="major", color="#666666", linestyle="-", alpha=0.5)
    png = _render(fig)

    # ── data dict ──
    data_dict = {
        "plot_type":    "cb_vs_tec",
        "title":        title,
        "xlabel":       "AbsolTEC, TECU",
        "ylabel":       "CB",
        "figure_width":  width_px / dpi,
        "figure_height": height_px / dpi,
        "dpi":           dpi,
        "metadata": {
            "year": year, "doy_start": doy_start, "doy_end": doy_end,
            "station": station,
        },
        "series": {
            "tec": tec_vals, "cb": cb_vals,
        },
    }
    return PlotResult(png=png, data=data_dict)


# ── Plot 5: Per-station averages ─────────────────────────────────────────────

def plot_per_station_averages_cb(
    responses: list,
    year: int,
    doy_start: int,
    doy_end: int,
    stations: list[str],
    width_px: int = settings.plot_width_px,
    height_px: int = settings.plot_height_px,
    dpi: int = settings.plot_dpi,
) -> PlotResult:
    """
    Average CB across stations for each day in the range.

    Each day gets a separate subplot.
    """
    n_days = len(responses)
    if n_days == 0:
        # Empty plot
        fig, ax = _new_fig(width_px, height_px, dpi)
        ax.text(0.5, 0.5, "No data", ha="center", va="center", fontsize=20)
        png = _render(fig)
        return PlotResult(png=png, data={})

    # Calculate grid dimensions
    cols = min(4, n_days)
    rows = (n_days + cols - 1) // cols

    fig, axes = plt.subplots(rows, cols, figsize=(width_px / dpi, height_px / dpi), dpi=dpi)
    if rows == 1 and cols == 1:
        axes = [axes]
    elif rows == 1:
        axes = axes.flatten()
    else:
        axes = axes.flatten()

    data_dict = {
        "plot_type": "cb_per_station_averages",
        "title": f"CB averages {year} DOY {doy_start}-{doy_end}",
        "xlabel": "Time, UT [h]",
        "ylabel": "CB",
        "figure_width": width_px / dpi,
        "figure_height": height_px / dpi,
        "dpi": dpi,
        "metadata": {
            "year": year, "doy_start": doy_start, "doy_end": doy_end,
            "stations": stations,
        },
        "series": {},
    }

    for i, response in enumerate(responses):
        ax = axes[i]
        ut = [p.ut for p in response.points]
        mean_cb = [p.mean_cb for p in response.points]
        ci = [p.student_ci for p in response.points]

        date = _doy_to_date(response.year, response.doy)
        ax.plot(ut, mean_cb, "o-", markersize=3, label="average CB")
        ax.errorbar(ut, mean_cb, yerr=ci, fmt=".k", capsize=4,
                    label=f"CI α={response.alpha}", zorder=3)

        ax.set_title(f"{date}", fontsize=11)
        ax.set_xlabel("UT [h]", fontsize=10)
        ax.set_ylabel("CB", fontsize=10)
        ax.grid(True, alpha=0.3)
        ax.legend(loc="upper left", fontsize=10)

        # Plotly-friendly: each day as its own {x: [...], y: [...]} series.
        data_dict["series"][date] = {
            "x": ut,
            "y": mean_cb,
            "student_ci": ci,
            "doy": response.doy,
        }

    # Hide unused subplots
    for i in range(n_days, len(axes)):
        axes[i].set_visible(False)

    fig.suptitle(
        f"CB per-station averages {year} DOY {doy_start}-{doy_end}",
        fontsize=_TITLE_FS,
    )
    plt.tight_layout()
    png = _render(fig)

    return PlotResult(png=png, data=data_dict)
