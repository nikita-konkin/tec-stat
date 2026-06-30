"""
Propagation-parameter matplotlib plotting.

All functions return PlotResult(png, data) so the router can serve PNG, JSON,
or a standalone Python script from one computation path.
"""

import datetime
import io
from typing import Literal, Optional

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
from matplotlib.ticker import FuncFormatter

from app.config import settings
from app.models.schemas import (
    PropagationPointAbsoltec,
    PropagationStatisticsPoint,
    PropagationTecPoint,
)
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

MetricName = Literal["b_k", "gdd"]

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


def _new_fig(width_px: int, height_px: int, dpi: int):
    return plt.subplots(figsize=(width_px / dpi, height_px / dpi), dpi=dpi)


def _render(fig) -> bytes:
    buf = io.BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    return buf.read()


def _empty_result(message: str = "No data") -> PlotResult:
    fig, ax = plt.subplots(figsize=(6, 3), dpi=80)
    ax.text(
        0.5,
        0.5,
        message,
        ha="center",
        va="center",
        transform=ax.transAxes,
        fontsize=14,
        color="gray",
    )
    ax.set_axis_off()
    return PlotResult(
        png=_render(fig),
        data={
            "plot_type": "empty",
            "title": message,
            "xlabel": "",
            "ylabel": "",
            "figure_width": 6.0,
            "figure_height": 3.0,
            "dpi": 80,
            "metadata": {},
            "series": {},
            "plot_options": {},
        },
    )


def _doy_to_date(year: int, doy: int) -> str:
    date = datetime.date(year, 1, 1) + datetime.timedelta(days=doy - 1)
    return date.strftime("%Y-%m-%d")


def _metric_label(metric: MetricName) -> str:
    return "B_k" if metric == "b_k" else "GDD"


def _metric_keys(metric: MetricName) -> tuple[str, str, str, str]:
    if metric == "b_k":
        return "mean_b_k", "variance_b_k", "std_dev_b_k", "student_ci_b_k"
    return "mean_gdd", "variance_gdd", "std_dev_gdd", "student_ci_gdd"


def _frequency_caption(f_hz: float, signal_band: Optional[str]) -> str:
    if signal_band:
        return f"{signal_band} ({f_hz / 1e6:.3f} MHz)"
    return f"{f_hz / 1e6:.3f} MHz"


def plot_average(
    points: list[PropagationStatisticsPoint],
    year: int,
    doy_start: int,
    doy_end: int,
    station: str,
    total_days: int,
    metric: MetricName,
    f_hz: float,
    signal_band: Optional[str] = None,
    show_student_ci: bool = True,
    show_variance: bool = False,
    width_px: int = settings.plot_width_px,
    height_px: int = settings.plot_height_px,
    dpi: int = settings.plot_dpi,
) -> PlotResult:
    """Mean propagation metric vs time with optional CI and variance bars."""
    if not points:
        return _empty_result("No propagation statistics")

    mean_key, variance_key, std_key, ci_key = _metric_keys(metric)
    metric_label = _metric_label(metric)

    ut = [p.ut for p in points]
    mean = [getattr(p, mean_key) for p in points]
    variance = [getattr(p, variance_key) for p in points]
    std_dev = [getattr(p, std_key) for p in points]
    student_ci = [getattr(p, ci_key) for p in points]
    mean_tec = [p.mean_tec for p in points]
    mean_nt = [p.mean_nt for p in points]
    n_vals = [p.n for p in points]
    g_lon = [p.mean_g_lon for p in points]
    g_lat = [p.mean_g_lat for p in points]

    d_start = _doy_to_date(year, doy_start)
    d_end = _doy_to_date(year, doy_end)
    title = (
        f"{station.upper()} - {metric_label} {d_start} to {d_end} "
        f"({total_days} days, {_frequency_caption(f_hz, signal_band)})"
    )

    fig, ax = _new_fig(width_px, height_px, dpi)
    ax.plot(ut, mean, "--", label=f"average {metric_label}")
    if show_student_ci:
        ax.errorbar(
            ut,
            mean,
            yerr=student_ci,
            fmt=".k",
            capsize=8,
            label="Student CI",
            zorder=3,
        )
    if show_variance:
        ax.errorbar(
            ut,
            mean,
            yerr=variance,
            fmt="o",
            capsize=4,
            label="variance",
            alpha=0.6,
            zorder=2,
        )

    ax.set_title(title, fontsize=_TITLE_FS)
    ax.set_xlabel("Time, UT [h]", fontsize=_LABEL_FS)
    ax.set_ylabel(metric_label, fontsize=_LABEL_FS)
    _apply_ut_axis(ax)
    ax.grid(True, which="major", color="#666666", linestyle="-", alpha=0.5)
    ax.minorticks_on()
    ax.grid(True, which="minor", color="#999999", linestyle="-", alpha=0.2)
    ax.legend(loc="upper left", fontsize=_LEGEND_FS)
    png = _render(fig)

    data = {
        "plot_type": "propagation_absoltec_average",
        "title": title,
        "xlabel": "Time, UT [h]",
        "ylabel": metric_label,
        "figure_width": width_px / dpi,
        "figure_height": height_px / dpi,
        "dpi": dpi,
        "metadata": {
            "year": year,
            "doy_start": doy_start,
            "doy_end": doy_end,
            "station": station,
            "metric": metric,
            "f_hz": f_hz,
            "signal_band": signal_band,
            "total_days": total_days,
        },
        "series": {
            "ut": ut,
            "mean_tec": mean_tec,
            "mean_nt": mean_nt,
            "mean_b_k": [p.mean_b_k for p in points],
            "variance_b_k": [p.variance_b_k for p in points],
            "std_dev_b_k": [p.std_dev_b_k for p in points],
            "student_ci_b_k": [p.student_ci_b_k for p in points],
            "mean_gdd": [p.mean_gdd for p in points],
            "variance_gdd": [p.variance_gdd for p in points],
            "std_dev_gdd": [p.std_dev_gdd for p in points],
            "student_ci_gdd": [p.student_ci_gdd for p in points],
            "n": n_vals,
            "mean_g_lon": g_lon,
            "mean_g_lat": g_lat,
        },
        "plot_options": {
            "metric": metric,
            "show_student_ci": show_student_ci,
            "show_variance": show_variance,
        },
    }
    return PlotResult(png=png, data=data)


def plot_single_day(
    points: list[PropagationPointAbsoltec],
    year: int,
    doy: int,
    station: str,
    metric: MetricName,
    f_hz: float,
    signal_band: Optional[str] = None,
    width_px: int = settings.plot_width_px,
    height_px: int = settings.plot_height_px,
    dpi: int = settings.plot_dpi,
) -> PlotResult:
    """Single-day propagation metric derived from AbsolTEC."""
    metric_label = _metric_label(metric)
    data_pts = [p for p in points if getattr(p, metric) is not None]
    if not data_pts:
        return _empty_result("No valid propagation samples")

    ut = [p.ut for p in data_pts]
    values = [getattr(p, metric) for p in data_pts]
    title = (
        f"{station.upper()} - {metric_label} {_doy_to_date(year, doy)} "
        f"({_frequency_caption(f_hz, signal_band)})"
    )

    fig, ax = _new_fig(width_px, height_px, dpi)
    ax.plot(ut, values, "o-", markersize=4, label=metric_label)
    ax.set_title(title, fontsize=_TITLE_FS)
    ax.set_xlabel("Time, UT [h]", fontsize=_LABEL_FS)
    ax.set_ylabel(metric_label, fontsize=_LABEL_FS)
    _apply_ut_axis(ax)
    ax.grid(True, which="major", color="#666666", linestyle="-", alpha=0.5)
    ax.minorticks_on()
    ax.grid(True, which="minor", color="#999999", linestyle="-", alpha=0.2)
    ax.legend(loc="upper left", fontsize=_LEGEND_FS)
    png = _render(fig)

    data = {
        "plot_type": "propagation_absoltec_day",
        "title": title,
        "xlabel": "Time, UT [h]",
        "ylabel": metric_label,
        "figure_width": width_px / dpi,
        "figure_height": height_px / dpi,
        "dpi": dpi,
        "metadata": {
            "year": year,
            "doy": doy,
            "station": station,
            "metric": metric,
            "f_hz": f_hz,
            "signal_band": signal_band,
        },
        "series": {
            "ut": [p.ut for p in points],
            "tec": [p.tec for p in points],
            "nt": [p.nt for p in points],
            "b_k": [p.b_k for p in points],
            "gdd": [p.gdd for p in points],
            "g_lon": [p.g_lon for p in points],
            "g_lat": [p.g_lat for p in points],
        },
        "plot_options": {"metric": metric},
    }
    return PlotResult(png=png, data=data)


def plot_tec_satellite(
    points: list[PropagationTecPoint],
    year: int,
    doy: int,
    station: str,
    satellite: str,
    observable: str,
    metric: MetricName,
    valid_only: bool = True,
    f_hz: float = 0.0,
    signal_band: Optional[str] = None,
    width_px: int = settings.plot_width_px,
    height_px: int = settings.plot_height_px,
    dpi: int = settings.plot_dpi,
) -> PlotResult:
    """Propagation metric for one TEC-suite satellite pass."""
    metric_label = _metric_label(metric)
    data_pts = [p for p in points if (not valid_only or p.validity == 0) and getattr(p, metric) is not None]
    if not data_pts:
        return _empty_result("No valid propagation observations")

    hours = [p.hour for p in data_pts]
    values = [getattr(p, metric) for p in data_pts]
    title = (
        f"{station.upper()} - {satellite} - {metric_label} DOY {doy:03d}/{year} "
        f"({_frequency_caption(f_hz, signal_band)})"
    )

    fig, ax = _new_fig(width_px, height_px, dpi)
    ax.plot(hours, values, ".", markersize=4, label=f"{satellite} {metric_label}")
    ax.set_title(title, fontsize=_TITLE_FS)
    ax.set_xlabel("Time, UT [h]", fontsize=_LABEL_FS)
    ax.set_ylabel(metric_label, fontsize=_LABEL_FS)
    _apply_ut_axis(ax)
    ax.grid(True, which="major", color="#666666", linestyle="-", alpha=0.5)
    ax.minorticks_on()
    ax.grid(True, which="minor", color="#999999", linestyle="-", alpha=0.2)
    ax.legend(loc="upper left", fontsize=_LEGEND_FS)
    png = _render(fig)

    data = {
        "plot_type": "propagation_tec_satellite",
        "title": title,
        "xlabel": "Time, UT [h]",
        "ylabel": metric_label,
        "figure_width": width_px / dpi,
        "figure_height": height_px / dpi,
        "dpi": dpi,
        "metadata": {
            "year": year,
            "doy": doy,
            "station": station,
            "satellite": satellite,
            "observable": observable,
            "metric": metric,
            "valid_only": valid_only,
            "f_hz": f_hz,
            "signal_band": signal_band,
        },
        "series": {
            "hour": [p.hour for p in points],
            "tec": [p.tec for p in points],
            "nt": [p.nt for p in points],
            "b_k": [p.b_k for p in points],
            "gdd": [p.gdd for p in points],
            "el": [p.el for p in points],
            "az": [p.az for p in points],
            "validity": [p.validity for p in points],
        },
        "plot_options": {
            "metric": metric,
            "observable": observable,
            "valid_only": valid_only,
        },
    }
    return PlotResult(png=png, data=data)
