from types import SimpleNamespace

from fastapi.testclient import TestClient

from app.main import app
from app.plotting import PlotResult
import app.routers.plots as plots_router


client = TestClient(app)


def _plot_result(plot_type: str) -> PlotResult:
    return PlotResult(
        png=b"png-bytes",
        data={
            "plot_type": plot_type,
            "title": plot_type,
            "xlabel": "x",
            "ylabel": "y",
            "series": {},
            "metadata": {},
        },
    )


def test_plot_cb_average_json(monkeypatch):
    monkeypatch.setattr(
        plots_router,
        "compute_statistics_cb",
        lambda *args, **kwargs: SimpleNamespace(points=[SimpleNamespace(ut=0.0)], total_days=1),
    )
    monkeypatch.setattr(plots_router.cp, "plot_average_cb", lambda *args, **kwargs: _plot_result("cb_average"))

    response = client.get(
        "/plots/cb/average",
        params={"year": 2026, "doy_start": 1, "doy_end": 2, "station": "alex", "format": "json"},
    )

    assert response.status_code == 200
    assert response.json()["plot_type"] == "cb_average"


def test_plot_cb_day_json(monkeypatch):
    monkeypatch.setattr(plots_router, "get_raw_data_cb", lambda *args, **kwargs: [SimpleNamespace(ut=0.0, cb=1.0)])
    monkeypatch.setattr(plots_router.cp, "plot_single_day_cb", lambda *args, **kwargs: _plot_result("cb_single_day"))

    response = client.get(
        "/plots/cb/day",
        params={"year": 2026, "doy": 1, "station": "alex", "format": "json"},
    )

    assert response.status_code == 200
    assert response.json()["plot_type"] == "cb_single_day"


def test_plot_cb_multi_station_json(monkeypatch):
    monkeypatch.setattr(
        plots_router,
        "get_raw_data_range_cb",
        lambda *args, **kwargs: [{"station": "alex", "concat_ut": 0.0, "cb": 1.0}],
    )
    monkeypatch.setattr(plots_router.cp, "plot_multi_station_cb", lambda *args, **kwargs: _plot_result("cb_multi_station"))

    response = client.get(
        "/plots/cb/multi-station",
        params=[
            ("year", 2026),
            ("doy_start", 1),
            ("doy_end", 2),
            ("stations", "alex"),
            ("stations", "alme"),
            ("format", "json"),
        ],
    )

    assert response.status_code == 200
    assert response.json()["plot_type"] == "cb_multi_station"


def test_plot_cb_vs_tec_json(monkeypatch):
    monkeypatch.setattr(
        plots_router,
        "get_raw_data_range_cb",
        lambda *args, **kwargs: [{"station": "alex", "concat_ut": 0.0, "cb": 1.0, "tec": 10.0}],
    )
    monkeypatch.setattr(plots_router.cp, "plot_cb_vs_tec", lambda *args, **kwargs: _plot_result("cb_vs_tec"))

    response = client.get(
        "/plots/cb/vs-tec",
        params={"year": 2026, "doy_start": 1, "doy_end": 2, "station": "alex", "format": "json"},
    )

    assert response.status_code == 200
    assert response.json()["plot_type"] == "cb_vs_tec"


def test_plot_cb_per_station_averages_json(monkeypatch):
    monkeypatch.setattr(
        plots_router,
        "compute_statistics_per_station_day_cb",
        lambda *args, **kwargs: [SimpleNamespace(doy=1, points=[SimpleNamespace(ut=0.0)])],
    )
    monkeypatch.setattr(
        plots_router.cp,
        "plot_per_station_averages_cb",
        lambda *args, **kwargs: _plot_result("cb_per_station_averages"),
    )

    response = client.get(
        "/plots/cb/per-station-averages/1",
        params=[
            ("year", 2026),
            ("doy_start", 1),
            ("doy_end", 2),
            ("stations", "alex"),
            ("stations", "alme"),
            ("format", "json"),
        ],
    )

    assert response.status_code == 200
    assert response.json()["plot_type"] == "cb_per_station_averages"


def test_plot_cb_raw_day_by_day_json(monkeypatch):
    monkeypatch.setattr(
        plots_router,
        "get_raw_data_range_cb",
        lambda *args, **kwargs: [{"station": "alex", "concat_ut": 0.0, "cb": 1.0, "tec": 10.0}],
    )
    monkeypatch.setattr(plots_router.cp, "plot_multi_station_cb", lambda *args, **kwargs: _plot_result("cb_multi_station"))

    response = client.get(
        "/plots/cb/raw/day-by-day",
        params=[
            ("year", 2026),
            ("doy_start", 1),
            ("doy_end", 2),
            ("stations", "alex"),
            ("stations", "alme"),
            ("format", "json"),
        ],
    )

    assert response.status_code == 200
    assert response.json()["plot_type"] == "cb_multi_station"


def test_plot_cb_raw_day_by_day_requires_station_or_stations():
    response = client.get(
        "/plots/cb/raw/day-by-day",
        params={"year": 2026, "doy_start": 1, "doy_end": 2, "format": "json"},
    )

    assert response.status_code == 422
