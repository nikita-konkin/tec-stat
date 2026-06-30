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


def test_plot_propagation_absoltec_average_json(monkeypatch):
    monkeypatch.setattr(
        plots_router,
        "compute_statistics_propagation_absoltec",
        lambda *args, **kwargs: SimpleNamespace(points=[SimpleNamespace(ut=0.0)], total_days=1),
    )
    monkeypatch.setattr(
        plots_router.pp,
        "plot_average",
        lambda *args, **kwargs: _plot_result("propagation_absoltec_average"),
    )

    response = client.get(
        "/plots/propagation/absoltec/average",
        params={
            "year": 2026,
            "doy_start": 1,
            "doy_end": 2,
            "station": "alex",
            "signal_band": "GPS_L1",
            "format": "json",
        },
    )

    assert response.status_code == 200
    assert response.json()["plot_type"] == "propagation_absoltec_average"


def test_plot_propagation_absoltec_day_json(monkeypatch):
    monkeypatch.setattr(
        plots_router,
        "get_raw_data_propagation_absoltec",
        lambda *args, **kwargs: [SimpleNamespace(ut=0.0, b_k=1.0, gdd=-1.0e-9)],
    )
    monkeypatch.setattr(
        plots_router.pp,
        "plot_single_day",
        lambda *args, **kwargs: _plot_result("propagation_absoltec_day"),
    )

    response = client.get(
        "/plots/propagation/absoltec/day",
        params={
            "year": 2026,
            "doy": 1,
            "station": "alex",
            "signal_band": "GPS_L1",
            "format": "json",
        },
    )

    assert response.status_code == 200
    assert response.json()["plot_type"] == "propagation_absoltec_day"


def test_plot_propagation_tec_satellite_json(monkeypatch):
    monkeypatch.setattr(
        plots_router,
        "get_raw_data_propagation_tec",
        lambda *args, **kwargs: SimpleNamespace(points=[SimpleNamespace(hour=0.0, b_k=1.0, gdd=-1.0e-9, validity=0)]),
    )
    monkeypatch.setattr(
        plots_router.pp,
        "plot_tec_satellite",
        lambda *args, **kwargs: _plot_result("propagation_tec_satellite"),
    )

    response = client.get(
        "/plots/propagation/tec/satellite",
        params={
            "year": 2026,
            "doy": 1,
            "station": "alex",
            "satellite": "G01",
            "signal_band": "GPS_L1",
            "format": "json",
        },
    )

    assert response.status_code == 200
    assert response.json()["plot_type"] == "propagation_tec_satellite"
