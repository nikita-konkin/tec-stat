from fastapi.testclient import TestClient

from app.main import app
import app.routers.absoltec as absoltec_router
import app.routers.tec as tec_router
import app.routers.plots as plots_router


client = TestClient(app)


def test_absoltec_raw_range_requires_station_or_stations():
    response = client.get("/absoltec/raw/range", params={
        "year": 2026,
        "doy_start": 1,
        "doy_end": 2,
    })
    assert response.status_code == 422


def test_absoltec_raw_range_station_json(monkeypatch):
    def fake_get_raw_data_range(year, doy_start, doy_end, stations, data_root):
        assert stations == ["aksu"]
        return [
            {
                "year": year,
                "doy": doy_start,
                "station": "aksu",
                "concat_ut": 0.0,
                "ut": 0.0,
                "tec": 10.0,
                "g_lon": 54.8,
                "g_lat": 50.8,
                "g_q_lon": None,
                "g_q_lat": None,
                "g_t": None,
                "g_q_t": None,
            }
        ]

    monkeypatch.setattr(absoltec_router, "get_raw_data_range", fake_get_raw_data_range)

    response = client.get("/absoltec/raw/range", params={
        "year": 2026,
        "doy_start": 1,
        "doy_end": 2,
        "station": "aksu",
    })
    assert response.status_code == 200
    payload = response.json()
    assert len(payload) == 1
    assert payload[0]["station"] == "aksu"
    assert payload[0]["concat_ut"] == 0.0


def test_tec_raw_range_with_multiple_stations(monkeypatch):
    def fake_get_tec_data_range(year, doy_start, doy_end, stations, data_root):
        assert stations == ["aksu", "arsk"]
        return [
            {
                "year": year,
                "doy": doy_start,
                "station": "aksu",
                "satellite": "G24",
                "concat_hour": 0.0,
                "hour": 0.0,
                "tsn": 1,
                "el": 30.0,
                "az": 180.0,
                "tec_l1l2": 10.0,
                "tec_c1p2": 10.2,
                "validity": 0,
            }
        ]

    monkeypatch.setattr(tec_router, "get_tec_data_range", fake_get_tec_data_range)

    response = client.get("/tec/raw/range", params=[
        ("year", 2026),
        ("doy_start", 1),
        ("doy_end", 2),
        ("stations", "aksu"),
        ("stations", "arsk"),
    ])
    assert response.status_code == 200
    payload = response.json()
    assert len(payload) == 1
    assert payload[0]["satellite"] == "G24"


def test_plot_absoltec_raw_day_by_day_json(monkeypatch):
    def fake_get_raw_data_range(year, doy_start, doy_end, stations, data_root):
        return [
            {"station": "aksu", "concat_ut": 0.0, "tec": 10.0, "g_lon": 54.8},
            {"station": "aksu", "concat_ut": 0.5, "tec": 10.2, "g_lon": 54.9},
            {"station": "arsk", "concat_ut": 0.0, "tec": 12.0, "g_lon": 50.1},
            {"station": "arsk", "concat_ut": 0.5, "tec": 12.3, "g_lon": 50.2},
        ]

    monkeypatch.setattr(plots_router, "get_raw_data_range", fake_get_raw_data_range)

    response = client.get("/plots/absoltec/raw/day-by-day", params=[
        ("year", 2026),
        ("doy_start", 1),
        ("doy_end", 2),
        ("stations", "aksu"),
        ("stations", "arsk"),
        ("columns", "tec"),
        ("columns", "g_lon"),
        ("format", "json"),
    ])

    assert response.status_code == 200
    payload = response.json()
    assert payload["plot_type"] == "absoltec_day_by_day_raw"
    assert "aksu:tec" in payload["series"]
    assert "arsk:g_lon" in payload["series"]


def test_plot_absoltec_raw_day_by_day_invalid_column():
    response = client.get("/plots/absoltec/raw/day-by-day", params={
        "year": 2026,
        "doy_start": 1,
        "doy_end": 2,
        "station": "aksu",
        "columns": "invalid_col",
        "format": "json",
    })
    assert response.status_code == 422
