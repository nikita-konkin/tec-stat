from types import SimpleNamespace

import app.services.cb as cb_service


def test_get_raw_data_range_cb_derives_from_absoltec_and_adds_cb(monkeypatch):
    def fake_abs_range(**_kwargs):
        return [
            {"station": "b", "concat_ut": 2.0, "tec": 10.0},
            {"station": "a", "concat_ut": 1.0, "tec": 0.0},
        ]

    monkeypatch.setattr(cb_service, "get_absoltec_raw_data_range", fake_abs_range)

    rows = cb_service.get_raw_data_range_cb(
        year=2026,
        doy_start=1,
        doy_end=2,
        stations=["a", "b"],
        data_root="/data",
    )

    assert [r["station"] for r in rows] == ["a", "b"]  # sorted by (station, concat_ut)
    assert "cb" in rows[0] and rows[0]["cb"] == 0.0
    assert "cb" in rows[1] and rows[1]["cb"] > 0.0


def test_get_raw_data_cb_derives_from_absoltec_raw(monkeypatch):
    def fake_abs_raw(*_args, **_kwargs):
        return [
            SimpleNamespace(ut=0.0, tec=10.0, g_lon=1.0, g_lat=2.0),
            SimpleNamespace(ut=0.5, tec=0.0, g_lon=None, g_lat=None),
        ]

    monkeypatch.setattr(cb_service, "get_absoltec_raw_data", fake_abs_raw)

    points = cb_service.get_raw_data_cb(year=2026, doy=1, station="alex", data_root="/data")

    assert len(points) == 2
    assert points[0].tec == 10.0
    assert points[0].cb > 0.0
    assert points[0].g_lon == 1.0
    assert points[1].tec == 0.0
    assert points[1].cb == 0.0

