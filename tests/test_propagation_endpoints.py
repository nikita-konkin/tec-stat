from types import SimpleNamespace

from fastapi.testclient import TestClient

from app.main import app
import app.routers.propagation as propagation_router


client = TestClient(app)


def test_propagation_calc_json(monkeypatch):
    monkeypatch.setattr(
        propagation_router,
        "calculate_propagation",
        lambda tec, f_hz, signal_band=None: SimpleNamespace(
            tec=tec,
            nt=tec * 1e16,
            f_hz=f_hz,
            signal_band=signal_band,
            b_k=123.0,
            gdd=-1.0e-9,
        ),
    )

    response = client.get("/propagation/calc", params={"tec": 10.0, "f_hz": 1.57542e9})

    assert response.status_code == 200
    payload = response.json()
    assert payload["tec"] == 10.0
    assert payload["b_k"] == 123.0


def test_propagation_calc_accepts_signal_band():
    response = client.get("/propagation/calc", params={"tec": 10.0, "signal_band": "GPS_L1"})

    assert response.status_code == 200
    payload = response.json()
    assert payload["signal_band"] == "GPS_L1"
    assert payload["f_hz"] == 1575420000.0


def test_propagation_absoltec_raw_json(monkeypatch):
    monkeypatch.setattr(
        propagation_router,
        "get_raw_data_propagation_absoltec",
        lambda *args, **kwargs: [
            SimpleNamespace(
                ut=0.0,
                tec=10.0,
                nt=1.0e17,
                f_hz=1.57542e9,
                signal_band="GPS_L1",
                b_k=100.0,
                gdd=-1.0e-9,
                g_lon=50.0,
                g_lat=60.0,
            ),
        ],
    )

    response = client.get(
        "/propagation/absoltec/raw",
        params={"year": 2026, "doy": 1, "station": "aksu", "f_hz": 1.57542e9},
    )

    assert response.status_code == 200
    payload = response.json()
    assert len(payload) == 1
    assert payload[0]["tec"] == 10.0
    assert payload[0]["signal_band"] == "GPS_L1"
    assert payload[0]["b_k"] == 100.0


def test_propagation_absoltec_statistics_json(monkeypatch):
    monkeypatch.setattr(
        propagation_router,
        "compute_statistics_propagation_absoltec",
        lambda *args, **kwargs: SimpleNamespace(
            year=2026,
            doy_start=1,
            doy_end=2,
            station="aksu",
            alpha=0.05,
            f_hz=1.57542e9,
            signal_band="GPS_L1",
            total_days=2,
            points=[
                SimpleNamespace(
                    ut=0.0,
                    mean_tec=10.0,
                    mean_nt=1.0e17,
                    mean_b_k=100.0,
                    variance_b_k=4.0,
                    std_dev_b_k=2.0,
                    student_ci_b_k=1.0,
                    mean_gdd=-1.0e-9,
                    variance_gdd=1.0e-20,
                    std_dev_gdd=1.0e-10,
                    student_ci_gdd=5.0e-11,
                    n=2,
                    mean_g_lon=50.0,
                    mean_g_lat=60.0,
                )
            ],
        ),
    )

    response = client.get(
        "/propagation/absoltec/statistics",
        params={"year": 2026, "doy_start": 1, "doy_end": 2, "station": "aksu", "f_hz": 1.57542e9},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["station"] == "aksu"
    assert payload["signal_band"] == "GPS_L1"
    assert payload["points"][0]["mean_b_k"] == 100.0


def test_propagation_tec_raw_json(monkeypatch):
    monkeypatch.setattr(
        propagation_router,
        "get_raw_data_propagation_tec",
        lambda *args, **kwargs: SimpleNamespace(
            year=2026,
            doy=1,
            station="aksu",
            satellite="G01",
            observable="tec_l1l2",
            f_hz=1.57542e9,
            signal_band="GPS_L1",
            points=[
                SimpleNamespace(
                    tsn=1,
                    hour=0.0,
                    el=30.0,
                    az=180.0,
                    observable="tec_l1l2",
                    tec=11.0,
                    nt=1.1e17,
                    f_hz=1.57542e9,
                    signal_band="GPS_L1",
                    b_k=95.0,
                    gdd=-1.2e-9,
                    validity=0,
                )
            ],
        ),
    )

    response = client.get(
        "/propagation/tec/raw",
        params={
            "year": 2026,
            "doy": 1,
            "station": "aksu",
            "satellite": "G01",
            "observable": "tec_l1l2",
            "f_hz": 1.57542e9,
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["satellite"] == "G01"
    assert payload["signal_band"] == "GPS_L1"
    assert payload["points"][0]["tec"] == 11.0
    assert payload["points"][0]["b_k"] == 95.0


def test_propagation_absoltec_statistics_rejects_invalid_range():
    response = client.get(
        "/propagation/absoltec/statistics",
        params={"year": 2026, "doy_start": 2, "doy_end": 1, "station": "aksu", "f_hz": 1.57542e9},
    )

    assert response.status_code == 422


def test_propagation_requires_frequency_or_signal_band():
    response = client.get(
        "/propagation/absoltec/raw",
        params={"year": 2026, "doy": 1, "station": "aksu"},
    )

    assert response.status_code == 422
