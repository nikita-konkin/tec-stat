from types import SimpleNamespace
import math

import pandas as pd
import pytest
from scipy.stats import t as student_t

import app.services.propagation as propagation_service
from app.db.columns import UT


def test_calculate_propagation_formula():
    tec = 2.9
    f_hz = 1.57542e9
    nt = tec * 1e16
    expected_b_k = math.sqrt((3e8 * (f_hz ** 3)) / (80.5 * math.pi * nt))
    expected_gdd = -(3 * 80.5 * nt) / (2 * 3e8 * math.pi * (f_hz ** 3))

    result = propagation_service.calculate_propagation(tec, f_hz)

    assert result.nt == pytest.approx(nt)
    assert result.b_k == pytest.approx(expected_b_k)
    assert result.gdd == pytest.approx(expected_gdd)


def test_resolve_frequency_from_signal_band():
    f_hz, signal_band = propagation_service.resolve_frequency(None, "gps_l1")

    assert signal_band == "GPS_L1"
    assert f_hz == pytest.approx(1.57542e9)


def test_resolve_frequency_prefers_explicit_frequency():
    f_hz, signal_band = propagation_service.resolve_frequency(1.2e9, "GPS_L1")

    assert f_hz == pytest.approx(1.2e9)
    assert signal_band is None


def test_resolve_frequency_glonass_bands():
    f_hz, signal_band = propagation_service.resolve_frequency(None, "GLO_L1")
    assert signal_band == "GLO_L1"
    assert f_hz == pytest.approx(1.602e9)

    # The full-name alias normalizes to the canonical GLO_* preset.
    f_hz, signal_band = propagation_service.resolve_frequency(None, "glonass_l2")
    assert signal_band == "GLO_L2"
    assert f_hz == pytest.approx(1.246e9)

    f_hz, signal_band = propagation_service.resolve_frequency(None, "glo_l3")
    assert signal_band == "GLO_L3"
    assert f_hz == pytest.approx(1.202025e9)


def test_calculate_b_k_none_for_non_positive_nt():
    assert propagation_service.calculate_b_k(0.0, 1.0) is None
    assert propagation_service.calculate_b_k(None, 1.0) is None


def test_calculate_gdd_none_for_non_positive_nt():
    assert propagation_service.calculate_gdd(0.0, 1.0) is None
    assert propagation_service.calculate_gdd(None, 1.0) is None


def test_get_raw_data_propagation_absoltec_derives_from_absoltec(monkeypatch):
    def fake_abs_raw(*_args, **_kwargs):
        return [
            SimpleNamespace(ut=0.0, tec=10.0, g_lon=1.0, g_lat=2.0),
            SimpleNamespace(ut=0.5, tec=0.0, g_lon=None, g_lat=None),
        ]

    monkeypatch.setattr(propagation_service, "get_absoltec_raw_data", fake_abs_raw)

    points = propagation_service.get_raw_data_propagation_absoltec(
        year=2026,
        doy=1,
        station="alex",
        f_hz=1.57542e9,
        signal_band="GPS_L1",
        data_root="/data",
    )

    assert len(points) == 2
    assert points[0].tec == 10.0
    assert points[0].nt == pytest.approx(10.0 * 1e16)
    assert points[0].signal_band == "GPS_L1"
    assert points[0].b_k is not None
    assert points[0].gdd is not None
    assert points[1].tec == 0.0
    assert points[1].nt == 0.0
    assert points[1].b_k is None
    assert points[1].gdd is None


def test_get_raw_data_propagation_tec_derives_from_selected_observable(monkeypatch):
    def fake_get_tec_data(*_args, **_kwargs):
        return SimpleNamespace(
            year=2026,
            doy=1,
            station="alex",
            satellite="G01",
            points=[
                SimpleNamespace(tsn=1, hour=0.0, el=30.0, az=180.0, tec_l1l2=12.0, tec_c1p2=13.0, validity=0),
            ],
        )

    monkeypatch.setattr(propagation_service, "get_tec_data", fake_get_tec_data)

    result = propagation_service.get_raw_data_propagation_tec(
        year=2026,
        doy=1,
        station="alex",
        satellite="G01",
        observable="tec_c1p2",
        f_hz=1.57542e9,
        signal_band="GPS_L1",
        data_root="/data",
    )

    assert result.observable == "tec_c1p2"
    assert result.signal_band == "GPS_L1"
    assert len(result.points) == 1
    assert result.points[0].tec == 13.0
    assert result.points[0].signal_band == "GPS_L1"
    assert result.points[0].b_k is not None


def test_build_stats_points_propagation_student_ci():
    n = 15
    std_b_k = 2.5
    std_gdd = 1.5e-9
    alpha = 0.05
    t_crit = student_t.ppf(1.0 - alpha / 2.0, df=n - 1)

    df = pd.DataFrame({
        UT: [0.0],
        "mean_tec": [10.0],
        "mean_nt": [10.0 * 1e16],
        "mean_b_k": [100.0],
        "variance_b_k": [std_b_k ** 2],
        "std_dev_b_k": [std_b_k],
        "mean_gdd": [-2.0e-9],
        "variance_gdd": [std_gdd ** 2],
        "std_dev_gdd": [std_gdd],
        "n": [n],
        "mean_g_lon": [50.0],
        "mean_g_lat": [60.0],
    })

    points = propagation_service._build_stats_points_propagation(df, alpha)

    assert len(points) == 1
    assert points[0].student_ci_b_k == pytest.approx(t_crit * std_b_k / math.sqrt(n))
    assert points[0].student_ci_gdd == pytest.approx(t_crit * std_gdd / math.sqrt(n))
