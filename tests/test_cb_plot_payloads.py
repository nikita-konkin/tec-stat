from types import SimpleNamespace

from app.plotting import cb_plots as cp


def test_plot_multi_station_cb_returns_plotly_friendly_series():
    rows = [
        {"station": "alex", "concat_ut": 0.0, "cb": 1.0},
        {"station": "alex", "concat_ut": 0.5, "cb": 1.1},
        {"station": "aksu", "concat_ut": 0.0, "cb": 2.0},
    ]
    result = cp.plot_multi_station_cb(
        data=rows,
        year=2026,
        doy_start=1,
        doy_end=1,
        stations=["alex", "aksu"],
        width_px=600,
        height_px=300,
        dpi=100,
    )

    series = result.data["series"]
    assert "alex" in series and "aksu" in series
    assert set(series["alex"].keys()) >= {"x", "y"}
    assert len(series["alex"]["x"]) == len(series["alex"]["y"])


def test_plot_per_station_averages_cb_returns_plotly_friendly_series():
    responses = [
        SimpleNamespace(
            year=2026,
            doy=1,
            alpha=0.05,
            points=[
                SimpleNamespace(ut=0.0, mean_cb=1.0, student_ci=0.1),
                SimpleNamespace(ut=0.5, mean_cb=1.1, student_ci=0.1),
            ],
        ),
        SimpleNamespace(
            year=2026,
            doy=2,
            alpha=0.05,
            points=[
                SimpleNamespace(ut=0.0, mean_cb=2.0, student_ci=0.2),
                SimpleNamespace(ut=0.5, mean_cb=2.1, student_ci=0.2),
            ],
        ),
    ]
    result = cp.plot_per_station_averages_cb(
        responses=responses,
        year=2026,
        doy_start=1,
        doy_end=2,
        stations=["alex", "aksu"],
        width_px=600,
        height_px=300,
        dpi=100,
    )

    series = result.data["series"]
    assert series  # should not be empty
    first_key = sorted(series.keys())[0]
    assert set(series[first_key].keys()) >= {"x", "y"}
    assert len(series[first_key]["x"]) == len(series[first_key]["y"])

