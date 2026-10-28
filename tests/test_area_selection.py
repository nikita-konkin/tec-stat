"""
Selecting stations by area: /stations/nearby and near/radius_km/bbox on the
range endpoints.

The fixture network sits around Kazan (kazn): mork ~92 km away, arsk ~60 km,
moks ~720 km (Moscow region) and aksu ~260 km, each with a TEC-suite file
carrying its position in the parquet header; kazn and arsk also have AbsolTEC.
"""

import math
import os

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.services import area as area_service
from app.services.area import Area, haversine_km, parse_bbox, parse_near

STATIONS = {
    "kazn": (55.790, 49.120),
    "arsk": (56.090, 49.880),
    "mork": (56.433, 48.987),
    "aksu": (54.838, 50.842),
    "moks": (55.750, 37.620),
}


def _write_tec(root, year, doy, station, lat, lon):
    folder = os.path.join(root, str(year), f"{doy:03d}", station)
    os.makedirs(folder, exist_ok=True)
    header = f"# Site: {station}\n# Position (L, B, H): {lon}, {lat}, 100.0\n"
    table = pa.table(
        {
            "tsn": [0, 120],
            "hour": [0.0, 1.0],
            "el": [30.0, 31.0],
            "az": [90.0, 91.0],
            "tec.l1l2": [10.0, 11.0],
            "tec.c1p2": [20.0, 21.0],
            "validity": [0, 0],
        }
    )
    table = table.replace_schema_metadata({"tec_suite_meta": header})
    pq.write_table(table, os.path.join(folder, f"{station}_G05_{doy:03d}_{str(year)[-2:]}.parquet"))


def _write_absoltec(root, year, doy, station):
    folder = os.path.join(root, str(year), f"{doy:03d}", f"{station}{doy:03d}0")
    os.makedirs(folder, exist_ok=True)
    pd.DataFrame(
        {
            "UT": [0.0, 0.5],
            "I_v": [12.0, 13.0],
            "G_lon": [0.0, 0.0],
            "G_lat": [0.0, 0.0],
            "G_q_lon": [0.0, 0.0],
            "G_q_lat": [0.0, 0.0],
            "G_t": [0.0, 0.0],
            "G_q_t": [0.0, 0.0],
        }
    ).to_parquet(os.path.join(folder, f"{station}_{doy:03d}_{year}.parquet"))


@pytest.fixture
def network(tmp_path):
    tec_root, abs_root = str(tmp_path / "tec"), str(tmp_path / "abs")
    for station, (lat, lon) in STATIONS.items():
        _write_tec(tec_root, 2025, 1, station, lat, lon)
    # kazn has no data on day 2; the others do, so day 2 tests a centre station
    # known only from an earlier lookup.
    for station in ("arsk", "mork", "aksu", "moks"):
        _write_tec(tec_root, 2025, 2, station, *STATIONS[station])
    _write_absoltec(abs_root, 2025, 1, "kazn")
    _write_absoltec(abs_root, 2025, 1, "arsk")
    area_service.clear_position_cache()
    yield {"tec": tec_root, "abs": abs_root}
    area_service.clear_position_cache()


@pytest.fixture
def client(network, monkeypatch):
    from app.config import settings

    monkeypatch.setattr(type(settings), "get_tec_root", lambda self, override=None: network["tec"])
    monkeypatch.setattr(type(settings), "get_absoltec_root", lambda self, override=None: network["abs"])
    return TestClient(app)


DAY1 = {"year": 2025, "doy_start": 1, "doy_end": 1}


# ── Geometry and parsing ─────────────────────────────────────────────────────

def test_haversine_matches_known_distances():
    assert haversine_km(0, 0, 0, 1) == pytest.approx(111.195, rel=1e-4)  # 1° of equator
    assert haversine_km(55.79, 49.12, 55.75, 37.62) == pytest.approx(720, abs=5)  # Kazan–Moscow
    assert haversine_km(10, 20, 10, 20) == 0.0


def test_parse_near_and_bbox():
    assert parse_near("55.79, 49.12") == (55.79, 49.12, None)
    assert parse_near("KAZN") == (None, None, "kazn")
    for bad in ("91,0", "0,181", "a,b", ""):
        with pytest.raises(ValueError):
            parse_near(bad)
    assert parse_bbox("48,55,51,57") == (48.0, 55.0, 51.0, 57.0)
    for bad in ("1,2,3", "0,60,10,50", "200,0,10,10"):
        with pytest.raises(ValueError):
            parse_bbox(bad)


def test_bbox_crossing_the_antimeridian():
    box = Area(bbox=(170.0, -10.0, -170.0, 10.0))
    assert box.contains(0.0, 175.0)
    assert box.contains(0.0, -175.0)
    assert not box.contains(0.0, 0.0)


# ── /stations/nearby ─────────────────────────────────────────────────────────

def test_nearby_by_station_is_sorted_with_distances(client):
    r = client.get("/stations/nearby", params={**DAY1, "near": "kazn", "radius_km": 300})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["centre"] == {"lat": 55.79, "lon": 49.12, "station": "kazn"}
    names = [s["station"] for s in body["stations"]]
    assert names == ["kazn", "arsk", "mork", "aksu"]  # moks (~720 km) is outside
    distances = [s["distance_km"] for s in body["stations"]]
    assert distances == sorted(distances)
    assert distances[0] == 0.0
    assert distances[2] == pytest.approx(
        haversine_km(55.79, 49.12, *STATIONS["mork"]), abs=0.1
    )
    by_name = {s["station"]: s for s in body["stations"]}
    assert by_name["kazn"]["absoltec_days"] == 1 and by_name["mork"]["absoltec_days"] == 0
    assert all(s["tec_days"] == 1 for s in body["stations"])


def test_nearby_by_point_and_source_filter(client):
    r = client.get(
        "/stations/nearby", params={**DAY1, "near": "55.79,49.12", "radius_km": 100, "source": "absoltec"}
    )
    assert r.status_code == 200, r.text
    assert [s["station"] for s in r.json()["stations"]] == ["kazn", "arsk"]
    assert r.json()["centre"]["station"] is None


def test_nearby_bbox(client):
    r = client.get("/stations/nearby", params={**DAY1, "bbox": "48.5,55.5,50.5,57"})
    assert r.status_code == 200, r.text
    assert sorted(s["station"] for s in r.json()["stations"]) == ["arsk", "kazn", "mork"]
    assert r.json()["bbox"] == [48.5, 55.5, 50.5, 57.0]
    assert all(s["distance_km"] is None for s in r.json()["stations"])


def test_centre_station_without_data_in_range_uses_its_last_known_position(client):
    unknown = client.get("/stations/nearby", params={"year": 2025, "doy_start": 2, "doy_end": 2,
                                                     "near": "kazn", "radius_km": 100})
    assert unknown.status_code == 404
    assert "near=lat,lon" in unknown.json()["detail"]

    client.get("/stations/nearby", params={**DAY1, "near": "kazn", "radius_km": 100})  # learns kazn
    known = client.get("/stations/nearby", params={"year": 2025, "doy_start": 2, "doy_end": 2,
                                                   "near": "kazn", "radius_km": 100})
    assert known.status_code == 200, known.text
    assert [s["station"] for s in known.json()["stations"]] == ["arsk", "mork"]


@pytest.mark.parametrize(
    "params, status, message",
    [
        ({"near": "kazn"}, 422, "together"),
        ({"radius_km": 100}, 422, "together"),
        ({"near": "kazn", "radius_km": 100, "bbox": "1,2,3,4"}, 422, "not both"),
        ({"bbox": "1,2,3"}, 422, "bbox must be"),
        ({"near": "zzzz", "radius_km": 100}, 404, "No position known"),
        ({"near": "kazn", "radius_km": 0}, 422, None),
    ],
)
def test_nearby_rejects_bad_areas(client, params, status, message):
    r = client.get("/stations/nearby", params={**DAY1, **params})
    assert r.status_code == status, r.text
    if message:
        assert message in r.json()["detail"]


# ── Area selection on range endpoints ────────────────────────────────────────

def test_tec_raw_range_selects_stations_by_area(client):
    r = client.get("/tec/raw/range", params={**DAY1, "near": "kazn", "radius_km": 100})
    assert r.status_code == 200, r.text
    assert sorted({row["station"] for row in r.json()}) == ["arsk", "kazn", "mork"]


def test_absoltec_area_keeps_only_stations_with_absoltec_data(client):
    r = client.get("/absoltec/raw/range", params={**DAY1, "near": "kazn", "radius_km": 300})
    assert r.status_code == 200, r.text
    assert sorted({row["station"] for row in r.json()}) == ["arsk", "kazn"]


def test_listed_stations_and_area_combine(client):
    r = client.get("/tec/raw/range", params={**DAY1, "near": "kazn", "radius_km": 10, "stations": "moks"})
    assert r.status_code == 200, r.text
    assert sorted({row["station"] for row in r.json()}) == ["kazn", "moks"]


def test_empty_area_is_404_and_oversized_area_is_422(client, monkeypatch):
    from app.config import settings

    empty = client.get("/tec/raw/range", params={**DAY1, "near": "0,0", "radius_km": 50})
    assert empty.status_code == 404
    assert "No stations with tec data" in empty.json()["detail"]

    monkeypatch.setattr(settings, "max_area_stations", 2)
    big = client.get("/tec/raw/range", params={**DAY1, "near": "kazn", "radius_km": 1000})
    assert big.status_code == 422
    assert "the limit is 2" in big.json()["detail"]


def test_per_station_day_statistics_accept_an_area(client):
    r = client.get(
        "/absoltec/statistics/per-station-day", params={**DAY1, "near": "kazn", "radius_km": 300}
    )
    assert r.status_code == 200, r.text


def test_listing_stations_still_works_without_an_area(client):
    r = client.get("/absoltec/raw/range", params={**DAY1, "station": "kazn"})
    assert r.status_code == 200, r.text
    assert {row["station"] for row in r.json()} == {"kazn"}
    missing = client.get("/absoltec/raw/range", params=DAY1)
    assert missing.status_code == 422


def test_positions_are_cached_per_range(client, monkeypatch):
    calls = []
    real = area_service.list_stations_with_meta_range

    def counting(*args, **kwargs):
        calls.append(1)
        return real(*args, **kwargs)

    monkeypatch.setattr(area_service, "list_stations_with_meta_range", counting)
    for _ in range(3):
        assert client.get("/stations/nearby", params={**DAY1, "near": "kazn", "radius_km": 50}).status_code == 200
    assert len(calls) == 1
    assert math.isfinite(haversine_km(*STATIONS["kazn"], *STATIONS["moks"]))
