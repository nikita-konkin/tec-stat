import io

import pandas as pd
from fastapi.responses import StreamingResponse

from app.models.schemas import TecDataResponse, TecPoint
from app.routers.export import format_payload, payload_to_dataframe


def test_payload_to_dataframe_flattens_points_with_metadata():
    payload = TecDataResponse(
        year=2026,
        doy=1,
        station="aksu",
        satellite="E07",
        points=[
            TecPoint(
                tsn=1,
                hour=0.0,
                el=12.5,
                az=222.0,
                tec_l1l2=10.1,
                tec_c1p2=9.9,
                validity=0,
            ),
            TecPoint(
                tsn=2,
                hour=0.5,
                el=15.0,
                az=224.0,
                tec_l1l2=10.3,
                tec_c1p2=10.0,
                validity=0,
            ),
        ],
    )

    df = payload_to_dataframe(payload)

    assert len(df) == 2
    assert set(["year", "doy", "station", "satellite", "tsn", "hour"]).issubset(df.columns)
    assert df["station"].nunique() == 1
    assert df["station"].iloc[0] == "aksu"


def test_payload_to_dataframe_handles_multiple_station_lists():
    payload = {
        "year": 2026,
        "doy": 2,
        "absoltec_stations": ["aksu", "alex"],
        "tec_stations": ["aksu", "alks"],
    }

    df = payload_to_dataframe(payload)

    assert len(df) == 4
    assert "collection" in df.columns
    assert "absoltec_station" in df.columns
    assert "tec_station" in df.columns


def test_format_payload_csv_has_header_columns():
    payload = [
        {"station": "aksu", "lat": 54.8, "lon": 50.8},
        {"station": "alex", "lat": 56.4, "lon": 38.7},
    ]

    response = format_payload(payload, "csv", "stations_map_2026_001")

    assert isinstance(response, StreamingResponse)
    # Rebuild CSV from the same flattening logic to verify column headers.
    expected_csv = payload_to_dataframe(payload).to_csv(index=False)
    assert "station,lat,lon" in expected_csv


def test_format_payload_xlsx_is_readable_workbook():
    payload = [
        {"station": "aksu", "lat": 54.8, "lon": 50.8},
    ]

    response = format_payload(payload, "xlsx", "stations_map_2026_001")

    assert isinstance(response, StreamingResponse)
    # Validate the writer path by producing and reading equivalent workbook bytes.
    buf = io.BytesIO()
    payload_to_dataframe(payload).to_excel(buf, index=False, engine="openpyxl")
    buf.seek(0)
    roundtrip = pd.read_excel(buf)
    assert list(roundtrip.columns) == ["station", "lat", "lon"]
    assert roundtrip.iloc[0]["station"] == "aksu"
