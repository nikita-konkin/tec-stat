# TEC Analysis Backend

HTTP backend for **AbsolTEC** (TayAbsTEC — http://www.gnss-lab.org/tay-abs-tec.html),
**TEC-suite** (http://www.gnss-lab.org/tec-suite), and **CB (Coherence Band)** data
stored as Parquet files. Built with FastAPI + DuckDB + matplotlib.

CB values are calculated from AbsolTEC data using the formula:
```
cb = sqrt(4*3*10^8 * 1^3 * 10^27) / sqrt(80.5 * π * abs_tec * 10^16)
```

---

## Quick start

```bash
# Docker Compose (recommended)
export DATA_PATH=/path/to/your/parq-data
docker-compose up --build -d
open http://localhost:8000/docs

# Local development
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env          # set DATA_ROOT=
uvicorn app.main:app --reload
```

---

## Data structure

### AbsolTEC files (TayAbsTEC output)

```
{DATA_ROOT}/
└── {year}_parq/
    └── {doy:03d}/
        └── {station}*/        ← any suffix: aksu0010, armv001g33, armv001k00 …
            └── {station}_{doy:03d}_{year}.parquet
```

Example: `/data/2026_parq/001/armv001g33/armv_001_2026.parquet`

**Important:** The station folder suffix is completely variable and is **ignored**
by the service. Only the file name pattern is used for lookup. This means
`armv001g33` and `armv001k00` both resolve to station code `armv`.

**Parquet schema** (case-sensitive — verified from real files):

| Column    | Type  | Description                                       |
|-----------|-------|---------------------------------------------------|
| `UT`      | float | Universal time in decimal hours [0.0 – 23.5, step 0.5] |
| `I_v`     | float | Absolute vertical TEC (TECU) — **I uppercase, v lowercase** |
| `G_lon`   | float | Sub-ionospheric point (SIP) geographic longitude (°) |
| `G_lat`   | float | SIP geographic latitude (°)                       |
| `G_q_lon` | float | Quality indicator for G_lon                       |
| `G_q_lat` | float | Quality indicator for G_lat                       |
| `G_t`     | float | TayAbsTEC temporal parameter                      |
| `G_q_t`   | float | Quality indicator for G_t                         |

All column names are stored in `app/db/columns.py` as constants. DuckDB
queries quote them (`"UT"`, `"I_v"` …) to respect the exact casing.

### TEC-suite files

```
{DATA_ROOT}/
└── {year}_parq/
    └── {doy:03d}/
        └── {station_4letter}*/          ← only first 4 letters of station code + any suffix
            └── {station_full}_{satellite}_{doy:03d}_{year2d}.parquet
```

Examples:
- `/data/2026_parq/001/aksu001i14/aksu_E07_001_26.parquet`   ← 4-letter code, folder = `aksu001i14`
- `/data/2026_parq/001/arsk001m39/arskm39_G24_001_26.parquet` ← extended code `arskm39`, folder starts with `arsk`

**Extended station codes:** When the station code is longer than 4 characters (e.g. `arskm39`,
`aksui14`), the folder is named with only the **first 4 letters** as the prefix (e.g. `arsk001m39`).
The service automatically uses the 4-letter prefix for folder discovery and the full code for
file matching, so both short and extended station codes are resolved correctly.

**Parquet schema** (all lowercase, verified from real files):

| Column      | Type  | Notes                                            |
|-------------|-------|--------------------------------------------------|
| `tsn`       | int   | Time sequence number                             |
| `hour`      | float | Time in decimal hours                            |
| `el`        | float | Satellite elevation (°)                          |
| `az`        | float | Satellite azimuth (°)                            |
| `tec.l1l2`  | float | TEC from L1/L2 carrier phase (TECU) — **dot in name** |
| `tec.c1p2`  | float | TEC from C1/P2 pseudorange (TECU) — **dot in name** |
| `validity`  | int   | Quality flag (0 = valid)                         |

The dot-named columns are quoted in SQL (`"tec.l1l2"`) and aliased to
underscore names (`tec_l1l2`) so all downstream Python code uses clean keys.

#### Station metadata

TEC-suite header convention — **L = longitude, B = latitude** (Russian geodetic):
```
# Position (L, B, H): 50.84156, 54.83785, 126.22   ← L=lon, B=lat !
# Position (X, Y, Z): 2324706.11, 2854596.64, 5191112.72
# Site: aksu
```

Scientific notation values (e.g. `6.378E+06`) are fully supported in the position fields.

The parser reads this from the embedded Parquet schema metadata. Two key formats are supported:

1. **`dat_parquet_handler.header_lines`** — used by the `dat_parquet_handler` converter.
   Value is a **JSON array of strings**, one per header line:
   ```json
   ["# Created on ...", "# Site: aksu", "# Position (L, B, H): 50.84, 54.83, 126.22", ...]
   ```
2. **`tec_suite_meta`** — plain multiline text (older convention).

The service auto-detects the format and decodes accordingly. A last-resort scan of all
metadata values is performed when neither key is found.

Embed metadata when writing parquet (`dat_parquet_handler` style):
```python
import json, pyarrow as pa, pyarrow.parquet as pq

header_lines = [
    "# Site: aksu",
    "# Position (L, B, H): 50.84156, 54.83785, 126.22",
    "# Position (X, Y, Z): 2324706.11, 2854596.64, 5191112.72",
]
schema = pa.schema(
    [("tsn", pa.int32()), ("hour", pa.float64()), ...],
    metadata={"dat_parquet_handler.header_lines": json.dumps(header_lines)}
)
pq.write_table(pa.Table.from_pandas(df, schema=schema), "aksu_E07_001_26.parquet")
```

### CB (Coherence Band) data

CB values are derived from AbsolTEC data using the physical formula above. The CB endpoints
use the same data availability as AbsolTEC (same stations, same days), but compute CB
values on-the-fly from the `I_v` (TEC) column.

**CB schema** (derived from AbsolTEC):

| Column    | Type  | Description                                       |
|-----------|-------|---------------------------------------------------|
| `ut`      | float | Universal time in decimal hours [0.0 – 23.5, step 0.5] |
| `tec`     | float | Original absolute vertical TEC (TECU)             |
| `cb`      | float | Calculated coherence band value                   |
| `g_lon`   | float | Sub-ionospheric point (SIP) geographic longitude (°) |
| `g_lat`   | float | SIP geographic latitude (°)                       |
| `g_q_lon` | float | Quality indicator for G_lon                       |
| `g_q_lat` | float | Quality indicator for G_lat                       |
| `g_t`     | float | TayAbsTEC temporal parameter                      |
| `g_q_t`   | float | Quality indicator for G_t                         |

CB statistics use the same formulas as AbsolTEC but computed on the derived CB values.

---

## Data export formats — JSON, CSV, or XLSX

Every **data-fetch** endpoint (AbsolTEC, TEC-suite, and Stations) accepts a `format` query
parameter that controls the response format:

| Value  | Content-Type | Response |
|--------|--------------|----------|
| `json` | `application/json` (default) | Standard JSON body — same as before |
| `csv`  | `text/csv` (UTF-8 BOM) | Flat, named-column CSV — download or `pd.read_csv()` |
| `xlsx` | `application/vnd.openxmlformats-officedocument.spreadsheetml.sheet` | Excel workbook attachment |

For `csv` and `xlsx` the response includes a `Content-Disposition: attachment; filename="..."` header
with a descriptive auto-generated filename.

```bash
# JSON (default)
curl "http://localhost:8000/tec/stations?year=2026&doy=1"

# CSV download
curl "http://localhost:8000/tec/stations?year=2026&doy=1&format=csv" -o stations.csv

# Excel download
curl "http://localhost:8000/absoltec/raw?year=2026&doy=1&station=aksu&format=xlsx" -o aksu.xlsx

# Per-station statistics as CSV
curl "http://localhost:8000/absoltec/statistics/per-station-day?year=2026&doy_start=1&doy_end=10&stations=aksu,arsk&format=csv"
```

The CSV/XLSX flattener recursively normalises nested JSON (dicts, lists) into tabular rows.
Nested-list fields are expanded one row per element; dict fields are dot-separated into individual
columns using `pd.json_normalize`. A `collection` column is added when a single response contains
multiple top-level lists (e.g. `/stations/available` with `absoltec_stations` + `tec_stations`).

---

## Response format — PNG, JSON, or Python script

Every plot endpoint accepts a `format` query parameter:

| Value    | Response                              | Use case |
|----------|---------------------------------------|----------|
| `png`    | `image/png` (default)                 | Web display, `<img src="...">` |
| `json`   | `application/json` column-oriented dict | JavaScript charts, custom analysis |
| `script` | `text/x-python` attachment (`.py`)    | Reproducible standalone figure |

```bash
# PNG (default)
curl "http://localhost:8000/plots/absoltec/average?year=2026&doy_start=1&doy_end=10&station=aksu"

# JSON data for client-side charting
curl "...&format=json"

# Standalone Python script
curl "...&format=script" -o my_plot.py
python3 my_plot.py          # pip install matplotlib scipy numpy
```

The JSON `series` object is column-oriented:
```json
{
  "plot_type": "absoltec_average",
  "title": "...",
  "metadata": {"year": 2026, "station": "aksu", ...},
  "series": {
    "ut":         [0.0, 0.5, 1.0, ... 23.5],
    "mean_tec":   [10.1, 10.3, ...],
    "student_ci": [0.4, 0.5, ...],
    "variance":   [0.2, 0.3, ...],
    "std_dev":    [0.44, 0.55, ...],
    "n":          [10, 10, ...],
    "mean_g_lon": [54.8, ...],
    "mean_g_lat": [50.8, ...]
  },
  "plot_options": {"show_student_ci": true, "show_variance": false}
}
```

The generated Python script embeds all data as literals so it runs offline
with only `matplotlib`, `scipy`, and `numpy` installed.

---

## API reference

Full interactive docs at `/docs` (Swagger UI) and `/redoc`.

### AbsolTEC (`&format=json|csv|xlsx` on all data endpoints)

| Method | Path | Description |
|--------|------|-------------|
| GET | `/absoltec/stations?year=&doy=` | Available stations |
| GET | `/absoltec/days?year=&station=` | Available days |
| GET | `/absoltec/raw?year=&doy=&station=` | Raw 48-point series (all 8 columns) |
| GET | `/absoltec/raw/range?year=&doy_start=&doy_end=&station=` | Raw rows concatenated by time over day range (single station) |
| GET | `/absoltec/raw/range?year=&doy_start=&doy_end=&stations=` | Raw rows concatenated by time over day range (multiple stations via repeated `stations`) |
| GET | `/absoltec/statistics?year=&doy_start=&doy_end=&station=&alpha=` | Mean ± CI |
| GET | `/absoltec/statistics/per-station-day?year=&doy_start=&doy_end=&stations=` | Network average |

### CB (Coherence Band) (`&format=json|csv|xlsx` on all data endpoints)

| Method | Path | Description |
|--------|------|-------------|
| GET | `/cb/stations?year=&doy=` | Available stations (same as AbsolTEC) |
| GET | `/cb/days?year=&station=` | Available days (same as AbsolTEC) |
| GET | `/cb/raw?year=&doy=&station=` | Raw 48-point CB series (TEC + CB + SIP columns) |
| GET | `/cb/raw/range?year=&doy_start=&doy_end=&station=` | Raw CB rows concatenated by time over day range (single station) |
| GET | `/cb/raw/range?year=&doy_start=&doy_end=&stations=` | Raw CB rows concatenated by time over day range (multiple stations via repeated `stations`) |
| GET | `/cb/statistics?year=&doy_start=&doy_end=&station=&alpha=` | Mean CB ± CI |
| GET | `/cb/statistics/per-station-day?year=&doy_start=&doy_end=&stations=` | Network CB average |

### TEC-suite (`&format=json|csv|xlsx` on all data endpoints)

| Method | Path | Description |
|--------|------|-------------|
| GET | `/tec/stations?year=&doy=` | Stations + coordinates |
| GET | `/tec/satellites?year=&doy=&station=` | Available satellites |
| GET | `/tec/data?year=&doy=&station=&satellite=` | Full observation series |
| GET | `/tec/raw/range?year=&doy_start=&doy_end=&station=` | Raw TEC rows (all satellites) concatenated over day range |
| GET | `/tec/raw/range?year=&doy_start=&doy_end=&stations=` | Same as above for multiple stations (repeat `stations`) |

### Plots (`&format=png|json|script` on all)

| Method | Path | Description |
|--------|------|-------------|
| GET | `/plots/absoltec/average` | Mean TEC ± CI over day range |
| GET | `/plots/absoltec/day` | Single day TEC (optional Savitzky-Golay) |
| GET | `/plots/absoltec/multi-station` | Multiple stations, one day |
| GET | `/plots/absoltec/raw/day-by-day` | Raw day-range plot with selectable columns and multi-station overlay |
| GET | `/plots/absoltec/per-station-averages/{doy}` | Network average for one day |
| GET | `/plots/cb/average` | Mean CB ± CI over day range |
| GET | `/plots/cb/day` | Single day CB time series |
| GET | `/plots/cb/multi-station` | Multiple stations CB over day range |
| GET | `/plots/cb/vs-tec` | Scatter plot of CB vs AbsolTEC values |
| GET | `/plots/cb/per-station-averages/{doy}` | Network CB average for one day |
| GET | `/plots/tec/satellite` | Satellite TEC time series |
| GET | `/plots/tec/sky-track` | Polar sky-track (el/az coloured by TEC) |
| GET | `/plots/tec/all-satellites` | All satellites overlaid |

#### New range/day-by-day examples

```bash
# AbsolTEC raw range, one station (JSON)
curl "http://localhost:8000/absoltec/raw/range?year=2026&doy_start=1&doy_end=3&station=aksu"

# AbsolTEC raw range, multiple stations (CSV)
curl "http://localhost:8000/absoltec/raw/range?year=2026&doy_start=1&doy_end=3&stations=aksu&stations=arsk&format=csv" -o absoltec_range.csv

# CB raw range, one station (JSON)
curl "http://localhost:8000/cb/raw/range?year=2026&doy_start=1&doy_end=3&station=aksu"

# CB statistics over day range
curl "http://localhost:8000/cb/statistics?year=2026&doy_start=1&doy_end=10&station=aksu"

# CB vs AbsolTEC scatter plot
curl "http://localhost:8000/plots/cb/vs-tec?year=2026&doy_start=1&doy_end=10&station=aksu&format=png" -o cb_vs_tec.png

# TEC raw range, multiple stations (XLSX)
curl "http://localhost:8000/tec/raw/range?year=2026&doy_start=1&doy_end=2&stations=aksu&stations=arsk&format=xlsx" -o tec_range.xlsx

# AbsolTEC day-by-day raw plot, two stations, two columns
curl "http://localhost:8000/plots/absoltec/raw/day-by-day?year=2026&doy_start=1&doy_end=4&stations=aksu&stations=arsk&columns=tec&columns=g_lon&format=png" -o day_by_day.png
```

Notes:
- AbsolTEC range endpoint adds `concat_ut` where `concat_ut = (doy - doy_start) * 24 + ut`.
- TEC range endpoint adds `concat_hour` where `concat_hour = (doy - doy_start) * 24 + hour`.
- Day-by-day plot endpoint supports `columns=tec,g_lon,g_lat,g_q_lon,g_q_lat,g_t,g_q_t` (repeat `columns` query param).

### Stations / map (`&format=json|csv|xlsx` on all data endpoints)

| Method | Path | Description |
|--------|------|-------------|
| GET | `/stations/available?year=&doy=&source=both` | Availability (absoltec\|tec\|both) |
| GET | `/stations/map?year=&doy=` | Stations + geodetic coords for world-map |

---

## Statistics

Exact reproduction of the original `Count_statistics()` formulas for both AbsolTEC and CB data:

| Metric | Formula |
|--------|---------|
| **Mean** | `AVG("I_v")` or `AVG(cb)` per time slot across N days |
| **Variance** | `VAR_POP("I_v")` or `VAR_POP(cb)` — population variance (denominator = N) |
| **Std dev** | `STDDEV_POP("I_v")` or `STDDEV_POP(cb)` |
| **Student CI** | `t_ppf(1 - α/2, df=N-1) × σ / √N` |

Default α = 0.05 (95 % confidence). Override with `?alpha=0.01`.

The SIP coordinates (`G_lon`, `G_lat`) are also averaged per slot and
returned in the statistics response for spatial analysis.

---

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `DATA_ROOT` | `/data` | Root of the parquet tree |
| `DEFAULT_ALPHA` | `0.05` | Student-t significance level |
| `PLOT_DPI` | `100` | Plot resolution |
| `PLOT_WIDTH_PX` | `1200` | Plot width in pixels |
| `PLOT_HEIGHT_PX` | `600` | Plot height in pixels |
| `SAVGOL_POLYNOMIAL_ORDER` | `3` | Savitzky-Golay polynomial degree |
| `API_PREFIX` | `` | URL prefix behind a reverse proxy |
| `WORKERS` | `2` | uvicorn worker count |

---

## Running tests

```bash
pip install pytest
pytest tests/ -v
```

Test suite coverage:
- `test_columns.py` — column name constants and SQL quoting
- `test_folder_detection.py` — variable-suffix folder discovery (no real parquet needed)
- `test_statistics.py` — statistics formulas against scipy (no I/O)
- `test_cb.py` — CB calculation formula and statistics validation
- `test_metadata_parsing.py` — TEC-suite header parser including L/B convention
- `test_script_generator.py` — generated Python script validity and content

---

## File structure

```
tec-backend/
├── app/
│   ├── main.py
│   ├── config.py
│   ├── db/
│   │   ├── columns.py          ← all column name constants (single source of truth)
│   │   └── engine.py           ← DuckDB connection + folder-agnostic path helpers
│   ├── models/schemas.py
│   ├── services/
│   │   ├── absoltec.py         ← DuckDB queries, statistics, quoted column names
│   │   ├── cb.py               ← CB calculation and statistics from AbsolTEC data
│   │   └── tec.py              ← satellite data + metadata parsing (L=lon, B=lat)
│   ├── plotting/
│   │   ├── __init__.py         ← PlotResult(png, data) namedtuple
│   │   ├── absoltec_plots.py
│   │   ├── cb_plots.py          ← CB plotting functions
│   │   ├── tec_plots.py
│   │   └── script_generator.py ← format=script handler
│   └── routers/
│       ├── absoltec.py
│       ├── cb.py               ← CB API endpoints
│       ├── tec.py
│       ├── plots.py            ← format=png|json|script dispatch
│       └── stations.py
├── tests/
│   ├── test_columns.py
│   ├── test_folder_detection.py
│   ├── test_statistics.py
│   ├── test_cb.py              ← CB calculation and statistics tests
│   ├── test_metadata_parsing.py
│   └── test_script_generator.py
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
└── .env.example
```
