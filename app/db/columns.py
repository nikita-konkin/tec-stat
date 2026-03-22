"""
Column name constants for AbsolTEC and TEC-suite parquet files.

Having a single place for these names means a column rename in the parquet
schema only requires one edit here — every SQL query and every test
automatically picks up the change.

Verified from actual parquet files (March 2026):
  AbsolTEC  → armv_001_2026.parquet
  TEC-suite → aksu_E07_001_26.parquet
"""

# ──────────────────────────────────────────────────────────────────────────────
# AbsolTEC (TayAbsTEC output)
# Tool homepage: http://www.gnss-lab.org/tay-abs-tec.html
# ──────────────────────────────────────────────────────────────────────────────

# Primary observation columns
UT    = "UT"       # Universal time in decimal hours, 0.0–23.5, step 0.5
I_V   = "I_v"      # Absolute vertical TEC (TECU) — note mixed case: I uppercase, v lowercase

# Sub-ionospheric point (SIP) geographic coordinates
G_LON   = "G_lon"    # SIP geographic longitude (degrees)
G_LAT   = "G_lat"    # SIP geographic latitude (degrees)
G_Q_LON = "G_q_lon"  # Quality indicator for G_lon
G_Q_LAT = "G_q_lat"  # Quality indicator for G_lat
G_T     = "G_t"      # TayAbsTEC temporal parameter
G_Q_T   = "G_q_t"    # Quality indicator for G_t

# Full ordered list matching the parquet schema
ABSOLTEC_COLS: list[str] = [UT, I_V, G_LON, G_LAT, G_Q_LON, G_Q_LAT, G_T, G_Q_T]

# SQL SELECT fragment (quoted because I_v contains mixed case — DuckDB is
# case-sensitive inside read_parquet() for column references)
ABSOLTEC_SELECT = ", ".join(f'"{c}"' for c in ABSOLTEC_COLS)


# ──────────────────────────────────────────────────────────────────────────────
# TEC-suite output
# Tool homepage: http://www.gnss-lab.org/tec-suite
# ──────────────────────────────────────────────────────────────────────────────

# Column names as stored in parquet (all lowercase; dots from the original
# .dat header are preserved because pyarrow allows them)
TSN         = "tsn"
HOUR        = "hour"
EL          = "el"
AZ          = "az"
TEC_L1L2_RAW = "tec.l1l2"   # ← dot in name; must be quoted in SQL
TEC_C1P2_RAW = "tec.c1p2"   # ← same
VALIDITY    = "validity"

# Python-friendly aliases used after the SQL SELECT … AS … statement.
# Dot → underscore so we can use them as dict keys and attribute names.
TEC_L1L2 = "tec_l1l2"
TEC_C1P2 = "tec_c1p2"

# SQL SELECT fragment — aliases dot-named columns to underscore names
TEC_SELECT = (
    f'{TSN}, {HOUR}, {EL}, {AZ}, '
    f'"{TEC_L1L2_RAW}" AS {TEC_L1L2}, '
    f'"{TEC_C1P2_RAW}" AS {TEC_C1P2}, '
    f'{VALIDITY}'
)

# All columns in raw parquet order (before aliasing)
TEC_COLS_RAW: list[str] = [TSN, HOUR, EL, AZ, TEC_L1L2_RAW, TEC_C1P2_RAW, VALIDITY]
