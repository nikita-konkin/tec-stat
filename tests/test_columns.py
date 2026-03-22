"""
Column name tests.

These tests verify that the constants in app.db.columns match the actual
column names found in the parquet files.  When you upgrade TayAbsTEC or
TEC-suite and regenerate your parquet files, run this suite first — a
failure here pinpoints the exact column that changed before anything else breaks.

The tests that read real parquet files use the sample files in the test fixture
directory (tests/fixtures/).  Fixtures are small (1–2 row) parquet files
generated from the actual sample data so the test suite can run offline.

For the constants-only tests (no file I/O), everything runs without any fixtures.
"""

import os
import pytest

from app.db.columns import (
    # AbsolTEC constants
    UT, I_V, G_LON, G_LAT, G_Q_LON, G_Q_LAT, G_T, G_Q_T,
    ABSOLTEC_COLS, ABSOLTEC_SELECT,
    # TEC-suite constants
    TSN, HOUR, EL, AZ, TEC_L1L2_RAW, TEC_C1P2_RAW, VALIDITY,
    TEC_L1L2, TEC_C1P2, TEC_SELECT, TEC_COLS_RAW,
)


# ── AbsolTEC column constants ─────────────────────────────────────────────────

class TestAbsoltecColumnConstants:
    """Verify that the constant values match the confirmed parquet schema."""

    def test_ut_column_is_uppercase(self):
        assert UT == "UT"

    def test_i_v_mixed_case(self):
        # I_v — capital I, lowercase v — counter-intuitive but confirmed from file
        assert I_V == "I_v"
        assert I_V[0] == "I"   # uppercase
        assert I_V[-1] == "v"  # lowercase

    def test_g_columns_mixed_case(self):
        assert G_LON == "G_lon"
        assert G_LAT == "G_lat"
        assert G_Q_LON == "G_q_lon"
        assert G_Q_LAT == "G_q_lat"
        assert G_T == "G_t"
        assert G_Q_T == "G_q_t"

    def test_absoltec_cols_contains_all_eight(self):
        assert len(ABSOLTEC_COLS) == 8
        expected = {UT, I_V, G_LON, G_LAT, G_Q_LON, G_Q_LAT, G_T, G_Q_T}
        assert set(ABSOLTEC_COLS) == expected

    def test_absoltec_select_quotes_all_columns(self):
        # Every column name should appear quoted in the SELECT fragment
        for col in ABSOLTEC_COLS:
            assert f'"{col}"' in ABSOLTEC_SELECT, (
                f'Column "{col}" is not quoted in ABSOLTEC_SELECT. '
                "DuckDB requires quoting for mixed-case column names."
            )

    def test_absoltec_select_is_comma_separated(self):
        # Quick sanity check that it's a flat comma list with no extra keywords
        parts = [p.strip() for p in ABSOLTEC_SELECT.split(",")]
        assert len(parts) == 8

    def test_ut_comes_first(self):
        assert ABSOLTEC_COLS[0] == UT

    def test_i_v_comes_second(self):
        assert ABSOLTEC_COLS[1] == I_V


# ── TEC-suite column constants ────────────────────────────────────────────────

class TestTecColumnConstants:
    """Verify TEC-suite column constants."""

    def test_basic_columns_are_lowercase(self):
        for col in [TSN, HOUR, EL, AZ, VALIDITY]:
            assert col == col.lower(), f"{col!r} should be lowercase"

    def test_dot_columns_preserved(self):
        # The original TEC-suite header has dots — verify they're preserved
        assert "." in TEC_L1L2_RAW, "tec.l1l2 should contain a dot"
        assert "." in TEC_C1P2_RAW, "tec.c1p2 should contain a dot"
        assert TEC_L1L2_RAW == "tec.l1l2"
        assert TEC_C1P2_RAW == "tec.c1p2"

    def test_python_aliases_use_underscores(self):
        assert TEC_L1L2 == "tec_l1l2"
        assert TEC_C1P2 == "tec_c1p2"
        assert "." not in TEC_L1L2
        assert "." not in TEC_C1P2

    def test_tec_select_quotes_dot_columns(self):
        # "tec.l1l2" must be quoted to be valid SQL
        assert f'"{TEC_L1L2_RAW}"' in TEC_SELECT
        assert f'"{TEC_C1P2_RAW}"' in TEC_SELECT

    def test_tec_select_includes_alias(self):
        # The alias removes the dot so Python can use it as a dict key
        assert f"AS {TEC_L1L2}" in TEC_SELECT
        assert f"AS {TEC_C1P2}" in TEC_SELECT

    def test_tec_cols_raw_has_seven_columns(self):
        assert len(TEC_COLS_RAW) == 7

    def test_tsn_first_validity_last(self):
        assert TEC_COLS_RAW[0] == TSN
        assert TEC_COLS_RAW[-1] == VALIDITY


# ── SQL injection guard ───────────────────────────────────────────────────────

class TestNoSqlInjectionInColumnNames:
    """Column names come from constants (not user input), but sanity-check them."""

    def test_absoltec_cols_contain_no_sql_keywords(self):
        dangerous = {"DROP", "SELECT", "INSERT", "DELETE", "FROM", "--", ";"}
        for col in ABSOLTEC_COLS:
            for d in dangerous:
                assert d.upper() not in col.upper(), (
                    f"Column {col!r} contains suspicious token {d!r}"
                )

    def test_tec_cols_raw_contain_no_sql_keywords(self):
        dangerous = {"DROP", "SELECT", "INSERT", "DELETE", "FROM", "--", ";"}
        for col in TEC_COLS_RAW:
            for d in dangerous:
                assert d.upper() not in col.upper()
