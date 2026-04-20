"""
Unit tests for the file-discovery helpers in app/db/engine.py.

These tests don't touch the filesystem: they monkeypatch glob() and verify
that the correct patterns are used and the selection logic is deterministic.
"""

import os

from app.db import engine


class TestAbsoltecDiscovery:

    def test_find_absoltec_file_builds_expected_glob_pattern(self, monkeypatch):
        captured: list[str] = []

        def fake_glob(pattern: str):
            captured.append(pattern)
            return []

        monkeypatch.setattr(engine.glob, "glob", fake_glob)

        result = engine.find_absoltec_file("/data", 2026, 1, "AKSU")
        assert result is None

        expected = {
            os.path.join("/data", "2026", "001", "aksu*", "aksu_001_2026.parquet"),
            os.path.join("/data", "2026_parq", "001", "aksu*", "aksu_001_2026.parquet"),
        }
        assert set(captured) == expected

    def test_find_absoltec_file_is_deterministic(self, monkeypatch):
        def fake_glob(_pattern: str):
            return ["z.parquet", "a.parquet", "m.parquet"]

        monkeypatch.setattr(engine.glob, "glob", fake_glob)
        assert engine.find_absoltec_file("/data", 2026, 1, "aksu") == "a.parquet"


class TestTecDiscovery:

    def test_tec_station_folder_prefix(self):
        assert engine._tec_station_folder_prefix("arskm39") == "arsk"
        assert engine._tec_station_folder_prefix("AKSU") == "aksu"

    def test_find_tec_file_builds_expected_glob_pattern(self, monkeypatch):
        captured: list[str] = []

        def fake_glob(pattern: str):
            captured.append(pattern)
            return []

        monkeypatch.setattr(engine.glob, "glob", fake_glob)

        result = engine.find_tec_file("/data", 2026, 1, "arskm39", "G01")
        assert result is None

        expected = {
            os.path.join("/data", "2026", "001", "arsk*", "arsk_G01_001_26.parquet"),
            os.path.join("/data", "2026_parq", "001", "arsk*", "arsk_G01_001_26.parquet"),
            os.path.join("/data", "2026", "001", "arsk*", "arskm39_G01_001_26.parquet"),
            os.path.join("/data", "2026_parq", "001", "arsk*", "arskm39_G01_001_26.parquet"),
        }
        assert set(captured) == expected
