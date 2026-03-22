"""
Central configuration for the TEC backend.
All values are overridable via environment variables or a .env file.

Data path strategy
------------------
AbsolTEC and TEC-suite data can live in separate directory trees or share
one root.  Three environment variables control this:

  ABSOLTEC_DATA_ROOT   path to the AbsolTEC parquet tree  (default /data/absoltec)
  TEC_DATA_ROOT        path to the TEC-suite parquet tree (default /data/tec)
  DATA_ROOT            shortcut: sets BOTH roots at once when they share a tree

Priority (highest first):
  1. ABSOLTEC_DATA_ROOT / TEC_DATA_ROOT  — product-specific, always wins
  2. DATA_ROOT                           — shared root fallback
  3. Built-in defaults below             — used when nothing is set

Examples:

  # Both products under /archive
  DATA_ROOT=/archive

  # Separate trees
  ABSOLTEC_DATA_ROOT=/mnt/nas/absoltec
  TEC_DATA_ROOT=/mnt/nas/tec-suite

  # Mixed: TEC-suite on a different disk, AbsolTEC on shared
  DATA_ROOT=/data
  TEC_DATA_ROOT=/mnt/fast-disk/tec
"""

import os
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # ---------------------------------------------------------------- data paths

    # Product-specific roots.  Set these independently if the data lives in
    # separate directories.
    absoltec_data_root: str = "/data/absoltec"
    tec_data_root:      str = "/data/tec"

    # Shared root shortcut.  When set, it overrides BOTH product roots unless
    # those are also explicitly set.  Leave empty to use the roots above.
    data_root: str = ""

    # ------------------------------------------------------------ statistics
    default_alpha: float = 0.05        # significance level for Student t-CI
    time_slot_step: float = 0.5        # 30-minute slots → 48 per day
    time_slots_per_day: int = 48

    # -------------------------------------------------------------- plotting
    plot_dpi: int = 100
    plot_width_px: int = 1200
    plot_height_px: int = 600
    savgol_polynomial_order: int = 3   # Savitzky-Golay polynomial degree

    # ------------------------------------------------------------------- API
    api_title: str = "TEC Analysis Backend"
    api_version: str = "1.0.0"
    api_prefix: str = ""               # set to e.g. "/api/v1" behind a proxy

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    # ---------------------------------------------------------------- helpers

    def get_absoltec_root(self, override: str | None = None) -> str:
        """
        Resolve the AbsolTEC data root with the priority chain:
          1. `override` argument  (passed in from a query parameter)
          2. ABSOLTEC_DATA_ROOT env / setting
          3. DATA_ROOT env / setting  (shared fallback)
        """
        if override:
            return override
        # If a shared DATA_ROOT was explicitly provided, it wins over the
        # product-specific default — but not over an explicit product-specific value.
        if self.data_root and self.absoltec_data_root == "/data/absoltec":
            return self.data_root
        return self.absoltec_data_root

    def get_tec_root(self, override: str | None = None) -> str:
        """
        Resolve the TEC-suite data root with the same priority chain.
        """
        if override:
            return override
        if self.data_root and self.tec_data_root == "/data/tec":
            return self.data_root
        return self.tec_data_root


# Single importable instance
settings = Settings()
