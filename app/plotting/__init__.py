"""
Shared types for the plotting layer.

PlotResult is a NamedTuple so each plot function returns both the rendered
PNG bytes AND the raw data dict in one value. The router decides which to
send based on the client's requested format (png / json / script).
"""

from typing import NamedTuple


class PlotResult(NamedTuple):
    """
    Return type of every plot_* function.

    png:  Raw PNG bytes — returned as-is for format=png.
    data: JSON-serialisable dict conforming to the PlotData schema.
          Used for format=json (returned directly) and format=script
          (fed to the script generator to produce a standalone .py file).
    """
    png:  bytes
    data: dict


def nan_for_none(values: list) -> list:
    """
    Map None to NaN for matplotlib calls that reject None (errorbar's yerr).

    Series keep None so the JSON payload stays valid; this is only for drawing,
    where NaN means "no bar here" (e.g. the CI of a single-sample slot).
    """
    return [float("nan") if v is None else v for v in values]
