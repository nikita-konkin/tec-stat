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
