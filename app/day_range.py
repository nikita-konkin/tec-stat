"""
Day ranges shared by every range endpoint.

A range can be given two ways:
  year + doy_start + doy_end   — days of one year (the original form)
  date_start + date_end        — calendar dates, which may cross New Year

Both resolve to a DayRange, which lists the (year, doy) days to read and
places any day/hour on one continuous axis measured from the range start:
  concat_hours = (date - date_start).days * 24 + hours
Within one year this equals the original (doy - doy_start) * 24 + hours.
"""

from __future__ import annotations

import datetime
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Callable, Optional, TypeVar

from fastapi import HTTPException, Query

from app.config import settings

DayKey = tuple[int, int]  # (year, doy)
T = TypeVar("T")


@dataclass(frozen=True)
class DayRange:
    start: datetime.date
    end: datetime.date

    @classmethod
    def from_doys(cls, year: int, doy_start: int, doy_end: int) -> "DayRange":
        """Range over days of one year; raises ValueError for a day the year lacks."""
        return cls(_doy_to_date(year, doy_start), _doy_to_date(year, doy_end))

    @property
    def year(self) -> int:
        return self.start.year

    @property
    def doy_start(self) -> int:
        return self.start.timetuple().tm_yday

    @property
    def doy_end(self) -> int:
        """DOY of the last day, in that day's own year (smaller than doy_start across New Year)."""
        return self.end.timetuple().tm_yday

    @property
    def n_days(self) -> int:
        return (self.end - self.start).days + 1

    def days(self) -> list[DayKey]:
        """Every (year, doy) in the range, in order."""
        out = []
        for offset in range(self.n_days):
            day = self.start + datetime.timedelta(days=offset)
            out.append((day.year, day.timetuple().tm_yday))
        return out

    def concat_hours(self, year: int, doy: int, hours: float) -> float:
        """Hours from the start of the range to `hours` on day (year, doy)."""
        return float((_doy_to_date(year, doy) - self.start).days * 24 + hours)

    def response_fields(self) -> dict:
        """Range fields shared by the statistics responses."""
        return {
            "year": self.year, "doy_start": self.doy_start, "doy_end": self.doy_end,
            "date_start": self.start, "date_end": self.end,
        }

    def label(self) -> str:
        """Compact form for download file names."""
        if self.start.year == self.end.year:
            return f"{self.year}_{self.doy_start:03d}_{self.doy_end:03d}"
        return f"{self.start.isoformat()}_{self.end.isoformat()}"


def map_days(fn: Callable[[int, int], T], days: list[DayKey]) -> list[T]:
    """
    Apply fn(year, doy) to each day on a thread pool, results in day order.

    Day discovery is directory listing, which is I/O-bound, so threads make a
    year-long availability scan fit inside a proxy timeout.
    """
    with ThreadPoolExecutor(max_workers=settings.discovery_workers) as pool:
        return list(pool.map(lambda day: fn(*day), days))


def _doy_to_date(year: int, doy: int) -> datetime.date:
    day = datetime.date(year, 1, 1) + datetime.timedelta(days=doy - 1)
    if day.year != year:
        raise ValueError(f"{year} has no day {doy}")
    return day


def day_range_query(
    year: Optional[int] = Query(
        None, ge=2000, le=2100,
        description="Year of a DOY range; use with doy_start and doy_end",
    ),
    doy_start: Optional[int] = Query(None, ge=1, le=366, description="First day of year (with year)"),
    doy_end: Optional[int] = Query(None, ge=1, le=366, description="Last day of year (with year)"),
    date_start: Optional[datetime.date] = Query(
        None,
        description="First day as YYYY-MM-DD; use with date_end instead of year/doy_start/doy_end. "
                    "The range may cross New Year.",
    ),
    date_end: Optional[datetime.date] = Query(None, description="Last day as YYYY-MM-DD (with date_start)"),
) -> DayRange:
    """FastAPI dependency resolving either range form into a DayRange."""
    doy_form = (year, doy_start, doy_end)
    date_form = (date_start, date_end)

    if any(v is not None for v in date_form):
        if any(v is not None for v in doy_form):
            raise HTTPException(422, "Give either date_start/date_end or year/doy_start/doy_end, not both")
        if date_start is None or date_end is None:
            raise HTTPException(422, "date_start and date_end must be given together")
        if not (2000 <= date_start.year <= 2100 and 2000 <= date_end.year <= 2100):
            raise HTTPException(422, "Dates must fall in the years 2000-2100")
        if date_start > date_end:
            raise HTTPException(422, "date_start must be ≤ date_end")
        day_range = DayRange(date_start, date_end)
    else:
        if any(v is None for v in doy_form):
            raise HTTPException(422, "Provide year, doy_start and doy_end, or date_start and date_end")
        if doy_start > doy_end:
            raise HTTPException(422, "doy_start must be ≤ doy_end")
        try:
            day_range = DayRange.from_doys(year, doy_start, doy_end)
        except ValueError as exc:
            raise HTTPException(422, str(exc)) from exc

    if day_range.n_days > settings.max_range_days:
        raise HTTPException(
            422, f"Range spans {day_range.n_days} days; the limit is {settings.max_range_days}",
        )
    return day_range
