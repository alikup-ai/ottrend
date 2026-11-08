"""In-memory provider: the reference semantics, and what the tests run on.

This adapter is the definition of correct behaviour for the other three. When
a real adapter and this one disagree about what a bucket means at an interval
boundary, this one is right and the real one has a bug.

No dependencies, so the test suite and the demo run on a bare install.
"""

from __future__ import annotations

import bisect
from datetime import datetime, timedelta

from ottrend.provider import TrendUnavailable, UnknownTag, UnsupportedQuery
from ottrend.types import Aggregate, Bucket, Capabilities, Quality, Sample

_NATIVE = frozenset(Aggregate)


class MemoryTrend:
    """Samples held in a dict, aggregated on demand."""

    def __init__(self, series: dict[str, list[Sample]] | None = None) -> None:
        self._series: dict[str, list[Sample]] = {}
        for tag, samples in (series or {}).items():
            self._series[tag] = sorted(samples, key=lambda s: s.ts)
        self._down = False

    # --- test controls -----------------------------------------------------
    def put(self, tag: str, sample: Sample) -> None:
        series = self._series.setdefault(tag, [])
        bisect.insort(series, sample, key=lambda s: s.ts)

    def set_down(self, down: bool = True) -> None:
        """Simulate an unreachable historian, to exercise the fail-closed path."""
        self._down = down

    # --- provider ----------------------------------------------------------
    def capabilities(self) -> Capabilities:
        return Capabilities(
            name="memory",
            native_aggregates=_NATIVE,
            arbitrary_timestamps=True,
            bulk_tags=False,
            quality=True,
        )

    async def value_at(self, tag: str, when: datetime) -> Sample | None:
        series = self._get(tag)
        # Last sample at or before `when`: a historian reports the value the
        # process held, which is the previous recorded one, not an
        # interpolation towards a future the caller cannot have seen yet.
        idx = bisect.bisect_right([s.ts for s in series], when)
        return series[idx - 1] if idx else None

    async def raw(
        self, tag: str, start: datetime, end: datetime, *, limit: int | None = None
    ) -> list[Sample]:
        series = [s for s in self._get(tag) if start <= s.ts <= end]
        return series[:limit] if limit is not None else series

    async def buckets(
        self,
        tag: str,
        start: datetime,
        end: datetime,
        *,
        every: timedelta,
        agg: Aggregate,
    ) -> list[Bucket]:
        if agg not in _NATIVE:
            raise UnsupportedQuery(f"{agg} is not supported")
        if every.total_seconds() <= 0:
            raise ValueError("every must be a positive duration")
        series = self._get(tag)

        out: list[Bucket] = []
        edge = start
        while edge < end:
            stop = min(edge + every, end)
            # Half-open [edge, stop) so a sample on a boundary lands in exactly
            # one bucket. The closing bucket takes its right edge.
            inside = [s for s in series
                      if edge <= s.ts < stop or (stop == end and s.ts == end)]
            good = [s for s in inside if s.quality.usable]
            out.append(
                Bucket(
                    start=edge,
                    end=stop,
                    value=_apply(agg, [s.value for s in good]),
                    good_fraction=(len(good) / len(inside)) if inside else 0.0,
                )
            )
            edge = stop
        return out

    # --- internals ---------------------------------------------------------
    def _get(self, tag: str) -> list[Sample]:
        if self._down:
            raise TrendUnavailable("historian is down")
        if tag not in self._series:
            raise UnknownTag(tag)
        return self._series[tag]


def _apply(agg: Aggregate, values: list[float]) -> float | None:
    if not values:
        return None
    if agg is Aggregate.MIN:
        return min(values)
    if agg is Aggregate.MAX:
        return max(values)
    if agg is Aggregate.AVG:
        return sum(values) / len(values)
    if agg is Aggregate.RANGE:
        return max(values) - min(values)
    if agg is Aggregate.COUNT:
        return float(len(values))
    if agg is Aggregate.STDDEV:
        if len(values) < 2:
            return 0.0
        mean = sum(values) / len(values)
        var = sum((v - mean) ** 2 for v in values) / (len(values) - 1)
        return var**0.5
    raise UnsupportedQuery(str(agg))  # unreachable: guarded by the caller
