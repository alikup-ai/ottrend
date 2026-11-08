"""Core value types.

Two ideas carry most of the design:

**Quality is not optional.** Every historian reports whether a value is good,
bad or questionable, and an operating envelope judged on 10 % good data is
worse than no envelope at all — it looks like an answer. Quality therefore
travels with every sample and every bucket, and the derived queries in
:mod:`ottrend.derive` refuse to answer when it is too thin.

**A derived answer says how it was derived.** No historian computes rate of
change natively (see README), so `ottrend` emulates it from bucketed ranges.
A caller deciding whether to let a write through needs to know that, and needs
the bucket size, because the emulation is only as sharp as its buckets.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum


class Quality(Enum):
    """Coarse quality, mapped from each historian's own scheme."""

    GOOD = "good"
    QUESTIONABLE = "questionable"
    BAD = "bad"

    @property
    def usable(self) -> bool:
        return self is Quality.GOOD


class Aggregate(Enum):
    """Aggregates every supported historian computes server-side.

    Deliberately small. The rule for this enum: a member belongs here only if
    PI Web API, Ignition and OPC UA HA can all evaluate it without the client
    pulling raw samples. Anything else is a derived query, not an aggregate —
    see :mod:`ottrend.derive`.
    """

    MIN = "min"
    MAX = "max"
    AVG = "avg"
    RANGE = "range"
    COUNT = "count"
    STDDEV = "stddev"


@dataclass(frozen=True, slots=True)
class Sample:
    """One value at one instant."""

    ts: datetime
    value: float
    quality: Quality = Quality.GOOD


@dataclass(frozen=True, slots=True)
class Bucket:
    """One aggregate over one sub-interval of a window.

    ``good_fraction`` is the share of the interval covered by good data, which
    every target historian can report (PI ``PercentGood``, OPC UA
    ``PercentGood``, Ignition quality columns). ``value`` is ``None`` when the
    interval held nothing to aggregate — an empty interval is not a zero.
    """

    start: datetime
    end: datetime
    value: float | None
    good_fraction: float = 1.0

    @property
    def duration(self) -> timedelta:
        return self.end - self.start


@dataclass(frozen=True, slots=True)
class Capabilities:
    """What an adapter can do natively, so callers can budget round trips.

    otgate's rate check runs on the hot path of every write to a rate-limited
    tag. Whether that costs one request or a thousand raw samples is the
    difference between a usable gateway and an unusable one, so adapters have
    to declare it rather than let the caller find out in production.
    """

    name: str
    native_aggregates: frozenset[Aggregate]
    #: can return a value at an arbitrary instant without the client
    #: interpolating between raw samples
    arbitrary_timestamps: bool
    #: can answer for several tags in one round trip
    bulk_tags: bool
    #: reports per-sample or per-interval quality
    quality: bool

    def supports(self, agg: Aggregate) -> bool:
        return agg in self.native_aggregates


@dataclass(frozen=True, slots=True)
class RateResult:
    """The answer to "how fast did this tag move", and how it was reached.

    ``emulated`` is always true today and the field exists so it can stop
    being true: if a historian ever exposes a derivative aggregate, the same
    call should start reporting a native answer without the caller changing.
    """

    max_rate_per_second: float
    #: the bucket the maximum was observed in, for pointing a human at it
    at: Bucket
    bucket_size: timedelta
    good_fraction: float
    emulated: bool = True

    def per(self, interval: timedelta) -> float:
        """Rate expressed over ``interval`` — policies say "5 per 60 s"."""
        return self.max_rate_per_second * interval.total_seconds()
