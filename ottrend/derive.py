"""Queries nobody's historian answers natively, built on the three primitives.

The research behind this module, in one line: **PI Web API, Ignition and
OPC UA HA all compute min / max / range / average / count / stddev
server-side, and none of them computes a rate of change.** OPC UA Part 13
defines 37 standard aggregates and not one is a derivative; PI's summary types
are statistical; Ignition's aggregation modes are the same family.

So rate of change is emulated here, once, rather than in each adapter — and
the result says so, because a caller gating a physical write deserves to know
its baseline is an approximation whose sharpness equals the bucket size.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from ottrend.provider import TrendProvider, TrendUnavailable, require_value_at
from ottrend.types import Aggregate, Bucket, RateResult

#: Below this, an answer is refused rather than returned with a caveat.
#: An envelope decision taken on mostly-bad data is worse than no decision,
#: because it looks like one.
MIN_GOOD_FRACTION = 0.8


async def max_rate(
    provider: TrendProvider,
    tag: str,
    start: datetime,
    end: datetime,
    *,
    bucket: timedelta,
    min_good_fraction: float = MIN_GOOD_FRACTION,
) -> RateResult:
    """Fastest observed movement of ``tag`` over ``[start, end]``.

    **The result is a lower bound on the true maximum rate.** Read the
    direction of that carefully, because it decides how you may use this:

    * "too fast" is definitive — the movement really happened, so denying on
      it is sound.
    * "slow enough" is not — it means *nothing faster was visible at this
      bucket size, in the data the historian kept*. Permitting on it is only
      as good as those two things.

    So: **use this to refuse an action, not to justify one.**

    Two bounds are combined, because either alone has a blind spot:

    * ``range`` inside one bucket, over ``bucket`` — misses movement that
      straddles a bucket boundary.
    * the span across two adjacent buckets, over ``2 * bucket`` — misses
      nothing at a boundary, but is averaged over twice the time.

    The pair term is what makes a smaller ``bucket`` give a sharper answer.
    Without it, buckets finer than the historian's sample interval contain one
    sample each, every range collapses to zero, and the reported rate falls to
    zero as the bucket shrinks — precisely backwards.

    Neither term can recover movement the historian never stored. PI-style
    exception and compression deadbands discard samples at the source, so a
    spike shorter than the effective recording interval is invisible here at
    any bucket size. ``RateResult.bucket_size`` is reported so a caller can
    say how sharp the answer was; nothing can report what was never kept.

    Raises:
        TrendUnavailable: the window holds no usable data, or too little of it
            is good to answer honestly.
    """
    seconds = bucket.total_seconds()
    if seconds <= 0:
        raise ValueError("bucket must be a positive duration")

    mins = await provider.buckets(tag, start, end, every=bucket, agg=Aggregate.MIN)
    maxs = await provider.buckets(tag, start, end, every=bucket, agg=Aggregate.MAX)

    def usable(b: Bucket) -> bool:
        return b.value is not None and b.good_fraction >= min_good_fraction

    pairs = [
        (lo, hi) for lo, hi in zip(mins, maxs) if usable(lo) and usable(hi)
    ]
    if not pairs:
        raise TrendUnavailable(
            f"no bucket of {tag} in [{start.isoformat()}, {end.isoformat()}] "
            f"reached {min_good_fraction:.0%} good data"
        )

    best_rate = 0.0
    best_bucket = pairs[0][1]

    # within one bucket
    for lo, hi in pairs:
        rate = (float(hi.value) - float(lo.value)) / seconds  # type: ignore[arg-type]
        if rate > best_rate:
            best_rate, best_bucket = rate, hi

    # across two adjacent buckets — only for pairs that really are adjacent,
    # since a gap of unusable buckets between them spans more time than this
    # term assumes.
    for (lo_a, hi_a), (lo_b, hi_b) in zip(pairs, pairs[1:]):
        if hi_a.end != hi_b.start:
            continue
        span = max(float(hi_a.value), float(hi_b.value)) - min(  # type: ignore[arg-type]
            float(lo_a.value), float(lo_b.value)                 # type: ignore[arg-type]
        )
        rate = span / (2 * seconds)
        if rate > best_rate:
            best_rate, best_bucket = rate, hi_b

    return RateResult(
        max_rate_per_second=best_rate,
        at=best_bucket,
        bucket_size=bucket,
        good_fraction=sum(hi.good_fraction for _, hi in pairs) / len(pairs),
    )


async def drift_since(
    provider: TrendProvider,
    tag: str,
    since: datetime,
    *,
    now_value: float | None = None,
) -> float:
    """Signed change in ``tag`` from ``since`` until now.

    ``now_value`` lets a caller that already holds the live value — a gateway
    about to judge a write, for instance — skip a round trip and compare a
    *proposed* value against the historical baseline.
    """
    then = await require_value_at(provider, tag, since)
    if now_value is None:
        current = await require_value_at(provider, tag, datetime.now(since.tzinfo))
        now_value = current.value
    return now_value - then.value


async def was_outside(
    provider: TrendProvider,
    tag: str,
    start: datetime,
    end: datetime,
    *,
    lo: float,
    hi: float,
    bucket: timedelta,
) -> bool:
    """Did ``tag`` leave ``[lo, hi]`` at any point in the window?

    Two native aggregates rather than a raw dump: a bucket's min below ``lo``
    or max above ``hi`` is an excursion. Bucketing cannot miss one, because an
    excursion inside a bucket still moves that bucket's min or max.
    """
    mins = await provider.buckets(tag, start, end, every=bucket, agg=Aggregate.MIN)
    maxs = await provider.buckets(tag, start, end, every=bucket, agg=Aggregate.MAX)
    for b in mins:
        if b.value is not None and b.value < lo:
            return True
    for b in maxs:
        if b.value is not None and b.value > hi:
            return True
    return False


def coverage(buckets: list[Bucket]) -> float:
    """Share of the window covered by good data, for reporting alongside a
    decision. A number a human can argue with beats a silent assumption."""
    if not buckets:
        return 0.0
    return sum(b.good_fraction for b in buckets) / len(buckets)
