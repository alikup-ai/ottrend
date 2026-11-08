"""The derived queries — the part that exists because no historian has it."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from ottrend import (
    Aggregate,
    MemoryTrend,
    Quality,
    Sample,
    TrendUnavailable,
    UnknownTag,
    coverage,
    drift_since,
    max_rate,
    was_outside,
)

T0 = datetime(2026, 11, 7, 12, 0, tzinfo=timezone.utc)
SEC = timedelta(seconds=1)


def series(*pairs: tuple[int, float], quality: Quality = Quality.GOOD):
    return [Sample(T0 + s * SEC, v, quality) for s, v in pairs]


# --- max_rate ---------------------------------------------------------------
async def test_max_rate_finds_the_fastest_bucket():
    # Flat, then a 30-unit jump inside one 10 s bucket.
    t = MemoryTrend({"T": series(
        (0, 50.0), (5, 50.5), (10, 51.0), (15, 51.2),
        (20, 51.0), (25, 81.0),          # the jump
        (30, 81.2), (35, 81.1),
    )})
    r = await max_rate(t, "T", T0, T0 + 40 * SEC, bucket=timedelta(seconds=10))

    assert r.emulated is True
    assert r.bucket_size == timedelta(seconds=10)
    # range inside the [20, 30) bucket is 30 units over 10 s
    assert r.max_rate_per_second == pytest.approx(3.0)
    assert r.at.start == T0 + 20 * SEC


async def test_rate_is_expressed_over_the_policy_interval():
    """Policies say "5 per 60 s", so the result has to speak that language."""
    t = MemoryTrend({"T": series((0, 10.0), (30, 20.0))})
    r = await max_rate(t, "T", T0, T0 + 60 * SEC, bucket=timedelta(seconds=60))
    assert r.per(timedelta(seconds=60)) == pytest.approx(10.0)


async def test_a_finer_bucket_gives_a_sharper_answer():
    """A smaller bucket must not make a fast move look slower.

    This is the regression for the first implementation, which measured only
    `range` inside each bucket. Once the bucket is finer than the historian's
    sample interval every bucket holds one sample, every range collapses to
    zero, and the reported rate fell to zero as the bucket shrank — exactly
    backwards for a safety check. The adjacent-bucket term fixes it.
    """
    spike = MemoryTrend({"T": series((0, 50.0), (3, 90.0), (6, 50.0), (9, 50.0))})
    coarse = await max_rate(spike, "T", T0, T0 + 10 * SEC, bucket=timedelta(seconds=10))
    fine = await max_rate(spike, "T", T0, T0 + 10 * SEC, bucket=timedelta(seconds=2))
    assert fine.max_rate_per_second > coarse.max_rate_per_second
    assert fine.max_rate_per_second > 0.0


async def test_rate_is_a_lower_bound_on_the_truth():
    """Every reported rate really happened; the true peak may be higher.

    The contract is "use it to refuse, not to permit", and that only holds if
    the number is never an overestimate.
    """
    # 40 units of real movement between t=0 and t=3 -> true peak >= 13.3/s
    t = MemoryTrend({"T": series((0, 50.0), (3, 90.0))})
    r = await max_rate(t, "T", T0, T0 + 10 * SEC, bucket=timedelta(seconds=10))
    true_peak = 40.0 / 3.0
    assert r.max_rate_per_second <= true_peak + 1e-9


# --- quality ----------------------------------------------------------------
async def test_mostly_bad_data_refuses_to_answer():
    """An envelope decision on bad data is worse than none: it looks like one."""
    t = MemoryTrend({"T": series((0, 50.0), (5, 99.0), quality=Quality.BAD)})
    with pytest.raises(TrendUnavailable, match="good data"):
        await max_rate(t, "T", T0, T0 + 10 * SEC, bucket=timedelta(seconds=10))


async def test_good_fraction_is_reported_with_the_answer():
    t = MemoryTrend({"T": [
        Sample(T0, 50.0, Quality.GOOD),
        Sample(T0 + 2 * SEC, 55.0, Quality.GOOD),
        Sample(T0 + 4 * SEC, 1e9, Quality.BAD),   # excluded from the aggregate
    ]})
    r = await max_rate(t, "T", T0, T0 + 10 * SEC, bucket=timedelta(seconds=10),
                       min_good_fraction=0.5)
    # the bad spike must not become the answer
    assert r.max_rate_per_second == pytest.approx(0.5)
    assert r.good_fraction == pytest.approx(2 / 3)


# --- fail closed ------------------------------------------------------------
async def test_unreachable_historian_raises_rather_than_defaults():
    t = MemoryTrend({"T": series((0, 50.0))})
    t.set_down(True)
    with pytest.raises(TrendUnavailable):
        await max_rate(t, "T", T0, T0 + 10 * SEC, bucket=timedelta(seconds=5))


async def test_unknown_tag_is_distinguishable_from_an_outage():
    """A caller gating a write needs to tell "no such tag" from "no answer"."""
    t = MemoryTrend({"T": series((0, 50.0))})
    with pytest.raises(UnknownTag):
        await t.value_at("Nope", T0)


async def test_empty_window_refuses_rather_than_returning_zero():
    t = MemoryTrend({"T": series((0, 50.0))})
    with pytest.raises(TrendUnavailable):
        await max_rate(t, "T", T0 + 100 * SEC, T0 + 200 * SEC,
                       bucket=timedelta(seconds=10))


# --- drift ------------------------------------------------------------------
async def test_drift_since_against_a_proposed_value():
    """The gateway case: judge a value that has not been written yet."""
    t = MemoryTrend({"T": series((0, 50.0), (30, 52.0))})
    assert await drift_since(t, "T", T0, now_value=61.0) == pytest.approx(11.0)


async def test_value_at_returns_the_held_value_not_an_interpolation():
    t = MemoryTrend({"T": series((0, 50.0), (60, 80.0))})
    held = await t.value_at("T", T0 + 30 * SEC)
    assert held is not None and held.value == 50.0


async def test_value_at_before_the_series_is_none_not_zero():
    t = MemoryTrend({"T": series((10, 50.0))})
    assert await t.value_at("T", T0) is None


# --- excursions -------------------------------------------------------------
async def test_was_outside_detects_an_excursion_inside_a_bucket():
    t = MemoryTrend({"T": series((0, 50.0), (3, 95.0), (6, 50.0))})
    assert await was_outside(t, "T", T0, T0 + 10 * SEC, lo=40, hi=80,
                             bucket=timedelta(seconds=10)) is True


async def test_was_outside_is_false_when_the_tag_stayed_in_band():
    t = MemoryTrend({"T": series((0, 50.0), (3, 60.0), (6, 55.0))})
    assert await was_outside(t, "T", T0, T0 + 10 * SEC, lo=40, hi=80,
                             bucket=timedelta(seconds=5)) is False


# --- capabilities -----------------------------------------------------------
async def test_capabilities_are_declared_without_io():
    """Callers budget round trips off this, so it must not touch the network."""
    t = MemoryTrend()
    t.set_down(True)
    caps = t.capabilities()           # must not raise despite being "down"
    assert caps.supports(Aggregate.RANGE)
    assert caps.arbitrary_timestamps is True


async def test_coverage_of_an_empty_window_is_zero():
    assert coverage([]) == 0.0
