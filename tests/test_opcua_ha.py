"""The OPC UA HA adapter, against a scripted session.

Two kinds of test live here.

The first kind checks **the request we send**, because that is where an OPC UA
adapter actually goes wrong: a window whose end is exclusive when the caller
said inclusive, a limit applied after the response instead of at the
historian, a continuation point abandoned without being released. None of that
shows up in a happy-path integration test, and all of it shows up in
production.

The second kind checks **the answer we build**, by driving the adapter from a
session backed by :class:`~ottrend.adapters.memory.MemoryTrend` — the
reference semantics — and asserting the derived queries come out identical
over the wire types. The README claims that where a real adapter and the
in-memory one disagree the adapter has a bug; this is that claim as a test.

Live tests against a real ``opc.tcp://`` server are in
``test_opcua_ha_live.py``.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

pytest.importorskip("asyncua")

from asyncua import ua  # noqa: E402

from ottrend import (  # noqa: E402
    Aggregate,
    MemoryTrend,
    Quality,
    Sample,
    TrendUnavailable,
    UnknownTag,
    UnsupportedQuery,
    max_rate,
    was_outside,
)
from ottrend.adapters.opcua_ha import OpcUaHistoryTrend  # noqa: E402

T0 = datetime(2026, 11, 7, 12, 0, tzinfo=timezone.utc)
SEC = timedelta(seconds=1)
TAG = "ns=2;s=TIC101.PV"
TICK = timedelta(microseconds=1)

GOOD = ua.StatusCode(ua.StatusCodes.Good)
BAD_NO_DATA = ua.StatusCode(ua.StatusCodes.BadNoData)
UNCERTAIN = ua.StatusCode(ua.StatusCodes.UncertainDataSubNormal)


# --- scripted session -------------------------------------------------------
def dv(ts: datetime | None, value: float | None, status: ua.StatusCode = GOOD):
    variant = ua.Variant(None) if value is None else ua.Variant(value, ua.VariantType.Double)
    return ua.DataValue(Value=variant, StatusCode=status, SourceTimestamp=ts)


def history(values, *, status=GOOD, cont=None):
    return ua.HistoryReadResult(
        StatusCode=status,
        ContinuationPoint=cont,
        HistoryData=ua.HistoryData(DataValues=list(values)),
    )


class FakeSession:
    """Returns scripted responses and remembers every request."""

    def __init__(self, *, history_results=None, read_results=None, browse_results=None):
        self.history_calls: list[ua.HistoryReadParameters] = []
        self.read_calls: list[ua.ReadParameters] = []
        self.browse_calls: list[ua.BrowseParameters] = []
        self._history = list(history_results or [])
        self._read = list(read_results or [])
        self._browse = list(browse_results or [])

    async def history_read(self, params):
        self.history_calls.append(params)
        if params.ReleaseContinuationPoints:
            return [history([])]
        if not self._history:
            raise AssertionError("adapter made an unscripted history_read")
        return self._history.pop(0)

    async def read(self, params):
        self.read_calls.append(params)
        if not self._read:
            raise AssertionError("adapter made an unscripted read")
        return self._read.pop(0)

    async def browse(self, params):
        self.browse_calls.append(params)
        if not self._browse:
            raise AssertionError("adapter made an unscripted browse")
        return self._browse.pop(0)


def details_of(params) -> object:
    return params.HistoryReadDetails


# --- value_at: the request --------------------------------------------------
async def test_value_at_asks_for_the_held_value_not_an_interpolation():
    """A reverse raw read of one value, not HistoryReadProcessed/Interpolative.

    Interpolation answers with a value the process never held. The design
    sketch said Interpolative; the reference semantics says "last value at or
    before", and the reference semantics wins.
    """
    session = FakeSession(history_results=[[history([dv(T0 + 20 * SEC, 51.0)])]])
    provider = OpcUaHistoryTrend(session, lookback=timedelta(hours=1))

    sample = await provider.value_at(TAG, T0 + 22 * SEC)

    assert sample is not None and sample.value == 51.0
    assert sample.ts == T0 + 20 * SEC
    assert sample.quality is Quality.GOOD

    det = details_of(session.history_calls[0])
    assert isinstance(det, ua.ReadRawModifiedDetails)
    assert det.StartTime > det.EndTime, "a reverse range is what makes it newest-first"
    assert det.NumValuesPerNode == 1, "one value, not a window dump"
    assert det.IsReadModified is False


async def test_value_at_includes_a_sample_recorded_exactly_at_the_instant():
    """Part 11 ranges exclude the far bound; ottrend's `when` is inclusive."""
    session = FakeSession(history_results=[[history([dv(T0 + 10 * SEC, 70.0)])]])
    provider = OpcUaHistoryTrend(session)

    sample = await provider.value_at(TAG, T0 + 10 * SEC)

    assert sample is not None and sample.value == 70.0
    assert details_of(session.history_calls[0]).StartTime == T0 + 10 * SEC + TICK


async def test_value_at_search_window_is_bounded_by_lookback():
    """No unbounded scan: the one deliberate divergence from the reference."""
    session = FakeSession(history_results=[[history([])]], read_results=[[]])
    provider = OpcUaHistoryTrend(session, lookback=timedelta(days=2))

    await provider.value_at(TAG, T0)

    assert details_of(session.history_calls[0]).EndTime == T0 - timedelta(days=2)


async def test_value_at_never_returns_a_value_from_after_the_instant():
    """A server that ignores the reverse-order hint must not corrupt the answer."""
    session = FakeSession(
        history_results=[[history([dv(T0 + 5 * SEC, 50.0), dv(T0 + 40 * SEC, 99.0)])]]
    )
    provider = OpcUaHistoryTrend(session)

    sample = await provider.value_at(TAG, T0 + 10 * SEC)

    assert sample is not None and sample.value == 50.0


# --- value_at / raw: "no such tag" vs "no data" -----------------------------
async def test_an_empty_answer_is_checked_against_the_node_existing():
    """asyncua's server answers Good+empty for a node it does not have.

    A caller gating a write has to tell "there is no such tag" from "I could
    not find out", so an empty read costs one attribute read to settle it.
    """
    session = FakeSession(
        history_results=[[history([])]],
        read_results=[[ua.DataValue(StatusCode=ua.StatusCode(ua.StatusCodes.BadNodeIdUnknown))]],
    )
    provider = OpcUaHistoryTrend(session)

    with pytest.raises(UnknownTag):
        await provider.value_at(TAG, T0)

    assert session.read_calls[0].NodesToRead[0].AttributeId == ua.AttributeIds.NodeClass


async def test_an_empty_answer_for_a_tag_that_exists_is_none_not_an_error():
    session = FakeSession(
        history_results=[[history([])]],
        read_results=[[ua.DataValue(Value=ua.Variant(2), StatusCode=GOOD)]],
    )
    provider = OpcUaHistoryTrend(session)

    assert await provider.value_at(TAG, T0) is None


async def test_the_existence_check_can_be_switched_off():
    session = FakeSession(history_results=[[history([])]])
    provider = OpcUaHistoryTrend(session, verify_unknown_tags=False)

    assert await provider.value_at(TAG, T0) is None
    assert session.read_calls == [], "the extra round trip was not asked for"


async def test_a_tag_that_is_not_a_nodeid_is_an_unknown_tag():
    provider = OpcUaHistoryTrend(FakeSession())
    with pytest.raises(UnknownTag, match="NodeId"):
        await provider.value_at("TIC101.PV", T0)


# --- raw --------------------------------------------------------------------
async def test_raw_applies_the_limit_at_the_historian():
    """Truncating locally means the pull already happened. That is the bug."""
    session = FakeSession(
        history_results=[[history([dv(T0 + i * SEC, 50.0 + i) for i in range(3)])]]
    )
    provider = OpcUaHistoryTrend(session)

    samples = await provider.raw(TAG, T0, T0 + 60 * SEC, limit=3)

    assert [s.value for s in samples] == [50.0, 51.0, 52.0]
    assert details_of(session.history_calls[0]).NumValuesPerNode == 3


async def test_raw_window_is_inclusive_at_both_ends():
    end = T0 + 10 * SEC
    session = FakeSession(history_results=[[history([dv(end, 55.0)])]])
    provider = OpcUaHistoryTrend(session)

    samples = await provider.raw(TAG, T0, end)

    assert [s.ts for s in samples] == [end]
    assert details_of(session.history_calls[0]).EndTime == end + TICK


async def test_raw_follows_continuation_points_to_the_end_of_the_window():
    session = FakeSession(
        history_results=[
            [history([dv(T0, 50.0)], cont=b"more")],
            [history([dv(T0 + SEC, 51.0)])],
        ]
    )
    provider = OpcUaHistoryTrend(session)

    samples = await provider.raw(TAG, T0, T0 + 60 * SEC)

    assert [s.value for s in samples] == [50.0, 51.0]
    assert session.history_calls[1].NodesToRead[0].ContinuationPoint == b"more"


async def test_raw_refuses_to_return_a_silently_truncated_series():
    """Three outcomes: all of it, an error, or a short answer that looks whole.

    The third is the dangerous one, so an unbounded read that hits the ceiling
    raises — and hands the continuation point back on the way out.
    """
    session = FakeSession(
        history_results=[
            [history([dv(T0, 50.0), dv(T0 + SEC, 51.0)], cont=b"more")],
        ]
    )
    provider = OpcUaHistoryTrend(session, max_raw_samples=2)

    with pytest.raises(TrendUnavailable, match="more than 2 raw samples"):
        await provider.raw(TAG, T0, T0 + 600 * SEC)

    released = [c for c in session.history_calls if c.ReleaseContinuationPoints]
    assert released and released[0].NodesToRead[0].ContinuationPoint == b"more"


async def test_raw_with_a_limit_stops_at_the_limit_without_raising():
    session = FakeSession(
        history_results=[[history([dv(T0, 50.0), dv(T0 + SEC, 51.0)], cont=b"more")]]
    )
    provider = OpcUaHistoryTrend(session)

    samples = await provider.raw(TAG, T0, T0 + 600 * SEC, limit=2)

    assert len(samples) == 2
    assert any(c.ReleaseContinuationPoints for c in session.history_calls)


# --- buckets ----------------------------------------------------------------
def processed(values, percents=None, *, status=GOOD, pct_status=GOOD):
    results = [history(values, status=status)]
    if percents is not None:
        results.append(history(percents, status=pct_status))
    return results


async def test_buckets_get_values_and_quality_in_one_round_trip():
    """AggregateType is positional against NodesToRead — so send the node twice.

    This is the whole reason `buckets` costs one request rather than two: the
    aggregate and its PercentGood ride in the same HistoryRead.
    """
    session = FakeSession(
        history_results=[
            processed(
                [dv(T0, 50.0), dv(T0 + 10 * SEC, 81.0)],
                [dv(T0, 100.0), dv(T0 + 10 * SEC, 100.0)],
            )
        ]
    )
    provider = OpcUaHistoryTrend(session)

    buckets = await provider.buckets(
        TAG, T0, T0 + 20 * SEC, every=timedelta(seconds=10), agg=Aggregate.MIN
    )

    assert len(session.history_calls) == 1
    params = session.history_calls[0]
    assert len(params.NodesToRead) == 2
    det = details_of(params)
    assert isinstance(det, ua.ReadProcessedDetails)
    assert det.ProcessingInterval == 10_000.0, "ProcessingInterval is milliseconds"
    assert det.AggregateType == [
        ua.NodeId(ua.ObjectIds.AggregateFunction_Minimum),
        ua.NodeId(ua.ObjectIds.AggregateFunction_PercentGood),
    ]
    assert [b.value for b in buckets] == [50.0, 81.0]
    assert [b.good_fraction for b in buckets] == [1.0, 1.0]


async def test_percent_good_becomes_the_good_fraction():
    session = FakeSession(
        history_results=[processed([dv(T0, 50.0)], [dv(T0, 62.5)])]
    )
    provider = OpcUaHistoryTrend(session)

    buckets = await provider.buckets(
        TAG, T0, T0 + 10 * SEC, every=timedelta(seconds=10), agg=Aggregate.MAX
    )

    assert buckets[0].good_fraction == pytest.approx(0.625)


async def test_without_percent_good_the_fallback_is_pessimistic():
    """"Unknown" must not round up to "fine" in a safety check.

    A server that cannot do PercentGood leaves the real fraction unknown, so an
    uncertain interval reports no good data and the derived queries refuse
    rather than answer off it.
    """
    session = FakeSession(
        history_results=[
            processed(
                [dv(T0, 50.0), dv(T0 + 10 * SEC, 60.0, UNCERTAIN)],
                [],
                pct_status=ua.StatusCode(ua.StatusCodes.BadAggregateNotSupported),
            )
        ]
    )
    provider = OpcUaHistoryTrend(session)

    buckets = await provider.buckets(
        TAG, T0, T0 + 20 * SEC, every=timedelta(seconds=10), agg=Aggregate.MIN
    )

    assert [b.good_fraction for b in buckets] == [1.0, 0.0]
    assert [b.value for b in buckets] == [50.0, 60.0]


async def test_historian_info_bits_do_not_change_the_quality():
    """A good value carries historian bits in the low half of the StatusCode."""
    calculated = ua.StatusCode(ua.StatusCodes.Good | 0x1)  # HistorianBits: Calculated
    session = FakeSession(history_results=[processed([dv(T0, 50.0, calculated)], [])])
    provider = OpcUaHistoryTrend(session)

    buckets = await provider.buckets(
        TAG, T0, T0 + 10 * SEC, every=timedelta(seconds=10), agg=Aggregate.AVG
    )

    assert buckets[0].value == 50.0
    assert buckets[0].good_fraction == 1.0


async def test_an_empty_interval_is_none_not_zero():
    """Servers omit trailing and empty intervals; the grid is built locally.

    The shape has to match the in-memory reference: one bucket per interval,
    `value=None` where there was nothing to aggregate.
    """
    session = FakeSession(
        history_results=[
            processed(
                [dv(T0, 50.0), dv(T0 + 10 * SEC, None, BAD_NO_DATA)],
                [dv(T0, 100.0), dv(T0 + 10 * SEC, 0.0)],
            )
        ]
    )
    provider = OpcUaHistoryTrend(session)

    buckets = await provider.buckets(
        TAG, T0, T0 + 30 * SEC, every=timedelta(seconds=10), agg=Aggregate.MIN
    )

    assert len(buckets) == 3, "the interval the server left out is still an interval"
    assert [b.value for b in buckets] == [50.0, None, None]
    assert [b.good_fraction for b in buckets] == [1.0, 0.0, 0.0]
    assert buckets[-1].end == T0 + 30 * SEC


async def test_a_partial_last_bucket_is_clamped_to_the_window():
    session = FakeSession(history_results=[processed([dv(T0, 50.0)], [dv(T0, 100.0)])])
    provider = OpcUaHistoryTrend(session)

    buckets = await provider.buckets(
        TAG, T0, T0 + 15 * SEC, every=timedelta(seconds=10), agg=Aggregate.MIN
    )

    assert [(b.start, b.end) for b in buckets] == [
        (T0, T0 + 10 * SEC),
        (T0 + 10 * SEC, T0 + 15 * SEC),
    ]


async def test_intervals_on_a_different_grid_are_refused():
    """A server answering on its own grid is not answering the question."""
    session = FakeSession(
        history_results=[
            processed([dv(T0 + 3 * SEC, 50.0), dv(T0 + 13 * SEC, 51.0), dv(T0 + 23 * SEC, 52.0)], [])
        ]
    )
    provider = OpcUaHistoryTrend(session)

    with pytest.raises(TrendUnavailable, match="do not align"):
        await provider.buckets(
            TAG, T0, T0 + 20 * SEC, every=timedelta(seconds=10), agg=Aggregate.MIN
        )


# --- error mapping ----------------------------------------------------------
@pytest.mark.parametrize(
    "code, expected",
    [
        (ua.StatusCodes.BadNotImplemented, UnsupportedQuery),
        (ua.StatusCodes.BadHistoryOperationUnsupported, UnsupportedQuery),
        (ua.StatusCodes.BadAggregateNotSupported, UnsupportedQuery),
        (ua.StatusCodes.BadNodeIdUnknown, UnknownTag),
        (ua.StatusCodes.BadTimeout, TrendUnavailable),
        (ua.StatusCodes.BadUserAccessDenied, TrendUnavailable),
    ],
)
async def test_a_failed_read_raises_the_right_kind_and_never_defaults(code, expected):
    session = FakeSession(history_results=[[history([], status=ua.StatusCode(code))]])
    provider = OpcUaHistoryTrend(session)

    with pytest.raises(expected):
        await provider.raw(TAG, T0, T0 + 10 * SEC)


async def test_a_good_status_with_no_body_is_not_mistaken_for_no_data():
    session = FakeSession(
        history_results=[[ua.HistoryReadResult(StatusCode=GOOD, HistoryData=None)]]
    )
    provider = OpcUaHistoryTrend(session)

    with pytest.raises(TrendUnavailable, match="no readable history body"):
        await provider.raw(TAG, T0, T0 + 10 * SEC)


async def test_a_non_numeric_tag_is_refused_rather_than_coerced():
    text = ua.DataValue(
        Value=ua.Variant("RUNNING", ua.VariantType.String),
        StatusCode=GOOD,
        SourceTimestamp=T0,
    )
    session = FakeSession(history_results=[[history([text])]])
    provider = OpcUaHistoryTrend(session)

    with pytest.raises(UnsupportedQuery, match="not a number"):
        await provider.raw(TAG, T0, T0 + 10 * SEC)


async def test_a_naive_datetime_is_refused():
    provider = OpcUaHistoryTrend(FakeSession())
    with pytest.raises(ValueError, match="timezone-aware"):
        await provider.value_at(TAG, datetime(2026, 11, 7, 12, 0))


# --- capabilities -----------------------------------------------------------
class ExplodingSession:
    """Any I/O at all is a test failure."""

    async def history_read(self, params):
        raise AssertionError("capabilities() must not do I/O")

    async def read(self, params):
        raise AssertionError("capabilities() must not do I/O")

    async def browse(self, params):
        raise AssertionError("capabilities() must not do I/O")


async def test_capabilities_do_no_io():
    """Callers budget round trips off this, so it cannot cost one."""
    caps = OpcUaHistoryTrend(ExplodingSession()).capabilities()
    assert caps.name == "opcua-ha"
    assert caps.supports(Aggregate.RANGE)
    assert caps.arbitrary_timestamps is True


def reference(node_id: int):
    return ua.ReferenceDescription(NodeId=ua.ExpandedNodeId(node_id))


async def test_discovery_narrows_capabilities_to_what_the_server_has():
    """Part 13 lets a server implement any subset of the 37 aggregates."""
    session = FakeSession(
        browse_results=[
            [
                ua.BrowseResult(
                    StatusCode=GOOD,
                    References=[
                        reference(ua.ObjectIds.AggregateFunction_Minimum),
                        reference(ua.ObjectIds.AggregateFunction_Maximum),
                        reference(ua.ObjectIds.AggregateFunction_Interpolative),
                    ],
                )
            ]
        ],
        read_results=[
            [
                ua.DataValue(Value=ua.Variant(5000, ua.VariantType.UInt32), StatusCode=GOOD),
                ua.DataValue(Value=ua.Variant(True, ua.VariantType.Boolean), StatusCode=GOOD),
            ]
        ],
    )
    provider = OpcUaHistoryTrend(session)

    caps = await provider.discover_capabilities()

    assert caps.native_aggregates == frozenset({Aggregate.MIN, Aggregate.MAX})
    assert provider.max_return_data_values == 5000
    assert provider.data_history is True


async def test_a_discovered_gap_is_refused_without_a_round_trip():
    session = FakeSession(
        browse_results=[[ua.BrowseResult(StatusCode=GOOD, References=[])]],
        read_results=[[]],
    )
    provider = OpcUaHistoryTrend(session)
    await provider.discover_capabilities()

    with pytest.raises(UnsupportedQuery, match="does not implement"):
        await provider.buckets(
            TAG, T0, T0 + 10 * SEC, every=timedelta(seconds=10), agg=Aggregate.MIN
        )
    assert session.history_calls == []


async def test_a_server_with_no_data_history_says_so_before_the_first_read():
    session = FakeSession(
        browse_results=[[ua.BrowseResult(StatusCode=GOOD, References=[])]],
        read_results=[
            [
                ua.DataValue(StatusCode=ua.StatusCode(ua.StatusCodes.BadNodeIdUnknown)),
                ua.DataValue(Value=ua.Variant(False, ua.VariantType.Boolean), StatusCode=GOOD),
            ]
        ],
    )
    provider = OpcUaHistoryTrend(session)
    await provider.discover_capabilities()

    assert provider.data_history is False
    with pytest.raises(UnsupportedQuery, match="AccessHistoryDataCapability"):
        await provider.value_at(TAG, T0)
    assert session.history_calls == []


# --- the reference cross-check ----------------------------------------------
class MemoryBackedSession:
    """An OPC UA session whose answers come from the in-memory reference.

    Not a simulator of a historian — a translator. It takes the request the
    adapter really sends, answers it with :class:`MemoryTrend`, and encodes the
    result in OPC UA wire types. So a disagreement between the two sides is a
    bug in the adapter's translation, which is exactly what it is here to
    catch.
    """

    def __init__(self, memory: MemoryTrend) -> None:
        self._memory = memory

    @staticmethod
    def _tag(node_id) -> str:
        return node_id.Identifier

    async def history_read(self, params):
        det = params.HistoryReadDetails
        if params.ReleaseContinuationPoints:
            return [history([])]
        if isinstance(det, ua.ReadRawModifiedDetails):
            tag = self._tag(params.NodesToRead[0].NodeId)
            if det.StartTime > det.EndTime:  # reverse read: value_at
                sample = await self._memory.value_at(tag, det.StartTime - TICK)
                if sample is None or sample.ts < det.EndTime:
                    return [history([])]
                return [history([self._encode(sample)])]
            samples = await self._memory.raw(
                tag,
                det.StartTime,
                det.EndTime - TICK,
                limit=det.NumValuesPerNode or None,
            )
            return [history([self._encode(s) for s in samples])]
        if isinstance(det, ua.ReadProcessedDetails):
            tag = self._tag(params.NodesToRead[0].NodeId)
            every = timedelta(milliseconds=det.ProcessingInterval)
            out = []
            for node_agg in det.AggregateType:
                if node_agg.Identifier == ua.ObjectIds.AggregateFunction_PercentGood:
                    buckets = await self._memory.buckets(
                        tag, det.StartTime, det.EndTime, every=every, agg=Aggregate.COUNT
                    )
                    out.append(
                        history([dv(b.start, b.good_fraction * 100.0) for b in buckets])
                    )
                    continue
                agg = _AGG_BY_NODE[node_agg.Identifier]
                buckets = await self._memory.buckets(
                    tag, det.StartTime, det.EndTime, every=every, agg=agg
                )
                out.append(
                    history(
                        [
                            dv(b.start, b.value, GOOD if b.value is not None else BAD_NO_DATA)
                            for b in buckets
                        ]
                    )
                )
            return out
        raise AssertionError(f"unexpected details type {type(det).__name__}")

    async def read(self, params):
        return [ua.DataValue(Value=ua.Variant(2), StatusCode=GOOD)]

    async def browse(self, params):
        raise AssertionError("not used")

    @staticmethod
    def _encode(sample: Sample):
        status = {
            Quality.GOOD: GOOD,
            Quality.QUESTIONABLE: UNCERTAIN,
            Quality.BAD: ua.StatusCode(ua.StatusCodes.BadOutOfService),
        }[sample.quality]
        return dv(sample.ts, sample.value, status)


_AGG_BY_NODE = {
    ua.ObjectIds.AggregateFunction_Minimum: Aggregate.MIN,
    ua.ObjectIds.AggregateFunction_Maximum: Aggregate.MAX,
    ua.ObjectIds.AggregateFunction_Average: Aggregate.AVG,
    ua.ObjectIds.AggregateFunction_Range: Aggregate.RANGE,
    ua.ObjectIds.AggregateFunction_Count: Aggregate.COUNT,
    ua.ObjectIds.AggregateFunction_StandardDeviationSample: Aggregate.STDDEV,
}

_SERIES = [
    (0, 50.0), (5, 50.5), (10, 51.0), (15, 51.2),
    (20, 51.0), (25, 81.0), (30, 81.2), (35, 81.1),
]


def _both():
    memory = MemoryTrend({"T": [Sample(T0 + s * SEC, v) for s, v in _SERIES]})
    return memory, OpcUaHistoryTrend(MemoryBackedSession(memory))


async def test_buckets_match_the_in_memory_reference():
    memory, opcua = _both()
    for agg in Aggregate:
        mine = await opcua.buckets(
            "ns=2;s=T", T0, T0 + 40 * SEC, every=timedelta(seconds=10), agg=agg
        )
        theirs = await memory.buckets(
            "T", T0, T0 + 40 * SEC, every=timedelta(seconds=10), agg=agg
        )
        assert mine == theirs, f"{agg} differs from the reference"


async def test_max_rate_over_opcua_matches_the_in_memory_reference():
    """The derived queries are the point; they have to come out the same."""
    memory, opcua = _both()
    for bucket in (timedelta(seconds=2), timedelta(seconds=10), timedelta(seconds=20)):
        mine = await max_rate(opcua, "ns=2;s=T", T0, T0 + 40 * SEC, bucket=bucket)
        theirs = await max_rate(memory, "T", T0, T0 + 40 * SEC, bucket=bucket)
        assert mine.max_rate_per_second == pytest.approx(theirs.max_rate_per_second)
        assert mine.at.start == theirs.at.start
        assert mine.good_fraction == pytest.approx(theirs.good_fraction)


async def test_was_outside_over_opcua_matches_the_in_memory_reference():
    memory, opcua = _both()
    for lo, hi in ((40.0, 80.0), (40.0, 100.0), (60.0, 100.0)):
        mine = await was_outside(
            opcua, "ns=2;s=T", T0, T0 + 40 * SEC, lo=lo, hi=hi, bucket=timedelta(seconds=10)
        )
        theirs = await was_outside(
            memory, "T", T0, T0 + 40 * SEC, lo=lo, hi=hi, bucket=timedelta(seconds=10)
        )
        assert mine is theirs


async def test_value_at_matches_the_in_memory_reference():
    memory, opcua = _both()
    for offset in (0, 7, 12, 30, 39):
        when = T0 + offset * SEC
        mine = await opcua.value_at("ns=2;s=T", when)
        theirs = await memory.value_at("T", when)
        assert mine == theirs, f"disagreement at +{offset}s"


async def test_raw_matches_the_in_memory_reference():
    memory, opcua = _both()
    mine = await opcua.raw("ns=2;s=T", T0 + 5 * SEC, T0 + 25 * SEC)
    theirs = await memory.raw("T", T0 + 5 * SEC, T0 + 25 * SEC)
    assert mine == theirs
