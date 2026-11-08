"""The OPC UA HA adapter against a real ``opc.tcp://`` server.

These run over the actual wire protocol — an ``asyncua`` server started
in-process on a free port, with history written straight into its historian so
the timestamps are exact instead of whatever a subscription happened to see.
They are the reason the adapter can be trusted at all without vendor access:
every request in ``test_opcua_ha.py`` is checked against a fake, and these
prove the same requests are ones a real server accepts.

Two of them are about what asyncua's server *cannot* do, and they are the most
useful tests in the file:

* its historian implements raw reads only, so ``HistoryReadProcessed`` comes
  back ``BadNotImplemented`` — which is how we know ``buckets`` degrades into
  :class:`~ottrend.provider.UnsupportedQuery` rather than something that looks
  like data.
* it answers a history read for a node it has never heard of with ``Good`` and
  an empty result set, which is exactly the case the extra NodeClass read
  exists for. That behaviour is permitted by Part 11, so no adapter should
  assume otherwise.

Run just these with::

    pytest -m integration

Skipped automatically when ``asyncua`` is not installed.
"""

from __future__ import annotations

import asyncio
import socket
from datetime import datetime, timedelta, timezone

import pytest

pytest.importorskip("asyncua")

from asyncua import Server, ua  # noqa: E402

from ottrend import (  # noqa: E402
    Aggregate,
    Quality,
    TrendUnavailable,
    UnknownTag,
    UnsupportedQuery,
    max_rate,
)
from ottrend.adapters.opcua_ha import OpcUaHistoryTrend  # noqa: E402

pytestmark = pytest.mark.integration

T0 = datetime(2026, 11, 7, 12, 0, tzinfo=timezone.utc)
SEC = timedelta(seconds=1)
NAMESPACE_URI = "http://ottrend/test"
IDENTIFIER = "TIC101.PV"

#: Flat, then a jump: the same shape the derived-query tests use.
SERIES = [(0, 50.0), (5, 50.5), (10, 51.0), (20, 51.0), (25, 81.0), (30, 81.2)]


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture
async def endpoint():
    """A real OPC UA server with history, on a fresh port."""
    ep = f"opc.tcp://127.0.0.1:{_free_port()}/ottrend/test"
    server = Server()
    await server.init()
    server.set_endpoint(ep)
    idx = await server.register_namespace(NAMESPACE_URI)
    plant = await server.nodes.objects.add_object(idx, "Plant")
    node = await plant.add_variable(
        ua.NodeId(IDENTIFIER, idx), "PV", ua.Variant(50.0, ua.VariantType.Double)
    )
    await server.historize_node_data_change(node, period=None, count=1000)
    await server.start()
    # Written into the historian directly: a data-change subscription would
    # stamp the values with "now", and these tests are about exact instants.
    storage = server.iserver.history_manager.storage
    for offset, value in SERIES:
        await storage.save_node_value(
            node.nodeid,
            ua.DataValue(
                Value=ua.Variant(value, ua.VariantType.Double),
                StatusCode=ua.StatusCode(ua.StatusCodes.Good),
                SourceTimestamp=T0 + offset * SEC,
            ),
        )
    await asyncio.sleep(0.1)
    try:
        yield ep, f"ns={idx};s={IDENTIFIER}"
    finally:
        await server.stop()


# --- raw --------------------------------------------------------------------
async def test_raw_round_trips_the_stored_series(endpoint):
    ep, tag = endpoint
    async with OpcUaHistoryTrend.connect(ep, discover=False) as trend:
        samples = await trend.raw(tag, T0, T0 + 30 * SEC)

    assert [(s.ts, s.value) for s in samples] == [(T0 + o * SEC, v) for o, v in SERIES]
    assert all(s.quality is Quality.GOOD for s in samples)


async def test_raw_includes_a_sample_recorded_exactly_at_the_window_end(endpoint):
    """The inclusive end is ottrend's contract; Part 11 ranges are half-open."""
    ep, tag = endpoint
    async with OpcUaHistoryTrend.connect(ep, discover=False) as trend:
        samples = await trend.raw(tag, T0, T0 + 25 * SEC)

    assert samples[-1].ts == T0 + 25 * SEC
    assert samples[-1].value == 81.0


async def test_raw_limit_is_honoured_by_the_server(endpoint):
    ep, tag = endpoint
    async with OpcUaHistoryTrend.connect(ep, discover=False) as trend:
        samples = await trend.raw(tag, T0, T0 + 30 * SEC, limit=2)

    assert [s.value for s in samples] == [50.0, 50.5]


# --- value_at ---------------------------------------------------------------
async def test_value_at_returns_the_held_value_over_the_wire(endpoint):
    """Between 12:00:10 and 12:00:20 the plant sat at 51.0.

    An ``Interpolative`` read would answer with something between 51.0 and
    51.0's successor here — a value the process never held. This is the
    assertion that pins the mapping to a bounding raw read.
    """
    ep, tag = endpoint
    async with OpcUaHistoryTrend.connect(ep, discover=False) as trend:
        held = await trend.value_at(tag, T0 + 17 * SEC)
        at_a_sample = await trend.value_at(tag, T0 + 25 * SEC)

    assert held is not None and held.value == 51.0
    assert held.ts == T0 + 10 * SEC
    assert at_a_sample is not None and at_a_sample.value == 81.0


async def test_value_at_before_the_series_is_none_not_zero(endpoint):
    ep, tag = endpoint
    async with OpcUaHistoryTrend.connect(ep, discover=False) as trend:
        assert await trend.value_at(tag, T0 - 60 * SEC) is None


async def test_value_at_does_not_look_further_back_than_lookback(endpoint):
    """The bounded search window is real, not decoration."""
    ep, tag = endpoint
    async with OpcUaHistoryTrend.connect(
        ep, discover=False, lookback=timedelta(seconds=5)
    ) as trend:
        assert await trend.value_at(tag, T0 + 17 * SEC) is None
        reachable = await trend.value_at(tag, T0 + 12 * SEC)

    assert reachable is not None and reachable.value == 51.0


# --- "no such tag" ----------------------------------------------------------
async def test_an_unknown_node_is_an_unknown_tag_despite_a_good_status(endpoint):
    """asyncua answers Good+empty for a node it does not have.

    Part 11 permits that, so the adapter settles it with one NodeClass read
    instead of reporting "no data" for a tag that does not exist.
    """
    ep, _ = endpoint
    async with OpcUaHistoryTrend.connect(ep, discover=False) as trend:
        with pytest.raises(UnknownTag):
            await trend.value_at("ns=2;s=NoSuchTag", T0)
        with pytest.raises(UnknownTag):
            await trend.raw("ns=2;s=NoSuchTag", T0, T0 + 30 * SEC)


async def test_a_node_that_exists_but_is_not_historized_is_not_unknown(endpoint):
    """"Exists but keeps no history" is an empty answer, not UnknownTag."""
    ep, _ = endpoint
    async with OpcUaHistoryTrend.connect(ep, discover=False) as trend:
        # Server_ServerStatus_StartTime is in every server's address space and
        # is not historized here.
        tag = f"i={ua.ObjectIds.Server_ServerStatus_StartTime}"
        assert await trend.value_at(tag, T0) is None


# --- what this server cannot do ---------------------------------------------
async def test_processed_reads_degrade_into_unsupported_query(endpoint):
    """asyncua's historian does raw reads only: BadNotImplemented.

    The mapping matters more than the limitation: a caller gets
    UnsupportedQuery, which is a refusal, rather than an empty bucket list that
    would read as "nothing happened".
    """
    ep, tag = endpoint
    async with OpcUaHistoryTrend.connect(ep, discover=False) as trend:
        with pytest.raises(UnsupportedQuery):
            await trend.buckets(
                tag, T0, T0 + 30 * SEC, every=timedelta(seconds=10), agg=Aggregate.MIN
            )


async def test_a_derived_query_refuses_rather_than_guessing(endpoint):
    """max_rate needs the processed read; without it there is no answer to give."""
    ep, tag = endpoint
    async with OpcUaHistoryTrend.connect(ep, discover=False) as trend:
        with pytest.raises((UnsupportedQuery, TrendUnavailable)):
            await max_rate(trend, tag, T0, T0 + 30 * SEC, bucket=timedelta(seconds=10))


async def test_discovery_reports_this_server_has_no_native_aggregates(endpoint):
    """HistoryServerCapabilities/AggregateFunctions is empty here, and true.

    After discovery the adapter refuses the query locally, without a round trip
    — which is the point of making an adapter declare what it can do.
    """
    ep, tag = endpoint
    async with OpcUaHistoryTrend.connect(ep) as trend:  # discover=True
        caps = trend.capabilities()
        assert caps.name == "opcua-ha"
        assert caps.native_aggregates == frozenset()
        assert caps.quality is True
        with pytest.raises(UnsupportedQuery, match="does not implement"):
            await trend.buckets(
                tag, T0, T0 + 30 * SEC, every=timedelta(seconds=10), agg=Aggregate.MAX
            )
