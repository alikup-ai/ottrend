"""OPC UA Historical Access adapter.

Implemented against OPC UA Part 11 (Historical Access services) and Part 13
(aggregates), and exercised in CI against a real ``opc.tcp://`` server — see
``tests/test_opcua_ha_live.py``. This is the one adapter that can be finished
without vendor access, because the standard is public and ``asyncua`` speaks
it.

Mapping
-------

===========================  ====================================================
ottrend                      OPC UA HA
===========================  ====================================================
``value_at``                 ``HistoryReadRaw``, reverse range, one value
``raw``                      ``HistoryReadRaw`` (``NumValuesPerNode`` = limit)
``buckets``                  ``HistoryReadProcessed``, ``ProcessingInterval``
                             is the bucket, aggregate by NodeId:
``Aggregate.MIN/MAX/AVG``    ``Minimum`` / ``Maximum`` / ``Average``
``Aggregate.RANGE``          ``Range``
``Aggregate.COUNT``          ``Count``
``Aggregate.STDDEV``         ``StandardDeviationSample``
``good_fraction``            ``PercentGood`` in the same request
===========================  ====================================================

Four things that mapping table hides, all of them found while making it work
against a real server:

**value_at is a bounding raw read, not Interpolative.** The design sketch said
to use ``HistoryReadProcessed`` with the ``Interpolative`` aggregate, and that
is wrong. Interpolation invents a value the process never held: for a tag
recorded at 50 and then at 80 a minute later, ``Interpolative`` answers 65 in
the middle while the plant sat at 50 the whole time. The reference semantics
in :class:`~ottrend.adapters.memory.MemoryTrend` is the last value at or
before the instant, so this adapter asks for exactly that — ``StartTime``
later than ``EndTime``, which Part 11 defines as a reverse read, with
``NumValuesPerNode=1``.

**That reverse read is bounded by** ``lookback``. Part 11 allows a null
``StartTime``, which would mean "however far back you have to go", but servers
disagree about what a null bound implies (asyncua's own historian ignores the
*other* bound entirely when one is null), and an unbounded scan is the thing
this library refuses to do to a production historian. So the search window is
explicit and configurable, and a tag that has not been written in longer than
``lookback`` reads as ``None``. **This is the one deliberate divergence from
the in-memory reference**, which looks back forever.

**Quality comes back in the same round trip.** ``AggregateType`` is a list
matched positionally against ``NodesToRead``, so the same node can appear
twice under two different aggregates. ``buckets`` sends ``[agg, PercentGood]``
and gets the values and their good fraction in one request instead of two. A
server that does not implement ``PercentGood`` fails that half on its own, and
the per-interval StatusCode is used instead.

**"No such tag" has to be asked for separately.** Part 11 lets a server answer
a history read for a node it does not have with ``Good`` and an empty result
set, and asyncua's server does exactly that. ottrend's convention (borrowed
from otgate) is that a caller gating a physical write must be able to tell
"there is no such tag" from "I could not find out", so when a read comes back
empty this adapter spends one extra round trip reading the node's NodeClass
attribute: ``BadNodeIdUnknown`` there means
:class:`~ottrend.provider.UnknownTag`, anything else means the tag exists and
genuinely has no data. Pass ``verify_unknown_tags=False`` to skip it.

No derivative aggregate exists among Part 13's 37 standard functions —
``Delta`` is the net change across an interval, signed and blind to a round
trip inside it — so :func:`ottrend.derive.max_rate` stays emulated.

Tags are NodeId strings in the usual OPC UA spelling (``ns=2;s=TIC101.PV``,
``i=2256``), parsed by ``asyncua.ua.NodeId.from_string``. Timestamps must be
timezone-aware: a window read in the wrong zone answers about the wrong hour
and looks like an answer, so a naive datetime is rejected rather than assumed.
"""

from __future__ import annotations

import contextlib
from collections.abc import AsyncIterator, Iterable
from datetime import datetime, timedelta, timezone
from typing import Any, Protocol

try:
    from asyncua import ua
except ImportError as exc:  # pragma: no cover - exercised by the install docs
    raise ImportError(
        "the OPC UA HA adapter needs asyncua: pip install 'ottrend[opcua]'"
    ) from exc

from ottrend.provider import TrendUnavailable, UnknownTag, UnsupportedQuery
from ottrend.types import Aggregate, Bucket, Capabilities, Quality, Sample

__all__ = ["HistorySession", "OpcUaHistoryTrend"]

#: Smallest step a Python datetime can express. Used to turn ottrend's
#: inclusive window ends into the half-open ranges Part 11 defines.
_TICK = timedelta(microseconds=1)

#: ottrend aggregate -> Part 13 standard aggregate function NodeId.
#:
#: ``AVG`` maps to ``Average`` (arithmetic mean of the values in the interval)
#: rather than ``TimeAverage`` (time-weighted), because that is what the
#: in-memory reference computes. ``TimeAverage`` is the more physically
#: meaningful number for a process value and is worth exposing one day — but
#: not under the same name as something that means something else.
_AGGREGATE_NODE: dict[Aggregate, int] = {
    Aggregate.MIN: ua.ObjectIds.AggregateFunction_Minimum,
    Aggregate.MAX: ua.ObjectIds.AggregateFunction_Maximum,
    Aggregate.AVG: ua.ObjectIds.AggregateFunction_Average,
    Aggregate.RANGE: ua.ObjectIds.AggregateFunction_Range,
    Aggregate.COUNT: ua.ObjectIds.AggregateFunction_Count,
    Aggregate.STDDEV: ua.ObjectIds.AggregateFunction_StandardDeviationSample,
}

_NODE_AGGREGATE: dict[int, Aggregate] = {v: k for k, v in _AGGREGATE_NODE.items()}

_PERCENT_GOOD = ua.ObjectIds.AggregateFunction_PercentGood

#: Severity and info bits are separate: a historian marks a good value with
#: info bits in the low half of the StatusCode (raw / calculated / partial),
#: so a code is identified by its top 16 bits only.
_CODE_MASK = 0xFFFF0000


def _top_of(code: int) -> int:
    return code & _CODE_MASK


_UNKNOWN_TAG_CODES = frozenset(
    _top_of(c)
    for c in (
        ua.StatusCodes.BadNodeIdUnknown,
        ua.StatusCodes.BadNodeIdInvalid,
        ua.StatusCodes.BadNoEntryExists,
    )
)

_UNSUPPORTED_CODES = frozenset(
    _top_of(c)
    for c in (
        ua.StatusCodes.BadNotImplemented,
        ua.StatusCodes.BadNotSupported,
        ua.StatusCodes.BadHistoryOperationUnsupported,
        ua.StatusCodes.BadAggregateNotSupported,
        ua.StatusCodes.BadAggregateInvalidInputs,
        ua.StatusCodes.BadAggregateConfigurationRejected,
        ua.StatusCodes.BadBoundNotSupported,
    )
)


class HistorySession(Protocol):
    """The three OPC UA services this adapter uses.

    Narrower than asyncua's client on purpose: the ``uaclient`` of a connected
    ``asyncua.Client`` satisfies it, and so does a fake in a test — which is
    how the whole adapter is testable without a server. It is also the exact
    list of what has to be re-checked against a new asyncua major.
    """

    async def history_read(
        self, params: ua.HistoryReadParameters
    ) -> list[ua.HistoryReadResult]: ...

    async def read(self, params: ua.ReadParameters) -> list[ua.DataValue]: ...

    async def browse(self, params: ua.BrowseParameters) -> list[ua.BrowseResult]: ...


class OpcUaHistoryTrend:
    """OPC UA Historical Access provider. Read-only by construction.

    Args:
        session: anything satisfying :class:`HistorySession` — in practice the
            ``uaclient`` of a connected ``asyncua.Client``. Prefer
            :meth:`connect` or :meth:`from_client` over reaching for it.
        lookback: how far back :meth:`value_at` may search for the held value.
        max_raw_samples: ceiling on an unbounded :meth:`raw` read. Reaching it
            raises instead of returning a truncated series, because a silently
            short answer is the worst of the three possible outcomes.
        verify_unknown_tags: spend one extra round trip turning an empty
            answer into :class:`~ottrend.provider.UnknownTag` when the node
            really does not exist.
    """

    def __init__(
        self,
        session: HistorySession,
        *,
        lookback: timedelta = timedelta(days=7),
        max_raw_samples: int = 100_000,
        verify_unknown_tags: bool = True,
    ) -> None:
        if lookback.total_seconds() <= 0:
            raise ValueError("lookback must be a positive duration")
        if max_raw_samples <= 0:
            raise ValueError("max_raw_samples must be positive")
        self._session = session
        self.lookback = lookback
        self.max_raw_samples = max_raw_samples
        self.verify_unknown_tags = verify_unknown_tags
        #: Set by :meth:`discover_capabilities`. Until then capabilities are
        #: asserted rather than known, and nothing is refused up front.
        self._discovered: Capabilities | None = None
        #: The server's own ceiling on one response, when it declares one.
        self.max_return_data_values: int | None = None
        #: ``AccessHistoryDataCapability``, when the server declares it.
        self.data_history: bool | None = None

    # --- construction ------------------------------------------------------
    @classmethod
    def from_client(cls, client: Any, **kwargs: Any) -> OpcUaHistoryTrend:
        """Wrap a connected ``asyncua.Client``."""
        return cls(client.uaclient, **kwargs)

    @classmethod
    @contextlib.asynccontextmanager
    async def connect(
        cls,
        endpoint: str,
        *,
        timeout: float = 4.0,
        discover: bool = True,
        **kwargs: Any,
    ) -> AsyncIterator[OpcUaHistoryTrend]:
        """Connect to ``endpoint`` and yield a provider for that session.

        ``discover=True`` asks the server what it can actually do before the
        first read, which is the entire point of
        :class:`~ottrend.types.Capabilities` being honest.
        """
        from asyncua import Client

        client = Client(url=endpoint, timeout=timeout)
        async with client:
            provider = cls.from_client(client, **kwargs)
            if discover:
                await provider.discover_capabilities()
            yield provider

    # --- capabilities ------------------------------------------------------
    def capabilities(self) -> Capabilities:
        """What this adapter can do. Never does I/O — see the protocol.

        Before :meth:`discover_capabilities` has run, this is an assertion
        about a standard-conforming server rather than a fact about this one.
        """
        if self._discovered is not None:
            return self._discovered
        return Capabilities(
            name="opcua-ha",
            native_aggregates=frozenset(_AGGREGATE_NODE),
            arbitrary_timestamps=True,
            bulk_tags=True,
            quality=True,
        )

    async def discover_capabilities(self) -> Capabilities:
        """Narrow :meth:`capabilities` to what the server declares.

        Reads ``HistoryServerCapabilities``: the ``AggregateFunctions`` folder
        (Part 13 lets a server implement any subset of the 37 standard
        functions), plus ``AccessHistoryDataCapability`` and
        ``MaxReturnDataValues``. After this, a query the server cannot answer
        is refused without a round trip.
        """
        aggregates = await self._browse_aggregates()
        await self._read_history_limits()
        self._discovered = Capabilities(
            name="opcua-ha",
            native_aggregates=aggregates,
            arbitrary_timestamps=True,
            bulk_tags=True,
            quality=True,
        )
        return self._discovered

    async def _browse_aggregates(self) -> frozenset[Aggregate]:
        folder = ua.NodeId(ua.ObjectIds.HistoryServerCapabilities_AggregateFunctions)
        description = ua.BrowseDescription(
            NodeId=folder,
            BrowseDirection=ua.BrowseDirection.Forward,
            ReferenceTypeId=ua.NodeId(ua.ObjectIds.HierarchicalReferences),
            IncludeSubtypes=True,
            NodeClassMask=0,
            ResultMask=ua.BrowseResultMask.All,
        )
        params = ua.BrowseParameters(
            # An explicitly null view: asyncua's default ViewDescription stamps
            # "now" into Timestamp, and a server is entitled to reject a view
            # that has a timestamp but no ViewId.
            View=ua.ViewDescription(
                ViewId=ua.NodeId(), Timestamp=ua.get_win_epoch(), ViewVersion=0
            ),
            RequestedMaxReferencesPerNode=0,
            NodesToBrowse=[description],
        )
        results = await self._session.browse(params)
        if not results:
            raise TrendUnavailable("server returned no result for the capability browse")
        result = results[0]
        if _is_bad(result.StatusCode):
            raise TrendUnavailable(
                "could not read HistoryServerCapabilities/AggregateFunctions: "
                f"{result.StatusCode.name}"
            )
        found: set[Aggregate] = set()
        for ref in result.References or []:
            identifier = getattr(ref.NodeId, "Identifier", None)
            agg = _NODE_AGGREGATE.get(identifier)  # type: ignore[arg-type]
            if agg is not None:
                found.add(agg)
        return frozenset(found)

    async def _read_history_limits(self) -> None:
        """Best-effort: these two nodes are optional in the standard."""
        nodes = [
            ua.ReadValueId(
                NodeId=ua.NodeId(
                    ua.ObjectIds.HistoryServerCapabilities_MaxReturnDataValues
                ),
                AttributeId=ua.AttributeIds.Value,
            ),
            ua.ReadValueId(
                NodeId=ua.NodeId(
                    ua.ObjectIds.HistoryServerCapabilities_AccessHistoryDataCapability
                ),
                AttributeId=ua.AttributeIds.Value,
            ),
        ]
        params = ua.ReadParameters(
            MaxAge=0,
            TimestampsToReturn=ua.TimestampsToReturn.Neither,
            NodesToRead=nodes,
        )
        try:
            values = await self._session.read(params)
        except Exception:  # noqa: BLE001 - optional nodes, absence is not fatal
            return
        limit = _plain_value(values[0]) if len(values) > 0 else None
        access = _plain_value(values[1]) if len(values) > 1 else None
        if isinstance(limit, int) and not isinstance(limit, bool) and limit > 0:
            self.max_return_data_values = limit
        if isinstance(access, bool):
            self.data_history = access

    # --- primitives --------------------------------------------------------
    async def value_at(self, tag: str, when: datetime) -> Sample | None:
        _require_aware(when=when)
        self._require_data_history()
        node = _node_id(tag)
        details = ua.ReadRawModifiedDetails(
            IsReadModified=False,
            # Reverse range: Part 11 returns data newest-first when StartTime
            # is the later bound. The tick makes a sample recorded exactly at
            # `when` count, matching the in-memory reference.
            StartTime=when + _TICK,
            EndTime=when - self.lookback,
            NumValuesPerNode=1,
            ReturnBounds=False,
        )
        values, _ = await self._history_read(tag, node, details)
        # Belt and braces: a server that ignores the reverse-order hint could
        # hand back something after `when`, and a value from the future must
        # never be presented as the value a tag held.
        at_or_before = [dv for dv in values if _timestamp(dv, tag) <= when]
        if not at_or_before:
            await self._confirm_exists(tag, node)
            return None
        newest = max(at_or_before, key=lambda dv: _timestamp(dv, tag))
        return self._sample(newest, tag)

    async def raw(
        self,
        tag: str,
        start: datetime,
        end: datetime,
        *,
        limit: int | None = None,
    ) -> list[Sample]:
        _require_aware(start=start, end=end)
        if end < start:
            raise ValueError("end must not precede start")
        if limit is not None and limit <= 0:
            return []
        self._require_data_history()
        node = _node_id(tag)

        ceiling = limit if limit is not None else self.max_raw_samples
        collected: list[ua.DataValue] = []
        cont: bytes | None = None
        while True:
            details = ua.ReadRawModifiedDetails(
                IsReadModified=False,
                StartTime=start,
                EndTime=end + _TICK,  # ottrend windows are inclusive at both ends
                # The limit is applied at the historian, never by truncating a
                # response we have already paid for.
                NumValuesPerNode=ceiling - len(collected),
                ReturnBounds=False,
            )
            values, cont = await self._history_read(tag, node, details, cont=cont)
            collected.extend(values)
            if not cont:
                break
            if len(collected) >= ceiling:
                await self._release(node, cont)
                if limit is None:
                    raise TrendUnavailable(
                        f"{tag} has more than {ceiling} raw samples in "
                        f"[{start.isoformat()}, {end.isoformat()}]; pass limit= or "
                        "narrow the window rather than pulling the whole series"
                    )
                break

        samples = [self._sample(dv, tag) for dv in collected]
        samples = [s for s in samples if start <= s.ts <= end]
        samples.sort(key=lambda s: s.ts)
        if not samples:
            await self._confirm_exists(tag, node)
        return samples[:limit] if limit is not None else samples

    async def buckets(
        self,
        tag: str,
        start: datetime,
        end: datetime,
        *,
        every: timedelta,
        agg: Aggregate,
    ) -> list[Bucket]:
        _require_aware(start=start, end=end)
        if every.total_seconds() <= 0:
            raise ValueError("every must be a positive duration")
        if agg not in _AGGREGATE_NODE:
            raise UnsupportedQuery(f"{agg} has no standard OPC UA aggregate")
        if self._discovered is not None and not self._discovered.supports(agg):
            # Discovery already told us; no point spending a round trip to be
            # told the same thing by the server.
            raise UnsupportedQuery(
                f"server does not implement the {agg.value} aggregate "
                "(HistoryServerCapabilities/AggregateFunctions)"
            )
        self._require_data_history()
        if end <= start:
            return []
        node = _node_id(tag)

        details = ua.ReadProcessedDetails(
            StartTime=start,
            EndTime=end,
            ProcessingInterval=every.total_seconds() * 1000.0,  # milliseconds
            # Positional against NodesToRead: the same node twice, once for
            # the aggregate and once for its good fraction. One round trip.
            AggregateType=[ua.NodeId(_AGGREGATE_NODE[agg]), ua.NodeId(_PERCENT_GOOD)],
            AggregateConfiguration=ua.AggregateConfiguration(
                UseServerCapabilitiesDefaults=True
            ),
        )
        read_value = ua.HistoryReadValueId(NodeId=node)
        params = ua.HistoryReadParameters(
            HistoryReadDetails=details,
            TimestampsToReturn=ua.TimestampsToReturn.Both,
            ReleaseContinuationPoints=False,
            NodesToRead=[read_value, read_value],
        )
        results = await self._session.history_read(params)
        if not results:
            raise TrendUnavailable(f"server returned no result for {tag}")

        values = self._unwrap(results[0], tag)
        # The quality half is allowed to fail on its own: a server without
        # PercentGood still gives per-interval StatusCodes to fall back on.
        percent: list[ua.DataValue] | None = None
        if len(results) > 1 and not _is_bad(results[1].StatusCode):
            with contextlib.suppress(Exception):
                percent = self._unwrap(results[1], tag)

        return self._assemble(tag, values, percent, start, end, every)

    # --- internals ---------------------------------------------------------
    def _assemble(
        self,
        tag: str,
        values: list[ua.DataValue],
        percent: list[ua.DataValue] | None,
        start: datetime,
        end: datetime,
        every: timedelta,
    ) -> list[Bucket]:
        """Lay the server's intervals onto the grid the caller asked for.

        Servers differ on whether a trailing partial interval and wholly empty
        intervals come back at all, so the grid is built locally and the
        response is matched onto it. That keeps the shape identical to the
        in-memory reference: one bucket per interval, ``value=None`` wherever
        there was nothing to aggregate.
        """
        edges: list[datetime] = []
        edge = start
        while edge < end:
            edges.append(edge)
            edge = edge + every

        pct_by_edge: dict[datetime, ua.DataValue] = {}
        for dv in percent or []:
            if dv.SourceTimestamp is not None:
                pct_by_edge[_as_utc(dv.SourceTimestamp)] = dv

        by_edge: dict[datetime, ua.DataValue] = {}
        stamped = [dv for dv in values if dv.SourceTimestamp is not None]
        if stamped:
            on_grid = {_as_utc(dv.SourceTimestamp) for dv in stamped} & set(edges)  # type: ignore[arg-type]
            if on_grid:
                for dv in stamped:
                    by_edge[_as_utc(dv.SourceTimestamp)] = dv  # type: ignore[arg-type]
            elif len(values) == len(edges):
                # Timestamps we cannot place, but exactly one value per
                # interval: positional is then unambiguous.
                by_edge = dict(zip(edges, values))
            else:
                raise TrendUnavailable(
                    f"server returned {len(values)} intervals for {tag} that do not "
                    f"align with the requested {every} grid starting "
                    f"{start.isoformat()}"
                )
        elif values:
            if len(values) != len(edges):
                raise TrendUnavailable(
                    f"server returned {len(values)} undated intervals for {tag}, "
                    f"expected {len(edges)}"
                )
            by_edge = dict(zip(edges, values))

        out: list[Bucket] = []
        for edge in edges:
            stop = min(edge + every, end)
            dv = by_edge.get(edge)
            if dv is None:
                out.append(Bucket(start=edge, end=stop, value=None, good_fraction=0.0))
                continue
            value = None if _is_bad(dv.StatusCode) else _number(dv, tag)
            out.append(
                Bucket(
                    start=edge,
                    end=stop,
                    value=value,
                    good_fraction=_good_fraction(dv, pct_by_edge.get(edge)),
                )
            )
        return out

    async def _history_read(
        self,
        tag: str,
        node: ua.NodeId,
        details: Any,
        *,
        cont: bytes | None = None,
    ) -> tuple[list[ua.DataValue], bytes | None]:
        params = ua.HistoryReadParameters(
            HistoryReadDetails=details,
            TimestampsToReturn=ua.TimestampsToReturn.Both,
            ReleaseContinuationPoints=False,
            NodesToRead=[ua.HistoryReadValueId(NodeId=node, ContinuationPoint=cont)],
        )
        results = await self._session.history_read(params)
        if not results:
            raise TrendUnavailable(f"server returned no result for {tag}")
        result = results[0]
        return self._unwrap(result, tag), result.ContinuationPoint

    def _unwrap(self, result: ua.HistoryReadResult, tag: str) -> list[ua.DataValue]:
        """One HistoryReadResult to DataValues, raising rather than defaulting."""
        status = result.StatusCode
        if _is_bad(status):
            self._fail(tag, status)
        values: Iterable[ua.DataValue] | None = getattr(
            result.HistoryData, "DataValues", None
        )
        if values is None:
            # A server that reports Good but sends a body we cannot read (or
            # none at all) has not answered the question, and must not be made
            # to look like "no data".
            raise TrendUnavailable(
                f"server returned no readable history body for {tag} "
                f"(status {status.name if status is not None else 'Good'})"
            )
        return [dv for dv in values if dv is not None]

    async def _release(self, node: ua.NodeId, cont: bytes) -> None:
        """Hand a continuation point back when we stop reading early.

        One we abandon stays allocated on the server until it times out, and a
        historian has a small pool of them. Failing to free it is not worth
        raising over; keeping it is not worth the risk.
        """
        params = ua.HistoryReadParameters(
            HistoryReadDetails=ua.ReadRawModifiedDetails(
                IsReadModified=False,
                StartTime=None,
                EndTime=None,
                NumValuesPerNode=0,
                ReturnBounds=False,
            ),
            TimestampsToReturn=ua.TimestampsToReturn.Both,
            ReleaseContinuationPoints=True,
            NodesToRead=[ua.HistoryReadValueId(NodeId=node, ContinuationPoint=cont)],
        )
        with contextlib.suppress(Exception):
            await self._session.history_read(params)

    async def _confirm_exists(self, tag: str, node: ua.NodeId) -> None:
        """Turn an empty answer into :class:`UnknownTag` when it is one."""
        if not self.verify_unknown_tags:
            return
        params = ua.ReadParameters(
            MaxAge=0,
            TimestampsToReturn=ua.TimestampsToReturn.Neither,
            NodesToRead=[
                ua.ReadValueId(NodeId=node, AttributeId=ua.AttributeIds.NodeClass)
            ],
        )
        try:
            values = await self._session.read(params)
        except Exception:  # noqa: BLE001
            # The check improves the answer, it is not the answer. If it
            # cannot be made, the empty result stands.
            return
        if values and _top(values[0].StatusCode) in _UNKNOWN_TAG_CODES:
            raise UnknownTag(tag)

    def _require_data_history(self) -> None:
        if self.data_history is False:
            raise UnsupportedQuery(
                "server reports AccessHistoryDataCapability=False: it keeps no "
                "data history to read"
            )

    def _fail(self, tag: str, status: ua.StatusCode) -> None:
        """Map a service-level StatusCode onto ottrend's error convention."""
        code = _top(status)
        if code in _UNKNOWN_TAG_CODES:
            raise UnknownTag(tag)
        if code in _UNSUPPORTED_CODES:
            raise UnsupportedQuery(
                f"server cannot answer this history query for {tag}: {status.name}"
            )
        raise TrendUnavailable(f"history read for {tag} failed: {status.name}")

    def _sample(self, dv: ua.DataValue, tag: str) -> Sample:
        return Sample(
            ts=_timestamp(dv, tag),
            value=_require_number(dv, tag),
            quality=_quality(dv.StatusCode),
        )


# --- free helpers -----------------------------------------------------------
def _node_id(tag: str) -> ua.NodeId:
    if isinstance(tag, ua.NodeId):  # tolerated, not advertised
        return tag
    try:
        return ua.NodeId.from_string(tag)
    except Exception as exc:  # noqa: BLE001
        raise UnknownTag(
            f"{tag!r} is not an OPC UA NodeId; tags look like 'ns=2;s=TIC101.PV'"
        ) from exc


def _require_aware(**stamps: datetime) -> None:
    for name, value in stamps.items():
        if value.tzinfo is None or value.tzinfo.utcoffset(value) is None:
            raise ValueError(
                f"{name} must be timezone-aware: a history window read in the "
                "wrong zone answers about the wrong hour"
            )


def _as_utc(ts: datetime) -> datetime:
    return ts.replace(tzinfo=timezone.utc) if ts.tzinfo is None else ts


def _timestamp(dv: ua.DataValue, tag: str) -> datetime:
    ts = dv.SourceTimestamp or dv.ServerTimestamp
    if ts is None:
        raise TrendUnavailable(
            f"server returned a value for {tag} with no timestamp; a sample "
            "without a time cannot be placed in a window"
        )
    return _as_utc(ts)


def _plain_value(dv: ua.DataValue | None) -> Any:
    if dv is None or _is_bad(dv.StatusCode) or dv.Value is None:
        return None
    return dv.Value.Value


def _number(dv: ua.DataValue, tag: str) -> float | None:
    raw = _plain_value(dv)
    if raw is None:
        return None
    try:
        return float(raw)
    except (TypeError, ValueError) as exc:
        raise UnsupportedQuery(
            f"{tag} holds {type(raw).__name__}, not a number; ottrend samples "
            "are numeric"
        ) from exc


def _require_number(dv: ua.DataValue, tag: str) -> float:
    value = _number(dv, tag)
    if value is None:
        raise TrendUnavailable(
            f"server returned an empty value for {tag} where a sample was expected"
        )
    return value


def _quality(status: ua.StatusCode | None) -> Quality:
    if status is None or status.is_good():
        return Quality.GOOD
    if status.is_uncertain():
        return Quality.QUESTIONABLE
    return Quality.BAD


def _good_fraction(dv: ua.DataValue, percent: ua.DataValue | None) -> float:
    """PercentGood when the server gave it, the StatusCode otherwise.

    The fallback is deliberately pessimistic: an uncertain interval reports no
    good data, so a derived query refuses rather than answering off it. Without
    PercentGood the real fraction is genuinely unknown, and "unknown" must not
    round up to "fine".
    """
    if percent is not None:
        raw = _plain_value(percent)
        if raw is not None:
            try:
                return min(1.0, max(0.0, float(raw) / 100.0))
            except (TypeError, ValueError):
                pass
    status = dv.StatusCode
    if status is None or status.is_good():
        return 1.0
    return 0.0


def _is_bad(status: ua.StatusCode | None) -> bool:
    return status is not None and status.is_bad()


def _top(status: ua.StatusCode | None) -> int:
    return 0 if status is None else status.value & _CODE_MASK
