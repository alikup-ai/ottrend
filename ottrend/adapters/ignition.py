"""Inductive Automation Ignition adapter — skeleton.

Ignition is the awkward one, and worth saying so plainly: it has no
historian REST API out of the box. Three routes, in descending order of how
much they ask of the plant:

1. **WebDev module** — a small Python endpoint in the gateway wrapping
   ``system.tag.queryTagHistory``. Full fidelity (every aggregation mode,
   server-side bucketing via ``returnSize``/``aggregationMode``), but it means
   installing a resource on the gateway.
2. **SQL against the historian schema** — Ignition's tag historian stores into
   ``sqlt_data_*`` partition tables with ``sqlth_te`` as the tag dictionary.
   Read-only and no gateway changes, but the schema is internal and partition
   handling is fiddly.
3. **OPC UA** — only live values, no history. Not a route for this adapter.

Route 1 is the intended default; route 2 is the fallback where installing a
gateway resource is not allowed, which in OT it frequently is not.

``system.tag.queryTagHistory`` aggregation modes cover
``Average``/``MinMax``/``Min``/``Max``/``Range``/``Count``/``StdDev`` among
others, so the six aggregates in :class:`~ottrend.types.Aggregate` all map.
As everywhere else, there is no rate-of-change mode.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from ottrend.provider import TrendUnavailable
from ottrend.types import Aggregate, Bucket, Capabilities, Sample

_TODO = (
    "Ignition adapter is not implemented yet — see the route notes in "
    "ottrend/adapters/ignition.py."
)


class IgnitionTrend:
    """Ignition provider. Skeleton — every read raises.

    Args:
        gateway_url: base URL of the Ignition gateway.
        route: ``"webdev"`` (default) or ``"sql"``.
    """

    def __init__(self, gateway_url: str, route: str = "webdev") -> None:
        self._url = gateway_url.rstrip("/")
        self._route = route

    def capabilities(self) -> Capabilities:
        return Capabilities(
            name=f"ignition:{self._route}",
            native_aggregates=frozenset(Aggregate),
            arbitrary_timestamps=True,
            # queryTagHistory takes several paths at once; the SQL route can
            # too, with a less pleasant query.
            bulk_tags=True,
            quality=True,
        )

    async def value_at(self, tag: str, when: datetime) -> Sample | None:
        raise TrendUnavailable(_TODO)

    async def raw(
        self, tag: str, start: datetime, end: datetime, *, limit: int | None = None
    ) -> list[Sample]:
        raise TrendUnavailable(_TODO)

    async def buckets(
        self, tag: str, start: datetime, end: datetime, *,
        every: timedelta, agg: Aggregate,
    ) -> list[Bucket]:
        raise TrendUnavailable(_TODO)
