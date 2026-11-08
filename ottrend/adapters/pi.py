"""AVEVA / OSIsoft PI Web API adapter — skeleton.

Not implemented: writing this against a real PI server is the next piece of
work, and a plausible-looking adapter nobody has run against PI would be worse
than an honest stub. What *is* settled is the mapping, which was the open
design question — it is recorded here so the implementation is mechanical.

Verified against the PI Web API Stream and Calculation controllers
(2026-11-07):

===========================  ====================================================
ottrend                      PI Web API
===========================  ====================================================
``value_at``                 ``GET /streams/{webId}/recordedattime?time=``
                             with ``retrievalMode=AtOrBefore``. **Not**
                             ``interpolatedattimes``, which this table used to
                             say: interpolation answers with a value the
                             process never held, and the reference semantics
                             is the last *recorded* value at or before the
                             instant. Settled while implementing the OPC UA
                             adapter, where the same mistake was waiting.
``raw``                      ``GET /streams/{webId}/recorded``
                             Pass ``maxCount`` for ``limit`` — server-side, so
                             an unbounded window cannot flood the client.
``buckets``                  ``GET /streams/{webId}/summary``
                             ``summaryDuration`` is the bucket, ``summaryType``
                             the aggregate (repeatable). Pass
                             ``calculationBasis=EventWeighted``: the default is
                             ``TimeWeighted``, which is a different number from
                             the arithmetic mean the in-memory reference
                             computes. (OPC UA has the same fork —
                             ``Average`` vs ``TimeAverage``.)
``Aggregate.MIN/MAX/AVG``    ``summaryType=Minimum|Maximum|Average``
``Aggregate.RANGE``          ``summaryType=Range``
``Aggregate.COUNT``          ``summaryType=Count``
``Aggregate.STDDEV``         ``summaryType=StdDev``
``good_fraction``            ``summaryType=PercentGood`` — request it alongside
                             the wanted aggregate in the same call, since
                             ``summaryType`` may be repeated.
bulk                         ``/streamsets/`` — several webIds per round trip.
===========================  ====================================================

**No rate-of-change aggregate exists**, which is why ``max_rate`` is emulated
in :mod:`ottrend.derive` rather than delegated here. PI does offer
``/calculation/summary`` with Performance Equation expressions, which could in
principle evaluate a derivative server-side; it is deliberately not used,
because the emulation is one code path shared by every adapter and a
PE-specific path would be correct only for PI and untestable without a PI
server. Revisit if the round trips ever actually hurt.

PI addresses streams by opaque ``webId``, not by tag name, so an
implementation needs a resolve-and-cache step
(``GET /points?path=\\\\SERVER\\TAG`` → ``WebId``) in front of every call here.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from ottrend.provider import TrendUnavailable
from ottrend.types import Aggregate, Bucket, Capabilities, Sample


class PIWebAPITrend:
    """PI Web API provider. Skeleton — every read raises.

    Args:
        base_url: e.g. ``https://pi.example.com/piwebapi``
        auth: whatever the deployment uses (Kerberos, Basic, bearer). PI
            deployments differ enough that this is left to the caller's HTTP
            client rather than guessed at here.
    """

    def __init__(self, base_url: str, auth: object | None = None) -> None:
        self._base = base_url.rstrip("/")
        self._auth = auth

    def capabilities(self) -> Capabilities:
        return Capabilities(
            name="pi-web-api",
            native_aggregates=frozenset(Aggregate),  # all six, see table above
            arbitrary_timestamps=True,               # recordedattime, AtOrBefore
            bulk_tags=True,                          # /streamsets/
            quality=True,                            # PercentGood
        )

    async def value_at(self, tag: str, when: datetime) -> Sample | None:
        raise TrendUnavailable(_TODO)

    async def raw(
        self, tag: str, start: datetime, end: datetime, *, limit: int | None = None
    ) -> list[Sample]:
        raise TrendUnavailable(_TODO)

    async def buckets(
        self,
        tag: str,
        start: datetime,
        end: datetime,
        *,
        every: timedelta,
        agg: Aggregate,
    ) -> list[Bucket]:
        raise TrendUnavailable(_TODO)


_TODO = (
    "PI Web API adapter is not implemented yet — see the mapping table in "
    "ottrend/adapters/pi.py. Raising rather than returning a default is "
    "deliberate: a caller gating a physical write must not receive a "
    "plausible-looking answer from an adapter that cannot answer."
)
