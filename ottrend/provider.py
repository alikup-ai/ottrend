"""The provider contract: three primitives, nothing else.

Every adapter implements exactly these three reads. They were chosen because
each one maps onto a single native call in all three target historians — see
the mapping table in the README. Anything a caller actually wants to know
("how fast is this moving", "how far has it drifted") is built on top of them
in :mod:`ottrend.derive`, once, instead of three times in three adapters.

Error convention, borrowed from otgate because it is the right one for OT: a
tag that does not exist raises :class:`UnknownTag` (a normal answer — no such
tag), while a historian that cannot be reached raises
:class:`TrendUnavailable` (a failure). A caller gating a write on trend data
must be able to tell those apart, and must never receive a plausible-looking
default in place of either. No method here returns a fallback value.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Protocol, runtime_checkable

from ottrend.types import Aggregate, Bucket, Capabilities, Sample


class TrendError(RuntimeError):
    """Base class for everything this package raises."""


class UnknownTag(TrendError):
    """The historian has no such tag. A normal answer, not a failure."""


class TrendUnavailable(TrendError):
    """The historian could not be reached, or refused the query.

    Callers gating a physical action on trend data should treat this as a
    reason to deny, never as a reason to proceed.
    """


class UnsupportedQuery(TrendError):
    """The adapter cannot express this query against its historian."""


@runtime_checkable
class TrendProvider(Protocol):
    """Read-only history access. No adapter is permitted to write."""

    def capabilities(self) -> Capabilities:
        """Describe what this adapter does natively. Must not do I/O."""
        ...

    async def value_at(self, tag: str, when: datetime) -> Sample | None:
        """The value of ``tag`` at ``when``.

        ``None`` means the historian holds no value at or before that instant
        — a tag younger than the question. It does not mean zero.

        Raises:
            UnknownTag: no such tag.
            TrendUnavailable: the historian could not be reached.
        """
        ...

    async def raw(
        self,
        tag: str,
        start: datetime,
        end: datetime,
        *,
        limit: int | None = None,
    ) -> list[Sample]:
        """Recorded samples in ``[start, end]``, oldest first.

        ``limit`` caps what is pulled back. Adapters must apply it at the
        historian rather than truncating locally: an unbounded raw read of a
        fast tag over a long window is how you take down a production
        historian, and this library will not be the thing that does that.
        """
        ...

    async def buckets(
        self,
        tag: str,
        start: datetime,
        end: datetime,
        *,
        every: timedelta,
        agg: Aggregate,
    ) -> list[Bucket]:
        """``agg`` over each ``every``-long interval of ``[start, end]``.

        This is the workhorse: it is what makes derived queries cost one round
        trip instead of a raw dump. Adapters whose historian cannot evaluate
        ``agg`` natively should raise :class:`UnsupportedQuery` rather than
        silently emulating it — emulation belongs in one place, and a caller
        that cares about round trips has
        :meth:`~ottrend.types.Capabilities.supports` to ask first.
        """
        ...


async def require_value_at(provider: TrendProvider, tag: str, when: datetime) -> Sample:
    """:meth:`TrendProvider.value_at`, but absence is an error.

    For the fail-closed path: a caller that cannot proceed without a baseline
    should not have to turn ``None`` into a raise at every call site.
    """
    sample = await provider.value_at(tag, when)
    if sample is None:
        raise TrendUnavailable(
            f"{tag} has no recorded value at or before {when.isoformat()}"
        )
    return sample
