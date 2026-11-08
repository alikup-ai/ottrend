"""ottrend — one way to ask a historian what a tag has been doing.

Three primitives every historian answers natively (:mod:`ottrend.provider`),
the queries none of them answer built once on top (:mod:`ottrend.derive`), and
adapters that declare what they can actually do (:mod:`ottrend.types`).

``OpcUaHistoryTrend`` is importable from here but resolved lazily, because it
needs ``asyncua`` and the core deliberately needs nothing at all.
"""

from typing import TYPE_CHECKING, Any

from ottrend.adapters.memory import MemoryTrend
from ottrend.derive import coverage, drift_since, max_rate, was_outside
from ottrend.provider import (
    TrendError,
    TrendProvider,
    TrendUnavailable,
    UnknownTag,
    UnsupportedQuery,
    require_value_at,
)
from ottrend.types import (
    Aggregate,
    Bucket,
    Capabilities,
    Quality,
    RateResult,
    Sample,
)

if TYPE_CHECKING:  # pragma: no cover
    from ottrend.adapters.opcua_ha import OpcUaHistoryTrend

__version__ = "0.1.0"

__all__ = [
    "Aggregate", "Bucket", "Capabilities", "Quality", "RateResult", "Sample",
    "MemoryTrend", "OpcUaHistoryTrend",
    "TrendProvider", "TrendError", "TrendUnavailable", "UnknownTag",
    "UnsupportedQuery", "require_value_at",
    "max_rate", "drift_since", "was_outside", "coverage",
]


def __getattr__(name: str) -> Any:
    if name == "OpcUaHistoryTrend":
        from ottrend.adapters.opcua_ha import OpcUaHistoryTrend

        return OpcUaHistoryTrend
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
