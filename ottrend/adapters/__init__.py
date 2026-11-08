"""Historian adapters.

:class:`~ottrend.adapters.memory.MemoryTrend` is the reference semantics and
needs nothing installed. :class:`~ottrend.adapters.opcua_ha.OpcUaHistoryTrend`
is implemented and tested against a real ``opc.tcp://`` server, and needs the
``opcua`` extra. The PI and Ignition adapters carry their mapping tables in
their module docstrings — the design work is done, the I/O is not.

The OPC UA adapter is imported lazily so that a bare install, the test suite
and the demo never drag in ``asyncua``.
"""

from typing import TYPE_CHECKING, Any

from ottrend.adapters.memory import MemoryTrend

if TYPE_CHECKING:  # pragma: no cover
    from ottrend.adapters.opcua_ha import OpcUaHistoryTrend

__all__ = ["MemoryTrend", "OpcUaHistoryTrend"]


def __getattr__(name: str) -> Any:
    if name == "OpcUaHistoryTrend":
        from ottrend.adapters.opcua_ha import OpcUaHistoryTrend

        return OpcUaHistoryTrend
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
