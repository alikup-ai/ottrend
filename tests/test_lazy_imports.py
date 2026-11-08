"""The promise that a bare install needs nothing.

``pip install ottrend`` pulls no dependencies, and that is a promise about
import time, not just about the dependency list: if ``import ottrend`` reached
into ``asyncua``, the core would be unusable exactly where it is most useful —
a policy engine with no OPC UA stack on the box.

Checked in subprocesses, because the rest of this suite imports the OPC UA
adapter and would hide the problem.
"""

from __future__ import annotations

import subprocess
import sys
import textwrap

BLOCK_ASYNCUA = """
import sys

class Blocked:
    def find_module(self, name, path=None):
        return None
    def find_spec(self, name, target=None, path=None):
        if name == "asyncua" or name.startswith("asyncua."):
            raise ImportError("asyncua is not installed (simulated)")
        return None

sys.meta_path.insert(0, Blocked())
"""


def run(script: str, *, without_asyncua: bool = False) -> subprocess.CompletedProcess:
    prelude = BLOCK_ASYNCUA if without_asyncua else ""
    return subprocess.run(
        [sys.executable, "-c", prelude + textwrap.dedent(script)],
        capture_output=True,
        text=True,
    )


def test_importing_ottrend_does_not_import_asyncua():
    done = run(
        """
        import sys
        import ottrend
        assert "asyncua" not in sys.modules, "the core pulled in asyncua"
        ottrend.MemoryTrend()
        print("ok")
        """
    )
    assert done.returncode == 0, done.stderr
    assert "ok" in done.stdout


def test_the_core_works_with_asyncua_unavailable():
    done = run(
        """
        import asyncio
        from datetime import datetime, timedelta, timezone
        from ottrend import MemoryTrend, Sample, max_rate

        t0 = datetime(2026, 11, 7, 12, 0, tzinfo=timezone.utc)
        trend = MemoryTrend({"T": [Sample(t0, 50.0), Sample(t0 + timedelta(seconds=5), 80.0)]})
        r = asyncio.run(
            max_rate(trend, "T", t0, t0 + timedelta(seconds=10), bucket=timedelta(seconds=10))
        )
        assert r.max_rate_per_second == 3.0, r
        print("ok")
        """,
        without_asyncua=True,
    )
    assert done.returncode == 0, done.stderr
    assert "ok" in done.stdout


def test_the_opcua_adapter_says_which_extra_it_needs():
    """A bare ImportError for 'asyncua' is not an answer a user can act on."""
    done = run(
        """
        import ottrend
        try:
            ottrend.OpcUaHistoryTrend
        except ImportError as exc:
            assert "ottrend[opcua]" in str(exc), str(exc)
            print("ok")
        else:
            raise AssertionError("expected ImportError without asyncua")
        """,
        without_asyncua=True,
    )
    assert done.returncode == 0, done.stderr
    assert "ok" in done.stdout


def test_the_opcua_adapter_is_reachable_from_the_package_root():
    done = run(
        """
        import ottrend
        from ottrend.adapters import OpcUaHistoryTrend as viaAdapters
        assert ottrend.OpcUaHistoryTrend is viaAdapters
        print("ok")
        """
    )
    if "ModuleNotFoundError: No module named 'asyncua'" in done.stderr:
        import pytest

        pytest.skip("asyncua not installed")
    assert done.returncode == 0, done.stderr
    assert "ok" in done.stdout
