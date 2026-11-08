# ottrend

One way to ask an industrial historian what a tag has been doing.

PI System, Ignition and OPC UA Historical Access all answer the same questions and none of them answer them the same way. `ottrend` puts three primitives in front of them, builds the queries none of them have on top, and makes every adapter declare what it can actually do before you spend a round trip finding out.

> **Status: alpha.** The design is settled, the in-memory provider is complete, and the **OPC UA Historical Access adapter is implemented and tested against a real `opc.tcp://` server** in CI. The PI Web API and Ignition adapters are still stubs that raise — their mapping tables are written down (see below and in each module), the I/O is not. Nothing here has touched a production historian; "a real server" means asyncua, not a plant.

## Why this exists

Two gaps, one library.

An agent that is allowed to write to a plant needs to know more than a tag's current value. *How fast has it been moving? How far has it drifted this shift? Was it in alarm while that happened?* Live reads cannot answer any of it. [otgate](https://github.com/alikup-ai/otgate), a process-aware policy gateway, is the first consumer: its rate limit used to be judged against the tag's live value and otgate's own record of its last write, which are two points in time and blind to anything that happened between them — an operator ramping a setpoint and putting it back left no trace in either. With `history_rate` switched on it asks `ottrend` for the tag's own recorded rate and holds the process to the same limit.

Separately, agents have no way to read a historian at all. [top-industrial-ai-agents](https://github.com/alikup-ai/top-industrial-ai-agents) lists "MCP server for PI System and the Ignition historian" as an open niche, verified empty on 2026-11-07.

`ottrend` is the same component from both directions: a trend provider for policy engines, and — once the MCP wrapper lands — a historian MCP server for agents.

## The finding that shaped the design

Checked on 2026-11-07, against the PI Web API Stream and Calculation controllers, OPC UA Part 13's standard aggregate set, and Ignition's `system.tag.queryTagHistory`:

**All three compute min, max, range, average, count and standard deviation server-side. None of them computes a rate of change.**

OPC UA Part 13 defines 37 standard aggregate functions and not one is a derivative — `Delta` is the net change across an interval, which is signed and blind to a round trip inside it. PI's summary types are statistical. Ignition's aggregation modes are the same family.

That splits the library cleanly:

- **Primitives** map one-to-one onto a native call. Three of them, no more.
- **Derived queries** are the ones nobody has. They are built once, on the primitives, and their results record that they were derived and how.

The alternative — pulling raw samples and computing client-side — is what you do if you want to take down a production historian. A year of a fast tag is tens of millions of samples.

## The three primitives

```python
async def value_at(tag, when) -> Sample | None
async def raw(tag, start, end, *, limit=None) -> list[Sample]
async def buckets(tag, start, end, *, every, agg) -> list[Bucket]
```

`buckets` is the workhorse: it is what makes a derived query cost one round trip instead of a raw dump.

## The derived queries

```python
r = await max_rate(provider, "TIC101.SP", start, end, bucket=timedelta(seconds=10))
r.per(timedelta(seconds=60))   # "5 per 60 s" is how policies are written
r.emulated                     # True — and the field exists so it can stop being
r.bucket_size                  # the sharpness of the answer
r.good_fraction                # how much of it was real data
```

`max_rate` returns a **lower bound** on the true maximum rate, and the direction matters:

- **"too fast" is definitive.** The movement really happened, so denying on it is sound.
- **"slow enough" is not.** It means nothing faster was visible *at this bucket size, in the data the historian kept*.

So **use it to refuse an action, not to justify one.**

It combines two bounds, because either alone has a blind spot: the `range` inside one bucket over `bucket`, and the span across two adjacent buckets over `2 × bucket`. The pair term is what makes a smaller bucket give a sharper answer — the first version of this used range alone, and once the bucket was finer than the historian's sample interval every bucket held one sample, every range collapsed to zero, and the reported rate *fell* as the bucket shrank. Exactly backwards for a safety check, and the regression test for it is in `tests/test_derive.py`.

Neither term can recover what the historian never stored. PI-style exception and compression deadbands discard samples at the source, so a spike shorter than the effective recording interval is invisible at any bucket size. The result reports the bucket size so you can say how sharp the answer was; nothing can report what was never kept.

Also: `drift_since`, `was_outside`, `coverage`.

## The OPC UA HA adapter

```python
from datetime import datetime, timedelta, timezone
from ottrend import OpcUaHistoryTrend, max_rate

async with OpcUaHistoryTrend.connect("opc.tcp://plant:4840/") as trend:
    now = datetime.now(timezone.utc)
    held = await trend.value_at("ns=2;s=TIC101.SP", now - timedelta(minutes=5))
    r = await max_rate(trend, "ns=2;s=TIC101.SP", now - timedelta(hours=1), now,
                       bucket=timedelta(seconds=10))
```

`connect` asks the server what it can actually do before the first read, so `trend.capabilities().native_aggregates` is what *this* server implements rather than what the standard allows. Tags are NodeId strings; timestamps must be timezone-aware, because a window read in the wrong zone answers about the wrong hour and still looks like an answer.

Four things worth knowing, all of them found while making the adapter work against a real server:

**`value_at` is a bounding raw read, not `Interpolative`.** The first mapping said to use `HistoryReadProcessed` with the `Interpolative` aggregate, and that was wrong. Interpolation invents a value the process never held: for a tag recorded at 50 and then at 80 a minute later, `Interpolative` answers 65 in the middle while the plant sat at 50 the whole time. The reference semantics is the last value at or before the instant, so the adapter asks for exactly that — `StartTime` later than `EndTime`, which Part 11 defines as a reverse read, one value.

**That reverse read is bounded by `lookback` (7 days by default).** Part 11 allows a null `StartTime` meaning "however far back you have to go", but servers disagree about what a null bound implies — asyncua's own historian ignores the *other* bound entirely when one is null — and an unbounded scan is the thing this library refuses to do to a production historian. So a tag that has not been written in longer than `lookback` reads as `None`. This is the one deliberate divergence from the in-memory reference, which looks back forever.

**Quality costs no extra round trip.** `AggregateType` is a list matched positionally against `NodesToRead`, so the same node can appear twice under two different aggregates. `buckets` sends `[agg, PercentGood]` and gets the values and their good fraction in one request. A server without `PercentGood` fails that half on its own and the per-interval StatusCode is used instead — pessimistically, because without `PercentGood` the real fraction is unknown and "unknown" must not round up to "fine".

**"No such tag" has to be asked for separately.** Part 11 lets a server answer a history read for a node it does not have with `Good` and an empty result set, and asyncua's server does exactly that. Since a caller gating a write has to tell "there is no such tag" from "I could not find out", an empty read costs one extra NodeClass read to settle it. Pass `verify_unknown_tags=False` to skip it.

An unbounded `raw` read that hits `max_raw_samples` **raises instead of returning a short series**, and hands its continuation point back on the way out. Of the three possible outcomes — all of it, an error, or a truncated answer that looks whole — the third is the dangerous one.

## Quality is not optional

Every historian reports whether a value is good, bad or questionable, and an operating envelope judged on 10 % good data is worse than no envelope — it looks like an answer. So quality travels with every sample and every bucket, bad samples are excluded from aggregates, and a derived query **refuses** rather than answering when too little of the window is good:

```python
TrendUnavailable: no bucket of T in [...] reached 80% good data
```

## Failing closed

Borrowed from otgate, because it is the right convention for anything a physical action depends on:

| Situation | Result |
|---|---|
| No such tag | `UnknownTag` |
| Historian unreachable | `TrendUnavailable` |
| Adapter cannot express the query | `UnsupportedQuery` |
| Window holds no usable data | `TrendUnavailable` |
| Tag younger than the question | `value_at` returns `None` |

No method returns a fallback value, and the first two are deliberately distinguishable — a caller gating a write needs to tell "there is no such tag" from "I could not find out".

## Adapter mapping

| | PI Web API | OPC UA HA ✅ | Ignition |
|---|---|---|---|
| `value_at` | `/streams/{id}/recorded` back from the instant | `HistoryReadRaw`, reverse range, one value | `queryTagHistory` |
| `raw` | `/streams/{id}/recorded` (`maxCount`) | `HistoryReadRaw` (`numValuesPerNode`) | `queryTagHistory` |
| `buckets` | `/streams/{id}/summary` (`summaryDuration`, `summaryType`) | `HistoryReadProcessed` (`processingInterval` + aggregate NodeId) | `queryTagHistory` (`returnSize`, `aggregationMode`) |
| quality | `summaryType=PercentGood` | `PercentGood` / StatusCode | quality columns |
| bulk | `/streamsets/` | several `nodesToRead` | several paths |
| **rate of change** | **none** | **none** | **none** |

Three notes worth having before you start:

- **The `value_at` row changed once the OPC UA adapter was real, and it changes for PI too.** It used to say `interpolatedattimes` / `Interpolative`, which answers with a value the process never held. Whatever the historian calls it, `value_at` is the last *recorded* value at or before the instant — so on PI it is a backwards `recorded` read with `maxCount=1`, not an interpolation.
- **PI addresses streams by opaque `webId`, not tag name.** An implementation needs a resolve-and-cache step in front of every call.
- **Ignition has no historian REST API out of the box.** Either install a small WebDev endpoint wrapping `system.tag.queryTagHistory` (full fidelity, but a gateway change), or read the `sqlt_data_*` partition tables directly (no gateway change, internal schema). The second route matters because in OT, permission to install a gateway resource frequently does not exist.

PI also offers `/calculation/summary` with Performance Equation expressions, which could in principle evaluate a derivative server-side. Deliberately unused: the emulation is one code path shared by every adapter, while a PE path would be correct only for PI and untestable without a PI server. Worth revisiting if the round trips ever actually hurt.

## Install

```bash
pip install -e .                 # core: protocol, derived queries, MemoryTrend — no dependencies
pip install -e ".[opcua]"        # + asyncua
pip install -e ".[pi]"           # + httpx
pip install -e ".[ignition]"     # + httpx
pip install -e ".[mcp]"          # + mcp, for the server wrapper (roadmap item 2, not written yet)
pip install -e ".[dev]"          # + pytest
```

Python 3.11+.

## Tests

```bash
pytest -m "not integration"   # no OPC UA stack needed
pytest -m integration         # against a real asyncua server over opc.tcp://
pytest                        # both
```

`MemoryTrend` is the reference semantics: where a real adapter and the in-memory one disagree about what a bucket means at an interval boundary, the in-memory one is right and the adapter has a bug. That is not just a claim in a README — the OPC UA adapter is driven by a session whose answers come from `MemoryTrend`, encoded into OPC UA wire types, and `max_rate`, `was_outside`, `value_at`, `raw` and all six aggregates have to come out identical on both sides.

The integration tests additionally pin two behaviours that only a real server shows: a `HistoryReadProcessed` refused with `BadNotImplemented` (which must surface as `UnsupportedQuery`, not an empty result that reads as "nothing happened"), and a history read for a node the server has never heard of answered with `Good` and no data.

## Roadmap

1. ~~OPC UA HA adapter~~ — done, tested against a real server in CI
2. MCP server wrapper — closes the historian-MCP gap on its own
3. PI Web API adapter — needs a PI server to test against
4. Ignition adapter, WebDev route, then SQL route
5. Bulk reads: OPC UA already takes several `nodesToRead` per request and `Capabilities.bulk_tags` already says so, but the provider protocol has no multi-tag primitive to use it
6. Event frames / alarm history, so `was_in_alarm(window)` becomes a real query

## License

Apache-2.0.
