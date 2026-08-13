# streaming-starter

A minimal **Apache Flink (PyFlink)** streaming job running on a real Flink cluster (JobManager + TaskManager) via the official Flink Docker image. It's the warm-start for a real-time market-data pipeline — the primitives here (source → keyBy → stateful aggregation → sink) are what the full project extends.

## Run it

```bash
# 1. Build the image + start the Flink cluster (JobManager + TaskManager)
docker compose up -d --build

# 2. Submit the PyFlink job to the cluster
docker compose exec jobmanager flink run -py /opt/job/job.py

# 3. See the output — the print sink goes to the TaskManager's stdout,
#    which this console-mode cluster surfaces in its container logs.
#    PyFlink prints records as Flink Rows (+I[...]), so grep loosely:
docker compose logs taskmanager | grep -iE 'aapl|msft|goog'

# 4. Stop everything
docker compose down
```

**Flink Web UI:** http://localhost:8081 — watch the job run, see the job graph, and view _Task Managers → (the TM) → Stdout_ for the sink output directly.

Expected output (running per-symbol totals as events arrive):

```
('AAPL', 1)
('MSFT', 1)
('AAPL', 2)
('AAPL', 3)
('MSFT', 2)
('GOOG', 1)
('AAPL', 4)
('MSFT', 3)
```

## What it does

```
synthetic trade events  →  keyBy(symbol)  →  running sum(qty)  →  print
```

`job.py` is ~30 lines and deliberately minimal — source, keying, stateful aggregation, sink. The real work in Phase 3 is a swap-in (live feed + event-time windowing), not a rebuild.

## The one thing this taught me (worth knowing)

The first version used a **processing-time** tumbling window — and it emitted **nothing**. Reason: the bounded source produces all events in milliseconds and the job finishes long before a 5-second wall-clock timer fires, so the window never triggers. Real windowing on bounded/finite data needs **event time + watermarks**, which fire deterministically on end-of-input. That processing-time-vs-event-time distinction is the core of stream processing — so windowing is v1, done properly with event time.

## Where this is going (roadmap)

- [x] **v0 — hello world:** bounded source, keyed running sum, on a real Flink cluster. _(this)_
- [ ] **v1 — event-time windowing:** assign timestamps + watermarks, tumbling event-time windows per symbol (fires correctly on bounded and live data).
- [ ] **v2 — live source:** replace the bounded list with a continuous feed (SEC EDGAR API or a generated market-data stream).
- [ ] **v3 — serve + harden:** expose results via a small API; add an architecture diagram, cost notes, and a "what I'd change at 100k events/sec" section.

## Notes

- Base image `flink:1.18.1-scala_2.12-java11`; PyFlink pinned to the same `1.18.1`, image built `linux/amd64` (PyFlink's `pemja` has no ARM64 wheel, so amd64 avoids a from-source build on Apple Silicon).
- `print()` writes to the TaskManager's stdout; in this cluster that surfaces via `docker compose logs taskmanager`, not the log4j app logs.
