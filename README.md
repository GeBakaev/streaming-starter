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

### Run v2 (live Kafka source)

v2 needs Kafka, which `docker-compose.yml` starts alongside Flink. Steps 1–3 return straight away.
Steps 4 and 5 keep running, so use two terminals (run 5 in a second one):

```bash
# 1. Build and start the docker containers
docker compose up -d --build

# 2. Create the `trades` topic (Kafka has no persistent storage here, so topics
#    are lost when the container is recreated). Safe to run every time.
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --if-not-exists --topic trades --partitions 1 --replication-factor 1

# 3. Submit the job.
docker compose exec jobmanager flink run -d -py /opt/job/job_v2.py

# 4. Start the producer: one trade every ~200 ms, with deliberate stragglers
docker compose exec jobmanager python /opt/job/producer.py

# 5. Watch window summaries and late trades
docker compose logs -f --since 1m taskmanager | grep -E 'AAPL|MSFT|GOOG|LATE'
```

The job never finishes on its own (the source is unbounded). Stop the producer with Ctrl+C and cancel the job in the Flink UI (http://localhost:8081 → the job → Cancel) before resubmitting, otherwise two copies run and every line prints twice.

Expected output: one summary per symbol every 10s as `(symbol, window_start, window_end, total_qty, n_trades)`, plus about two late trades per window:

```
LATE: ('AAPL', 1, 1792766696859)
LATE: ('GOOG', 8, 1792766701952)
(MSFT,1792766710000,1792766720000,116,19)
(GOOG,1792766710000,1792766720000,68,12)
(AAPL,1792766710000,1792766720000,42,8)
```

## What it does

```
synthetic trade events  →  keyBy(symbol)  →  running sum(qty)  →  print
```

`job.py` is ~30 lines and deliberately minimal — source, keying, stateful aggregation, sink. The real work in Phase 3 is a swap-in (live feed + event-time windowing), not a rebuild.

## The one thing this taught me (worth knowing)

### v0: a processing-time tumbling window.

The first version used a **processing-time** tumbling window and it emitted **nothing**. Reason: the bounded source produces all events in milliseconds and the job finishes long before a 5-second wall-clock timer fires, so the window never triggers. Real windowing on bounded/finite data needs **event time + watermarks**, which fire deterministically on end-of-input.

### v1: a watermark is a periodic signal, not a per-event calculation

I predicted the out-of-order trade `("MSFT", 9, 3s)` would be dropped as late: it arrives after
a trade at 26s, far past the 5s bound. It was counted instead (MSFT [0,10) = 17, not 8).

Why: Flink emits watermarks on a wall-clock timer (`pipeline.auto-watermark-interval`, default
200 ms), not after every record. The bounded source pushed all 12 trades through in a few
milliseconds, before the first tick, so the watermark never advanced mid-stream.

### v2: with a live source, late data is real

- **What I ran:** `producer.py` → Kafka → Flink, 10s event-time windows, 5s out-of-orderness bound. Every 10th trade is stamped 3s in the past, every 25th 20s in the past.
- **What happened:** The 3s stragglers were absorbed into their windows. The 20s stragglers went to the late-data side output (~2 per window), and each window's trade count dropped by exactly that much.
- **Contrast with v1:** There the bounded source finished before the first watermark tick, so the clock never moved and nothing was late. With a live source the watermark advances between trades, and the theory finally matches reality.

## Where this is going (roadmap)

- [x] **v0 — hello world:** bounded source, keyed running sum, on a real Flink cluster. _(this)_
- [x] **v1 — event-time windowing:** assign timestamps + watermarks, tumbling event-time windows per symbol (fires correctly on bounded and live data).
- [x] **v2 — live source:** Kafka feed from `producer.py` (with out-of-order stragglers and an `account_id`), late trades routed to a side output instead of silently dropped. _(`job_v2.py`, `producer.py`)_
- [ ] **v3 — the lake:** sink to **Apache Iceberg** tables on MinIO (S3-compatible) via a REST catalog, partitioned by day, with account IDs **HMAC-hashed** before they land.
- [ ] **v4 — the model:** dbt + DuckDB star schema (`fct_trade`, `dim_symbol`, `dim_date`, `dim_account` as SCD2), with tests and a reconciliation check against the producer's control totals.
- [ ] **v5 — serve + harden:** architecture diagram, a small query API, an "AWS mapping" with cost estimates, and a "what I'd change at 100k events/sec" section.
- [ ] **v6 — Spark on the same lake:** PySpark batch rollup, Iceberg compaction, late-data backfill.

## Notes

- Base image `flink:1.18.1-scala_2.12-java11`; PyFlink pinned to the same `1.18.1`, image built `linux/amd64` (PyFlink's `pemja` has no ARM64 wheel, so amd64 avoids a from-source build on Apple Silicon).
