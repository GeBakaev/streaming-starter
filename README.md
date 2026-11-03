# streaming-starter

A small **streaming lakehouse** for market data, built step by step and run entirely in Docker: a live trade feed goes through **Kafka** into **Apache Flink (PyFlink)**, which handles event time, watermarks and late data, and writes **Apache Iceberg** tables on S3-compatible storage. Account IDs are HMAC-hashed before anything lands, and the producer writes control totals so the lake can be reconciled against what was sent.

Each version (v0 → v3 so far) adds one idea, and the README records what each one taught me.

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

### Run v3 (the lake: Iceberg on S3-compatible storage)

v3 adds SeaweedFS (S3-compatible storage) and an Iceberg REST catalog, both started by `docker-compose.yml`. Same two-terminal pattern as v2:

```bash
# 1. Build and start everything
docker compose up -d --build

# 2. Create the `trades` topic (safe to run every time)
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --if-not-exists --topic trades --partitions 1 --replication-factor 1

# 3. Submit the job: ONE job writes all three Iceberg tables
docker compose exec jobmanager flink run -d -py /opt/job/job_v3.py

# 4. Start the producer (also writes per-minute control totals)
docker compose exec jobmanager python /opt/job/producer.py

# 5. After ~1 minute, in the second terminal: read the tables back from outside Flink
docker compose run --rm lake-tools python /work/check_lake.py

# 6. The producer's control totals (one line per closed minute)
cat control/control_totals.jsonl
```

Optional: copy `.env.example` to `.env` and set a real `HMAC_KEY` (the default is a clearly labelled dev-only key). To see the files Iceberg writes:
`docker compose run --rm --entrypoint aws s3-init --endpoint-url http://seaweedfs:8333 s3 ls --recursive s3://warehouse/`

Expected `check_lake.py` output (abridged): rows arrive in batches, one new snapshot per checkpoint.

```
=== lake.raw_trades ===
  snapshots (one per checkpoint commit): 9
  partition spec: [1000: trade_date: identity(5)]
  rows: 1216
=== lake.window_summaries ===
  rows: 9      (3 windows x 3 symbols)
=== lake.late_trades ===
  rows: 8      (~2 late trades per 10s window)
```

## What it does

```
synthetic trade events  →  keyBy(symbol)  →  running sum(qty)  →  print
```

`job.py` is ~30 lines and deliberately minimal — source, keying, stateful aggregation, sink. The real work in Phase 3 is a swap-in (live feed + event-time windowing), not a rebuild.

**v3, the current pipeline:**

```
producer.py ──► Kafka `trades` ──► Flink job_v3.py (one job, checkpoint every 30s)
     │                               │  parse + HMAC-SHA256(account_id)
     │                               ├──────────────────────────────► lake.raw_trades        (by trade_date)
     │                               └─► watermarks (5s) → keyBy(symbol) → 10s windows
     │                                        ├──────────────────────► lake.window_summaries (by window_date)
     │                                        └─ late side output ───► lake.late_trades      (by trade_date)
     │
     └──► control/control_totals.jsonl   (trades + qty per event-time minute: the "should have" totals)

Iceberg tables live on SeaweedFS (S3 API) · table metadata in the Iceberg REST catalog
Read back from outside Flink with PyIceberg (check_lake.py)
```

| Local piece                 | AWS equivalent                   |
| --------------------------- | -------------------------------- |
| Kafka (container)           | Amazon MSK                       |
| Flink cluster (containers)  | Managed Service for Apache Flink |
| SeaweedFS                   | Amazon S3                        |
| Iceberg REST catalog        | AWS Glue Data Catalog            |
| PyIceberg (`check_lake.py`) | Athena / Redshift Spectrum       |

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

### v3: the lake commits on checkpoints

- **Checkpoints are commits.** The Iceberg sink only commits data when a checkpoint completes, so with a 30s interval rows appear in batches of ~150 (30s × 5 trades/s). Each commit also writes new small files (~2,880 a day per table), which is why streaming lakes need compaction (planned for v6).
- **Reconcile closed periods only.** A trade lands in `raw_trades` at the next checkpoint but in `window_summaries` only once its window fires, so `raw_trades ≥ summaries + late` until the window closes. The producer's control totals follow the same rule: a minute is written only after its 20s straggler window has passed.
- **The real world:** MinIO pulled its images in 2026, so storage moved to SeaweedFS with only the endpoint changing. Flink 2.3 was out but its connectors weren't, so the project runs on Flink 2.1.

## Where this is going (roadmap)

- [x] **v0 — hello world:** bounded source, keyed running sum, on a real Flink cluster. _(this)_
- [x] **v1 — event-time windowing:** assign timestamps + watermarks, tumbling event-time windows per symbol (fires correctly on bounded and live data).
- [x] **v2 — live source:** Kafka feed from `producer.py` (with out-of-order stragglers and an `account_id`), late trades routed to a side output instead of silently dropped. _(`job_v2.py`, `producer.py`)_
- [x] **v3 — the lake:** one Flink job writes three **Apache Iceberg** tables (`raw_trades`, `window_summaries`, `late_trades`) on SeaweedFS (S3-compatible) via a REST catalog, partitioned by day, with account IDs **HMAC-hashed** before they land. The producer writes per-minute control totals. _(`job_v3.py`, `producer.py`, `check_lake.py`)_
- [ ] **v4 — the model:** dbt + DuckDB star schema (`fct_trade`, `dim_symbol`, `dim_date`, `dim_account` as SCD2), with tests and a reconciliation check against the producer's control totals.
- [ ] **v5 — serve + harden:** architecture diagram, a small query API, an "AWS mapping" with cost estimates, and a "what I'd change at 100k events/sec" section.
- [ ] **v6 — Spark on the same lake:** PySpark batch rollup, Iceberg compaction, late-data backfill.

## Notes

- Base image `flink:2.1.3-scala_2.12-java21`; PyFlink pinned to the same `2.1.3`, image built `linux/amd64` (PyFlink's `pemja` has no ARM64 Linux wheel, so amd64 avoids a from-source build on Apple Silicon).
- Why Flink 2.1 and not 2.3: as of Oct 2026, 2.1 is the newest Flink with _released_ connectors for both Kafka (`5.0.0-2.1`) and Iceberg (`iceberg-flink-runtime-2.1`). v0–v2 were first built on Flink 1.18.
