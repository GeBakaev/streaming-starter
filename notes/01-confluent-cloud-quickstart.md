# 01 — Flink SQL on Confluent Cloud (quickstart)

_2026-09-30 · [Confluent quickstart](https://developer.confluent.io/quickstart/flink-on-confluent-cloud/)_

## What I did

Spun up a Flink compute pool on Confluent Cloud and ran Flink SQL from the CLI.
Every SQL command is submitted as a **statement**: a job that Flink schedules on the pool
(`PENDING → RUNNING/COMPLETED`).

## 1. Create a table and insert rows

```sql
CREATE TABLE quickstart(message STRING);
INSERT INTO quickstart VALUES ('hello'), ('world');
SELECT * FROM quickstart;
```

- On Confluent Cloud a table is backed by a **Kafka topic** (plus a schema), so `CREATE TABLE` creates the topic.
- The INSERT was slow to return. That's **not watermarks**: each statement gets
  provisioned and run as a job on the compute pool, and that startup is most of the wait.
  Watermarks only matter for event-time operations like windows.

## 2. Windowed aggregation: orders per day

```sql
-- Use the ts column as event time. Watermark = max ts seen (zero out-of-orderness).
ALTER TABLE shoe_orders MODIFY WATERMARK FOR ts AS ts;

SELECT window_start, COUNT(*) AS order_count
FROM TABLE(TUMBLE(TABLE shoe_orders, DESCRIPTOR(ts), INTERVAL '1' DAY))
GROUP BY window_start;
```

- Before the ALTER, the table's event time was Confluent's default (`$rowtime`, the Kafka record timestamp).
  The ALTER switches event time to the business timestamp `ts`.
- `WATERMARK FOR ts AS ts` means **no tolerance for out-of-order data**: any order older than
  the newest one seen is late. A tolerant version would be `ts - INTERVAL '5' SECOND`.
- Watching the results, new days appeared step by step: a day's window only
  **fires once the watermark passes the end of that day**.

## Same idea, two APIs (SQL vs. my PyFlink job)

| Concept           | Flink SQL                                       | PyFlink DataStream (`job_v1.py`)                                         |
| ----------------- | ----------------------------------------------- | ------------------------------------------------------------------------ |
| Event-time column | `WATERMARK FOR ts ...`                          | `TimestampAssigner.extract_timestamp`                                    |
| Out-of-orderness  | `ts - INTERVAL '5' SECOND`                      | `WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(5))` |
| Zero tolerance    | `ts AS ts`                                      | `WatermarkStrategy.for_monotonous_timestamps()`                          |
| Tumbling window   | `TUMBLE(..., DESCRIPTOR(ts), INTERVAL '1' DAY)` | `TumblingEventTimeWindows.of(Time.days(1))`                              |
| Per-window result | `GROUP BY window_start` + `COUNT(*)`            | `key_by` + `window` + `ProcessWindowFunction`                            |
