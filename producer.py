"""
v2 producer — a fake live market-data feed into Kafka.

Sends one trade every ~200 ms to the `trades` topic as JSON:
    {"symbol": "AAPL", "qty": 7, "ts": 1759240000123}

Some trades are deliberately stamped in the past (stragglers), so the Flink
job has real out-of-order and late data to deal with.

v3: it also writes per-minute CONTROL TOTALS (trade count + total qty per
event-time minute) to control/control_totals.jsonl. That's the "rows you SHOULD
have" reference: Week 4 reconciles the lake against it.

Run it (inside the Flink container, which has kafka-python installed):
    docker compose exec jobmanager python /opt/job/producer.py
Stop it with Ctrl+C.
"""

import json
import os
import random
import time

from kafka import KafkaProducer

TOPIC = "trades"
SYMBOLS = ["AAPL", "MSFT", "GOOG"]

# Control totals land in the repo folder (mounted at /opt/job), git-ignored.
CONTROL_FILE = "/opt/job/control/control_totals.jsonl"
MAX_STRAGGLER_MS = 20_000  # the oldest a trade's ts can be when it's sent

producer = KafkaProducer(
    bootstrap_servers="kafka:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)


def now_ms() -> int:
    return int(time.time() * 1000)


def add_to_totals(totals: dict, trade: dict) -> None:
    minute_start = trade["ts"] - (trade["ts"] % 60_000)
    if totals.get(minute_start) is None:
        totals[minute_start] = {"n_trades": 0, "total_qty": 0}
    totals[minute_start]["n_trades"] += 1
    totals[minute_start]["total_qty"] += trade["qty"]


def flush_closed_minutes(totals: dict) -> None:
    now = now_ms()
    closed_minutes = []
    for minute_start in list(totals.keys()):
        if now > minute_start + 60_000 + MAX_STRAGGLER_MS:
            closed_minutes.append(minute_start)

    if closed_minutes:
        os.makedirs(os.path.dirname(CONTROL_FILE), exist_ok=True)
        with open(CONTROL_FILE, "a") as f:
            for minute_start in closed_minutes:
                total = totals[minute_start]
                record = {
                    "minute_start": minute_start,
                    "n_trades": total["n_trades"],
                    "total_qty": total["total_qty"],
                }
                f.write(json.dumps(record) + "\n")
                del totals[minute_start]


def main():
    totals = {}
    i = 0
    while True:
        i += 1

        # build a trade
        trade = {
            "account_id": "account_" + str(random.randint(1, 50)),
            "symbol": random.choice(SYMBOLS),
            "qty": random.randint(1, 10),
            "ts": now_ms(),
        }

        # make some trades stragglers
        if i % 25 == 0:
            trade["ts"] -= 20000  # 20 seconds in the past
        elif i % 10 == 0:
            trade["ts"] -= 3000  # 3 seconds in the past

        # send it to Kafka, print it, and sleep 200 ms
        producer.send(TOPIC, trade)
        print(f"produced: {trade}")

        # write in the control totals
        add_to_totals(totals, trade)
        flush_closed_minutes(totals)

        time.sleep(0.2)


if __name__ == "__main__":
    try:
        main()
    finally:
        producer.flush()
