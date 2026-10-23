"""
v2 producer — a fake live market-data feed into Kafka.

Sends one trade every ~200 ms to the `trades` topic as JSON:
    {"symbol": "AAPL", "qty": 7, "ts": 1759240000123}

Some trades are deliberately stamped in the past (stragglers), so the Flink
job has real out-of-order and late data to deal with.

Run it (inside the Flink container, which has kafka-python installed):
    docker compose exec jobmanager python /opt/job/producer.py
Stop it with Ctrl+C.
"""

import json
import random
import time

from kafka import KafkaProducer

TOPIC = "trades"
SYMBOLS = ["AAPL", "MSFT", "GOOG"]

producer = KafkaProducer(
    bootstrap_servers="kafka:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)


def now_ms() -> int:
    return int(time.time() * 1000)


def main():
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
        time.sleep(0.2)


if __name__ == "__main__":
    try:
        main()
    finally:
        producer.flush()
