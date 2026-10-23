"""
v2 — the v1 pipeline on a live Kafka source, with late data captured.

    Kafka topic `trades` (JSON)
      → parse to (symbol, qty, ts)
      → timestamps + 5s watermark      (same as v1)
      → keyBy(symbol) → 10s tumbling event-time windows → WindowSummary   (same as v1)
      → print summaries
      → late trades → side output → print with "LATE:" prefix

Run it (two terminals):
    docker compose up -d --build        # rebuild once: adds the Kafka connector
    docker compose exec jobmanager flink run -d -py /opt/job/job_v2.py
    docker compose exec jobmanager python /opt/job/producer.py
    docker compose logs -f taskmanager | grep -E 'AAPL|MSFT|GOOG'

This job never finishes on its own (the source is unbounded). Cancel it from
the Flink UI (http://localhost:8081 → job → Cancel).
"""

import json

from pyflink.common import Duration, Time, Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import OutputTag, StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource
from pyflink.datastream.functions import ProcessWindowFunction
from pyflink.datastream.window import TumblingEventTimeWindows

# Same tuple shape as v1, so the v1 classes work unchanged.
TRADE_TYPE = Types.TUPLE([Types.STRING(), Types.INT(), Types.LONG()])
TRADE_TUPLE = Types.TUPLE([Types.STRING(), Types.INT(), Types.LONG()])


# Flink calls extract_timestamp() once per record. Return epoch millis (int).
class TradeTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp) -> int:
        return value[2]


# Called once per (key, window) when the window fires. `elements` is every
# record that landed in that window for that key.
# Emit ONE tuple: (symbol, window_start_ms, window_end_ms, total_qty, n_trades)
class WindowSummary(ProcessWindowFunction):
    def process(self, key, context, elements):
        total_qty = sum(e[1] for e in elements)
        n_trades = len(elements)
        yield (key, context.window().start, context.window().end, total_qty, n_trades)


def parse_trade(raw: str):
    trade_dict = json.loads(raw)
    return (trade_dict["symbol"], trade_dict["qty"], trade_dict["ts"])


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    # build the Kafka source
    # KafkaSource.builder() with: bootstrap servers "kafka:9092", topic "trades",
    # a group id, start from the LATEST offsets, and SimpleStringSchema() as the
    # value-only deserializer. Then turn it into a stream with
    # env.from_source(source, WatermarkStrategy.no_watermarks(), "kafka-trades")
    # (no watermarks here — you assign them after parsing, like v1).
    source = (
        KafkaSource.builder()
        .set_bootstrap_servers("kafka:9092")
        .set_topics("trades")
        .set_group_id("flink-trades")
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )
    raw = env.from_source(
        source=source,
        watermark_strategy=WatermarkStrategy.no_watermarks(),
        source_name="kafka-trades",
    )

    # parse the JSON string to a (symbol, qty, ts) tuple and assign timestamps + watermarks
    trades = raw.map(parse_trade, output_type=TRADE_TYPE)

    # the pipeline is the same as v1, except we capture late trades in a side output.
    late_tag = OutputTag("late-trades", TRADE_TUPLE)
    summaries = (
        trades.assign_timestamps_and_watermarks(
            WatermarkStrategy.for_bounded_out_of_orderness(
                Duration.of_seconds(5)
            ).with_timestamp_assigner(TradeTimestampAssigner())
        )
        .key_by(lambda t: t[0])
        .window(TumblingEventTimeWindows.of(Time.seconds(10)))
        .side_output_late_data(late_tag)
        .process(
            WindowSummary(),
            output_type=Types.TUPLE(
                [Types.STRING(), Types.LONG(), Types.LONG(), Types.INT(), Types.INT()]
            ),
        )
    )

    summaries.print()

    # print the late trades
    summaries.get_side_output(late_tag).map(
        lambda t: f"LATE: {t}", output_type=Types.STRING()
    ).print()

    env.execute("streaming-starter-v2-kafka")


if __name__ == "__main__":
    main()
