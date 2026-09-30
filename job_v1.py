"""
v1 — event-time tumbling windows per symbol.

    synthetic trades (with event timestamps, some out of order)
      → assign timestamps + watermarks
      → keyBy(symbol)
      → 10-second tumbling EVENT-time windows
      → per-window summary  →  print

Run it:
    docker compose up -d
    docker compose exec jobmanager flink run -py /opt/job/job_v1.py
    docker compose logs taskmanager | grep -E 'AAPL|MSFT|GOOG'

------------------------------------------------------------------------------
BEFORE YOU RUN — write your predictions here (this is the actual exercise):

  1. Which windows exist per symbol, and what is total_qty in each?
     AAPL: [0s,10s): 30
           [10s,20s): 6
           [20s,30s): 6
     MSFT: [0s,10s): 8
           [10s,20s): 8
           [20s,30s): 0
     GOOG: [0s,10s): 7
           [10s,20s): 0
           [20s,30s): 2

  2. The event ("MSFT", 9, 3s) arrives after events at 26s. With a 5s
     out-of-orderness bound, is it late? Will it be counted or dropped?
     Answer: It will def be dropped since it's way too late

  3. AFTER running: did reality match #2? If not, why not?
     Answer: It was counted, it didn't match. It's because flink sends out watermarks every 200ms.
     And the pipeline is so fast that it couldn't do that and drop the event.
------------------------------------------------------------------------------
"""

from pyflink.common import Duration, Time, WatermarkStrategy
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import ProcessWindowFunction
from pyflink.datastream.window import TumblingEventTimeWindows

# (symbol, qty, event_time_ms). Times are seconds * 1000 from an arbitrary t=0,
# so windows are [0s,10s), [10s,20s), [20s,30s).
EVENTS = [
    ("AAPL", 10, 1_000),
    ("MSFT", 5, 2_000),
    ("AAPL", 20, 4_000),
    ("GOOG", 7, 9_000),
    ("AAPL", 5, 12_000),
    ("MSFT", 3, 8_000),  # out of order, but only 4s behind the max seen
    ("MSFT", 8, 15_000),
    ("AAPL", 1, 18_000),
    ("GOOG", 2, 21_000),
    ("AAPL", 4, 26_000),
    ("MSFT", 9, 3_000),  # 23s behind the max seen — way past the bound
    ("AAPL", 2, 29_000),
]


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


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    stream = env.from_collection(EVENTS)

    # Allow events to be up to 5 seconds out of order.
    watermark_strategy = WatermarkStrategy.for_bounded_out_of_orderness(
        Duration.of_seconds(5)
    ).with_timestamp_assigner(TradeTimestampAssigner())

    # stream → assign timestamps/watermarks → key by symbol
    #        → 10s tumbling event-time window → WindowSummary
    summaries = (
        stream.assign_timestamps_and_watermarks(watermark_strategy)
        .key_by(lambda trade: trade[0])
        .window(TumblingEventTimeWindows.of(Time.seconds(10)))
        .process(WindowSummary())
    )

    summaries.print()
    env.execute("streaming-starter-v1-event-time-windows")


if __name__ == "__main__":
    main()


# --- STRETCH (after it works) -----------------------------------------------
# Late events are dropped *silently* by default. Capture them instead:
# look up `side_output_late_data(OutputTag)` and `get_side_output(...)`,
# and print late records with a "LATE:" prefix. Then try `.allowed_lateness()`
# and describe in one sentence how it differs from out-of-orderness.
