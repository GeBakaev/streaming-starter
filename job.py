"""
PyFlink hello-world  —  source → keyBy → running sum → print.

Run it:
    docker compose up -d --build
    docker compose exec jobmanager flink run -py /opt/job/job.py
    docker compose logs taskmanager | grep -E '\\((AAPL|MSFT|GOOG)'

Why no window in v0
-------------------
A tumbling *processing-time* window (wall-clock) will NOT fire on this bounded
source: all 8 events arrive in milliseconds and the job finishes long before the
window's 5-second timer, so nothing is ever emitted. Real windowing needs
*event time* + watermarks, which fire deterministically on end-of-input — that's
v1 on the roadmap. v0 sticks to the reliable core: a keyed, stateful running sum.

The primitives here (source, keyBy, stateful aggregation, sink) are what the
Phase 3 pipeline extends — swap the bounded list for a live feed, add event-time
windowing, and the shape is the same.
"""

from pyflink.datastream import StreamExecutionEnvironment


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)  # single worker — keeps the hello-world output readable

    # --- SOURCE -------------------------------------------------------------
    # Synthetic "trade" events: (symbol, qty). Bounded here for a clean demo;
    # Phase 3 replaces this with a continuous feed.
    events = [
        ("AAPL", 1), ("MSFT", 1), ("AAPL", 1), ("AAPL", 1),
        ("MSFT", 1), ("GOOG", 1), ("AAPL", 1), ("MSFT", 1),
    ]
    stream = env.from_collection(events)

    # --- keyBy symbol → running sum of qty (emits a new total per event) ----
    counts = (
        stream
        .key_by(lambda e: e[0])
        .reduce(lambda a, b: (a[0], a[1] + b[1]))
    )

    # --- SINK ---------------------------------------------------------------
    counts.print()

    env.execute("streaming-starter-hello-world")


if __name__ == "__main__":
    main()
