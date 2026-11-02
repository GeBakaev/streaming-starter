"""
v3: the lake. Kafka trades → Flink → Apache Iceberg tables on SeaweedFS (S3-compatible).

    Kafka topic `trades` (JSON)
      → parse; account_id replaced by HMAC-SHA256(account_id, HMAC_KEY)
      ├──► iceberg.lake.raw_trades         (every trade, partitioned by trade_date)
      └──► watermarks → key_by(symbol) → 10s event-time windows
             ├──► iceberg.lake.window_summaries   (one row per symbol per window)
             └──► side output ──► iceberg.lake.late_trades   (late data is kept, never dropped)

All three INSERTs run as ONE Flink job (a statement set), committing on every checkpoint.

Run it (two terminals):
    docker compose up -d --build              # rebuild once: adds the Iceberg jars
    docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
        --create --if-not-exists --topic trades --partitions 1 --replication-factor 1
    docker compose exec jobmanager flink run -d -py /opt/job/job_v3.py
    docker compose exec jobmanager python /opt/job/producer.py
    # after ~1 minute, in the second terminal:
    docker compose run --rm lake-tools python /work/check_lake.py

See the files Iceberg writes:
    docker compose run --rm --entrypoint aws s3-init --endpoint-url http://seaweedfs:8333 s3 ls --recursive s3://warehouse/
"""

import hashlib
import hmac
import json
import os
from datetime import datetime, timezone

from pyflink.common import Duration, Row, Time, Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import OutputTag, StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource
from pyflink.table import StreamTableEnvironment
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream.functions import ProcessWindowFunction
from pyflink.datastream.window import TumblingEventTimeWindows


def raw_trade_type():
    # A function, not a constant: every caller gets a fresh type object
    # (the v2 lesson: shared type objects can break PyFlink's pickling).
    return Types.ROW_NAMED(
        ["symbol", "qty", "ts", "account_hash", "trade_date"],
        [Types.STRING(), Types.INT(), Types.LONG(), Types.STRING(), Types.SQL_DATE()],
    )


def summary_type():
    return Types.ROW_NAMED(
        [
            "symbol",
            "window_start",
            "window_end",
            "total_qty",
            "n_trades",
            "window_date",
        ],
        [
            Types.STRING(),
            Types.LONG(),
            Types.LONG(),
            Types.INT(),
            Types.INT(),
            Types.SQL_DATE(),
        ],
    )


def hash_account(account_id: str) -> str:
    hash_key = os.environ.get("HMAC_KEY")
    if hash_key is None:
        raise RuntimeError("HMAC_KEY env var is not set")

    return hmac.new(hash_key.encode(), account_id.encode(), hashlib.sha256).hexdigest()


def parse_trade(raw: str) -> Row:
    trade_data = json.loads(raw)
    account_hash = hash_account(trade_data["account_id"])
    trade_date = datetime.fromtimestamp(trade_data["ts"] / 1000, tz=timezone.utc).date()
    return Row(
        trade_data["symbol"],
        trade_data["qty"],
        trade_data["ts"],
        account_hash,
        trade_date,
    )


# From V2: windowing and watermarking.
class TradeTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp) -> int:
        return value[2]


class WindowSummary(ProcessWindowFunction):
    def process(self, key, context, elements):
        total_qty = sum(e[1] for e in elements)
        n_trades = len(elements)
        window_date = datetime.fromtimestamp(
            context.window().start / 1000, tz=timezone.utc
        ).date()
        yield Row(
            key,
            context.window().start,
            context.window().end,
            total_qty,
            n_trades,
            window_date,
        )


def main():
    # Flink streaming environment: parallelism=1, checkpointing every 30s
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.enable_checkpointing(30_000)

    t_env = StreamTableEnvironment.create(env)

    # Kafka source: same as v2
    source = (
        KafkaSource.builder()
        .set_bootstrap_servers("kafka:9092")
        .set_topics("trades")
        .set_group_id("flink-trades-v3")
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )
    raw = env.from_source(source, WatermarkStrategy.no_watermarks(), "kafka-trades")
    trades = raw.map(parse_trade, output_type=raw_trade_type())

    # --- The window pipeline (from v2) --------------------------------
    late_tag = OutputTag(
        "late-trades", raw_trade_type()
    )  # fresh type object: avoids the v2 pickling bug
    summaries = (
        trades.assign_timestamps_and_watermarks(
            WatermarkStrategy.for_bounded_out_of_orderness(
                Duration.of_seconds(5)
            ).with_timestamp_assigner(TradeTimestampAssigner())
        )
        .key_by(lambda r: r[0])
        .window(TumblingEventTimeWindows.of(Time.seconds(10)))
        .side_output_late_data(late_tag)
        .process(WindowSummary(), output_type=summary_type())
    )
    late_trades = summaries.get_side_output(late_tag)

    # --- Register the Iceberg catalog ---------------------------------
    t_env.execute_sql("""
        CREATE CATALOG iceberg WITH (
            'type' = 'iceberg',
            'catalog-type' = 'rest',
            'uri' = 'http://iceberg-rest:8181',
            'warehouse' = 's3://warehouse/',
            'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
            's3.endpoint' = 'http://seaweedfs:8333',
            's3.path-style-access' = 'true'
        )
    """)

    # --- Create the database and the table ----------------------------
    # 1. CREATE DATABASE IF NOT EXISTS iceberg.lake
    # 2. CREATE TABLE IF NOT EXISTS iceberg.lake.raw_trades (...)
    #    columns: symbol STRING, qty INT, ts BIGINT, account_hash STRING, trade_date DATE
    #    partitioned by trade_date.
    #    Flink's DDL can't use Iceberg's hidden partitioning (days(ts)), which is
    #    why trade_date exists as a real column.
    t_env.execute_sql("CREATE DATABASE IF NOT EXISTS iceberg.lake")
    t_env.execute_sql("""
        CREATE TABLE IF NOT EXISTS iceberg.lake.raw_trades (
            symbol STRING,
            qty INT,
            ts BIGINT,
            account_hash STRING,
            trade_date DATE
        ) PARTITIONED BY (trade_date)
    """)

    # 1. iceberg.lake.window_summaries
    #      symbol STRING, window_start BIGINT, window_end BIGINT,
    #      total_qty INT, n_trades INT, window_date DATE
    #      partitioned by window_date
    # 2. iceberg.lake.late_trades
    #      same columns as raw_trades, partitioned by trade_date
    # Use CREATE TABLE IF NOT EXISTS, like raw_trades.
    t_env.execute_sql("""
        CREATE TABLE IF NOT EXISTS iceberg.lake.window_summaries (
            symbol STRING,
            window_start BIGINT,
            window_end BIGINT,
            total_qty INT,
            n_trades INT,
            window_date DATE
        ) PARTITIONED BY (window_date)
    """)
    t_env.execute_sql("""
        CREATE TABLE IF NOT EXISTS iceberg.lake.late_trades (
            symbol STRING,
            qty INT,
            ts BIGINT,
            account_hash STRING,
            trade_date DATE
        ) PARTITIONED BY (trade_date)
    """)

    # --- All three INSERTs as ONE job --------------------------------
    #   1. Register two more views: "summaries_view" (summaries) and "late_view" (late_trades).
    #   2. stmt = t_env.create_statement_set()
    #   3. stmt.add_insert_sql("INSERT INTO ... SELECT ... FROM ...") once per table (3x).
    #      Change the raw_trades INSERT below from execute_sql to add_insert_sql.
    #      Columns are matched by position: list them in each table's order.
    #   4. stmt.attach_as_datastream()   # attach the inserts to the DataStream job
    #   5. env.execute("streaming-starter-v3-lake")   # ONE job for everything
    # Check: the Flink UI shows one job whose graph ends in three sinks.
    t_env.create_temporary_view("trades_view", trades)
    t_env.create_temporary_view("summaries_view", summaries)
    t_env.create_temporary_view("late_view", late_trades)
    stmt = t_env.create_statement_set()
    stmt.add_insert_sql("""
        INSERT INTO iceberg.lake.raw_trades
        SELECT symbol, qty, ts, account_hash, trade_date
        FROM trades_view
    """)
    stmt.add_insert_sql("""
        INSERT INTO iceberg.lake.window_summaries
        SELECT symbol, window_start, window_end, total_qty, n_trades, window_date
        FROM summaries_view
    """)
    stmt.add_insert_sql("""
        INSERT INTO iceberg.lake.late_trades
        SELECT symbol, qty, ts, account_hash, trade_date
        FROM late_view
    """)
    stmt.attach_as_datastream()
    env.execute("streaming-starter-v3-lake")


if __name__ == "__main__":
    main()
