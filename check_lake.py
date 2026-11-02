"""
Read the Iceberg tables back, outside Flink, to prove the data landed.

    docker compose run --rm lake-tools python /work/check_lake.py

Uses PyIceberg (a pure-Python Iceberg client) against the same REST catalog
and S3 store (SeaweedFS) that Flink writes to. This is the same idea as querying the lake from
Athena while Flink writes from another service.
"""

import os

from pyiceberg.catalog import load_catalog

catalog = load_catalog(
    "lake",
    **{
        "type": "rest",
        "uri": "http://iceberg-rest:8181",
        "s3.endpoint": "http://seaweedfs:8333",
        "s3.access-key-id": os.environ.get("S3_ACCESS_KEY", "admin"),
        "s3.secret-access-key": os.environ.get("S3_SECRET_KEY", "password"),
        "s3.region": "us-east-1",
    },
)

print("Namespaces:", catalog.list_namespaces())

for table_name in ["lake.raw_trades", "lake.window_summaries", "lake.late_trades"]:
    print(f"\n=== {table_name} ===")
    try:
        table = catalog.load_table(table_name)
    except Exception as e:  # table not created yet
        print(f"  (not found: {type(e).__name__})")
        continue

    snapshots = table.snapshots()
    print(f"  snapshots (one per checkpoint commit): {len(snapshots)}")
    print(f"  partition spec: {table.spec()}")

    data = table.scan().to_arrow()
    print(f"  rows: {data.num_rows}")
    if data.num_rows:
        print(data.slice(max(0, data.num_rows - 5)).to_pandas().to_string(index=False))
