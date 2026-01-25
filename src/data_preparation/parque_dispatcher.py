import io
import time
from pathlib import Path
import pandas as pd

EXPECTED_COLS = [
    "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime",
    "passenger_count", "trip_distance", "RatecodeID", "store_and_fwd_flag",
    "PULocationID", "DOLocationID", "payment_type", "fare_amount",
    "extra", "mta_tax", "tip_amount", "tolls_amount",
    "improvement_surcharge", "total_amount", "congestion_surcharge", "Airport_fee"
]

def normalize_data_frame(df: pd.DataFrame) -> pd.DataFrame:
    if "airport_fee" in df.columns and "Airport_fee" not in df.columns:
        df = df.rename(columns={"airport_fee": "Airport_fee"})
    if "Airport_fee" not in df.columns:
        df["Airport_fee"] = 0.0
    df = df[EXPECTED_COLS].copy()
    df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"], errors="coerce")
    df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"], errors="coerce")
    ints = ["VendorID", "passenger_count", "RatecodeID", "PULocationID", "DOLocationID", "payment_type"]
    for col in ints:
        df[col] = pd.to_numeric(df[col], errors="coerce").round(0).astype("Int64")
    nums = ["trip_distance", "fare_amount", "extra", "mta_tax", "tip_amount",
            "tolls_amount", "improvement_surcharge", "total_amount", "congestion_surcharge", "Airport_fee"]
    for col in nums:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df

def copy_into(conn, df: pd.DataFrame, table: str):
    buf = io.StringIO()
    df.to_csv(buf, index=False, header=False, na_rep="\\N")
    buf.seek(0)
    with conn.cursor() as cur:
        with cur.copy(f"COPY {table} ({', '.join(EXPECTED_COLS)}) FROM STDIN WITH (FORMAT CSV, NULL '\\N')") as copy:
            copy.write(buf.getvalue())

def create_standard_mv(conn, sql, name="q1_mv"):
    with conn.cursor() as cur:
        cur.execute(f"DROP MATERIALIZED VIEW IF EXISTS {name} CASCADE")
        cur.execute(f"CREATE MATERIALIZED VIEW {name} AS {sql}")
    conn.commit()

def create_only_ivm(conn, sql, name="q1_ivm"):
    with conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {name} CASCADE")
        cur.execute("CREATE EXTENSION IF NOT EXISTS pg_ivm")
        cur.execute("SELECT pgivm.create_immv(%s, %s)", (name, sql))
    conn.commit()

def load_parquet_into_table(conn, df: pd.DataFrame):
    copy_into(conn, df, "yellow_trips")
    conn.commit()

def insert_batch(conn, df: pd.DataFrame):
    copy_into(conn, df, "yellow_trips")
    conn.commit()

def time_insert(conn, df: pd.DataFrame):
    start = time.perf_counter()
    insert_batch(conn, df)
    return time.perf_counter() - start

def time_refresh(conn, v_type, mv_name="q1_mv", ivm_name="q1_ivm"):
    start = time.perf_counter()
    with conn.cursor() as cur:
        if v_type == "mv":
            cur.execute(f"REFRESH MATERIALIZED VIEW {mv_name}")
        else:
            cur.execute(f"SELECT count(*) FROM {ivm_name}")
    conn.commit()
    return time.perf_counter() - start