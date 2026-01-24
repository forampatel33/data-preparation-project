import io
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

# Batch size for reading parquet in chunks
PARQUET_BATCH_ROWS = 200_000

# the expected columns in the parquet file
EXPECTED_COLS = [
    "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime",
    "passenger_count", "trip_distance", "RatecodeID", "store_and_fwd_flag",
    "PULocationID", "DOLocationID", "payment_type", "fare_amount",
    "extra", "mta_tax", "tip_amount", "tolls_amount",
    "improvement_surcharge", "total_amount", "congestion_surcharge", "Airport_fee"
]


def normalize_data_frame(data_frame: pd.DataFrame) -> pd.DataFrame:

    # rename airport_fee -> Airport_fee if needed
    if "airport_fee" in data_frame.columns and "Airport_fee" not in data_frame.columns:
        data_frame = data_frame.rename(columns={"airport_fee": "Airport_fee"})
    
    # add the missing Airport_fee column if needed
    if "Airport_fee" not in data_frame.columns:
        data_frame["Airport_fee"] = 0.0 

    # keep only the expected columns (in the right order)
    data_frame = data_frame[EXPECTED_COLS].copy()

    # datetime columns
    data_frame["tpep_pickup_datetime"] = pd.to_datetime(data_frame["tpep_pickup_datetime"], errors="coerce")
    data_frame["tpep_dropoff_datetime"] = pd.to_datetime(data_frame["tpep_dropoff_datetime"], errors="coerce")

    # integer columns
    integer_columns = ["VendorID", "passenger_count", "RatecodeID", "PULocationID", "DOLocationID", "payment_type"]
    for column in integer_columns:
        data_frame[column] = pd.to_numeric(data_frame[column], errors="coerce").round(0).astype("Int64")

    # store_and_fwd_flag should be 'Y' or 'N' or NULL
    data_frame["store_and_fwd_flag"] = data_frame["store_and_fwd_flag"].astype("string")
    data_frame.loc[~data_frame["store_and_fwd_flag"].isin(["Y", "N"]), "store_and_fwd_flag"] = None

    # numeric columns
    numeric_columns = [
        "trip_distance", "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount",
        "improvement_surcharge", "total_amount", "congestion_surcharge", "Airport_fee"
    ]
    for column in numeric_columns:
        data_frame[column] = pd.to_numeric(data_frame[column], errors="coerce")

    return data_frame


def copy_into(conn, data_frame: pd.DataFrame, table: str) -> None:
    buffer = io.StringIO()
    data_frame.to_csv(buffer, index=False, header=False, na_rep="\\N")
    buffer.seek(0)

    # prepare the COPY SQL statement
    columns_sql = ", ".join(EXPECTED_COLS)
    copy_sql = f"COPY {table} ({columns_sql}) FROM STDIN WITH (FORMAT CSV, NULL '\\N')"

    # copy the data into the table
    with conn.cursor() as cur:
        with cur.copy(copy_sql) as copy:
            copy.write(buffer.getvalue())
    conn.commit()


def create_base_table(conn, parquet_path: Path, batch_rows: int = PARQUET_BATCH_ROWS) -> None:

    # if the parquet file does not exist, raise an error
    if not parquet_path.exists():
        raise FileNotFoundError(f"Parquet not found: {parquet_path}")

    # create the yellow_trips table if it doesn't exist
    with conn.cursor() as cursor:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS yellow_trips (
                trip_id BIGSERIAL PRIMARY KEY,
                VendorID INTEGER,
                tpep_pickup_datetime TIMESTAMP,
                tpep_dropoff_datetime TIMESTAMP,
                passenger_count INTEGER,
                trip_distance NUMERIC(10,2),
                RatecodeID INTEGER,
                store_and_fwd_flag CHAR(1),
                PULocationID INTEGER,
                DOLocationID INTEGER,
                payment_type INTEGER,
                fare_amount NUMERIC(10,2),
                extra NUMERIC(10,2),
                mta_tax NUMERIC(10,2),
                tip_amount NUMERIC(10,2),
                tolls_amount NUMERIC(10,2),
                improvement_surcharge NUMERIC(10,2),
                total_amount NUMERIC(10,2),
                congestion_surcharge NUMERIC(10,2),
                Airport_fee NUMERIC(10,2)
            );
        """)
        cursor.execute("TRUNCATE yellow_trips;")
    conn.commit()

    # read the parquet file in batches (as set by PARQUET_BATCH_ROWS) and load into both tables
    parquet_file = pq.ParquetFile(str(parquet_path))
    for batch in parquet_file.iter_batches(batch_size=batch_rows):
        data_frame = normalize_data_frame(batch.to_pandas())

        # copy the same batch into both tables
        copy_into(conn, data_frame, "yellow_trips")

        # commit after each batch
        conn.commit()

def create_mv_and_ivm(conn, query_template: str, mv_name="trip_stats_mv", ivm_name="trip_stats_ivm"):

    # The query template should already reference yellow_trips, not need formatting
    # Just use it as-is for both MV and IVM
    view_query = query_template

    with conn.cursor() as cur:
        cur.execute(f"DROP MATERIALIZED VIEW IF EXISTS {mv_name} CASCADE;")
        cur.execute(f"DROP TABLE IF EXISTS {ivm_name} CASCADE;")
    conn.commit()

    with conn.cursor() as cur:
        cur.execute(f"CREATE MATERIALIZED VIEW {mv_name} AS {query_template};")
    conn.commit()

    with conn.cursor() as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS pg_ivm;")
        cur.execute("SELECT pgivm.create_immv(%s, %s);", (ivm_name, query_template))
    conn.commit()

# initial parquet chunk insertion

def insert_batch(conn, data_frame: pd.DataFrame) -> None:
    """
    Incrementally insert a batch of rows into yellow_trips.
    """
    copy_into(conn, data_frame, "yellow_trips")
    conn.commit()

def load_parquet_into_table(conn, parquet_path: Path, batch_rows: int = PARQUET_BATCH_ROWS):
    parquet_file = pq.ParquetFile(str(parquet_path))
    for batch in parquet_file.iter_batches(batch_size=batch_rows):
        df = normalize_data_frame(batch.to_pandas())
        copy_into(conn, df, "yellow_trips")
    conn.commit()
