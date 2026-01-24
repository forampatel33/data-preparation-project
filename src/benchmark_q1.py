import time
import pandas as pd
import psycopg
from pathlib import Path
from data_preparation.parquet_loader import normalize_data_frame

from data_preparation.parquet_loader import (
    load_parquet_into_table,
    insert_batch,
    create_mv_and_ivm
)

DB = dict(dbname="nyc_taxi", user="postgres", host="/var/run/postgresql")

Q1_SQL = """
SELECT payment_type, AVG(tip_amount) AS avg_tip
FROM yellow_trips
WHERE passenger_count = 1
GROUP BY payment_type
"""

SETUP = Path("q1_is100000_bs10000_ir50_br25/setup_db.parquet")
BATCH = Path("q1_is100000_bs10000_ir50_br25/insert_batch.parquet")

def time_refresh(conn, view_type):
    start = time.perf_counter()
    with conn.cursor() as cur:
        if view_type == "mv":
            cur.execute("REFRESH MATERIALIZED VIEW q1_mv;")
        else:
            cur.execute("SELECT pgivm.refresh_immv('q1_ivm', true);")
    return time.perf_counter() - start

def main():
    conn = psycopg.connect(**DB)
    try:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE yellow_trips;")
        conn.commit()

        load_parquet_into_table(conn, SETUP)
        create_mv_and_ivm(conn, Q1_SQL, mv_name="q1_mv", ivm_name="q1_ivm")

        batch_df = pd.read_parquet(BATCH)
        batch_df = normalize_data_frame(pd.read_parquet(BATCH))
        insert_batch(conn, batch_df)

        mv_t = time_refresh(conn, "mv")
        ivm_t = time_refresh(conn, "ivm")

        print("Q1 Incremental Benchmark")
        print(f"Rows inserted: {len(batch_df)}")
        print(f"MV refresh:  {mv_t*1000:.2f} ms")
        
        print(f"IVM refresh: {ivm_t*1000:.2f} ms")
        print(f"Speedup:     {mv_t/ivm_t:.2f}x")

    finally:
        conn.close()

if __name__ == "__main__":
    main()
