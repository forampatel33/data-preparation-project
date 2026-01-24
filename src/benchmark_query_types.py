#!/usr/bin/env python3
import time
import statistics as stats
import io
from pathlib import Path
from typing import Optional

import pandas as pd
import psycopg
import pyarrow.parquet as pq

from data_preparation.parquet_loader import create_base_table, create_mv_and_ivm, normalize_data_frame

# Database connection parameters for local Postgres instance
DB = dict(
    dbname="nyc_taxi", 
    user="postgres", 
    host="/var/run/postgresql"
)

# parquet file path
PARQUET_PATH = Path("data/raw/yellow_tripdata_2025-01.parquet")

VIEWS_QUERY = """
              SELECT 
                  PULocationID,
                  COUNT(*) as trip_count,
                  AVG(trip_distance) as avg_distance,
                  AVG(fare_amount) as avg_fare,
                  SUM(total_amount) as total_revenue,
                  AVG(passenger_count) as avg_passengers
              FROM yellow_trips
              WHERE PULocationID IS NOT NULL
              GROUP BY PULocationID
              """

# queries to benchmark
QUERIES = [
    ("""
     SELECT SUM(PULocationID),
            COUNT(*) AS trips,
            SUM(total_amount) AS revenue
     FROM yellow_trips
     """),
]

# schema for the yellow_trips table
SCHEMA_SQL = """ 
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
"""

# time the execution of a query and fetchone
def time_exec_refresh(conn, sql: str, view_type) -> float:
  if view_type == "mv":
    start_time = time.perf_counter()
    with conn.cursor() as cur:
        cur.execute(sql)
        res = cur.fetchone()
        cur.execute("REFRESH MATERIALIZED VIEW trip_stats_mv;")

    return time.perf_counter() - start_time, res

  elif view_type == "ivm":
    start_time = time.perf_counter()
    with conn.cursor() as cur:
        cur.execute(sql)
        res = cur.fetchone()
        cur.execute("SELECT pgivm.refresh_immv('trip_stats_ivm', true);")
        
    return time.perf_counter() - start_time, res


def main() -> None:
    conn = psycopg.connect(**DB)
    try:
        create_base_table(conn, PARQUET_PATH)
        create_mv_and_ivm(conn, VIEWS_QUERY)

        print("\nStarting incremental insert benchmark...\n")
        parquet_file = pq.ParquetFile(str(PARQUET_PATH))
        batch_id = 0
        for batch in parquet_file.iter_batches(batch_size=200_000):
            batch_id += 1
            batch_df = normalize_data_frame(batch.to_pandas())

            mv_t = time_incremental_refresh(conn, batch_df, "mv")
            ivm_t = time_incremental_refresh(conn, batch_df, "ivm")

            print(f"Batch {batch_id}")
            print(f"Rows inserted: {len(batch_df)}")
            print(f"MV refresh:  {mv_t*1000:.2f} ms")
            print(f"IVM refresh: {ivm_t*1000:.2f} ms")
            print(f"Speedup:     {mv_t/ivm_t:.2f}x")
            print("-"*60)

        # for query in QUERIES:
            
        #     mv_t, mv_res = time_exec_refresh(conn, query, "mv")
        #     ivm_t, ivm_res = time_exec_refresh(conn, query, "ivm")
        #     print(f"\nQuery benchmark results for:\n")
        #     print(f"MV result:  {mv_res}")
        #     print(f"IVM result: {ivm_res}")
        #     # print(query.strip())
        #     print(f"\nMV query:  {mv_t*1000:.2f} ms")
        #     print(f"IVM query: {ivm_t*1000:.2f} ms \n")
        #     print(f"Speedup:   {mv_t/ivm_t:.2f}x\n")
        #     print("-"*60)    
            
    finally:
        conn.close()

def time_incremental_refresh(conn, batch_df: pd.DataFrame, view_type: str):
    from data_preparation.parquet_loader import insert_batch

    # 1) Insert new batch
    insert_batch(conn, batch_df)

    # 2) Refresh view and measure
    if view_type == "mv":
        start = time.perf_counter()
        with conn.cursor() as cur:
            cur.execute("REFRESH MATERIALIZED VIEW trip_stats_mv;")
        return time.perf_counter() - start

    elif view_type == "ivm":
        start = time.perf_counter()
        with conn.cursor() as cur:
            cur.execute("SELECT pgivm.refresh_immv('trip_stats_ivm', true);")
        return time.perf_counter() - start

if __name__ == "__main__":
    main()

# initial parquet chunk insertion
