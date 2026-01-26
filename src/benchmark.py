import csv
import os
import pandas as pd
import psycopg
from pathlib import Path
from data_preparation import parque_dispatcher as parq_d
from data_preparation import generate_parquet as gen_input
import shutil

DB = dict(dbname="nyc_taxi", user="postgres", host="/var/run/postgresql")


SETUP = Path("/home/max/data-preparation-project/src/data_preparation/q1_is100000_bs100_ir100_br100/setup_db.parquet")
BATCH = Path("/home/max/data-preparation-project/src/data_preparation/q1_is100000_bs100_ir100_br100/insert_batch.parquet")


def create_mv(conn, sql, name="q1_mv"):
    with conn.cursor() as cur:
        cur.execute(f"DROP MATERIALIZED VIEW IF EXISTS {name} CASCADE")
        cur.execute(f"CREATE MATERIALIZED VIEW {name} AS {sql}")
    conn.commit()


def create_ivm(conn, sql, name="q1_ivm"):
    with conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {name} CASCADE")
        cur.execute("CREATE EXTENSION IF NOT EXISTS pg_ivm")
        cur.execute("SELECT pgivm.create_immv(%s, %s)", (name, sql))
    conn.commit()


def run_benchmark(folder_path, sql_query, iterations=1, verbose=True):
    # Construct file paths
    setup_file = os.path.join(folder_path, "setup_db.parquet")
    batch_file = os.path.join(folder_path, "insert_batch.parquet")

    # Pre-load dataframes to keep file I/O out of the measurement loop
    setup_df = parq_d.normalize_data_frame(pd.read_parquet(setup_file))
    batch_df = parq_d.normalize_data_frame(pd.read_parquet(batch_file))

    # Accumulators for averages
    stats = {
        "vm_ins": 0.0, "vm_ref": 0.0,
        "ivm_ins": 0.0, "ivm_ref": 0.0
    }

    conn = psycopg.connect(**DB)
    try:
        for i in range(iterations):
            if verbose:
                print(f"--- Iteration {i + 1}/{iterations} ---")

            # --- PHASE 1: VM BENCHMARK ---
            if verbose: print("  Running VM Phase...")
            with conn.cursor() as cur:
                cur.execute("DROP TABLE IF EXISTS q1_ivm CASCADE")
                cur.execute("TRUNCATE yellow_trips CASCADE")
            conn.commit()

            parq_d.load_parquet_into_table(conn, setup_df)
            create_mv(conn, sql_query)  # Using your setup function

            stats["vm_ins"] += parq_d.time_insert(conn, batch_df)
            stats["vm_ref"] += parq_d.time_refresh(conn, "mv")

            # --- PHASE 2: IVM BENCHMARK ---
            if verbose: print("  Running IVM Phase...")
            with conn.cursor() as cur:
                cur.execute("DROP MATERIALIZED VIEW IF EXISTS q1_mv CASCADE")
                cur.execute("TRUNCATE yellow_trips CASCADE")
            conn.commit()

            parq_d.load_parquet_into_table(conn, setup_df)
            create_ivm(conn, sql_query)  # Using your setup function

            stats["ivm_ins"] += parq_d.time_insert(conn, batch_df)
            stats["ivm_ref"] += parq_d.time_refresh(conn, "ivm")

        # --- CALCULATE AVERAGES ---
        avg_vm_ins = stats["vm_ins"] / iterations
        avg_vm_ref = stats["vm_ref"] / iterations
        avg_ivm_ins = stats["ivm_ins"] / iterations
        avg_ivm_ref = stats["ivm_ref"] / iterations

        avg_vm_total = avg_vm_ins + avg_vm_ref
        avg_ivm_total = avg_ivm_ins + avg_ivm_ref
        avg_speedup = avg_vm_total / avg_ivm_total if avg_ivm_total > 0 else 0

        if verbose:
            print("\n" + "=" * 45)
            print(f"AVERAGE RESULTS OVER {iterations} RUNS")
            print(f"Folder: {folder_path}")
            print("-" * 45)
            print(f"Avg VM Total:  {avg_vm_total * 1000:10.2f} ms")
            print(f"  - Insert:    {avg_vm_ins * 1000:10.2f} ms")
            print(f"  - Refresh:   {avg_vm_ref * 1000:10.2f} ms")
            print("-" * 45)
            print(f"Avg IVM Total: {avg_ivm_total * 1000:10.2f} ms")
            print(f"  - Insert:    {avg_ivm_ins * 1000:10.2f} ms")
            print(f"  - Query:     {avg_ivm_ref * 1000:10.2f} ms")
            print("-" * 45)
            print(f"Avg Speedup:   {avg_speedup:10.2f}x")
            print("=" * 45 + "\n")

        return {
            "avg_vm_insert_ms": avg_vm_ins * 1000,
            "avg_vm_refresh_ms": avg_vm_ref * 1000,
            "avg_vm_total_ms": avg_vm_total * 1000,
            "avg_ivm_insert_ms": avg_ivm_ins * 1000,
            "avg_ivm_refresh_ms": avg_ivm_ref * 1000,
            "avg_ivm_total_ms": avg_ivm_total * 1000,
            "avg_speedup": avg_speedup
        }

    finally:
        conn.close()


def run_batch(sql_query_index, initial_size, configs, output_file_name, iterations=3, verbose=True):
    """
    sql_query_index: The SQL query index to test
    initial_size: Fixed integer for the initial number of rows in the db
    configs: List of tuples [(batch_size, batch_relevant_rate), ...]
    iterations: Number of runs per combination for averaging
    """
    results_file = f"{output_file_name}.csv"

    if os.path.exists(results_file):
        os.remove(results_file)
        if verbose:
            print(f"Old results cleared.")

    file_exists = False
    results_list = []

    print(f"\n" + "/" * 50)
    print(f"-- TESTING QUERY: {sql_query_index}")
    print("/" * 50)

    query_string, relevant_filter = gen_input.QUERY_SUITE[sql_query_index-1]

    for b_size, r_rate in configs:
        if verbose:
            print(f"\n" + "=" * 50)
            print(f"-- TESTING CONFIG: Batch Size={b_size}, Initial Relevance Rate = 1, Batch Relevance Rate={r_rate}")
            print("=" * 50)

        # 1. Generate the input files to the benchmark
        folder_path = gen_input.generate_parquet(
            query_index=1,
            source_parquet=gen_input.SOURCE,
            initial_size=initial_size,
            batch_size=b_size,
            initial_relevant_rate=1.0,
            batch_relevant_rate=r_rate,
            is_relevant_func=relevant_filter,
            verbose=False
        )

        try:
            # 2. Call the benchmark function
            bench_results = run_benchmark(
                folder_path=folder_path,
                sql_query=query_string,
                iterations=iterations,
                verbose=verbose
            )

            # 3. Prepare result row
            row = {
                "initial_size": initial_size,
                "batch_size": b_size,
                "relevant_rate": r_rate,
                "iterations": iterations,
                "avg_vm_insert_ms": bench_results["avg_vm_insert_ms"],
                "avg_vm_refresh_ms": bench_results["avg_vm_refresh_ms"],
                "avg_ivm_insert_ms": bench_results["avg_ivm_insert_ms"],
                "avg_ivm_refresh_ms": bench_results["avg_ivm_refresh_ms"],
                "speedup": bench_results["avg_speedup"]
            }

            # 4. Write to CSV
            with open(results_file, 'a', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=row.keys())
                if not file_exists:
                    writer.writeheader()
                    file_exists = True
                writer.writerow(row)

            results_list.append(row)

        finally:
            # 5.) Cleanup: Delete the folder even if the benchmark fails
            if os.path.exists(folder_path):
                if verbose:
                    print(f"Cleaning up: Removing folder {folder_path}...")
                shutil.rmtree(folder_path)

    print(f"\nBatch processing complete. Results saved to {results_file}")
    return pd.DataFrame(results_list)

if __name__ == '__main__':
    num_iters = 3

    my_configs = [
         (100, 1.0),
         (1000, 1.0),
         (10000, 1.0),
         (100000, 1.0),
    ]

    # Query 1 insertion test
    run_batch(1, 100000, my_configs, "Q1-B1", num_iters)
    # Query 2 insertion test
    run_batch(2, 100000, my_configs, "Q2-B1", num_iters)
    # Query 3 insertion test
    run_batch(3, 100000, my_configs, "Q3-B1", num_iters)

    my_configs = [
        (10000, 0.0),
        (10000, 0.25),
        (10000, 0.75),
        (10000, 1.0),
    ]

    # Query 1 relative insertion test
    run_batch(1, 100000, my_configs, "Q1-B2", num_iters)
    # Query 2 relative insertion test
    run_batch(2, 100000, my_configs, "Q2-B2", num_iters)
    # Query 3 relative insertion test
    run_batch(3, 100000, my_configs, "Q3-B2", num_iters)