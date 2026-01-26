import pandas as pd
import os

# Display settings for previews
pd.set_option('display.max_columns', None)
pd.set_option('display.expand_frame_repr', False)
pd.set_option('display.width', 2000)

def generate_parquet(query_index, source_parquet, initial_size, batch_size, initial_relevant_rate, batch_relevant_rate,
                     is_relevant_func, verbose=False):
    output_dir = f"q{query_index}_is{initial_size}_bs{batch_size}_ir{int(100 * initial_relevant_rate)}_br{int(100 * batch_relevant_rate)}"

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Load Data
    full_data = pd.read_parquet(source_parquet)

    # Apply the passed relevance logic
    is_relevant = is_relevant_func(full_data)

    # Split the pool
    relevant_pool = full_data[is_relevant].copy()
    irrelevant_pool = full_data[~is_relevant].copy()

    def get_disjoint_sample(pool, n):
        if n > len(pool):
            raise ValueError(f"Requested {n} rows, but only {len(pool)} available in pool.")
        sample = pool.sample(n=n)
        updated_pool = pool.drop(sample.index)
        return sample, updated_pool

    # Generate initial state of the database
    curr_rel_n = int(initial_size * initial_relevant_rate)
    curr_irrel_n = initial_size - curr_rel_n

    curr_rel_df, relevant_pool = get_disjoint_sample(relevant_pool, curr_rel_n)
    curr_irrel_df, irrelevant_pool = get_disjoint_sample(irrelevant_pool, curr_irrel_n)

    # Concat and shuffle relevant and irrelevant initial rows
    setup_db_df = pd.concat([curr_rel_df, curr_irrel_df]).sample(frac=1).reset_index(drop=True)

    # Generate the batch to be inserted
    next_rel_n = int(batch_size * batch_relevant_rate)
    next_irrel_n = batch_size - next_rel_n

    next_rel_df, relevant_pool = get_disjoint_sample(relevant_pool, next_rel_n)
    next_irrel_df, irrelevant_pool = get_disjoint_sample(irrelevant_pool, next_irrel_n)

    # Concat and shuffle relevant and irrelevant batch rows
    insert_batch_df = pd.concat([next_rel_df, next_irrel_df]).sample(frac=1).reset_index(drop=True)

    # Save to parquet
    setup_path = os.path.join(output_dir, "setup_db.parquet")
    batch_path = os.path.join(output_dir, "insert_batch.parquet")

    setup_db_df.to_parquet(setup_path)
    insert_batch_df.to_parquet(batch_path)

    # Print Summary + preview (only if verbose is True)
    if verbose:
        print(f"Project directory created: {output_dir}")
        print("\n" + "=" * 80)
        print(f"PREVIEW: SETUP DB (Size: {len(setup_db_df)})")
        print("=" * 80)
        print(setup_db_df.head(10))

        print("\n" + "=" * 80)
        print(f"PREVIEW: INSERT BATCH (Size: {len(insert_batch_df)})")
        print("=" * 80)
        print(insert_batch_df.head(10))
        print("=" * 80 + "\n")

    return output_dir


# Group By: Average total amount by payment_type
Q1_SQL = f"""
SELECT 
    payment_type, 
    AVG(total_amount) AS avg_total_amount
FROM yellow_trips
WHERE trip_distance > 5
GROUP BY payment_type
"""

# Group By: Average total amount by payment_type, VendorID, passenger_count
Q2_SQL = f"""
SELECT 
    payment_type, 
    VendorID, 
    passenger_count,
    AVG(total_amount) AS avg_total_amount
FROM yellow_trips
WHERE trip_distance > 5
GROUP BY payment_type, VendorID, passenger_count
"""

# 5 Group By: Average total amount by payment_type, VendorID, passenger_count, PULocationID, DOLocationID
Q3_SQL = f"""
SELECT 
    payment_type, 
    VendorID, 
    passenger_count, 
    PULocationID, 
    DOLocationID,
    AVG(total_amount) AS avg_total_amount
FROM yellow_trips
WHERE trip_distance > 5
GROUP BY payment_type, VendorID, passenger_count, PULocationID, DOLocationID
"""


def relevant_filter(df):
    return (
        (df['trip_distance'] > 5)
    )

QUERY_SUITE = [
    (Q1_SQL, relevant_filter),
    (Q2_SQL, relevant_filter),
    (Q3_SQL, relevant_filter)
]

SOURCE = "data/raw/yellow_tripdata_2025-05.parquet"