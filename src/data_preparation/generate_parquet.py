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


Q1_SQL = f"""
SELECT 
    payment_type, 
    AVG(tip_amount) AS avg_tip
FROM yellow_trips
WHERE passenger_count = 1
GROUP BY payment_type
"""

Q2_SQL = """
SELECT 
    trip_id, 
    PULocationID, 
    total_amount, 
    trip_distance
FROM yellow_trips
WHERE PULocationID IS NOT NULL
  AND trip_distance BETWEEN 5 AND 15
"""

Q3_SQL = """
         SELECT PULocationID, \
                DOLocationID, \
                DATE_TRUNC('day', tpep_pickup_datetime)           as trip_day, 
                COUNT(*)                                          as num_trips, 
                SUM(total_amount)                                 as gross_revenue, 
                SUM(tip_amount)                                   as total_tips, 
                AVG(trip_distance)                                as avg_dist, 
                SUM(CASE WHEN payment_type = 1 THEN 1 ELSE 0 END) as credit_card_count, 
                SUM(CASE WHEN payment_type = 2 THEN 1 ELSE 0 END) as cash_count, 
                SUM(fare_amount + extra + mta_tax)                as base_costs_total
         FROM yellow_trips
         WHERE trip_distance > 5
         GROUP BY 1, 2, 3 \
         """


def q1_relevant_filter(df):
    return (
        (df['passenger_count'] == 1)
    )

def q2_relevant_filter(df):
    return (
        (df['PULocationID'].notnull()) &
        (df['trip_distance'] >= 5) &
        (df['trip_distance'] <= 15)
    )

def q3_relevant_filter(df):
    return (
        (df['trip_distance'] > 5)
    )

QUERY_SUITE = [
    (Q1_SQL, q1_relevant_filter),
    (Q2_SQL, q2_relevant_filter),
    (Q3_SQL, q3_relevant_filter)
]

SOURCE = "data/yellow_tripdata_2025-05.parquet"