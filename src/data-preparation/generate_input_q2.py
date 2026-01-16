import pandas as pd
import os

"""
DATA GENERATION DOCUMENTATION - Q2
----------------------------------
FOLDER NAMING CONVENTION:
    q2_is{initial_size}_bs{batch_size}_ir{initial_relevant_rate}_br{batch_relevant_rate}
    - is: Initial Size (total rows in state_current)
    - bs: Batch Size (total rows in state_next)
    - ir: Initial Relevance Rate (0.0 to 1.0)
    - br: Batch Relevance Rate (0.0 to 1.0)

FILE DESCRIPTIONS:
    - state_current.parquet: The 'baseline' dataset.
    - state_next.parquet:    The 'new' incoming batch. 
                             Rows are strictly disjoint from state_current.
"""

# --- PANDAS DISPLAY SETTINGS ---
# (For previews of generated parquet files)
pd.set_option('display.max_columns', None)
pd.set_option('display.expand_frame_repr', False)
pd.set_option('display.width', 2000)


def generate_q2(source_parquet, initial_size, batch_size, initial_relevant_rate, batch_relevant_rate):
    output_dir = f"q2_is{initial_size}_bs{batch_size}_ir{int(100 * initial_relevant_rate)}_br{int(100 * batch_relevant_rate)}"

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Load Data
    full_data = pd.read_parquet(source_parquet)

    int_cols = ['passenger_count', 'payment_type', 'RatecodeID', 'vendorID', 'PULocationID', 'DOLocationID']
    for col in int_cols:
        if col in full_data.columns:
            full_data[col] = full_data[col].astype('Int64')

    # Define the boolean mask for SQL logic
    is_relevant = (
            (full_data['passenger_count'] < 4) &
            (full_data['fare_amount'] > 20.0) &
            (full_data['payment_type'] != 4)
    )

    # Split the pool
    relevant_pool = full_data[is_relevant].copy()
    irrelevant_pool = full_data[~is_relevant].copy()

    def get_disjoint_sample(pool, n):
        if n > len(pool):
            raise ValueError(f"Requested {n} rows, but only {len(pool)} available in pool.")
        sample = pool.sample(n=n)
        updated_pool = pool.drop(sample.index)
        return sample, updated_pool

    # 1. Generate Current State
    curr_rel_n = int(initial_size * initial_relevant_rate)
    curr_irrel_n = initial_size - curr_rel_n

    curr_rel_df, relevant_pool = get_disjoint_sample(relevant_pool, curr_rel_n)
    curr_irrel_df, irrelevant_pool = get_disjoint_sample(irrelevant_pool, curr_irrel_n)

    current_state_df = pd.concat([curr_rel_df, curr_irrel_df]).reset_index(drop=True)

    # Generate Next Batch (Strictly disjoint from current)
    next_rel_n = int(curr_rel_n * batch_relevant_rate)
    next_irrel_n = batch_size - next_rel_n

    next_rel_df, relevant_pool = get_disjoint_sample(relevant_pool, next_rel_n)
    next_irrel_df, irrelevant_pool = get_disjoint_sample(irrelevant_pool, next_irrel_n)

    next_state_df = pd.concat([next_rel_df, next_irrel_df]).reset_index(drop=True)

    # Save to Parquet
    current_path = os.path.join(output_dir, "state_current.parquet")
    next_path = os.path.join(output_dir, "state_next.parquet")
    schema_path = os.path.join(output_dir, "empty_db_schema.parquet")

    current_state_df.to_parquet(current_path)
    next_state_df.to_parquet(next_path)
    full_data.iloc[:0].to_parquet(schema_path)

    # Previews
    print(f"Project directory created: {output_dir}")
    print("\n" + "=" * 80)
    print(f"PREVIEW: STATE CURRENT (Total: {len(current_state_df)})")
    print("=" * 80)
    print(current_state_df.head(10))

    print("\n" + "=" * 80)
    print(f"PREVIEW: STATE NEXT (Total: {len(next_state_df)})")
    print("=" * 80)
    print(next_state_df.head(10))
    print("=" * 80 + "\n")


# --- Execution ---
SOURCE = "taxi-data/yellow_tripdata_2025-05.parquet"

# Example:
# Data base hold 8 records of which 4 (= 8 * 0.5) are relevant for the view
# Afterwards we insert a batch of 2 records where 1 (= 4 * 0.25) is relevant to the view

generate_q2(
    source_parquet=SOURCE,
    initial_size=8,
    batch_size=2,
    initial_relevant_rate=0.5,
    batch_relevant_rate=0.25
)