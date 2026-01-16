import pandas as pd
import os

"""
DATA GENERATION DOCUMENTATION - Q1
----------------------------------
FOLDER NAMING CONVENTION:
    q1_is{initial_size}_bs{batch_size}
    - is: Initial Size (the first N rows of the source file)
    - bs: Batch Size (the next M rows following the initial set)

FILE DESCRIPTIONS:
    - state_current.parquet: The 'baseline' dataset (initial database state).
    - state_next.parquet:    The 'new' incoming batch for the transition.
                             Rows are strictly disjoint from state_current.
"""

# --- PANDAS DISPLAY SETTINGS ---
# (For previews of generated parquet files)
pd.set_option('display.max_columns', None)
pd.set_option('display.expand_frame_repr', False)
pd.set_option('display.width', 2000)


def generate_q1(source_parquet, initial_size, batch_size):
    # Setup Folder naming (initial size of database and size of arriving batch)
    output_dir = f"q1_is{initial_size}_bs{batch_size}"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Load Data
    full_data = pd.read_parquet(source_parquet)

    int_cols = ['payment_type', 'passenger_count', 'RatecodeID', 'vendorID', 'PULocationID', 'DOLocationID']
    for col in int_cols:
        if col in full_data.columns:
            full_data[col] = full_data[col].astype('Int64')

    # Initial state contains first initial_size rows
    current_state_df = full_data.iloc[0: initial_size]

    # Next state contains the batch_size rows (Strictly after current state)
    next_state_df = full_data.iloc[initial_size: initial_size + batch_size]

    # Define Paths
    current_path = os.path.join(output_dir, "state_current.parquet")
    next_path = os.path.join(output_dir, "state_next.parquet")
    schema_path = os.path.join(output_dir, "empty_db_schema.parquet")

    # Save to Parquet
    current_state_df.to_parquet(current_path)
    next_state_df.to_parquet(next_path)
    full_data.iloc[:0].to_parquet(schema_path)

    # Previews
    print(f"Project directory created: {output_dir}")
    print("\n" + "=" * 80)
    print(f"PREVIEW: STATE CURRENT (Size: {len(current_state_df)})")
    print("=" * 80)
    print(current_state_df.head(10))

    print("\n" + "=" * 80)
    print(f"PREVIEW: STATE NEXT (Size: {len(next_state_df)})")
    print("=" * 80)
    print(next_state_df.head(10))
    print("=" * 80 + "\n")


# --- Execution ---
SOURCE = "taxi-data/yellow_tripdata_2025-05.parquet"

# Example: Database starts at 10,000 rows, then we insert 500 rows.
generate_q1(
    source_parquet=SOURCE,
    initial_size=10000,
    batch_size=500
)