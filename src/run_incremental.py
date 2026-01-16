from pathlib import Path
import pandas as pd
import time

from simulate_changes import (
    simulate_inserts,
    simulate_updates,
    simulate_deletes,
)
from incremental_update import update_aggregates


# -------------------------------
# Paths
# -------------------------------
BASE_DIR = Path(__file__).resolve().parents[1]
TRIP_DATA_PATH = BASE_DIR / "data" / "raw" / "yellow_tripdata_2025-01.parquet"
ZONE_LOOKUP_PATH = BASE_DIR / "data" / "lookup" / "taxi_zone_lookup.csv"


# -------------------------------
# Data loading & preparation
# -------------------------------
def load_and_prepare():
    trips = pd.read_parquet(TRIP_DATA_PATH)
    zones = pd.read_csv(ZONE_LOOKUP_PATH)

    trips["tpep_pickup_datetime"] = pd.to_datetime(
        trips["tpep_pickup_datetime"]
    )

    trips = trips.merge(
        zones,
        left_on="PULocationID",
        right_on="LocationID",
        how="left"
    )

    trips["pickup_hour"] = trips["tpep_pickup_datetime"].dt.hour

    return trips


# -------------------------------
# Full recomputation (baseline)
# -------------------------------
def compute_full_aggregate(trips):
    return (
        trips
        .groupby(["Borough", "Zone", "pickup_hour"])
        .agg(
            trip_count=("total_amount", "count"),
            total_revenue=("total_amount", "sum")
        )
        .reset_index()
    )


# -------------------------------
# Recompute derived columns
# -------------------------------
def recompute_derived_columns(trips):
    trips = trips.copy()
    trips["pickup_hour"] = trips["tpep_pickup_datetime"].dt.hour
    return trips


# -------------------------------
# Main experiment
# -------------------------------
def main():
    print("Loading and preparing data...")
    trips = load_and_prepare()

    # --- Full recompute ---
    print("Computing baseline aggregate (full recompute)...")
    start = time.time()
    baseline_agg = compute_full_aggregate(trips)
    full_time = time.time() - start
    print(f"Full recompute time: {full_time:.2f}s")

    # --- Incremental processing ---
    print("\nSimulating incremental changes...")
    agg = baseline_agg.copy()

    start = time.time()

    # INSERTS
    inserts = simulate_inserts(trips, n=5000)
    agg = update_aggregates(agg, inserts, sign=+1)

    # UPDATES (delete + insert)
    trips_updated, update_idx = simulate_updates(trips.copy(), n=5000)
    old_rows = trips.loc[update_idx]
    new_rows = trips_updated.loc[update_idx]

    agg = update_aggregates(agg, old_rows, sign=-1)
    agg = update_aggregates(agg, new_rows, sign=+1)

    # DELETES
    trips_after_delete, delete_idx = simulate_deletes(trips_updated, n=5000)
    deleted_rows = trips_updated.loc[delete_idx]
    agg = update_aggregates(agg, deleted_rows, sign=-1)

    incremental_time = time.time() - start
    print(f"Incremental update time: {incremental_time:.2f}s")

    # -------------------------------
    # Build FINAL dataset correctly
    # -------------------------------
    final_trips = trips.copy()

    # apply inserts
    final_trips = pd.concat(
        [final_trips, inserts],
        ignore_index=True
    )

    # apply updates
    final_trips.loc[update_idx] = new_rows

    # apply deletes
    final_trips = final_trips.drop(delete_idx)

    # 🔴 critical: recompute derived attributes
    final_trips = recompute_derived_columns(final_trips)

    # -------------------------------
    # Validation
    # -------------------------------
    print("\nValidating correctness...")
    final_full = compute_full_aggregate(final_trips)

    comparison = agg.merge(
        final_full,
        on=["Borough", "Zone", "pickup_hour"],
        suffixes=("_inc", "_full"),
        how="outer"
    ).fillna(0)

    comparison["trip_diff"] = (
        comparison["trip_count_inc"] - comparison["trip_count_full"]
    ).abs()

    comparison["revenue_diff"] = (
        comparison["total_revenue_inc"] - comparison["total_revenue_full"]
    ).abs()

    max_trip_diff = comparison["trip_diff"].max()
    max_revenue_diff = comparison["revenue_diff"].max()

    print(f"Max trip count difference: {max_trip_diff}")
    print(f"Max revenue difference: {max_revenue_diff:.6f}")

    if max_trip_diff == 0 and max_revenue_diff < 1e-6:
        print("✅ Incremental results match full recomputation")
    else:
        print("❌ Mismatch detected")

    print("\nSpeedup factor:")
    print(f"{full_time / incremental_time:.2f}x faster than full recompute")


if __name__ == "__main__":
    main()
