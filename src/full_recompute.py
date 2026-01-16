from pathlib import Path
import pandas as pd
import time

BASE_DIR = Path(__file__).resolve().parents[1]
TRIP_DATA_PATH = BASE_DIR / "data" / "raw" / "yellow_tripdata_2025-01.parquet"
ZONE_LOOKUP_PATH = BASE_DIR / "data" / "lookup" / "taxi_zone_lookup.csv"

def main():
    start = time.time()

    trips = pd.read_parquet(TRIP_DATA_PATH)
    zones = pd.read_csv(ZONE_LOOKUP_PATH)

    trips["tpep_pickup_datetime"] = pd.to_datetime(trips["tpep_pickup_datetime"])
    trips = trips.merge(
        zones,
        left_on="PULocationID",
        right_on="LocationID",
        how="left"
    )

    trips["pickup_hour"] = trips["tpep_pickup_datetime"].dt.hour

    result = (
        trips
        .groupby(["Borough", "Zone", "pickup_hour"])
        .agg(
            trip_count=("total_amount", "count"),
            total_revenue=("total_amount", "sum")
        )
        .reset_index()
    )

    elapsed = time.time() - start
    print(result.head())
    print(f"Full recompute time: {elapsed:.2f} seconds")

if __name__ == "__main__":
    main()
