from pathlib import Path
import pandas as pd

BASE_DIR = Path(__file__).resolve().parents[1]
TRIP_DATA_PATH = BASE_DIR / "data" / "raw" / "yellow_tripdata_2025-01.parquet"
ZONE_LOOKUP_PATH = BASE_DIR / "data" / "lookup" / "taxi_zone_lookup.csv"

def main():
    print("Loading data...")
    trips = pd.read_parquet(TRIP_DATA_PATH)
    zones = pd.read_csv(ZONE_LOOKUP_PATH)

    # Timestamp parsing
    trips["tpep_pickup_datetime"] = pd.to_datetime(trips["tpep_pickup_datetime"])
    trips["tpep_dropoff_datetime"] = pd.to_datetime(trips["tpep_dropoff_datetime"])

    # Numeric enforcement
    trips["trip_distance"] = trips["trip_distance"].astype(float)
    trips["total_amount"] = trips["total_amount"].astype(float)

    # Basic data quality filters
    trips = trips[
        (trips["trip_distance"] > 0) &
        (trips["total_amount"] > 0)
    ]

    print(f"Trips after cleaning: {len(trips)}")

    # Join pickup zone metadata
    trips = trips.merge(
        zones,
        left_on="PULocationID",
        right_on="LocationID",
        how="left"
    )

    print("Joined with zone metadata")
    print(trips[["Borough", "Zone"]].head())

if __name__ == "__main__":
    main()
