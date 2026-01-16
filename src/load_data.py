import pandas as pd

# Paths
TRIP_DATA_PATH = "data/raw/yellow_tripdata_2025-01.parquet"
ZONE_LOOKUP_PATH = "data/lookup/taxi_zone_lookup.csv"

def main():
    print("Loading trip data...")
    trips = pd.read_parquet(TRIP_DATA_PATH)
    print(f"Trips loaded: {len(trips)} rows")

    print("\nLoading zone lookup...")
    zones = pd.read_csv(ZONE_LOOKUP_PATH)
    print(f"Zones loaded: {len(zones)} rows")

    print("\nTrip data columns:")
    print(trips.columns)

    print("\nZone lookup preview:")
    print(zones.head())

if __name__ == "__main__":
    main()
