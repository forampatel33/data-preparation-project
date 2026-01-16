import pandas as pd
import numpy as np

def simulate_inserts(trips, n=5000):
    new_rows = trips.sample(n, replace=True).copy()
    new_rows["total_amount"] *= np.random.uniform(0.9, 1.1, size=n)
    new_rows["tpep_pickup_datetime"] += pd.to_timedelta(
        np.random.randint(1, 3600, size=n), unit="s"
    )
    return new_rows

def simulate_updates(trips, n=5000):
    idx = trips.sample(n).index
    trips.loc[idx, "total_amount"] *= 1.05
    return trips, idx

def simulate_deletes(trips, n=5000):
    idx = trips.sample(n).index
    return trips.drop(idx), idx
