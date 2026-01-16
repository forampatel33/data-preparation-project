import pandas as pd

def update_aggregates(agg, rows, sign=+1):
    rows = rows.copy()

    # recompute derived attribute
    rows["pickup_hour"] = rows["tpep_pickup_datetime"].dt.hour

    delta_agg = (
        rows
        .groupby(["Borough", "Zone", "pickup_hour"])
        .agg(
            trip_count=("total_amount", "count"),
            total_revenue=("total_amount", "sum")
        )
        .reset_index()
    )

    delta_agg["trip_count"] *= sign
    delta_agg["total_revenue"] *= sign

    updated = pd.concat([agg, delta_agg], ignore_index=True)

    updated = (
        updated
        .groupby(["Borough", "Zone", "pickup_hour"], as_index=False)
        .sum()
    )

    return updated
