import generate_parquet as gen_input

"""
Q3: (trip_distance > 3) trips revenue breakdown (excluding unknown payment types) by Pickup Location 
"""
# WITH base AS (
#     SELECT
#         PULocationID,
#         total_amount,
#         fare_amount,
#         tip_amount,
#         tolls_amount,
#         congestion_surcharge,
#         trip_distance
#     FROM {table}
#     WHERE PULocationID IS NOT NULL
#       AND trip_distance > 3
#       AND payment_type != 5
# )
# SELECT
#     PULocationID,
#     COUNT(*) AS trips,
#     SUM(total_amount) AS revenue,
#     SUM(fare_amount) AS base_fare_total,
#     SUM(tip_amount) AS tips_total,
#     SUM(tolls_amount) AS tolls_total,
#     SUM(congestion_surcharge) AS congestion_fees_total,
#     AVG(total_amount) AS avg_total_per_trip
# FROM base
# GROUP BY PULocationID

def q3_relevant_filter(df):
    return (
        (df['PULocationID'].notnull()) &
        (df['trip_distance'] > 3) &
        (df['payment_type'] != 5)
    )

gen_input.generate_parquet(
    query_index=3,
    source_parquet=gen_input.SOURCE,
    initial_size=8,                         # TODO: Adjust for benchmarks
    batch_size=2,                           # TODO: Adjust for benchmarks
    initial_relevant_rate=0.5,              # TODO: Adjust for benchmarks
    batch_relevant_rate=0.5,                # TODO: Adjust for benchmarks
    is_relevant_func=q3_relevant_filter
)