import generate_parquet as gen_input

"""
Q2: Trips with trip distance between 5 and 15 where PULocationID is known
"""
# SELECT trip_id, PULocationID, total_amount, trip_distance
#     FROM {table}
#     WHERE PULocationID IS NOT NULL
#       AND trip_distance BETWEEN 5 AND 15

def q2_relevant_filter(df):
    return (
        (df['PULocationID'].notnull()) &
        (df['trip_distance'] >= 5) &
        (df['trip_distance'] <= 15)
    )

gen_input.generate_parquet(
    query_index=2,
    source_parquet=gen_input.SOURCE,
    initial_size=8,                         # TODO: Adjust for benchmarks
    batch_size=2,                           # TODO: Adjust for benchmarks
    initial_relevant_rate=0.5,              # TODO: Adjust for benchmarks
    batch_relevant_rate=0.5,                # TODO: Adjust for benchmarks
    is_relevant_func=q2_relevant_filter
)