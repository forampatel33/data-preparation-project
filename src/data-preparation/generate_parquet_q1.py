import generate_parquet as gen_input

"""
Q1: Average tips by payment type made by individuals 
"""
# SELECT payment_type, AVG(tip_amount) AS avg_tip
#     FROM {table}
#     WHERE passenger_count = 1
#     GROUP BY payment_type

def q1_relevant_filter(df):
    return (
        (df['passenger_count'] == 1)
    )

gen_input.generate_parquet(
    query_index=1,
    source_parquet=gen_input.SOURCE,
    initial_size=8,                         # TODO: Adjust for benchmarks
    batch_size=2,                           # TODO: Adjust for benchmarks
    initial_relevant_rate=0.5,              # TODO: Adjust for benchmarks
    batch_relevant_rate=0.5,                # TODO: Adjust for benchmarks
    is_relevant_func=q1_relevant_filter
)