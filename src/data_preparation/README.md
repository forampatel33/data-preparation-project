# NYC Taxi Data Preparation

Scripts to generate benchmark input data for specific queries. 
For each query we generate data for the initial state of the database and data that will be inserted in a batch.  

---

## 1. Targeted Queries

* **Q1**: Average tips by payment type made by individuals 
* **Q2**: Trips with trip distance between 5 and 15 where PULocationID is known
* **Q3**: (trip_distance > 3) trips revenue breakdown (excluding unknown payment types) by Pickup Location

---

## 2. Data Generation Logic

The scripts produce two disjoint datasets to test state transitions:

1.  **(`setup_db.parquet`):** Initial records used to populate the database.
2.  **(`insert_batch.parquet`):** New records sent to the database after it has been populated. Record the insertion time of these records. 

### Usage

* **Q1 Generation:** Run `generate_input_q1.py`. 
* **Q2 Generation:** Run `generate_input_q2.py`. 
* **Q3 Generation:** Run `generate_input_q3.py`.

For each query adjust sizes and relevancy parameters. These have the following format: 
```
gen_input.generate_parquet(
    query_index=1,
    source_parquet=gen_input.SOURCE,
    initial_size=8,                         # TODO: Adjust for benchmarks
    batch_size=2,                           # TODO: Adjust for benchmarks
    initial_relevant_rate=0.5,              # TODO: Adjust for benchmarks
    batch_relevant_rate=0.5,                # TODO: Adjust for benchmarks
    is_relevant_func=q1_relevant_filter
)
```
`Note:` A preview (containing 10 rows) of the parquet files is printed after file generation. 

### Output Folders
Results are stored in parameter-specific folders, for example:
`q2_is10000_bs500_ir50_br5`
* **q2**: query 2.
* **is10000**: 10,000 initial records.
* **bs500**: 500 records in the next batch.
* **ir50**: 50% relevance rate in the initial set.
* **br5**: 5% relevance rate in the batch.

## ------------ BENCHMARKING TASKS --------------

### Benchmarking Methodology (to be deleted)

#### 0.) Zero baseline Benchmark 

Fix r_db = 1 and r_batch = 1. Then:
- Do insertion on empty database for each query
- Vary the batch size
- Report Insertion and Query times for both ivm and vm
#### 1.) First benchmark
=> Reuse the batch size range from 0.) Find out witch initial databe size produces interesting results.  

Fix r_db = 1 and r_batch = 1. Then:
- Fix the initial db size
- Go trough a range of batch sizes such that we get values for ivm that are both faster and slower than vm
- Report Insertion and Query times for both ivm and vm

#### 2.) Second benchmark  

Fix db size, fix query size such that for r_db = 0.5 and r_batch = 0.5 the speedup is around 1. Then:
- Vary the r_batch size (0.0, 1.0] -> We expect ivm to perform worse as we increase r_batch
- Report Insertion and Query times for both ivm and vm

### Reporting results (to be deleted)

- Create three plots for benchmark for 0, 1, 2 showing speedups (where each plot contains all three queries)
- For 0 baseline create one plot how much was spent on insertion and how much on refresh for vm and ivm 