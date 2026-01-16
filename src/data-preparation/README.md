# NYC Taxi Data Preparation

Scripts to generate benchmark input data for specific queries. 
For each query we generate data for the initial state of the database and data that will be inserted in a batch.  

---

## 1. Targeted Queries

* **Q1 (Aggregation):** Calculates `AVG(tip_amount)` grouped by `payment_type` where `fare_amount > 0`.
* **Q2 (Filtering + with clause):** Selects trips where `passenger_count < 4`, `fare_amount > 20.0`, and `payment_type != 4`.

---

## 2. Data Generation Logic

The scripts produce two disjoint datasets to test state transitions:

1.  **Current State (`state_current.parquet`):** Initial records used to populate the database.
2.  **Next Batch (`state_next.parquet`):** New records sent to the database after the initial load.



### Usage

* **Q1 Generation:** Run `generate_input_q1.py`. It slices the source file sequentially to create the initial set and the subsequent batch.
* **Q2 Generation:** Run `generate_input_q2.py`. This script samples data based on a specific **relevance rate** (the % of rows that satisfy the SQL `WHERE` clause) for both the initial state and the batch. Note, for the batch the relevance rate is relative to the number of relevant records in the initial state.  

### Output Folders
Results are stored in parameter-specific folders, for example:
`q2_is10000_bs500_ir50_br5`
* **q2**: query 2
* **is10000**: 10,000 initial records.
* **bs500**: 500 records in the next batch.
* **ir50**: 50% relevance rate in the initial set.
* **br5**: 5% relevance rate in the batch.