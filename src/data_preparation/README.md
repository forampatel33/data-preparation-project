# NYC Taxi Data Preparation (`generate_parquet.py`) 

Scripts to generate benchmark input data for specific queries. 
For each query we generate data for the initial state of the database and data that will be inserted in a batch.  

---

## 1. Targeted Queries

* **Q1**: Average total_amount group by one column. 
* **Q2**: Average total_amount group by three columns. 
* **Q3**: Average total_amount group by five columns.

---

## 2. Data Generation Logic

The scripts produce two disjoint datasets to test state transitions:

1.  **(`setup_db.parquet`):** Initial records used to populate the database.
2.  **(`insert_batch.parquet`):** New records sent to the database after it has been populated. Record the insertion time of these records. 

### Usage

To generate `setup_db.parquet` and `insert_batch.parquet` adjust the following parameters: 

```
generate_parquet(
    query_index=1,                          # Index of the query (only influences the output folder name)
    source_parquet=SOURCE_FILE_PATH,        # File path of the source new york taxi ride parquet file
    initial_size=8,                         # Size of the initial state of the DB
    batch_size=2,                           # Size of the insertion batch 
    initial_relevant_rate=0.5,              # The relevancy rate of the initial size of the DB (0.5 = 50%)
    batch_relevant_rate=0.5,                # The relevancy rate of the batch (0.5 = 50%)
    is_relevant_func=relevant_func          # A function that returns if a row is relevant to the query. Used to pick relevant rows based on the relevancy                                             # rates  
)
```
`Note:` A preview (containing 10 rows) of `setup_db.parquet` and `insert_batch.parquet` are printed after file generation. 

### Output Folders
`setup_db.parquet` and `insert_batch.parquet` are stored in parameter-specific folders, for example:
`q2_is10000_bs500_ir50_br5`
* **q2**: query 2.
* **is10000**: 10,000 initial records.
* **bs500**: 500 records in the next batch.
* **ir50**: 50% relevance rate in the initial set.
* **br5**: 5% relevance rate in the batch.
