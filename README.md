# Pandas in Python – Incremental Processing Workflow

## Workflow

prepare_data → full_compute → simulate_changes → incremental_update → run_incremental

---

## Files and Purpose

| File                        | Purpose / Description                                                                                                         |
| --------------------------- | ----------------------------------------------------------------------------------------------------------------------------- |
| `src/prepare_data.py`       | Load raw trip data, clean it, parse timestamps, join with zone metadata. Prepares dataset for aggregation.                    |
| `src/full_compute.py`       | Compute baseline full aggregation (trip count, total revenue) per (Borough, Zone, pickup_hour). Measures full recompute time. |
| `src/simulate_changes.py`   | Functions to simulate inserts, updates, deletes on the dataset for incremental testing.                                       |
| `src/incremental_update.py` | Function `update_aggregates` to incrementally update aggregates based on simulated changes.                                   |
| `src/run_incremental.py`    | Main experiment pipeline.                                                                                                     |
