# Experimental Pipeline

The system consists of four main stages:

Data Generation (Relevance-aware)

Script:
src/data_preparation/generate_parquet_q1.py
src/data_preparation/generate_parquet.py

## Workflow

Purpose:
Generate two parquet files:

setup_db.parquet → initial database state
insert_batch.parquet → incremental update batch

## Flow

Parquet (NYC Taxi)
        ↓
Relevance Filtering (Q1)
        ↓
setup_db.parquet  → Load into Postgres
insert_batch.parquet → Incremental Insert
        ↓
Materialized View (MV)    Incremental View (IVM)
        ↓                         ↓
REFRESH MV           pgivm.refresh_immv
        ↓                         ↓
Compare execution time


