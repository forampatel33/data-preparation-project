# Experimental Pipeline

<!-- The system consists of four main stages:

Data Generation (Relevance-aware)

Script:
src/data_preparation/generate_parquet_q1.py
src/data_preparation/generate_parquet.py

### Workflow

Purpose:
Generate two parquet files:

setup_db.parquet → initial database state
insert_batch.parquet → incremental update batch

### Flow

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
Compare execution time -->

## Setup Instructions

### Prerequisites: PostgreSQL and pg_ivm Extension

Follow these steps to set up PostgreSQL with the Incremental View Maintenance (IVM) extension:

#### 1. Install PostgreSQL and Development Tools
```bash
sudo apt update
sudo apt install postgresql postgresql-server-dev-all build-essential git
```

#### 2. Verify PostgreSQL Installation
```bash
psql --version
```

#### 3. Download the pg_ivm Extension
```bash
git clone https://github.com/sraoss/pg_ivm.git
```

#### 4. Navigate to the Extension Directory
```bash
cd pg_ivm/
```

#### 5. Build and Install the Extension
```bash
make
sudo make install
```

#### 6. (OPTIONAL) Configure PostgreSQL Authentication
Edit the PostgreSQL authentication configuration file. Make sure to change `[postgres version]` to the version from the output of `psql --version`:
```bash
sudo nano /etc/postgresql/[postgres version]/main/pg_hba.conf
```
(e.g. `psql (PostgreSQL) 16.11 (Ubuntu 16.11-0ubuntu0.24.04.1)` becomes `/etc/postgresql/16/main/pg_hba.conf`)

Change the line:
```
local   all   postgres   peer
```

To one of:
- `local   all   postgres   trust` (for passwordless access)
- `local   all   postgres   scram-sha-256` (for secure password access)

After making changes, restart PostgreSQL:
```bash
sudo systemctl restart postgresql
```

#### 7. Create the Database
Open the PostgreSQL CLI:
- If you followed step 6: `psql -U postgres`
- If you skipped step 6: `sudo -u postgres psql`

Create the database:
```sql
CREATE DATABASE nyc_taxi;
```

#### 8. Install Required Python libraries
```bash
pip3 install -r requirements.txt
```

#### 9. Run the Test Script
```bash
python3 src/postgrestest.py
```



