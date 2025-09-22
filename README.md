# Ancient Gaming Data Team Technical Assessment

This repository contains the complete data pipeline solution for the Ancient Gaming Data Team Technical Assessment, including data generation, cleaning, and DBT analytics models.

## Quick Start - Testing the Solution

### 1. Start the Environment

Navigate to the repository root and start all services:

```bash
docker compose up -d --build
```

This will start:
- Apache Airflow (webserver, scheduler, worker)
- PostgreSQL database
- Redis for task queueing
- All necessary dependencies

### 2. Access Airflow Web Interface

Open your browser and go to: **http://localhost:8080**

Login credentials:
- **Username:** `airflow`
- **Password:** `airflow`

### 3. Run the Data Pipeline

In the Airflow UI, you'll see the following DAGs:

1. **`part_1_task_1_clean_data`** - Data cleaning and loading to PostgreSQL
2. **`part_1_task_2_extend_data`** - Data generation with random timestamps
3. **`part_2_dbt_model`** - DBT analytics models
4. **`part_3_dbt_analytics`** - DBT execution pipeline
5. **`part_4_complete_pipeline`** - End-to-end pipeline orchestration

**To test the complete solution:**
- The DAGs will start automatically once docker-compose finishes startup
- If needed, you can manually trigger the `part_4_complete_pipeline` DAG for the complete end-to-end pipeline
- Or restart individual DAGs in the Airflow UI for step-by-step testing

### 4. Connect to PostgreSQL Database

The results are stored in a PostgreSQL database that you can access from your local machine.

**Connection Details:**
- **Host:** `localhost`
- **Port:** `6969`
- **Database:** `postgres` (for DBT models) or `airflow` (for Airflow metadata)
- **Username:** `airflow`
- **Password:** `airflow`
- **Schema:** `data` (where the analytics tables are created)

**Example connection string:**
```
postgresql://airflow:airflow@localhost:6969/postgres
```


### 5. Verify the Results

Once the pipeline completes, you can query the following tables in the `data` schema:

**Source Tables:**
- `data.raw_players` - Cleaned player data
- `data.raw_affiliates` - Cleaned affiliate data  
- `data.raw_transactions` - Cleaned transaction data

**Analytics Models:**
- `data.part_2_model_1_player_daily_transaction` - Daily player transactions with summary
- `data.part_2_model_2_discord_kyc_deposit_by_country` - Discord KYC deposits by country
- `data.part_2_model_3_deposit_per_player` - Top 3 deposits per player


## Project Structure

- `airflow/dags/` - Airflow DAG definitions
- `airflow/utils/` - Common utility functions
- `dbt/models/` - DBT analytics models and schema definitions
- `data/raw/` - Original sample data
- `data/expanded/` - Generated expanded datasets
- `data/transformed/` - Cleaned parquet files
