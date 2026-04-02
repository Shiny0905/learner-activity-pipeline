# Learner Activity Data Pipeline

A working demonstration of a production-grade data pipeline built as part of a Data Engineer take-home exercise for the University of Michigan Center for Academic Innovation.

## What This Demonstrates

This pipeline showcases the core patterns required for reliable, secure, and scalable data ingestion into Snowflake:

- **Staging + Merge pattern** instead of destructive Truncate and Load
- **Idempotent loads** — running the pipeline twice produces the same result, no duplicates
- **Credentials via environment variables** — no secrets ever hardcoded in source code
- **Structured logging** with error handling and per-record failure recovery
- **Clean separation** of extraction and loading steps

## Pipeline Architecture
```
[Scheduler]
    -> [Python: Fetch API (paginated) -> extract records]
    -> [Snowflake: INSERT INTO staging_table]
    -> [Snowflake: MERGE staging INTO production ON natural_key]
    -> [Snowflake: TRUNCATE staging_table]
```

Each step is a checkpoint. Failure at any stage leaves the production table intact and queryable by downstream consumers.

## Why This Design

The original AI-suggested design had three critical flaws:

| Problem | Risk | Fix Applied |
|---|---|---|
| 5M rows loaded into Matillion memory | Crashes agent, no recovery point | Stage to cloud storage first |
| Truncate and Load pattern | Production table empty on failure | Staging + MERGE pattern |
| API key hardcoded in script | Security violation, not rotatable | Environment variables |

## Tech Stack

- **Python 3.14** — pipeline orchestration and API extraction
- **Snowflake** — cloud data warehouse
- **Open-Meteo API** — free public API used as stand-in data source
- **snowflake-connector-python** — Snowflake Python client

## Setup

### 1. Install dependencies
```bash
pip install snowflake-connector-python requests
```

### 2. Set environment variables
```bash
# Windows
setx SNOWFLAKE_USER "your_username"
setx SNOWFLAKE_PASSWORD "your_password"
setx SNOWFLAKE_ACCOUNT "your_account_identifier"

# Mac/Linux
export SNOWFLAKE_USER="your_username"
export SNOWFLAKE_PASSWORD="your_password"
export SNOWFLAKE_ACCOUNT="your_account_identifier"
```

### 3. Set up Snowflake
Run this in your Snowflake worksheet:
```sql
USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;
CREATE DATABASE IF NOT EXISTS PIPELINE_DB;
CREATE SCHEMA IF NOT EXISTS PIPELINE_DB.PIPELINE_SCHEMA;
USE DATABASE PIPELINE_DB;
USE SCHEMA PIPELINE_SCHEMA;
CREATE TABLE IF NOT EXISTS WEATHER_DATA (
    ID           NUMBER AUTOINCREMENT PRIMARY KEY,
    CITY         VARCHAR(100),
    COUNTRY      VARCHAR(10),
    TEMPERATURE  FLOAT,
    HUMIDITY     INT,
    WEATHER_DESC VARCHAR(200),
    WIND_SPEED   FLOAT,
    LOADED_AT    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### 4. Run the pipeline
```bash
python pipeline.py
```

## Idempotency Proof

Running the pipeline twice produces exactly 10 rows — not 20. The MERGE pattern ensures existing records are updated, not duplicated.
```sql
-- Run this after two pipeline executions
SELECT COUNT(*) FROM PIPELINE_DB.PIPELINE_SCHEMA.WEATHER_DATA;
-- Result: 10 (not 20)
```

## Notes

This pipeline uses weather data as a stand-in for learner activity logs. The architecture, patterns, and code are identical to what would be used in production — only the API endpoint and schema would change.
