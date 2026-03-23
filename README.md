# NYC Building Permits ETL Pipeline

A production-style ETL pipeline that extracts New York City building permit data from the NYC Open Data API, transforms it using Apache PySpark, and loads it into a Snowflake cloud data warehouse.

---

## Pipeline Architecture
```
NYC Open Data API → PySpark (local) → Snowflake (cloud warehouse)
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Extraction | Python, Requests, Socrata API |
| Transformation | Apache PySpark 4.1 |
| Loading | Snowflake, snowflake-connector-python |
| Language | Python 3.13 |
| Environment | Jupyter Notebooks |
| Version Control | Git, GitHub |

---

## Dataset

- **Source:** NYC Department of Buildings — DOB Permit Issuance
- **API:** NYC Open Data (Socrata)
- **Volume:** 50,000 most recent permit records
- **Coverage:** All five NYC boroughs

---

## Pipeline Stages

### Extract (`01_extract.ipynb`)
- Connects to the Socrata API endpoint
- Retrieves 50,000 permit records ordered by issuance date
- Performs data quality assessment across all columns
- Saves raw data as a CSV checkpoint

### Transform (`02_transform.ipynb`)
- Initializes a local PySpark session
- Standardizes column names to snake_case
- Enforces schema — timestamps, strings, doubles
- Handles missing values with standardized defaults
- Adds derived fields: `permit_year`, `permit_month`, `days_to_expiration`, `is_residential`
- Saves cleaned data as Parquet

### Load (`03_load.ipynb`)
- Connects to Snowflake via environment variables
- Creates `NYC_PERMITS` database and `BUILDING_PERMITS` table
- Loads 50,000 rows using `write_pandas`
- Verifies row count post-load

---

## Key Engineering Decisions

- **Parquet checkpointing** between transform and load stages preserves data integrity and enables reprocessing without re-calling the API
- **Environment variables** via `.env` keep credentials out of source control
- **`try_to_timestamp`** used over strict casting to handle inconsistent date formats in government data
- **Column pruning** from 60 to 20 source columns reduces processing overhead and improves downstream usability
- Pipeline structured for Airflow orchestration in a production deployment

---

## Setup & Reproduction

### Prerequisites
- Python 3.13
- Java 17 (required for PySpark)
- Snowflake account (free trial available)

### Installation
```bash
pip install pyspark snowflake-connector-python pandas requests python-dotenv
pip install "snowflake-connector-python[pandas]"
```

### Environment Variables
Create a `.env` file in the project root:
```
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=NYC_PERMITS
SNOWFLAKE_SCHEMA=PUBLIC
```

### Run the Pipeline
Execute notebooks in order:
1. `01_extract.ipynb`
2. `02_transform.ipynb`
3. `03_load.ipynb`

---

## Results

- 50,000 permit records successfully loaded into Snowflake
- 24 columns including 4 derived analytical fields
- Full pipeline executes in under 10 minutes on a local machine