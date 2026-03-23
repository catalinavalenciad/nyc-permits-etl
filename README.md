# NYC Building Permits ETL Pipeline

## Overview
This project builds a production-style ETL pipeline to extract, transform, and load New York City building permit data from the NYC Department of Buildings into a Snowflake cloud data warehouse. Using 50,000 records pulled live from the NYC Open Data API, the pipeline covers the full data engineering workflow from extraction and schema enforcement through transformation and cloud loading.

The project is structured as an end-to-end **ETL pipeline** applied to a real-world AEC (Architecture, Engineering, and Construction) dataset, with an emphasis on data quality, pipeline transparency, and cloud warehouse integration.

## Business Problem
Construction permit data is a critical operational resource for AEC firms, city planners, and real estate analysts:

- Permit activity signals construction demand across boroughs and building types
- Reliable, structured permit data enables workforce planning, market analysis, and project forecasting
- Raw government data requires significant cleaning before it is analytically useful

This pipeline transforms raw NYC DOB permit records into a clean, queryable dataset in Snowflake, making permit activity accessible for downstream BI and reporting use cases.

## Data Source
- **NYC DOB Permit Issuance Dataset**
  Publicly available through **NYC Open Data**
  https://data.cityofnewyork.us/Housing-Development/DOB-Permit-Issuance/ipu4-2q9a

**Dataset characteristics:**
- 50,000 records pulled via the Socrata API
- 60 source columns narrowed to 20 analytically relevant fields
- Coverage: all five NYC boroughs
- Includes permit type, job type, filing dates, contractor info, and GIS coordinates

## Tech Stack
- **Language:** Python 3.13
- **Processing:** Apache PySpark 4.1 (local mode)
- **Warehouse:** Snowflake
- **Environment:** Jupyter Notebooks
- **Key Libraries:** `pyspark`, `snowflake-connector-python`, `pandas`, `requests`, `python-dotenv`
- **Version Control:** Git, GitHub

## Pipeline Architecture
```
NYC Open Data API → Extract → PySpark Transform → Snowflake Load
```

## Methodology

### 1. Extract (`01_extract.ipynb`)
- Connected to the Socrata API endpoint for NYC DOB Permit Issuance
- Retrieved 50,000 permit records ordered by issuance date
- Performed data quality assessment — missing value analysis across all 60 columns
- Narrowed schema from 60 to 20 columns via column pruning
- Saved raw data as a CSV checkpoint prior to transformation

### 2. Transform (`02_transform.ipynb`)
- Initialized a local PySpark session for distributed processing
- Standardized column names to snake_case
- Enforced schema — timestamps, strings, doubles
- Applied `try_to_timestamp` to handle inconsistent date formats in government data
- Replaced null values with standardized defaults by field type
- Added derived fields: `permit_year`, `permit_month`, `days_to_expiration`, `is_residential`
- Saved cleaned dataset as Parquet

### 3. Load (`03_load.ipynb`)
- Connected to Snowflake via environment variables
- Created `NYC_PERMITS` database and `BUILDING_PERMITS` table
- Loaded 50,000 rows using `write_pandas`
- Verified row count and sampled records post-load

## Results

| Stage | Input | Output |
|---|---|---|
| Extract | NYC Open Data API | 50,000 rows, 20 columns |
| Transform | Raw CSV | 50,000 rows, 24 columns (4 derived) |
| Load | Parquet | 50,000 rows in Snowflake |

- Pipeline executes end-to-end in under 10 minutes on a local machine
- All 50,000 records successfully loaded and verified in Snowflake
- Designed for Airflow orchestration in a production deployment

## Repository Structure
```
nyc-permits-etl/
├── notebooks/
│   ├── 01_extract.ipynb
│   ├── 02_transform.ipynb
│   └── 03_load.ipynb
├── src/
│   ├── extract.py
│   ├── transform.py
│   └── load.py
├── docs/
│   ├── index.html
│   ├── 01_extract.html
│   ├── 02_transform.html
│   └── 03_load.html
├── data/
├── .gitignore
└── README.md
```

## Setup & Reproduction

### Prerequisites
- Python 3.13
- Java 17 (required for PySpark)
- Snowflake account

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
1. `notebooks/01_extract.ipynb`
2. `notebooks/02_transform.ipynb`
3. `notebooks/03_load.ipynb`

---

## Results

- 50,000 permit records successfully loaded into Snowflake
- 24 columns including 4 derived analytical fields
- Full pipeline executes in under 10 minutes on a local machine
