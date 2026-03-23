import os
import pandas as pd
import snowflake.connector
from pyspark.sql import SparkSession
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv

load_dotenv()


def create_spark_session() -> SparkSession:
    """Initialize a local PySpark session."""
    spark = SparkSession.builder \
        .appName("NYC Permits Load") \
        .master("local[*]") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def connect_snowflake() -> snowflake.connector.SnowflakeConnection:
    """Establish Snowflake connection using environment variables."""
    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA")
    )
    print("Snowflake connection established successfully!")
    return conn


def create_table(conn: snowflake.connector.SnowflakeConnection) -> None:
    """Create target database and table in Snowflake if not exists."""
    cursor = conn.cursor()
    cursor.execute("CREATE DATABASE IF NOT EXISTS NYC_PERMITS")
    cursor.execute("USE DATABASE NYC_PERMITS")
    cursor.execute("USE SCHEMA PUBLIC")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS BUILDING_PERMITS (
            borough STRING,
            zip_code STRING,
            block STRING,
            lot STRING,
            job_type STRING,
            permit_type STRING,
            permit_status STRING,
            filing_date TIMESTAMP,
            issuance_date TIMESTAMP,
            expiration_date TIMESTAMP,
            work_type STRING,
            residential STRING,
            bldg_type STRING,
            owner_s_business_name STRING,
            owner_s_business_type STRING,
            permittee_s_business_name STRING,
            permittee_s_license_type STRING,
            gis_latitude DOUBLE,
            gis_longitude DOUBLE,
            gis_nta_name STRING,
            permit_year INT,
            permit_month INT,
            days_to_expiration INT,
            is_residential BOOLEAN
        )
    """)
    print("Database and table created successfully!")
    cursor.close()


def load_to_snowflake(
    conn: snowflake.connector.SnowflakeConnection,
    parquet_path: str
) -> None:
    """Load cleaned Parquet data into Snowflake."""
    spark = create_spark_session()
    df = spark.read.parquet(parquet_path)

    df_pandas = df.toPandas()

    # Convert timestamps to strings
    date_cols = ["filing_date", "issuance_date", "expiration_date"]
    for col in date_cols:
        df_pandas[col] = df_pandas[col].astype(str).replace("NaT", None)

    # Replace NaN with None
    df_pandas = df_pandas.where(pd.notnull(df_pandas), None)

    # Uppercase columns for Snowflake
    df_pandas.columns = [c.upper() for c in df_pandas.columns]

    success, nchunks, nrows, _ = write_pandas(
        conn=conn,
        df=df_pandas,
        table_name="BUILDING_PERMITS",
        database="NYC_PERMITS",
        schema="PUBLIC"
    )

    print(f"Load complete — {nrows} rows loaded successfully!")


def verify_load(conn: snowflake.connector.SnowflakeConnection) -> None:
    """Verify row count in Snowflake after load."""
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM NYC_PERMITS.PUBLIC.BUILDING_PERMITS")
    count = cursor.fetchone()[0]
    print(f"Rows verified in Snowflake: {count}")
    cursor.close()


if __name__ == "__main__":
    conn = connect_snowflake()
    create_table(conn)
    load_to_snowflake(conn, "data/cleaned_permits.parquet")
    verify_load(conn)
    conn.close()
    print("Pipeline complete!")