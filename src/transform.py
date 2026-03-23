import re
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from dotenv import load_dotenv

load_dotenv()


def create_spark_session() -> SparkSession:
    """Initialize a local PySpark session."""
    spark = SparkSession.builder \
        .appName("NYC Permits ETL") \
        .master("local[*]") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    print(f"Spark version: {spark.version}")
    return spark


def clean_column_name(name: str) -> str:
    """Normalize column name to snake_case."""
    name = name.lower()
    name = re.sub(r'[^a-z0-9_]', '_', name)
    name = re.sub(r'_+', '_', name)
    return name.strip('_')


def transform_permits(spark: SparkSession, input_path: str):
    """
    Load raw CSV and apply all transformations.
    Returns a cleaned Spark DataFrame.
    """
    df = spark.read.csv(input_path, header=True, inferSchema=False)

    # Clean column names
    new_columns = [clean_column_name(c) for c in df.columns]
    df = df.toDF(*new_columns)

    # Cast date columns
    df = df.withColumn("filing_date", F.expr("try_to_timestamp(filing_date, 'MM/dd/yyyy')")) \
           .withColumn("issuance_date", F.expr("try_to_timestamp(issuance_date, 'yyyy-MM-dd')")) \
           .withColumn("expiration_date", F.expr("try_to_timestamp(expiration_date, 'MM/dd/yyyy')"))

    # Cast numeric columns
    df = df.withColumn("gis_latitude", F.col("gis_latitude").cast("double")) \
           .withColumn("gis_longitude", F.col("gis_longitude").cast("double"))

    # Handle missing values
    df = df.fillna({
        "borough": "UNKNOWN",
        "zip_code": "00000",
        "permit_status": "UNKNOWN",
        "work_type": "UNKNOWN",
        "residential": "UNKNOWN",
        "bldg_type": "UNKNOWN",
        "owner_s_business_name": "NOT PROVIDED",
        "owner_s_business_type": "NOT PROVIDED",
        "permittee_s_business_name": "NOT PROVIDED",
        "permittee_s_license_type": "NOT PROVIDED",
        "gis_nta_name": "UNKNOWN"
    })

    # Add derived columns
    df = df.withColumn("permit_year", F.year("filing_date")) \
           .withColumn("permit_month", F.month("filing_date")) \
           .withColumn("days_to_expiration", F.datediff("expiration_date", "filing_date")) \
           .withColumn("is_residential", F.when(F.col("residential") == "YES", True).otherwise(False))

    print(f"Transform complete: {df.count()} rows, {len(df.columns)} columns")
    return df


def save_parquet(df, output_path: str) -> None:
    """Save transformed DataFrame as Parquet."""
    df.write.mode("overwrite").parquet(output_path)
    print(f"Cleaned data saved to: {output_path}")


if __name__ == "__main__":
    spark = create_spark_session()
    df = transform_permits(spark, "data/raw_permits.csv")
    save_parquet(df, "data/cleaned_permits.parquet")