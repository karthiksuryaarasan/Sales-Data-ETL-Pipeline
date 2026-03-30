from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from sqlalchemy import create_engine
import pandas as pd

# Create Spark Session
spark = SparkSession.builder \
    .appName("Sales ETL Pipeline") \
    .getOrCreate()


def extract():
    df = spark.read.csv(
        "data/sales.csv",
        header=True,
        inferSchema=True
    )
    return df


def transform(df):
    df = df.dropDuplicates()

    df = df.filter(col("amount") > 0)

    return df


def load(df):

    # Convert Spark → Pandas (for PostgreSQL load)
    pandas_df = df.toPandas()

    db_url = "postgresql://postgres:postgres@localhost:5432/sales_db"

    engine = create_engine(db_url)

    pandas_df.to_sql(
        "sales",
        engine,
        if_exists="replace",
        index=False
    )


def run():
    df = extract()
    df = transform(df)
    load(df)

    print("PySpark ETL Completed Successfully")


if __name__ == "__main__":
    run()