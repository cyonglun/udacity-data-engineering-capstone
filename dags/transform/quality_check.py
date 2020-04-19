from pyspark.sql.types import *

s3_bucket_name = 's3://yonglun-udacity-capstone'

def check(path, table):
    df = spark.read.parquet(path)
    if len(df.columns) == 0 or df.count() == 0:
        raise ValueError(f"Data quality check failed. {table} returned no results")

check("{}/transformed/immigration/".format(s3_bucket_name), "immigration")
check("{}/transformed/temperature/".format(s3_bucket_name), "temperature")
