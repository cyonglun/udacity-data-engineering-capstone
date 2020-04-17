
from pyspark.sql.types import DateType
from pyspark.sql.functions import udf, year, month
from pyspark.sql.types import *
from datetime import datetime

import logging

s3_bucket_name = 's3://yonglun-udacity-capstone'

def parse_datetime(x):
    try:
        # Try parse yyyy-MM-dd
        return datetime.strptime(x, "%Y-%m-%d")
    except:
        try:
            # Try parse dd-MM-yy
            return datetime.strptime(x, "%d-%m-%y")
        except:
            return None
udf_parse_datetime = udf(lambda x: parse_datetime(x), DateType())

filepath = '{}/raw/global_temperature/GlobalLandTemperaturesByCity.csv'.format(s3_bucket_name)

# Load
logging.info("Loading Temperature Data...")
raw_temp_df = spark.read.format("csv").option("header", "true").load(filepath)

# Clean
logging.info("Cleaning Temperature Data...")
cleaned_temp_df = raw_temp_df\
    .filter(raw_temp_df.AverageTemperature.isNotNull())\
    .filter(raw_temp_df.AverageTemperatureUncertainty.isNotNull())\

# Transform
logging.info("Transforming Temperature Data...")
transformed_temp_df = cleaned_temp_df\
    .select("dt",
            "AverageTemperature",
            "AverageTemperatureUncertainty",
            "City",
            "Country",
            "Latitude",
            "Longitude")\
    .withColumn("dt", udf_parse_datetime("dt"))\
    .withColumnRenamed("AverageTemperature", "avg_temp")\
    .withColumnRenamed("AverageTemperatureUncertainty", "avg_temp_uncertainty")\
    .withColumnRenamed("City", "city")\
    .withColumnRenamed("Country", "country")\
    .withColumnRenamed("Latitude", "latitude")\
    .withColumnRenamed("Longitude", "longitude")\
    .withColumnRenamed("dt", "date_time")\
    .withColumn('month', month('date_time')) \
    .withColumn('year', year('date_time')) \

# Write
logging.info("Writing Temperature Data...")
transformed_temp_df.write\
    .partitionBy("year", "month")\
    .mode("append")\
    .parquet("{}/transformed/temperature/".format(s3_bucket_name))

