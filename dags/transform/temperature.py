
import re
import boto3

from pyspark.sql.types import DateType
from pyspark.sql.functions import udf, year, month
from pyspark.sql.types import *
from datetime import datetime

bucket_name = 'yonglun-udacity-capstone'
s3_bucket_name = 's3://yonglun-udacity-capstone'

sas_description_filekey = 'raw/I94_SAS_Labels_Descriptions.SAS'
sas_description_filename = '/tmp/I94_SAS_Labels_Descriptions.SAS'

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

def map_country(city):
    for key, value in valid_city.items():
        if city.lower() == value.lower():
            return key
udf_map_country = udf(lambda x : map_country(x), StringType())

#Parse Data Labels
# S3 client
s3 = boto3.resource('s3',
                    region_name="us-west-2",
                    aws_access_key_id=access_key,
                    aws_secret_access_key=secret_key,
                    )

# Get Label Descriptions File
s3.Bucket(bucket_name).download_file(sas_description_filekey, sas_description_filename)

with open(sas_description_filename) as header_file:
    lines = header_file.readlines()

    # valid_city: Line 10 to 298
    # valid_city len: 289
    city_regex = re.compile(r'([0-9]+)(.*)(\'.*\')(\s\;)?')
    valid_city = {}
    for line in lines[9:298]:
        match_groups = city_regex.search(line)
        valid_city[int(match_groups.group(1))] = match_groups.group(3).strip('\'')

filepath = '{}/raw/global_temperature/GlobalLandTemperaturesByCity.csv'.format(s3_bucket_name)

# Load
raw_temp_df = spark.read.format("csv").option("header", "true").load(filepath)

# Clean
cleaned_temp_df = raw_temp_df\
    .filter(raw_temp_df.AverageTemperature.isNotNull())\
    .filter(raw_temp_df.AverageTemperatureUncertainty.isNotNull())\

# Transform
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
    .withColumn("city_code", udf_map_country("country"))\
    .withColumnRenamed("City", "city")\
    .withColumnRenamed("Country", "country")\
    .withColumnRenamed("Latitude", "latitude")\
    .withColumnRenamed("Longitude", "longitude")\
    .withColumnRenamed("dt", "date_time")\
    .withColumn('month', month('date_time')) \
    .withColumn('year', year('date_time')) \

transformed_temp_df = transformed_temp_df.filter(transformed_temp_df.city_code != 'null')

# Write
transformed_temp_df.write\
    .partitionBy("city_code", "year", "month")\
    .mode("append")\
    .parquet("{}/transformed/temperature/".format(s3_bucket_name))

