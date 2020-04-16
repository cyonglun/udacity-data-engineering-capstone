import re

from pyspark.sql.types import *
from pyspark.sql.functions import udf, col
from pyspark.sql.types import *
from datetime import datetime, timedelta

s3_bucket_name = 's3://yonglun-udacity-capstone'

def sas_to_datetime(x):
    try:
        base_date = datetime(1960, 1, 1)
        return base_date + timedelta(days=int(x))
    except:
        return None
udf_sas_to_datetime = udf(lambda x: sas_to_datetime(x), DateType())

#Parse Data Labels
with open('../labels/I94_SAS_Labels_Descriptions.SAS') as header_file:
    lines = header_file.readlines()

    # valid_city: Line 10 to 298
    # valid_city len: 289
    city_regex = re.compile(r'([0-9]+)(.*)(\'.*\')(\s\;)?')
    valid_city = {}
    for line in lines[9:298]:
        match_groups = city_regex.search(line)
        valid_city[int(match_groups.group(1))] = match_groups.group(3).strip('\'')

    # valid_port: Line 303 to 962
    # valid_port len: 660
    port_regex = re.compile(r'\'(.+)\'(.*=.*)\'(.+)\'')
    valid_port = {}
    for line in lines[302:962]:
        match_groups = port_regex.search(line)
        valid_port[match_groups.group(1)] = match_groups.group(3).strip()

    # valid_addr: line 982 to 1036
    # valid_addr len:
    addr_regex = re.compile(r'\'(.{2})\'(.*=.*)\'(.+)\'')
    valid_addr = {}
    for line in lines[981:1036]:
        match_groups = addr_regex.search(line)
        valid_addr[match_groups.group(1)] = match_groups.group(3).strip()

filepath = '{}/raw/i94_immigration_data/i94_{}_sub.sas7bdat'.format(s3_bucket_name, month_year)

# Load
raw_immigration_df = spark.read.format('com.github.saurfang.sas.spark').load(filepath)

# Clean
cleaned_immigration_df = raw_immigration_df\
    .filter(raw_immigration_df.i94addr.isNotNull() and raw_immigration_df.i94addr.isin(list(valid_addr.keys())))\
    .filter(raw_immigration_df.i94cit.isin(list(valid_city.keys()))) \

# Transform
transformed_immigration_df = cleaned_immigration_df\
    .selectExpr(
        "cast(cicid as int) id",
        "cast(i94yr as int) year",
        "cast(i94mon as int) month",
        "i94cit as country_code",
        "i94port as port_code",
        "i94addr as state_code",
        "i94mode as arrival_mode",
        "i94bir as age",
        "gender",
        "cast(admnum as long) AS admission_no",
        "visatype",
        "arrdate",
        "depdate")\
    .withColumn("arrival_date", udf_sas_to_datetime("arrdate"))\
    .withColumn("departure_date", udf_sas_to_datetime("depdate"))

# Write
transformed_immigration_df.write\
    .partitionBy("year", "month")\
    .mode("append")\
    .parquet("{}/transformed/immigration/".format(s3_bucket_name))