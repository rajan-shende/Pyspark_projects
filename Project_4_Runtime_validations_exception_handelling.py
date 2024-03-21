# Databricks notebook source
# DBTITLE 1,Importing dependencies
import pyspark
import pyspark.sql.utils
import datetime
from pyspark.sql.types import *

# COMMAND ----------

# DBTITLE 1,Validation if the file created or not at the specified location during the execution
cur_date = datetime.date.today()
file_name = "s3://sample_bucket/sample_folder/sales_data_{}.csv".format(cur_date)
# print(file_name)
try:
    check_for_data = spark.read.load("s3://sample_bucket/sample_folder/sales_data123.csv",format="csv")
except pyspark.sql.utils.AnalysisException:
    print("No file created for date : {}".format(cur_date))

# -------------------------------------------- OUTPUT ----------------------------------
# s3://sample_bucket/sample_folder/sales_data/sales_data_2024-03-21.csv
# No file created for date : 2024-03-21

# COMMAND ----------

# DBTITLE 1,Validating the schema
from pyspark.sql.types import StructType,StructField, StringType

expected_schema = pyspark.sql.types.StructType([StructField("order_id",StringType(),True),
    StructField("customer_id",StringType(),True),\
    StructField("order_date",StringType(),True),\
    StructField("product_id",StringType(),True),\
    StructField("quantity",StringType(),True),\
    StructField("price123",StringType(),True)\
    ])
expected_df = spark.createDataFrame(data='',schema=expected_schema)
# print(type(expected_schema))
check_for__schema = spark.read.load("s3://sample_bucket/sample_folder/sales_data123.csv",format="csv",header="true")

try :
    assert check_for_schema.schema == expected_df.schema
except AssertionError:
    print("Schema does not match")

# --------------------------------------------  OUTPUT ----------------------
# Schema does not match

# COMMAND ----------


