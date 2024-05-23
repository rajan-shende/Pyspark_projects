# Resolving Dependencies

import pyspark as ps
import requests as rq 
import logging as lg
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, FloatType, DoubleType
import pandas as pd
import json
from pyspark.sql.functions import col,round
from pyspark.sql.functions import udf
import datetime

# ----------------------------------------------------------

# Source/ API authentication
#  We've tried a dummy API to autinticate.
mytoken = {"Authorization": "Bearer IAMAKINGOFSPARTA1223334444HHHHHHHHHHHHHHHHIIIIIIIjjjjjjjj"}
try :
    response_1 = rq.get(url="https://httpbin.org/bearer", headers=mytoken)
except rq.RequestException :
    lg.error("Some issue occured with requests. Please try sending with a new token")

#------------------------------------------------------------

# Fetching source data from API

if response_1.status_code == 200:
    data = rq.get(url="https://freetestapi.com/api/v1/weathers")
# print(data.json())

#------------------------------------------------------------

# Structuring data received from source API

weather_data = pd.DataFrame(data.json())
sp_weather_data = spark.createDataFrame(data=weather_data)
# sp_weather_data.show()
sp_weather_data.select('country','city','weather_description','temperature','wind_speed')

#------------------------------------------------------------

# Creating user defined function for conversion of wind speed and temperature

def temp_converter(x):
    return (x * 9/5) + 32

def speed_converter(x):
    return x * 1.151

temp_fh = udf(temp_converter,DoubleType())
speed_mph = udf(speed_converter,DoubleType())

# print(temp_converter(39.4))
# print(speed_converter(19.11))


#------------------------------------------------------------

# Transformations on data

df_tr_1 = sp_weather_data.select('country','city','weather_description','temperature','wind_speed')

df_tr_2 = df_tr_1.withColumn("temp_fh",round(temp_fh(col('temperature')))).withColumn("wind_speed_mph",round(speed_mph(col("wind_speed"))))

cleaned_usable_data = df_tr_2.select("country","city","temp_fh","wind_speed_mph","weather_description").withColumnRenamed("temp_fh","temperature").withColumnRenamed("wind_speed_mph","wind_speed")

cleaned_usable_data_1 = cleaned_usable_data.sort(col("country"))

#  Creating High temperature and wind_speed data for models to use

High_temp_data = cleaned_usable_data_1.select("country","city","temperature").orderBy("temperature",ascending=False)
High_wind_speed_data = cleaned_usable_data_1.select("country","city","wind_speed").orderBy("wind_speed",ascending=False)

#------------------------------------------------------------

# Storing temp. and wind data to cloud for models to use

stamp = str(datetime.datetime.now()).replace(' ','').replace('.','').replace(':','',2).replace('-','_',2)
filename_temp = "temperature_data_"+stamp
filename_wind = "wind_speed_data_"+stamp

# print(filename_wind)
# print(filename_temp)

High_temp_data.write.options(header="true",delimiter=',').csv("s3://sample_bucket/{}".format(filename_temp))
High_wind_speed_data.write.options(header="true",delimiter=',').csv("s3://sample_bucket/{}".format(filename_wind))

#------------------------------------------------------------


