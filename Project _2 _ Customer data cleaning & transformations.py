# Databricks notebook source
# DBTITLE 1,1. Adding Dependencies
# We have received the data from client as a response.
# We want to load the data into data frame and then clean it and do required transformations.
# Clean the phone numbers to remove non-numeric characters
# Fill in missing values in the phone column with "N/A" or "Unknown"
# Rename the columns for clarity
# Convert the registration_date column to a proper date format
# Create a new column for status based on the registration date
# Load the data in aws s3 at specific loaction a csv file.

import pyspark
from pyspark.sql.functions import regexp_replace, when, col
from pyspark.sql.functions import unix_timestamp, from_unixtime
from pyspark.sql.functions import to_date
from pyspark.sql.functions import *

# COMMAND ----------

# DBTITLE 1,2. load the json data into data frame
customer_data = spark.createDataFrame([
    (1, "John Doe", "john.doe@gmail.com", "123-456-7890", "2022-01-15"),
    (2, "Jane Smith", "jane.smith@hotmail.com", "(987)654-3210", "2021-11-30"),
    (3, "Alice Lee", "alice.lee@yahoo.com", "555-5555", "2023-03-10"),
    (4, "Bob Brown", "bob.brown@gmail.com", None, "2022-05-20")
], ["customer_id", "name", "email", "phone", "registration_date"])

customer_data.show()

# COMMAND ----------

# DBTITLE 1,2. Clean the phone numbers to remove non-numeric characters and null values
non_null_data = customer_data.na.fill("NA")
cleaned_phone = non_null_data.withColumn("Clean_phone",regexp_replace(col("phone"), "[^0-9]", "")).replace("","Unknown")
clean_data_1 = cleaned_phone.select("customer_id","name","email","registration_date","Clean_phone")
clean_data_1.show()

# COMMAND ----------

# DBTITLE 1,3. Rename the columns for clarity
clean_data_renamed = clean_data_1.withColumnRenamed("customer_id","Client_id").withColumnRenamed("name","Customer_name").withColumnRenamed("email","Customer_email").withColumnRenamed("registeration_date","Regstriation_date").withColumnRenamed("Clean_phone","Customer_phone")

# COMMAND ----------

# DBTITLE 1,4. Convert the registration_date column to a proper date format
clean_data_date_formatted = clean_data_renamed.select("Client_id",to_date(col("registration_date")).alias("registration_date"),"Customer_name","Customer_email","Customer_phone")
clean_data_date_formatted.printSchema()
clean_data_date_formatted.show()

# COMMAND ----------

# DBTITLE 1,5. Create a new column for age based on the registration date
# clean_data_date_formatted.select(year("registration_date")).show()
Final_df = clean_data_date_formatted.withColumn("Contract_status",when(year("registration_date") >= 2022, "Active").otherwise("Inactive"))

# COMMAND ----------

# DBTITLE 1,6.  Load the data in aws s3 at specific loaction a csv file.
Final_df.coalesce(1).write.option("header","true").csv("s3://tendril-airflow-dev-us/Test_Tendril/customer_Data_Cleaned_2")

# COMMAND ----------


