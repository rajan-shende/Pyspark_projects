# Databricks notebook source
# Requirements as below : 
# Client has shared some sample sales data on AWS S3. We want to create some analysis as per requirements.
# 1. Load the dataset into a PySpark DataFrame.
# 2. Calculate the total revenue for each customer.
# 3. Find the top-selling products (by total quantity sold) in the dataset.
# 4. Calculate the average quantity and price per customer.
# 5. Determine the total revenue for each customer.
# 6. Identify the date with the highest total revenue.

import pyspark
from pyspark.sql.functions import col

# COMMAND ----------

# DBTITLE 1,1. Importing sales data into a dataframe
sales_Data =spark.read.option("inferSchema", "true").option("header", "true").csv("s3://Sample_bucket/Folder/sales_data.csv")

# COMMAND ----------

sales_Data.show()

# COMMAND ----------

# DBTITLE 1,2. Calculate the total revenue for each customer.
sales_Data.groupBy("customer_id").sum("price").show()

# COMMAND ----------

# DBTITLE 1,3. Find the top-selling products (by total quantity sold) in the dataset.
max_qunat = sales_Data.groupBy("product_id").sum("quantity")
# print(type(max_qunat))
# display(max_qunat.orderBy("sum(quantity)").tail(1))
# I faced issue here with desc() function. So make sure to import the col as => from pyspark.sql.functions import col
max_qunat.orderBy(col("sum(quantity)").desc()).show(1)

# COMMAND ----------

# DBTITLE 1,4. Calculate the average quantity and price per customer.
sales_Data.groupBy("customer_id").avg("quantity").show()
sales_Data.groupBy("customer_id").avg("price").show()

# COMMAND ----------

# DBTITLE 1,5. Determine the total revenue for each customer.
sales_Data.groupBy("customer_id").sum("price").show()

# COMMAND ----------

# DBTITLE 1,6. Identify the date with the highest total revenue.
datewise_sales = sales_Data.groupBy("order_date").sum("price")
datewise_sales.orderBy(col("sum(price)").desc()).show(1)

# COMMAND ----------

