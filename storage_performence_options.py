import pyspark
from pyspark import sql
from pyspark.sql.functions import col
from pyspark.sql.functions import trim
import time

# COMMAND ----------

# DBTITLE 1,Reading a text file Vs cache
print("------------------------- Reading the file from storage and making actions on Data No Cache ------------------------")
start_time = time.time()
data = spark.read.text("s3://sample_bucket/sample_folder/sample_text")
data.count()
end_time = time.time()
print("Total time taken : {}".format(end_time - start_time))
print("----------------------- after cacheying the dataframe & making actions  --------------------------")
data.cache()
start_time = time.time()
data.count()
end_time = time.time()
print("Total time taken : {}".format(end_time - start_time))

# -------------------------------------- Output  -------------------------------------------------
# ------------------------- Reading the file from storage and making actions on Data No Cache ------------------------
# Total time taken : 0.5823574066162109
# ----------------------- after cacheying the dataframe & making actions  --------------------------
# Total time taken : 0.2608504295349121


# COMMAND ----------

# DBTITLE 1,Cache multiple times
print("----------------------- before cache --------------------------")

start_time = time.time()
data = spark.read.text("s3://sample_bucket/sample_folder/sample_text")
data.count()
end_time = time.time()
print("Total time taken : {}".format(end_time - start_time))

print("----------------------- after cache 1 --------------------------")
start_time = time.time()
data.count()
end_time = time.time()
print("Total time taken : {}".format(end_time - start_time))

print("----------------------- after cache 2--------------------------")
start_time = time.time()
data.cache().count()
end_time = time.time()
print("Total time taken : {}".format(end_time - start_time))

print("----------------------- after cache 3 --------------------------")
start_time = time.time()
data.cache().count()
end_time = time.time()
print("Total time taken : {}".format(end_time - start_time))

# -------------------------------------- Output  -------------------------------------------------
# ----------------------- before cache --------------------------
# Total time taken : 0.7796032428741455
# ----------------------- after cache 1 --------------------------
# Total time taken : 0.24378752708435059
# ----------------------- after cache 2--------------------------
# Total time taken : 0.28986549377441406
# ----------------------- after cache 3 --------------------------
# Total time taken : 0.2764098644256592

# COMMAND ----------

# DBTITLE 1,Persist in memory Or on disk Or in memory serialised
print("----------------------- before persist --------------------------")
start_time = time.time()
data = spark.read.text("s3://sample_bucket/sample_folder/sample_text")
data.count()
end_time = time.time()
print("Total time taken : {}".format(end_time - start_time))
data.persist()
print("----------------------- after persist --------------------------")
# By default persist is using memory But we can use persist with different storage options
start_time = time.time()
data.count()
end_time = time.time()
print("Total time taken : {}".format(end_time - start_time))

# -------------------------------------- Output  -------------------------------------------------
# ----------------------- before persist --------------------------
# Total time taken : 0.8194589614868164
# ----------------------- after persist --------------------------
# Total time taken : 0.23983216285705566


# COMMAND ----------

# DBTITLE 1,Different persist flavours for memory
import pyspark.storagelevel as storage

print("----------------------- Persisting into the memory only --------------------------")
data.persist(storageLevel=storage.StorageLevel.MEMORY_ONLY)
start_time = time.time()
data.count()
end_time = time.time()
print("Total time taken : {}".format(end_time - start_time))

print("----------------------- Persisting into the memory_only_2 --------------------------")
data.persist(storageLevel=storage.StorageLevel.MEMORY_ONLY_2)
#  The above options stores the partitions in to 2 different cluster nodes
start_time = time.time()
data.count()
end_time = time.time()
print("Total time taken : {}".format(end_time - start_time))


print("----------------------- Persisting into the memory_and_disk --------------------------")
data.persist(storageLevel=storage.StorageLevel.MEMORY_AND_DISK)
#  The above options stores the data into some memory and disk if required
start_time = time.time()
data.count()
end_time = time.time()
print("Total time taken : {}".format(end_time - start_time))

print("----------------------- Persisting into the disk only --------------------------")
data.persist(storageLevel=storage.StorageLevel.DISK_ONLY)
#  The above options stores the data into some space on disk only
start_time = time.time()
data.count()
end_time = time.time()
print("Total time taken : {}".format(end_time - start_time))

# Other options for persist
data.persist(storageLevel=storage.StorageLevel.DISK_ONLY_2)
data.persist(storageLevel=storage.StorageLevel.DISK_ONLY_3)
data.persist(storageLevel=storage.StorageLevel.MEMORY_AND_DISK_2)
data.persist(storageLevel=storage.StorageLevel.MEMORY_AND_DISK_DESER)
data.persist(storageLevel=storage.StorageLevel.OFF_HEAP)

#  Removing the stored dataframe 
#  The unpersist method will return the dataframe which will be unpersisted from storgae specified in persist.
rem = data.unpersist()
display(data.unpersist())


# -------------------------------------- Output  -------------------------------------------------
# ----------------------- Persisting into the memory only --------------------------
# Total time taken : 0.3752422332763672
# ----------------------- Persisting into the memory_only_2 --------------------------
# Total time taken : 0.10791659355163574
# ----------------------- Persisting into the memory_and_disk --------------------------
# Total time taken : 0.15527629852294922
# ----------------------- Persisting into the disk only --------------------------
# Total time taken : 0.12988495826721191

