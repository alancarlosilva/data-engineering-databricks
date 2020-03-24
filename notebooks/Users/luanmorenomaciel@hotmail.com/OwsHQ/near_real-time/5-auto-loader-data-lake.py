# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Auto Loader [Structured Streaming]
# MAGIC > auto loader is a new feature released on the databricks runtime 6.4 that efficiently loads files from a data lake inside azure databricks. this feature automatically recognizes new file arrival and loads them incrementally, here is the internal mechanism
# MAGIC <br>
# MAGIC 
# MAGIC #### Microsoft Azure
# MAGIC > automatically process new data coming from azure blob storage, data lake gen1 and gen2. using the **cloudfiles** option as source it opens a incremental lane to ingest data whenever a new file arrives. it uses the following technologies to achieve that:  
# MAGIC <br>
# MAGIC > **1** - Azure Event Grid = event delivery at massive scale (react to status changes)  
# MAGIC > **2** - Azure Queue Storage = durable queues for large-volume cloud services (simple, cost-effective, durable message queueing for large workloads)  
# MAGIC <br>
# MAGIC > using these two features **it removes the need to listen new files whenever it hits the storage** everything is set up automatically for you.
# MAGIC 
# MAGIC <br>
# MAGIC 
# MAGIC [Load Files ~ Azure Blob storage or Azure Data Lake Storage (Gen1/Gen2) using Auto Loader](https://docs.microsoft.com/en-us/azure/databricks/spark/latest/structured-streaming/auto-loader)  
# MAGIC [Copy Into (Delta Lake on Azure Databricks)](https://docs.microsoft.com/en-us/azure/databricks/spark/latest/spark-sql/language-manual/copy-into)
# MAGIC 
# MAGIC <br>
# MAGIC 
# MAGIC #### Amazon AWS
# MAGIC > automatically process new data coming from amazon s3. using the **cloudfiles** option as source it opens a incremental lane to ingest data whenever a new file arrives. it uses the following technologies to achieve that:  
# MAGIC <br>
# MAGIC > **1** - AWS SNS = fully managed pub/sub messaging for microservices, distributed systems, and serverless applications  
# MAGIC > **2** - AWS SQS = fully managed message queues for microservices, distributed systems, and serverless applications  
# MAGIC <br>
# MAGIC > using these two features **it removes the need to listen new files whenever it hits the storage** everything is set up automatically for you.
# MAGIC 
# MAGIC <br>
# MAGIC 
# MAGIC 
# MAGIC [Load Files ~ Amazon S3 using Auto Loader](https://docs.databricks.com/spark/latest/structured-streaming/auto-loader.html?_ga=2.166632573.259683946.1584961219-610653071.1557347896&_gac=1.54399322.1584386860.Cj0KCQjwx7zzBRCcARIsABPRscMR4u0EHQFotSGywdfz4TCJEPLA8sR2dXHYg6zdAVMcu51MUei91bMaAuQQEALw_wcB)  
# MAGIC [Copy Into (Delta Lake on Databricks)](https://docs.microsoft.com/en-us/azure/databricks/spark/latest/spark-sql/language-manual/copy-into)

# COMMAND ----------

# DBTITLE 1,Use a Database
# MAGIC %sql
# MAGIC 
# MAGIC -- use a database
# MAGIC USE yelp

# COMMAND ----------

# DBTITLE 1,List JSON File Location
# MAGIC %fs 
# MAGIC 
# MAGIC ls "dbfs:/mnt/bs-stg-files/reviews"

# COMMAND ----------

# DBTITLE 1,Using Auto Loader Feature to Read Data Lake Files
# MAGIC %python
# MAGIC 
# MAGIC # 1) importing library to get struct type
# MAGIC from pyspark.sql.types import *
# MAGIC 
# MAGIC # 2) input path location of json files
# MAGIC inputPath = "dbfs:/mnt/bs-stg-files/reviews"
# MAGIC 
# MAGIC # 3) defining json schema
# MAGIC jsonSchema = StructType(
# MAGIC [
# MAGIC  StructField('business_id', StringType(), True), 
# MAGIC  StructField('cool', LongType(), True), 
# MAGIC  StructField('date', StringType(), True), 
# MAGIC  StructField('funny', LongType(), True), 
# MAGIC  StructField('review_id', StringType(), True), 
# MAGIC  StructField('stars', LongType(), True), 
# MAGIC  StructField('text', StringType(), True), 
# MAGIC  StructField('useful', LongType(), True), 
# MAGIC  StructField('user_id', StringType(), True)
# MAGIC ]
# MAGIC )
# MAGIC 
# MAGIC # 4) structured streaming to read files using cloud files format
# MAGIC StreamFiles = (
# MAGIC   spark
# MAGIC   .readStream
# MAGIC   .schema(jsonSchema)
# MAGIC   .format("cloudFiles")
# MAGIC   .option("cloudFiles.format", "json")
# MAGIC   .load(inputPath)
# MAGIC )
# MAGIC 
# MAGIC # verify if dataframe is a stream
# MAGIC StreamFiles.isStreaming

# COMMAND ----------

# DBTITLE 1,Reading Delta Table
# MAGIC %sql
# MAGIC 
# MAGIC -- count amout of rows of a table
# MAGIC SELECT COUNT(*) AS reviews
# MAGIC FROM delta.`/delta/reviews`

# COMMAND ----------

# DBTITLE 1,Writing into a Delta Lake Table
# MAGIC %python
# MAGIC 
# MAGIC # writing stream to a delta lake table using checkpoint
# MAGIC StreamFiles \
# MAGIC   .writeStream \
# MAGIC   .format("delta") \
# MAGIC   .outputMode("append") \
# MAGIC   .option("checkpointLocation", "/delta/reviews/_checkpoints/stream-files-data-lake") \
# MAGIC   .start("/delta/reviews") \

# COMMAND ----------

