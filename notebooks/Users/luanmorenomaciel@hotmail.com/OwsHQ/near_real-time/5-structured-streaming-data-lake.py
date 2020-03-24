# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # SparkSQL ~ [Structured Streaming]
# MAGIC > spark-sql possui a habilidade de realizar streaming, 
# MAGIC > o streaming estruturado faz com que seje possível a conexão   
# MAGIC > com fontes de dados como file e kafka para ingestão dos dados em **stream** em grande escala, com sql
# MAGIC 
# MAGIC <br>
# MAGIC 
# MAGIC 
# MAGIC #### Structured Streaming
# MAGIC 
# MAGIC > **Stream Processing on Spark SQL Engine** = Fast | Scalable | Fault-Tolerant  
# MAGIC > **Deal with Complex Data & Complex Workloads** = Rich | Unified | High-Level APIs  
# MAGIC > **Rich Ecosystem of Data Sources** = Integrate with Many Storage Systems  
# MAGIC <br>
# MAGIC   
# MAGIC <img width="800px" src ='https://brzluanmoreno.blob.core.windows.net/stgfiles/png_files/structured-streaming.png'>
# MAGIC   
# MAGIC <br>
# MAGIC > sparksql + structured streaming  
# MAGIC > https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # delta.`/delta/reviews`

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC USE Yelp

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC > **reviews** são as informações de todas as revisões realizadas por um *usuário* para um ou mais *locais*, 
# MAGIC > com isso, iremos usar a capacidade do **Structured Streaming + SparkSQL + Delta** para persistir de forma eficiente,  
# MAGIC > as informações estruturadas no layer do *delta lake*

# COMMAND ----------

# DBTITLE 1,Listando Novos Arquivos vindo da Aplicação [Data Lake] - Landing Zone
# MAGIC %fs ls "dbfs:/mnt/bs-stg-files/reviews"

# COMMAND ----------

# DBTITLE 1,Structured Streaming com PySpark - [Anatomia do Streaming]
# importando biblioteca
from pyspark.sql.types import *

# definindo local de entrada [Data Lake]
inputPath = "dbfs:/mnt/bs-stg-files/reviews"

# streaming estruturado
# necessita a definição do streaming
# JSON vs. Parquet [implicit schema]

# estruturando a estrutura JSON
jsonSchema = StructType(
[
 StructField('business_id', StringType(), True), 
 StructField('cool', LongType(), True), 
 StructField('date', StringType(), True), 
 StructField('funny', LongType(), True), 
 StructField('review_id', StringType(), True), 
 StructField('stars', LongType(), True), 
 StructField('text', StringType(), True), 
 StructField('useful', LongType(), True), 
 StructField('user_id', StringType(), True)
]
)

# definindo structured streaming
StreamingDfReviews = (
     spark
    .readStream
    .schema(jsonSchema)
    .option("maxFilesPerTrigger", 1)
    .json(inputPath)
)

# verificando streaming
StreamingDfReviews.isStreaming

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ##![Spark Logo Tiny](https://kpistoropen.blob.core.windows.net/collateral/roadshow/logo_spark_tiny.png) Source
# MAGIC 
# MAGIC > como podemos ver, o DataFrame = **StreamingDfReviews** é de de fato um streaming pelo tipo de ingestão
# MAGIC > e por ser configurado como **readStream**

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <img width="300px" src="https://docs.delta.io/latest/_static/delta-lake-logo.png"> 
# MAGIC <br>
# MAGIC ### Sink 
# MAGIC 
# MAGIC > uma das capacidades do **Delta Lake** é ser configurado como **sink** utilizando *structured streaming*  
# MAGIC > isso irá comitar [ACID] os dados de forma *exactly-once processing* quando criado pelo streaming  
# MAGIC > e não irá competir contra por exemplo um bach executando ao mesmo momento

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC > salvando o streaming diretamente no **Delta Lake**  
# MAGIC > structured streaming com delta lake = https://docs.databricks.com/delta/delta-streaming.html

# COMMAND ----------

# DBTITLE 1,Removendo Checkpoint 
# MAGIC %fs rm -r "dbfs:/delta/reviews/_checkpoints/reviews-file-source"

# COMMAND ----------

# DBTITLE 1,Removendo Registros
# MAGIC %sql
# MAGIC 
# MAGIC DELETE FROM delta.`/delta/reviews`

# COMMAND ----------

# DBTITLE 1,Limpando Metadado [Files not Referenced Anymore]
# MAGIC %sql
# MAGIC 
# MAGIC VACUUM delta.`/delta/reviews`

# COMMAND ----------

# DBTITLE 1,Lendo Tabela [Streaming]
# MAGIC %sql
# MAGIC 
# MAGIC SELECT COUNT(*) AS reviews
# MAGIC FROM delta.`/delta/reviews`

# COMMAND ----------

# DBTITLE 1,Iniciando o Processamento com Structured Streaming [Delta Lake]
# configures the number of partitions that are used when shuffling data for joins or aggregations.
spark.conf.set("spark.sql.shuffle.partitions", "2")  

# https://docs.databricks.com/delta/delta-streaming.html
# ~100ms at best.

StreamingDfReviews \
  .writeStream \
  .format("delta") \
  .outputMode("append") \
  .option("checkpointLocation", "/delta/reviews/_checkpoints/reviews-file-source") \
  .start("/delta/reviews") \

# COMMAND ----------

# DBTITLE 1,Lendo Registros do Delta Lake em Streaming [/delta/reviews]
# MAGIC %sql
# MAGIC 
# MAGIC --1 = 5.996.996
# MAGIC --2 = 11.993.992
# MAGIC 
# MAGIC SELECT COUNT(*) AS reviews
# MAGIC FROM delta.`/delta/reviews`

# COMMAND ----------

# DBTITLE 1,Agrupamento dos Dados em Streaming usando Delta Lake
# MAGIC %sql
# MAGIC 
# MAGIC WITH data_summary_reviews AS 
# MAGIC (
# MAGIC SELECT date,
# MAGIC        SUM(stars) AS stars,
# MAGIC        COUNT(*) AS reviews
# MAGIC FROM delta.`/delta/reviews`
# MAGIC WHERE date BETWEEN '2018-01-01' AND '2018-02-01'
# MAGIC GROUP BY date
# MAGIC )
# MAGIC SELECT date,
# MAGIC        reviews
# MAGIC FROM data_summary_reviews
# MAGIC ORDER BY date ASC

# COMMAND ----------

from time import sleep 
sleep(30)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC WITH data_summary_reviews AS 
# MAGIC (
# MAGIC SELECT date,
# MAGIC        SUM(stars) AS stars,
# MAGIC        COUNT(*) AS reviews
# MAGIC FROM delta.`/delta/reviews`
# MAGIC WHERE date BETWEEN '2018-01-01' AND '2018-02-01'
# MAGIC GROUP BY date
# MAGIC )
# MAGIC SELECT date,
# MAGIC        reviews
# MAGIC FROM data_summary_reviews
# MAGIC ORDER BY date ASC

# COMMAND ----------

# DBTITLE 1,PySpark - Lendo Dados do Delta Lake
df_delta_reviews = spark.read.format("delta").load("/delta/reviews")
display(df_delta_reviews)

# COMMAND ----------

# DBTITLE 1,Contando Quantidade de Registros [PySpark]
# 2.3987.984
df_delta_reviews.count()

# COMMAND ----------

# DBTITLE 1,Contando Quantidade de Registros [SparkSQL]
# MAGIC %sql
# MAGIC 
# MAGIC --175.776.721
# MAGIC --313.707.629
# MAGIC --23.987.984
# MAGIC 
# MAGIC SELECT COUNT(*)
# MAGIC FROM delta.`/delta/reviews`

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Pipeline em Real-Time com Structured Streaming
# MAGIC > aplicação de transformações em near real-time utilizando delta architecture  
# MAGIC <br>
# MAGIC > https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html  
# MAGIC > https://databricks.com/blog/2019/10/03/simple-reliable-upserts-and-deletes-on-delta-lake-tables-using-python-apis.html  

# COMMAND ----------

# DBTITLE 1,Listando Data Lake [Blob Storage]
# MAGIC %fs ls "dbfs:/mnt/bs-stg-files/reviews"

# COMMAND ----------

# DBTITLE 1,Structured Streaming - Criação da Estrutura [Schema]
from pyspark.sql.types import *

inputPath = "dbfs:/mnt/bs-stg-files/reviews"

jsonSchema = StructType(
[
 StructField('business_id', StringType(), True), 
 StructField('cool', LongType(), True), 
 StructField('date', StringType(), True), 
 StructField('funny', LongType(), True), 
 StructField('review_id', StringType(), True), 
 StructField('stars', LongType(), True), 
 StructField('text', StringType(), True), 
 StructField('useful', LongType(), True), 
 StructField('user_id', StringType(), True)
]
)

bronze_df_reviews = (
     spark
    .readStream
    .schema(jsonSchema)
    .json(inputPath)
)

bronze_df_reviews.isStreaming

# COMMAND ----------

# DBTITLE 1,Cleaning Checkpoint Structure
# MAGIC %fs rm -r "dbfs:/delta/reviews/_checkpoints/stream_bronze_reviews_files"

# COMMAND ----------

# DBTITLE 1,Cleaning Checkpoint Structure
# MAGIC %fs rm -r "dbfs:/delta/reviews/_checkpoints/bronze_silver_stream"

# COMMAND ----------

# DBTITLE 1,Delete Bronze Table
# MAGIC %sql
# MAGIC 
# MAGIC DELETE FROM delta.`/delta/stream_bronze_reviews`

# COMMAND ----------

# DBTITLE 1,Delete Silver Table
# MAGIC %sql
# MAGIC 
# MAGIC DELETE FROM delta.`/delta/stream_silver_reviews`

# COMMAND ----------

# DBTITLE 1,Remove Metadata
# MAGIC %sql
# MAGIC 
# MAGIC VACUUM delta.`/delta/stream_bronze_reviews`

# COMMAND ----------

# DBTITLE 1,Remove Metadata
# MAGIC %sql
# MAGIC 
# MAGIC VACUUM delta.`/delta/stream_silver_reviews`

# COMMAND ----------

# MAGIC %md 
# MAGIC > https://docs.databricks.com/delta/delta-streaming.html 
# MAGIC 
# MAGIC > Structured Streaming is a scalable and fault-tolerant stream processing engine built on the Spark SQL engine. You can express your streaming computation the same way you would express a batch computation on static data. The Spark SQL engine will take care of running it incrementally and continuously and updating the final result as streaming data continues to arrive. You can use the Dataset/DataFrame API in Scala, Java, Python or R to express streaming aggregations, event-time windows, stream-to-batch joins, etc. The computation is executed on the same optimized Spark SQL engine. Finally, the system ensures end-to-end exactly-once fault-tolerance guarantees through checkpointing and Write-Ahead Logs. In short, Structured Streaming provides fast, scalable, fault-tolerant, end-to-end exactly-once stream processing without the user having to reason about streaming.
# MAGIC 
# MAGIC > Internally, by default, Structured Streaming queries are processed using a micro-batch processing engine, which processes data streams as a series of small batch jobs thereby achieving end-to-end latencies as low as 100 milliseconds and exactly-once fault-tolerance guarantees. However, since Spark 2.3, we have introduced a new low-latency processing mode called Continuous Processing, which can achieve end-to-end latencies as low as 1 millisecond with at-least-once guarantees. Without changing the Dataset/DataFrame operations in your queries, you will be able to choose the mode based on your application requirements.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Stage 1 - Bronze Table
# MAGIC > escrevendo a tabela de parquet para delta, fazendo isso iremos ganhar em armazenamento, velocidade e teremos diversas opções que estão listadas acima  
# MAGIC > a tabela **bronze_df_reviews** foi o DataFrame criado diretamente do **azure blob storage**  
# MAGIC 
# MAGIC > *lembre-se que esse layer não possui modificações, o dado vem direto do Data Lake para o Delta Lake Bronze Table*
# MAGIC 
# MAGIC <br>
# MAGIC 
# MAGIC <img width="200" src ='https://brzluanmoreno.blob.core.windows.net/stgfiles/png_files/bronze-table.png'>
# MAGIC <br>
# MAGIC <br>

# COMMAND ----------

# DBTITLE 1,Emptiness of Table
# MAGIC %sql
# MAGIC 
# MAGIC -- 0
# MAGIC 
# MAGIC SELECT COUNT(*)
# MAGIC FROM delta.`/delta/stream_bronze_reviews`

# COMMAND ----------

# DBTITLE 1,Inicialização do Processo de Streaming [Bronze Table] = Data Lake para Delta Lake
bronze_df_reviews \
  .writeStream \
  .format("delta") \
  .outputMode("append") \
  .option("checkpointLocation", "/delta/reviews/_checkpoints/stream_bronze_reviews_files") \
  .start("/delta/stream_bronze_reviews") \

# COMMAND ----------

# DBTITLE 1,Counting Streaming in Near-Real Time
# MAGIC %sql
# MAGIC 
# MAGIC -- 5.996.996
# MAGIC -- 11.993.992
# MAGIC -- 17.990.988
# MAGIC -- 23.987.984
# MAGIC -- 41.978.972
# MAGIC 
# MAGIC SELECT COUNT(*)
# MAGIC FROM delta.`/delta/stream_bronze_reviews`

# COMMAND ----------

# DBTITLE 1,Reading Streaming in Near-Real Time
# MAGIC %sql
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM delta.`/delta/stream_bronze_reviews`
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC > 1 - reading dataframe that reads data from data lake  
# MAGIC > 2 - format delta  
# MAGIC > 3 - location to store delta file  
# MAGIC > 4 - filter clause on stream  
# MAGIC > 5 - writing stream  
# MAGIC > 6 - in a new delta table  
# MAGIC > 7 - all rows will be inserted = complete | append | update (last trigger)  
# MAGIC > 8 - checkpoint to save state of the stream  
# MAGIC > 9 - initialize stream on this new delta table (store records)  

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Stage 2 - Silver Table
# MAGIC > quando uma modificação é realizada movemos para a **silver table**
# MAGIC > ela é a tabela responsável por receber informações que foram transformadas no **ETL** = filtros, limpeza e melhorias
# MAGIC 
# MAGIC 
# MAGIC <br>
# MAGIC 
# MAGIC <img width="300" src ='https://brzluanmoreno.blob.core.windows.net/stgfiles/png_files/silver-table.png'>
# MAGIC <br>
# MAGIC <br>

# COMMAND ----------

# DBTITLE 1,Structured Streaming - Inicialização do Processo de Streaming [Silver Table] - Applying Where Clause
df_silver_reviews = spark.readStream \
  .format("delta") \
  .load("/delta/stream_bronze_reviews") \
  .select("business_id", "user_id", "review_id", "cool", "funny", "useful", "stars", "date") \
  .where("useful > 0") \
  .writeStream \
  .format("delta") \
  .outputMode("append") \
  .option("mergeSchema", "true") \
  .option("checkpointLocation", "/delta/reviews/_checkpoints/bronze_silver_stream") \
  .start("/delta/stream_silver_reviews")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- 11.300.844
# MAGIC -- 22.601.688
# MAGIC 
# MAGIC SELECT COUNT(*)
# MAGIC FROM delta.`/delta/stream_silver_reviews`

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM delta.`/delta/stream_silver_reviews`
# MAGIC LIMIT 100

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT business_id, AVG(stars) AS avg_stars, COUNT(*) AS amount
# MAGIC FROM delta.`/delta/stream_silver_reviews`
# MAGIC GROUP BY business_id
# MAGIC ORDER BY amount DESC
# MAGIC LIMIT 100

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC # Summary
# MAGIC 
# MAGIC > resumo das atividades realizadas nesse notebook:
# MAGIC 
# MAGIC <br>
# MAGIC 
# MAGIC * visualização dos dados no data lake
# MAGIC * criação de um schema para o structured streaming
# MAGIC * trazendo os dados do data lake para o delta lake (bronze)
# MAGIC * modificando os dados em near real-time 
# MAGIC * colocando no delta lake (silver)
# MAGIC  

# COMMAND ----------

# MAGIC  %sql
# MAGIC 
# MAGIC SELECT COUNT(*)
# MAGIC FROM delta.`/delta/stream_silver_reviews`

# COMMAND ----------

