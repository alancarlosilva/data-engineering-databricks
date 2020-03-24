// Databricks notebook source
// MAGIC %md 
// MAGIC 
// MAGIC ### Ingerindo Dados do Apache Kafka com Structured Streaming
// MAGIC 
// MAGIC > nesse tutorial iremos ingerir dados do apache kafka com o structured streaming e utilizando o schema registry da confluent
// MAGIC 
// MAGIC <br>
// MAGIC 
// MAGIC <img width="1000px" src ='https://brzluanmoreno.blob.core.windows.net/stgfiles/png_files/sql-use-case.png'>
// MAGIC   
// MAGIC <br>

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC #### 1) Fonte de Dados SQL Server
// MAGIC > informações da fonte de dados  
// MAGIC > dns = sql-server-dev-ip.eastus2.cloudapp.azure.com  
// MAGIC > acessar sql-server-dev com microsoft remote desktop  
// MAGIC > bd = yelp, tabela = dbo.restaurant_week_2019
// MAGIC 
// MAGIC <br>
// MAGIC 
// MAGIC > 
// MAGIC SELECT TOP (1000) [name]  
// MAGIC       ,[street_address]  
// MAGIC       ,[review_count]  
// MAGIC       ,[phone]  
// MAGIC       ,[restaurant_type]  
// MAGIC       ,[average_review]  
// MAGIC       ,[food_review]  
// MAGIC       ,[service_review]  
// MAGIC       ,[ambience_review]  
// MAGIC       ,[value_review]  
// MAGIC       ,[price_range]  
// MAGIC       ,[restaurant_main_type]  
// MAGIC   FROM [Yelp].[dbo].[restaurant_week_2019]  

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC #### 2) Apache Kafka [Kafka Connect Source API]
// MAGIC 
// MAGIC > conectar no vm = dev-kafka-confluent.eastus2.cloudapp.azure.com  
// MAGIC > criar conector para ingerir dados do sql server  
// MAGIC > [kafka-connect-jdbc](https://docs.confluent.io/current/connect/kafka-connect-jdbc/source-connector/index.html)
// MAGIC 
// MAGIC <br>
// MAGIC 
// MAGIC > **connector status**  
// MAGIC curl -s "http://localhost:8083/connectors?expand=status" | \  
// MAGIC jq 'to_entries[] | [.key, .value.status.connector.state,.value.status.tasks[].state]|join(":|:")' | \  
// MAGIC column -s : -t| sed 's/\"//g'| sort  
// MAGIC 
// MAGIC > **deploying sql server connector**  
// MAGIC curl -X POST -H "Content-Type: application/json" --data '{ "name": "source-sql-server-restaurant-week", "config": { "connector.class":"io.confluent.connect.jdbc.JdbcSourceConnector", "key.converter":"io.confluent.connect.avro.AvroConverter", "key.converter.schema.registry.url":"http://localhost:8081", "value.converter":"io.confluent.connect.avro.AvroConverter", "value.converter.schema.registry.url":"http://localhost:8081", "connection.url":"jdbc:sqlserver://sql-server-dev-ip.eastus2.cloudapp.azure.com;database=Yelp;username=kafka_reader;password=demo@pass123", "connection.attempts":"2", "query":"SELECT * FROM dbo.restaurant_week_2019", "mode":"bulk", "topic.prefix":"source-sql-server-restaurant-week", "poll.interval.ms":"86400000", "tasks.max":"2", "validate.non.null":"false" } }' http://localhost:8083/connectors  
// MAGIC 
// MAGIC > **query rest api**  
// MAGIC curl localhost:8083/connectors  
// MAGIC source-sql-server-restaurant-week
// MAGIC 
// MAGIC > **get connector tasks**  
// MAGIC curl localhost:8083/connectors/source-sql-server-restaurant-week/tasks | jq
// MAGIC 
// MAGIC > **get status**  
// MAGIC curl localhost:8083/connectors/source-sql-server-restaurant-week/status | jq
// MAGIC 
// MAGIC > **list topics**  
// MAGIC kafka-topics --list --zookeeper localhost:2181  
// MAGIC source-sql-server-restaurant-week  
// MAGIC 
// MAGIC > **reading topic**  
// MAGIC kafka-avro-console-consumer --bootstrap-server localhost:9092 --property schema.registry.url=http://localhost:8081 --property print.key=true --from-beginning --topic source-sql-server-restaurant-week
// MAGIC 
// MAGIC > **restart connector**  
// MAGIC curl -X POST localhost:8083/connectors/source-sql-server-restaurant-week/tasks/0/restart  
// MAGIC 
// MAGIC > **deleting connector**   
// MAGIC curl -X DELETE localhost:8083/connectors/source-sql-server-restaurant-week

// COMMAND ----------

// DBTITLE 1,Kafka Broker
// MAGIC %sh telnet 52.177.14.43 9092

// COMMAND ----------

// DBTITLE 1,Zookeeper
// MAGIC %sh telnet 52.177.14.43 2181

// COMMAND ----------

// DBTITLE 1,Schema Registry
// MAGIC %sh telnet 52.177.14.43 8081

// COMMAND ----------

// DBTITLE 1,Confluent REST Proxy
// MAGIC %sh telnet 52.177.14.43 8082

// COMMAND ----------

// DBTITLE 1,Libraries
import org.apache.spark.sql.avro.functions.from_avro
import org.apache.avro.SchemaBuilder

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC https://spark.apache.org/docs/latest/sql-data-sources-avro.html

// COMMAND ----------

// DBTITLE 1,Var
//kafka
val kafkaBrokers = "52.177.14.43:9092"

//schema registry
val schemaRegistryAddr = "http://52.177.14.43:8081"

//topic
val mssql_topic = "source-sql-server-restaurant-week"

//avro
val mssql_avro = "source-sql-server-restaurant-week-value"

// COMMAND ----------

// DBTITLE 1,MSSQL ~ Apache Kafka ~ Structured Streaming - readStream
//kafka
val df_kafka_mssql = spark
  .readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", kafkaBrokers)
  .option("subscribe", mssql_topic)
  .option("startingOffsets", "earliest")
  .load()
  .select(from_avro($"value", mssql_avro, schemaRegistryAddr).as("value"))

// COMMAND ----------

// DBTITLE 1,Streaming de Dados [Validação e Verificação]
df_kafka_mssql.isStreaming

// COMMAND ----------

// MAGIC %fs rm -r "/delta/mssql/checkpoints/mssql-ingest-restaurant-week"

// COMMAND ----------

// MAGIC %sql DELETE FROM delta.`/delta/bronze/ingest-restaurant-week`

// COMMAND ----------

// DBTITLE 1,Lendo Streaming e Salvando e uma (Delta Table)
df_kafka_mssql 
  .writeStream
  .format("delta") 
  .outputMode("append")
  .option("checkpointLocation", "/delta/mssql/checkpoints/mssql-ingest-restaurant-week") 
  .start("/delta/bronze/ingest-restaurant-week")

// COMMAND ----------

// DBTITLE 1,[mssql] = Utilizando %sql para Acessar Tabela [Delta]
// MAGIC %sql
// MAGIC 
// MAGIC SELECT COUNT(*) AS amt
// MAGIC FROM delta.`/delta/bronze/ingest-restaurant-week`

// COMMAND ----------

// DBTITLE 1,Integração em Streaming com %sql
// MAGIC %sql
// MAGIC 
// MAGIC SELECT value.*
// MAGIC FROM delta.`/delta/bronze/ingest-restaurant-week`

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC USE yelp;

// COMMAND ----------

// DBTITLE 1,Silver Table = silver_restaurant_week
// MAGIC %sql
// MAGIC 
// MAGIC DROP TABLE IF EXISTS silver_restaurant_week;
// MAGIC 
// MAGIC CREATE TABLE silver_restaurant_week
// MAGIC USING delta
// MAGIC AS
// MAGIC SELECT value.name AS restaurant_name,
// MAGIC        value.review_count AS review_count,
// MAGIC        value.restaurant_type AS restaurant_type,
// MAGIC        value.average_review AS average_review,
// MAGIC        value.restaurant_main_type AS restaurant_main_type
// MAGIC FROM delta.`/delta/bronze/ingest-restaurant-week`

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC SELECT *
// MAGIC FROM silver_restaurant_week

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC > limpeza dos dados

// COMMAND ----------

// MAGIC %fs rm -r "/delta/mssql/checkpoints/mssql-ingest-restaurant-week"

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC DELETE FROM delta.`/delta/bronze/ingest-restaurant-week`

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC VACUUM delta.`/delta/bronze/ingest-restaurant-week`