# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Convertendo JSON para Apache Parquet
# MAGIC > recebendo os dados da aplicação ou de qualquer outra fonte de dados  
# MAGIC > convertendo para o tipo de arquivo mais otimizado para se trabalhar com Spark
# MAGIC <br>
# MAGIC   
# MAGIC <img width="500px" src ='https://brzluanmoreno.blob.core.windows.net/stgfiles/png_files/parquet-json.png'>
# MAGIC   
# MAGIC <br>
# MAGIC > informações sobre o **Apache Parquet**  
# MAGIC > https://parquet.apache.org/

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <img width="50px" src="https://brzluanmoreno.blob.core.windows.net/stgfiles/png_files/apache-parquet.png">
# MAGIC 
# MAGIC > o **apache parquet** é um tipo de arquivo colunar que acelera e otimiza arquivos no formato *csv* ou *json*  
# MAGIC > esse modelo colunar faz integração com quase todas os frameworks de processamento de dados de big data  
# MAGIC > é extremamente utilizado juntamente com o apache spark

# COMMAND ----------

# DBTITLE 1,Listando Landing Zone [JSON]
# MAGIC %fs ls "dbfs:/mnt/bs-stg-files/yelp_dataset/"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC > os arquivos dentro do data lake estão no format JSON, como explicado anteriormente para que você possa ganhar em otimização  
# MAGIC > é extremamente importante que você sempre realize a conversão dos tipos de dados, se estiver trabalhando com spark então definitivamente   
# MAGIC > utilize apache parquet

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC > o Apache Spark possibilita diversos tipos de leituras diferentes, segue opções disponíveis utilizando = **(spark.read.)**
# MAGIC <br>
# MAGIC 
# MAGIC * Parquet
# MAGIC * JSON
# MAGIC * CSV
# MAGIC * ORC
# MAGIC * JDBC

# COMMAND ----------

# DBTITLE 1,Lendo Arquivos no Format JSON
df_business = spark.read.json("dbfs:/mnt/stg-files/yelp_dataset/yelp_academic_dataset_business.json")
df_checkin = spark.read.json("dbfs:/mnt/stg-files/yelp_dataset/yelp_academic_dataset_checkin.json")
df_user = spark.read.json("dbfs:/mnt/stg-files/yelp_dataset/yelp_academic_dataset_user.json")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC > os dados são lidos pela engine e colocados dentro de um **dataframe** que é uma estrutura organizada em colunas, como se fosse uma tabela de banco de dados  
# MAGIC > porém distribuídas entre nós de computação

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC > **spark.read.json** automaticamente infere o schema do arquivo json utilizando  
# MAGIC > também disponível com **sql**  
# MAGIC > https://spark.apache.org/docs/latest/sql-data-sources-json.html
# MAGIC 
# MAGIC <br>
# MAGIC 
# MAGIC > CREATE TEMPORARY VIEW jsonTable  
# MAGIC   USING org.apache.spark.sql.json  
# MAGIC   OPTIONS (path "examples/src/main/resources/people.json")  
# MAGIC 
# MAGIC > SELECT * FROM jsonTable

# COMMAND ----------

# DBTITLE 1,Lendo DataFrame = users
display(df_user)

# COMMAND ----------

# DBTITLE 1,Lendo DataFrame = business
display(df_business)

# COMMAND ----------

# DBTITLE 1,Lendo DataFrame = check-in
display(df_checkin)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC > convertendo os dataframes para o apache parquet  
# MAGIC > algumas vantagens do **Apache Parquet**
# MAGIC 
# MAGIC > **1** - aceita estrutura complexas para arquivo  
# MAGIC > **2** - eficiente para compressão e encoding de schema  
# MAGIC > **3** - permite menos acesso ao disco e consultas mais rápidas no arquivo  
# MAGIC > **4** - menos overhead de I/O  
# MAGIC > **5** - aumento na velocidade de scan  

# COMMAND ----------

# DBTITLE 1,Lendo DataFrames e Escrevendo na [Production Zone] do Data Lake em Format Apache Parquet
df_business.write.mode("overwrite").parquet("/mnt/prod-files/yelp_business.parquet")
df_checkin.write.mode("overwrite").parquet("/mnt/prod-files/yelp_checkin.parquet")
df_user.write.mode("overwrite").parquet("/mnt/prod-files/yelp_user.parquet")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC > acessando os arquivos convertidos de *json* para *parquet* no data lake

# COMMAND ----------

# DBTITLE 1,Acessando [Production Zone] no Data Lake
# MAGIC %fs ls "dbfs:/mnt/prod-files/"

# COMMAND ----------

