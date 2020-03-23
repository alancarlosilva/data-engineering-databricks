# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Dimensão de User [Usuário] com Delta Lake [Dw]
# MAGIC 
# MAGIC > nesse caso como temos os dados armazenados no Data Lake iremos criar a seguinte estrutura:
# MAGIC <br>
# MAGIC 
# MAGIC > **Staging [Bronze]** = ingerindo os dados do mesmo jeito do que está na fonte de dados    
# MAGIC > **Transformação [Silver]** = aplicar as transformações para refinarmos a tabela  
# MAGIC > **Dw [Gold]** = criar a tabela que está tratada e preparada  
# MAGIC 
# MAGIC <br>
# MAGIC   
# MAGIC <img width="500px" src ='https://brzluanmoreno.blob.core.windows.net/stgfiles/png_files/dw-delta-lake-silver.png'>
# MAGIC   
# MAGIC <br>
# MAGIC > informações sobre o dataset do *JSON*  
# MAGIC > https://www.yelp.com/dataset/challenge
# MAGIC 
# MAGIC > **1** - Ingestão [Fase 1]  
# MAGIC > **2** - Exploração [Fase 2]     
# MAGIC > **3** - Transformação [Fase 3]  

# COMMAND ----------

# DBTITLE 1,Criando um Banco de Dados no Contexto do Apache Spark
# MAGIC %sql
# MAGIC 
# MAGIC CREATE DATABASE IF NOT EXISTS Yelp

# COMMAND ----------

# DBTITLE 1,Use para o Contexto do Notebook
# MAGIC %sql
# MAGIC 
# MAGIC USE Yelp

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 1. Ingestão e Exploração dos Dados

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC > estamos consultando o layer de produção porque já realizamos a conversão dos dados para o modo **parquet** e de fato esse é o melhor tipo para ser trabalhado no **spark**

# COMMAND ----------

# DBTITLE 1,Listando Dados da Production Zone
# MAGIC %fs ls "dbfs:/mnt/prod-files/"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC > o **DBFS** - databricks file system é uma camada de storage implementada pelo **Databricks** para aumentar a performance de consulta dos dados, esse layer possibilita certas otimizações para que independemente do storage que você estiver acessando - *blob storage, google cloud storage, s3, hdfs* ele tent otimizar as leituras e escritas.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### RDD [Resilient Distributed DataSet]
# MAGIC > An RDD stands for Resilient Distributed Datasets. It is Read-only partition collection of records. RDD is the fundamental data structure of Spark. It allows a programmer to perform in-memory computations on large clusters in a fault-tolerant manner. Thus, speed up the task. 
# MAGIC 
# MAGIC <br>
# MAGIC 
# MAGIC ### DataFrame
# MAGIC >  Unlike an RDD, data organized into named columns. For example a table in a relational database. It is an immutable distributed collection of data. DataFrame in Spark allows developers to impose a structure onto a distributed collection of data, allowing higher-level abstraction. 
# MAGIC 
# MAGIC <br>
# MAGIC 
# MAGIC ### DataSet
# MAGIC > Datasets in Apache Spark are an extension of DataFrame API which provides type-safe, object-oriented programming interface. Dataset takes advantage of Spark’s Catalyst optimizer by exposing expressions and data fields to a query planner. 

# COMMAND ----------

# DBTITLE 1,Carregando DataFrame com [spark.read.parquet]
ds_user = spark.read.parquet("dbfs:/mnt/prod-files/yelp_user.parquet/")
display(ds_user)

# COMMAND ----------

# DBTITLE 1,Removendo View [if exists]
# MAGIC %sql
# MAGIC 
# MAGIC DROP VIEW IF EXISTS ds_user

# COMMAND ----------

# DBTITLE 1,Carregando DataFrame com [org.apache.spark.sql.parquet]
# MAGIC %sql
# MAGIC 
# MAGIC CREATE TEMPORARY VIEW ds_user
# MAGIC USING org.apache.spark.sql.parquet
# MAGIC OPTIONS ( path "dbfs:/mnt/prod-files/yelp_user.parquet/" )

# COMMAND ----------

# DBTITLE 1,Lendo DataFrame [DBFS - Azure Blob Storage]
# MAGIC %sql
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM ds_user

# COMMAND ----------

# DBTITLE 1,Mostrando Schema do DataFrame de Usuário
ds_user.printSchema()

# COMMAND ----------

# DBTITLE 1,Explicando Plano de Execução [Catalyst]
sql("SELECT * FROM ds_user").explain()

# COMMAND ----------

# DBTITLE 1,Explicando Plano de Execução Lógico
# MAGIC %sql
# MAGIC 
# MAGIC EXPLAIN EXTENDED SELECT * FROM ds_user

# COMMAND ----------

# DBTITLE 1,Descrevendo Características da [ds_user]
# MAGIC %sql
# MAGIC 
# MAGIC DESCRIBE TABLE ds_user

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 2. Exploração
# MAGIC 
# MAGIC > **DataFrame** = ds_user

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC > após carregar os dados para o *dataframe* iremos iniciar o processo de exploração e enriquececimento dos dados para entendimento da nossa base  
# MAGIC > os arquivos que estavam no data lake foram carregados para o contexto do apache spark e agora iremos normatizar as informações

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC > o **spark** oferece a possibilidade de mover os dados de uma engine para outra ou seja, você pode escrever em **python** e mover para **sql** sendo que para isso mesmo precisa estar no contexto correto. Cada engine - *sql, scala, r, python* tem suas variáveis e processos porém o **SQL** é a engine que é construída como **core** do produto, portanto para mover o **dataframe ou dataset** é extremamente simples

# COMMAND ----------

# DBTITLE 1,Movendo do SparkSQL para a Engine Python [PySpark] com [df_user]
# MAGIC %python
# MAGIC 
# MAGIC df_user = spark.sql("SELECT * FROM ds_user")

# COMMAND ----------

# DBTITLE 1,PySpark - Where no Campo [Fans]
display(df_user.where("fans > 3"))

# COMMAND ----------

# DBTITLE 1,PySpark - Selecionando o Campo [Elite]
display(ds_user.select("elite"))

# COMMAND ----------

# DBTITLE 1,PySpark - Descrevendo o Campo [Useful]
display(ds_user.select("useful").describe())

# COMMAND ----------

# DBTITLE 1,PySpark - Agregação dos Dados de [Review_Count]
from pyspark.sql.types import *
from pyspark.sql import functions as f

display(ds_user.groupBy("elite").agg(f.avg("review_count")).orderBy("elite"))

# COMMAND ----------

# DBTITLE 1,Spark-SQL - Agregação dos Dados de [Review_Count]
# MAGIC %sql
# MAGIC 
# MAGIC SELECT elite, avg(review_count) AS review_count
# MAGIC FROM ds_user
# MAGIC GROUP BY elite
# MAGIC ORDER BY review_count DESC

# COMMAND ----------

# DBTITLE 1,Criando uma Tabela Utilizando CTAS [Create Table as Select]
# MAGIC %sql
# MAGIC 
# MAGIC DROP TABLE IF EXISTS yelp_user;
# MAGIC 
# MAGIC CREATE TABLE yelp_user
# MAGIC AS
# MAGIC SELECT user_id,
# MAGIC        name,
# MAGIC        average_stars,
# MAGIC        fans,
# MAGIC        review_count,
# MAGIC        useful,
# MAGIC        yelping_since
# MAGIC FROM ds_user

# COMMAND ----------

# DBTITLE 1,Selecionando Nova Tabela - [yelp_user]
# MAGIC %sql
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM yelp_user 
# MAGIC LIMIT 1000;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 3. Transformação
# MAGIC 
# MAGIC > **DataFrame** = yelp_user

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <img width="300px" src="https://docs.delta.io/latest/_static/delta-lake-logo.png">  
# MAGIC <br>
# MAGIC 
# MAGIC 
# MAGIC > *Delta Lake* é uma abstração ACID para tabelas em Spark  
# MAGIC > criada no format *parquet* para otimização de acesso e caching
# MAGIC <br>
# MAGIC 
# MAGIC <img width="800px" src ='https://brzluanmoreno.blob.core.windows.net/stgfiles/png_files/delta-lake-new.png'>
# MAGIC <br>
# MAGIC <br>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Stage 1 - Bronze Table
# MAGIC > escrevendo a tabela de parquet para delta, fazendo isso iremos ganhar em armazenamento, velocidade e teremos diversas opções que estão listadas acima  
# MAGIC > a tabela **ds_user** foi o DataFrame criado diretamente do **azure blob storage**  
# MAGIC 
# MAGIC > *lembre-se que esse layer não possui modificações, o dado vem direto do Data Lake para o Delta Lake Bronze Table*
# MAGIC 
# MAGIC <br>
# MAGIC 
# MAGIC <img width="200" src ='https://brzluanmoreno.blob.core.windows.net/stgfiles/png_files/bronze-table.png'>
# MAGIC <br>
# MAGIC <br>

# COMMAND ----------

# DBTITLE 1,Mostrando Dados
display(ds_user)

# COMMAND ----------

# DBTITLE 1,Gravando os Dados no Delta Lake [Bronze Table]
table("ds_user").write.mode("overwrite").format("delta").saveAsTable("bronze_user")

# COMMAND ----------

# DBTITLE 1,Descrevendo a Tabela [Bronze]
# MAGIC %sql
# MAGIC 
# MAGIC DESC FORMATTED bronze_user

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC > criando uma função para classificação do público para a plataforma  
# MAGIC > udf - https://docs.databricks.com/spark/latest/spark-sql/udf-in-python.html

# COMMAND ----------

# DBTITLE 1,PySpark - Criando e Registrando uma Nova Função para Classificação
def usr_importance(average_stars,fans,review_count):
  if average_stars >=3 and fans > 50 and review_count >= 15:
    return "rockstar"
  if average_stars <2 and fans < 20 and review_count < 15:
    return "low"
  else:
    return "normal"
  
# registering udf [core]
spark.udf.register("usr_importance",usr_importance) 

# COMMAND ----------

# DBTITLE 1,Acessando Função Criada no PySpark com SparkSQL
# MAGIC %sql
# MAGIC 
# MAGIC SELECT usr_importance(average_stars,fans,review_count), COUNT(*)
# MAGIC FROM bronze_user
# MAGIC GROUP BY usr_importance(average_stars,fans,review_count);

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

# DBTITLE 1,Persistindo Nova Coluna de "importance" em um Novo DataFrame [ds_silver_users]
ds_silver_users = table("bronze_user").selectExpr("usr_importance(average_stars,fans,review_count) importance", "*")

# COMMAND ----------

# DBTITLE 1,Nova Estrutura da Silver Table [silver_users]
display(ds_silver_users)

# COMMAND ----------

# DBTITLE 1,Escrevendo DataFrame em uma Tabela Silver no Delta [silver_users]
ds_silver_users.write.format("delta").mode("overwrite").save("/mnt/prod-files/silver-tables/")

# COMMAND ----------

# DBTITLE 1,Lendo Tabela Silver [silver_users]
df_users_silver = spark.read.format("delta").load("/mnt/prod-files/silver-tables/")

# COMMAND ----------

# DBTITLE 1,Visualizando Tabela Silver
display(df_users_silver)

# COMMAND ----------

# DBTITLE 1,Contando Quantidade de Registros
df_users_silver.count()

# COMMAND ----------

# DBTITLE 1,Spark SQL - Criando Tabela utilizando SQL - Silver Table
# MAGIC %sql
# MAGIC 
# MAGIC DROP TABLE IF EXISTS df_users_silver;
# MAGIC 
# MAGIC CREATE TABLE df_users_silver
# MAGIC USING delta 
# MAGIC OPTIONS
# MAGIC (
# MAGIC   path = "/mnt/prod-files/silver-tables/"
# MAGIC )

# COMMAND ----------

# DBTITLE 1,Contando Quantidade de Registros = [silver_users]
# MAGIC %sql
# MAGIC --1.518.169
# MAGIC 
# MAGIC SELECT COUNT(*)
# MAGIC FROM df_users_silver

# COMMAND ----------

# DBTITLE 1,Selecionando Dados da Tabela Delta = [silver_users]
# MAGIC %sql
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM df_users_silver
# MAGIC WHERE importance = 'rockstar'

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Transformações
# MAGIC 
# MAGIC > transformações finais para ter a tabela silver o mais limpa possível

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT user_id,
# MAGIC        name,
# MAGIC        average_stars,
# MAGIC        fans,
# MAGIC        review_count,
# MAGIC        importance,
# MAGIC        yelping_since
# MAGIC FROM df_users_silver

# COMMAND ----------

# DBTITLE 1,Criando Tabela Silver Final [silver_users_final]
# MAGIC %sql
# MAGIC 
# MAGIC DROP TABLE IF EXISTS silver_users;
# MAGIC 
# MAGIC CREATE TABLE silver_users
# MAGIC USING delta
# MAGIC AS
# MAGIC SELECT user_id,
# MAGIC        name,
# MAGIC        average_stars,
# MAGIC        fans,
# MAGIC        review_count,
# MAGIC        importance,
# MAGIC        yelping_since
# MAGIC FROM df_users_silver

# COMMAND ----------

# DBTITLE 1,Show Table History [Histórico da Tabela]
display(spark.sql("DESCRIBE HISTORY silver_users"))

# COMMAND ----------

# DBTITLE 1,Show Table Format [Formato da Tabela] 
display(spark.sql("DESCRIBE FORMATTED silver_users"))

# COMMAND ----------

# DBTITLE 1,Otimização Tabela Delta com Cache [Optimize]
display(spark.sql("OPTIMIZE silver_users_final"))

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC # Summary
# MAGIC 
# MAGIC > resumo das atividades realizadas nesse notebook:
# MAGIC 
# MAGIC <br>
# MAGIC 
# MAGIC * leitura do arquivo parquet = dbfs:/mnt/prod-files/yelp_user.parquet/
# MAGIC * ingestão dos dados parquet para uma tabela delta [bronze]
# MAGIC * modifições, melhorias e otimizações 
# MAGIC * finalizando com uma tabela silver = silver_users

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT COUNT(*) 
# MAGIC FROM silver_users

# COMMAND ----------

