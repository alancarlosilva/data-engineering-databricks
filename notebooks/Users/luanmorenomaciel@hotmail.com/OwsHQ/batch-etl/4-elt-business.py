# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Dimensão de Business [Locais] com Delta Lake [Dw]
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

# MAGIC %md
# MAGIC 
# MAGIC > spark-sql built in functions - https://spark.apache.org/docs/2.3.0/api/sql/index.html  

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 1. Ingest DataSet
# MAGIC 
# MAGIC dataset selected for analysis
# MAGIC <br>
# MAGIC <br>

# COMMAND ----------

# DBTITLE 1,Usando o Banco de Dados - Yelp
# MAGIC %sql
# MAGIC 
# MAGIC USE Yelp

# COMMAND ----------

# DBTITLE 1,Listando Arquivos no Production Zone
# MAGIC %fs ls "dbfs:/mnt/prod-files/"

# COMMAND ----------

# DBTITLE 1,Carregando DataFrame com [spark.read.parquet]
ds_business = spark.read.parquet("dbfs:/mnt/prod-files/yelp_business.parquet/")
display(ds_business)

# COMMAND ----------

# DBTITLE 1,Mostrando Schema do DataFrame de Locais
ds_business.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC > muita das vezes seu time é totalmente multidisciplinar, portanto trabalham em diversas linguagens de programação  
# MAGIC > por isso a função de **registerDataFrameAsTable** é extremamente útil para que possamos usar *spark-sql*

# COMMAND ----------

# DBTITLE 1,Registrando [tmp_business] no Contexto de SparkSQL
sqlContext.registerDataFrameAsTable(ds_business, "tmp_business")

# COMMAND ----------

# DBTITLE 1,Consultando a Tabela com SparkSQL
# MAGIC %sql
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM tmp_business
# MAGIC LIMIT 100;

# COMMAND ----------

# DBTITLE 1,Descrevendo Características da Tabela
# MAGIC %sql
# MAGIC 
# MAGIC DESCRIBE TABLE tmp_business

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 2. Exploração
# MAGIC 
# MAGIC > **DataFrame** = tmp_business

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Stage 1 - Bronze Table
# MAGIC > escrevendo a tabela de parquet para delta, fazendo isso iremos ganhar em armazenamento, velocidade e teremos diversas opções que estão listadas acima  
# MAGIC > a tabela **ds_business** foi o DataFrame criado diretamente do **azure blob storage**  
# MAGIC 
# MAGIC > *lembre-se que esse layer não possui modificações, o dado vem direto do Data Lake para o Delta Lake Bronze Table*
# MAGIC 
# MAGIC <br>
# MAGIC 
# MAGIC <img width="200" src ='https://brzluanmoreno.blob.core.windows.net/stgfiles/png_files/bronze-table.png'>
# MAGIC <br>
# MAGIC <br>

# COMMAND ----------

# DBTITLE 1,Gravando em uma Tabela Bronze = bronze_business
ds_business.write.format("delta").mode("overwrite").save("/mnt/prod-files/bronze-tables/bronze_business")

# COMMAND ----------

# DBTITLE 1,Lendo Tabela Bronze [bronze_business]
ds_business = spark.read.format("delta").load("/mnt/prod-files/bronze-tables/bronze_business")

# COMMAND ----------

# DBTITLE 1,Analisando com PySpark - Select das Colunas 
ds_business_select = ds_business.select(['business_id','name','categories','city','state','address','latitude','longitude','review_count','stars'])
display(ds_business_select)

# COMMAND ----------

# DBTITLE 1,Deletando Tabela
spark.sql("DROP TABLE IF EXISTS tmp_business")

# COMMAND ----------

# DBTITLE 1,Deletando Tabela
# MAGIC %sql
# MAGIC 
# MAGIC DROP TABLE tmp_business

# COMMAND ----------

# DBTITLE 1,Criando Delta Table utilizando spark.sql no Python
spark.sql("CREATE TABLE tmp_business USING DELTA LOCATION '/mnt/prod-files/bronze-tables/bronze_business'")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Data Exploration [Data Engineer]

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC > cidades que mais utilizam o **yelp** baseados na quantidade de reviews

# COMMAND ----------

# DBTITLE 1,SparkSQL - Reviews por Cidade
# MAGIC %sql
# MAGIC 
# MAGIC SELECT city, SUM(review_count)
# MAGIC FROM tmp_business
# MAGIC GROUP BY city
# MAGIC ORDER BY SUM(review_count) DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC > verificando as cidades mais revisadas  
# MAGIC > porém não necessariamente as mais melhores classificadas

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC WITH reviews AS 
# MAGIC (
# MAGIC SELECT city, SUM(review_count) AS review_count
# MAGIC FROM tmp_business
# MAGIC GROUP BY city
# MAGIC HAVING SUM(review_count) > 1000
# MAGIC )
# MAGIC SELECT rv.city, AVG(b.stars) AS stars
# MAGIC FROM reviews AS rv
# MAGIC INNER JOIN tmp_business AS b
# MAGIC ON rv.city = b.city
# MAGIC GROUP BY rv.city
# MAGIC ORDER BY stars DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC > plotando dados no mapa para melhor visualização

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT state,
# MAGIC        AVG(stars)
# MAGIC FROM tmp_business
# MAGIC WHERE state NOT IN ('11', 'WAR', '01', 'NYK', 'NW', 'HH', 'QC', 'B', 'BC', 'M', 'V', 'BY', '6', 
# MAGIC 'SP', 'O', 'PO', 'XMS', 'C', 'XGM', 'CC', 'VS', 'RP', 'AG', 'SG', 'TAM', 'ON', 'AB', 'G', 'CS', 'RCC', 'HU', '10', 
# MAGIC '4', 'NI', 'NLK', 'HE', 'CMA', 'LU', 'WHT', '45', 'ST', 'CRF')
# MAGIC GROUP BY state

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 3. Transformação
# MAGIC 
# MAGIC > **DataFrame** = tmp_business

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

# DBTITLE 1,Criando Tabela Delta com [Create Table using Delta]
# MAGIC %sql
# MAGIC 
# MAGIC DROP TABLE IF EXISTS silver_business;
# MAGIC 
# MAGIC CREATE TABLE silver_business
# MAGIC USING delta
# MAGIC AS
# MAGIC SELECT business_id,
# MAGIC        name,
# MAGIC        city, 
# MAGIC        state,
# MAGIC        regexp_extract(categories,"^(.+?),") AS category,
# MAGIC        categories AS subcategories,
# MAGIC        review_count,
# MAGIC        stars
# MAGIC FROM tmp_business

# COMMAND ----------

# DBTITLE 1,Consultando Quantidade de Registros
# MAGIC %sql
# MAGIC SELECT COUNT(*)
# MAGIC FROM silver_business

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM silver_business
# MAGIC LIMIT 10

# COMMAND ----------

# DBTITLE 1,Otimização Tabela Dela com Cache [Optimize]
display(spark.sql("OPTIMIZE silver_business"))

# COMMAND ----------

# DBTITLE 1,Verificando Nova Tabela Delta = [yelp_delta_business]
# MAGIC %sql
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM silver_business

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT COUNT(*) FROM silver_business

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC > https://docs.microsoft.com/en-us/azure/databricks/delta/optimizations/delta-cache

# COMMAND ----------

