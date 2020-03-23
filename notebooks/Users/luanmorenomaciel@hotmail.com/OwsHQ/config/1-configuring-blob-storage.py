# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Azure Blob Storage e Azure Databricks
# MAGIC > configuração do azure blob storage com o Azure Databricks  
# MAGIC > dados recebidos do Azure Data Factory [ADF]
# MAGIC <br>
# MAGIC   
# MAGIC <img width="500px" src ='https://brzluanmoreno.blob.core.windows.net/stgfiles/png_files/blob_storage_azure_databricks.png'>
# MAGIC   
# MAGIC <br>
# MAGIC > configurando acesso ao Azure Blob Storage do Microsoft Azure  
# MAGIC > https://docs.databricks.com/spark/latest/data-sources/azure/azure-storage.html

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC > nesse tutorial você irá aprender a realizar a configuração do Azure Databricks com o Azure Blob Storage da Microsoft. Na maioria dos casos o **Databricks** irá se conectar com um *Data Lake* para ingerir os dados do mesmo para o processamento, aqui iremos usar o *databricks command-line interface (CLI)* para realizar o cadastramento da chave de acesso do Azure Blob Storage, assim a mesma não ficará exposta dentro do Notebook.
# MAGIC 
# MAGIC > Esse tipo de configuração é essencial para um **Data Engineer** porque irá evitar qualquer brecha de segurança no Data Lake. o Databricks irá guardar essas informações no *vault* de segurança aonde o mesmo é criptografado.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### databricks command-line interface [CLI]
# MAGIC 
# MAGIC > https://docs.azuredatabricks.net/user-guide/secrets/secret-scopes.html  
# MAGIC > https://docs.azuredatabricks.net/user-guide/dev-tools/databricks-cli.html
# MAGIC 
# MAGIC 1. instalação do databricks-cli
# MAGIC > pip install databricks-cli  
# MAGIC 
# MAGIC 2. criação do databricks secrets e scope
# MAGIC > databricks secrets create-scope --scope az-bs-brzluanmoreno   
# MAGIC > databricks secrets list-scopes  
# MAGIC > databricks secrets put --scope az-bs-brzluanmoreno --key key-brzluanmoreno
# MAGIC <br>
# MAGIC 
# MAGIC > a configuração das credenciais do **azure blob storage** utilizando o **databricks cli** adiciona um layer de segurança, sendo assim não se faz necessário a   
# MAGIC > utilização das chaves de forma explícita dentro do notebook do Azure Databricks

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC > https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html  
# MAGIC 
# MAGIC > Iremos utilizar o **dbutils** para realizar a **mount** do Blob Storage com o Databricks, aqui estamos montando no local - **/mnt/**  
# MAGIC > dentro do Databricks porém se faz necessário as informações do Blob Storage como nome e a **key** esse é o nome da chave que será gerada no final da configuração anterior.

# COMMAND ----------

# DBTITLE 1,Pasta no Blob Storage [bs-stg-files] = Landing Zone
# MAGIC %python
# MAGIC 
# MAGIC dbutils.fs.mount(
# MAGIC   source = "wasbs://bs-stg-files@brzluanmoreno.blob.core.windows.net",
# MAGIC   mount_point = "/mnt/bs-stg-files",
# MAGIC   extra_configs = {"fs.azure.account.key.brzluanmoreno.blob.core.windows.net":dbutils.secrets.get(scope = "az-blob-storage-luanmoreno", key = "key-az-blob-storage")})

# COMMAND ----------

# DBTITLE 1,Listando Conteúdo da Landing Zone
display(dbutils.fs.ls("/mnt/bs-stg-files/yelp_dataset"))

# COMMAND ----------

# DBTITLE 1,Pasta no Blob Storage [bs-stg-files] = Production Zone
# MAGIC %python
# MAGIC 
# MAGIC dbutils.fs.mount(
# MAGIC   source = "wasbs://bs-production-yelp@brzluanmoreno.blob.core.windows.net",
# MAGIC   mount_point = "/mnt/bs-production-yelp",
# MAGIC   extra_configs = {"fs.azure.account.key.brzluanmoreno.blob.core.windows.net":dbutils.secrets.get(scope = "az-blob-storage-luanmoreno", key = "key-az-blob-storage")})

# COMMAND ----------

# DBTITLE 1,Listando Conteúdo da Production Zone
display(dbutils.fs.ls("/mnt/bs-production-yelp"))

# COMMAND ----------

# DBTITLE 1,Mount File = /mnt/bs-stg-files [Landing Zone Area]
display(dbutils.fs.ls("/mnt/bs-stg-files"))

# COMMAND ----------

# DBTITLE 1,Mount File = /mnt/prod-files [Production Zone Area]
display(dbutils.fs.ls("/mnt/prod-files"))

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC > se desejar remover a configuração do DBFS executar o comando abaixo

# COMMAND ----------

# DBTITLE 1,Removendo a Configuração do DBFS
dbutils.fs.unmount("/mnt/bs-stg-files/yelp_dataset")
dbutils.fs.unmount("/mnt/bs-production-yelp")