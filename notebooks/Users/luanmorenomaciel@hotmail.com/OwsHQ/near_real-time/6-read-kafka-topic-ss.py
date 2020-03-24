# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Apache Kafka + Structured Streaming [Apache Spark] 
# MAGIC > esse exercício tem como objetivo mostrar como é possível consultar os dados do Apache kafka em near real-time, segue informações sobre:  
# MAGIC 
# MAGIC <br>
# MAGIC 
# MAGIC ##### SQL Server
# MAGIC > nesse caso temos o SQL Server como fonte de dados aonde será disponibilizado uma view para consulta das informações, o nome da view é: **vw_sales_order**
# MAGIC 
# MAGIC 
# MAGIC <br>
# MAGIC 
# MAGIC #### Apache Kafka
# MAGIC > logo abaixo segue os comandos que já foram executados do apache kafka para disponibilizar os dados do SQL Server como um tópico.
# MAGIC 
# MAGIC <br>
# MAGIC 
# MAGIC > **connector status**  
# MAGIC curl -s "http://localhost:8083/connectors?expand=status" | \  
# MAGIC jq 'to_entries[] | [.key, .value.status.connector.state,.value.status.tasks[].state]|join(":|:")' | \  
# MAGIC column -s : -t| sed 's/\"//g'| sort  
# MAGIC 
# MAGIC > **deploying sql server connector**  
# MAGIC curl -X POST -H "Content-Type: application/json" --data '{ "name": "owshq-vw-sales-order", "config": { "connector.class":"io.confluent.connect.jdbc.JdbcSourceConnector", "key.converter":"io.confluent.connect.avro.AvroConverter", "key.converter.schema.registry.url":"http://localhost:8081", "value.converter":"io.confluent.connect.avro.AvroConverter", "value.converter.schema.registry.url":"http://localhost:8081", "connection.url":"jdbc:sqlserver://sql-server-dev-ip.eastus2.cloudapp.azure.com;database=OwsHQ;username=kafka_reader;password=demo@pass123", "connection.attempts":"2", "query":"SELECT * FROM dbo.vw_sales_order", "mode":"bulk", "topic.prefix":"owshq-vw-sales-order", "poll.interval.ms":"86400000", "tasks.max":"2", "validate.non.null":"false" } }' http://localhost:8083/connectors
# MAGIC 
# MAGIC > **query rest api**  
# MAGIC curl localhost:8083/connectors  
# MAGIC owshq-vw-sales-order
# MAGIC 
# MAGIC > **get connector tasks**  
# MAGIC curl localhost:8083/connectors/owshq-vw-sales-order/tasks | jq
# MAGIC 
# MAGIC > **get status**  
# MAGIC curl localhost:8083/connectors/owshq-vw-sales-order/status | jq
# MAGIC 
# MAGIC > **list topics**  
# MAGIC kafka-topics --list --zookeeper localhost:2181  
# MAGIC owshq-vw-sales-order
# MAGIC 
# MAGIC > **reading topic**  
# MAGIC kafka-avro-console-consumer --bootstrap-server localhost:9092 --property schema.registry.url=http://localhost:8081 --property print.key=true --from-beginning --topic owshq-vw-sales-order
# MAGIC 
# MAGIC > **restart connector**  
# MAGIC curl -X POST localhost:8083/connectors/owshq-vw-sales-order/tasks/0/restart  
# MAGIC 
# MAGIC > **deleting connector**   
# MAGIC curl -X DELETE localhost:8083/connectors/owshq-vw-sales-order
# MAGIC 
# MAGIC > **deleting topic**   
# MAGIC kafka-topics --zookeeper localhost:2181 --delete --topic owshq-vw-sales-order

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC # Tópico - owshq-vw-sales-order
# MAGIC > a partir daqui iniciamos nosso processo de entedimento, nesse momento temos o SQL Server como fontes de dados e todas as alterações que acontecem na fonte são enviadas de forma reativa como um evento para o Apache Kafka, além disso o Kafka está integrado com o Schema Registry que é reponsável por armazenar todos os schemas dos produtos no kafka, essas informações são armazenadas em formato .avro fazendo com que fique fácil consumí-las.  
# MAGIC 
# MAGIC > quando qualquer produtor envia dados para o kafka normalmente ele deve especificar qual é o tipo da mensagem (byte, string, json) porém com o schema registry, o mesmo armazena essa informações e todos os consumers ao invês de necessitar deserializar a mensagem pode consultar o schema direto do schema registry.  
# MAGIC 
# MAGIC > logo abaixo segue o schema da mensagem: 
# MAGIC 
# MAGIC <br>
# MAGIC 
# MAGIC >  
# MAGIC null    {"SalesOrderDetailID":12921,"ProductID":808,"ProductName":"LL Mountain Handlebars","UnitPrice":{"double":24.2945},"ProductNumber":"HB-M243","Amount":1,"SalesTimestamp":1585080024790}

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Lendo Dados do Tópico do Kafka com Structured Streaming
# MAGIC > iremos utilizar **scala** nesse momento porque é a única linguagem que pode se integrar com o schema registry, acredite isso irá facilicar bastante na leitura dos dados.

# COMMAND ----------

# DBTITLE 1,Libraries [Não Alterar]
# MAGIC %scala
# MAGIC 
# MAGIC // bibliotecas necessárias para ingestão dos dados
# MAGIC 
# MAGIC import org.apache.spark.sql.avro.functions.from_avro
# MAGIC import org.apache.avro.SchemaBuilder

# COMMAND ----------

# DBTITLE 1,Variables [Não Alterar]
# MAGIC %scala
# MAGIC 
# MAGIC // essas variáveis não devem ser modificadas, porque estão configuradas para apontar para 
# MAGIC // o apache kafka que está aguardando a conexão
# MAGIC 
# MAGIC //kafka
# MAGIC val kafkaBrokers = "52.177.14.43:9092"
# MAGIC 
# MAGIC //schema registry
# MAGIC val schemaRegistryAddr = "http://52.177.14.43:8081"
# MAGIC 
# MAGIC //topic
# MAGIC val mssql_topic = "owshq-vw-sales-order"
# MAGIC 
# MAGIC //avro
# MAGIC val mssql_avro = "owshq-vw-sales-order-value"

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC # o desafio é: ler as informações do apache kafka com o structured streaming
# MAGIC > maiores informações podem ser consultadas no GitHub = https://github.com/luanmorenomaciel/data-engineering-databricks

# COMMAND ----------

