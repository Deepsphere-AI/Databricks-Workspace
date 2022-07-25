# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC # Incremental Data Ingestion with Auto Loader
# MAGIC 
# MAGIC 
# MAGIC Databricks Auto Loader provides an easy-to-use mechanism for incrementally and efficiently processing new data files as they arrive in cloud file storage. In this notebook, you'll see Auto Loader in action.
# MAGIC 
# MAGIC Due to the benefits and scalability that Auto Loader delivers, Databricks recommends its use as general **best practice** when ingesting data from cloud object storage.
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, you should be able to:
# MAGIC * Execute Auto Loader code to incrementally ingest data from cloud storage to Delta Lake
# MAGIC * Describe what happens when a new file arrives in a directory configured for Auto Loader
# MAGIC * Query a table fed by a streaming Auto Loader query
# MAGIC 
# MAGIC ## Dataset Used
# MAGIC This demo uses simplified artificially generated medical data representing health iot device recordings delivered in the JSON format. 
# MAGIC 
# MAGIC | Field | Type |
# MAGIC | --- | --- |
# MAGIC | device_id | int |
# MAGIC | calories_burnt | double |
# MAGIC | id | int |
# MAGIC | miles_walked | double |
# MAGIC | num_steps | int |
# MAGIC | timestamp | timestamp |
# MAGIC | user_id | int |

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1.Viewing IOT data as Json Files in databricks-datasets folder

# COMMAND ----------

# MAGIC %fs ls /databricks-datasets/iot-stream/data-device

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. Read Sample Data

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- select count(*) as count,device_id from json.`dbfs:/databricks-datasets/iot-stream/data-device/part-00000.json.gz`
# MAGIC -- group by device_id
# MAGIC select * from json.`dbfs:/databricks-datasets/iot-stream/data-device/part-00000.json.gz`
# MAGIC -- group by device_id

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### 3.Create Source Directory in DBFS path

# COMMAND ----------

# %fs mkdir /dbfs/iot-data/

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4. Copy some data files to source directory

# COMMAND ----------

# MAGIC %fs cp dbfs:/databricks-datasets/iot-stream/data-device/part-00007.json.gz /dbfs/iot-data/

# COMMAND ----------

# MAGIC %fs ls /dbfs/iot-data/

# COMMAND ----------

# MAGIC %md
# MAGIC #### 5. Read and Write streaming data into Table

# COMMAND ----------

def autoload_to_table(data_source, source_format, table_name, checkpoint_directory):
    query = (spark.readStream
                  .format("cloudFiles")
                  .option("cloudFiles.format", source_format)
                  .option("cloudFiles.schemaLocation", checkpoint_directory)
                  .load(data_source)
                  .writeStream
                  .option("checkpointLocation", checkpoint_directory)
                  .option("mergeSchema", "true")
                  .table(table_name))
    return query

# COMMAND ----------

query = autoload_to_table(data_source = "/dbfs/iot-data",
                          source_format = "json",
                          table_name = "iot_health_device_data",
                          checkpoint_directory = "/user/prakash/iot_health_device_data")

# COMMAND ----------

# MAGIC %fs cp dbfs:/databricks-datasets/iot-stream/data-device/part-00008.json.gz /dbfs/iot-data/

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from iot_health_device_data

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) as count,device_id from iot_health_device_data group by device_id order by count desc
