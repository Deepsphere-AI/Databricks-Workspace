# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ### Copyright (c) DeepSphere.AI 2022
# MAGIC 
# MAGIC #### All rights reserved
# MAGIC 
# MAGIC ##### We are sharing this notebook for learning and research, and the idea behind us sharing the source code is to 
# MAGIC ##### stimulate ideas and thoughts for the learners to develop their Databricks knowledge.
# MAGIC 
# MAGIC ##### Author: # DeepSphere.AI | deepsphere.ai | dsschoolofai.com | info@deepsphere.ai
# MAGIC 
# MAGIC ##### Release: Initial release

# COMMAND ----------

# MAGIC %md
# MAGIC ##LakeHouse Overview
# MAGIC 
# MAGIC In this section of the tutorial, we begin using [Delta Lake](https://delta.io/). 
# MAGIC * Delta Lake is an open-source project that enables building a **Lakehouse architecture** on top of existing storage systems such as S3, ADLS, GCS, and HDFS.
# MAGIC    * Information on the **Lakehouse Architecture** can be found in this [paper](http://cidrdb.org/cidr2021/papers/cidr2021_paper17.pdf) that was presented at [CIDR 2021](http://cidrdb.org/cidr2021/index.html) and in this [video](https://www.youtube.com/watch?v=RU2dXoVU8hY)
# MAGIC 
# MAGIC * Key features of Delta Lake include:
# MAGIC   * **ACID Transactions**: Ensures data integrity and read consistency with complex, concurrent data pipelines.
# MAGIC   * **Unified Batch and Streaming Source and Sink**: A table in Delta Lake is both a batch table, as well as a streaming source and sink. Streaming data ingest, batch historic backfill, and interactive queries all just work out of the box. 
# MAGIC   * **Schema Enforcement and Evolution**: Ensures data cleanliness by blocking writes with unexpected.
# MAGIC   * **Time Travel**: Query previous versions of the table by time or version number.
# MAGIC   * **Deletes and upserts**: Supports deleting and upserting into tables with programmatic APIs.
# MAGIC   * **Open Format**: Stored as Parquet format in blob storage.
# MAGIC   * **Audit History**: History of all the operations that happened in the table.
# MAGIC   * **Scalable Metadata management**: Able to handle millions of files are scaling the metadata operations with Spark.
# MAGIC   
# MAGIC Reference: https://databricks.com/blog/2020/01/30/what-is-a-data-lakehouse.html?itm_data=lakehouse-link-lakehouseblog

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.Pre-requisites
# MAGIC 
# MAGIC #### &emsp;&emsp;1.Databricks Community Edition
# MAGIC #### &emsp;&emsp;2.Basic SQL,Cloud(GCP),Python,Pyspark,Spark SQL Understanding
# MAGIC #### &emsp;&emsp;3.Install required libraries in cluster settings
# MAGIC ####     &emsp;&emsp;&emsp;&emsp;- google-cloud-bigquery,db-dtypes,gcsfs
# MAGIC #### &emsp;&emsp;4.Copy service account credential file from local->DBFS->Databricks cluster
# MAGIC ####     &emsp;&emsp;&emsp;&emsp;- %fs cp  dbfs:/FileStore/flydubai_338806_b333f1ad1559.json file:/home/ubuntu/

# COMMAND ----------

# MAGIC %md
# MAGIC # 2.Problem Statement
# MAGIC Due to COIVID19, the client wasn&#39;t sure whether to operate a flight or cancel the flight thirty
# MAGIC days in advance. The client needed to recognize the booking revenue in advance to see if they
# MAGIC break even in their operating expense when they operate a flight with low booking. In summary,
# MAGIC the client wanted to optimize their booking and revenue forecast to determine whether to operate
# MAGIC the flight or cancel it thirty days in advance.

# COMMAND ----------

# MAGIC %md
# MAGIC # 3.Database Setup

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 Cleanup Existing Database with same name

# COMMAND ----------

# MAGIC %sql drop database if exists dsai_revenue_management cascade;

# COMMAND ----------

# MAGIC %sql
# MAGIC show databases;

# COMMAND ----------

# MAGIC %sql show tables;

# COMMAND ----------

# MAGIC %sql
# MAGIC create database DSAI_Revenue_Management;

# COMMAND ----------

# MAGIC %sql
# MAGIC Use DSAI_Revenue_Management

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables;

# COMMAND ----------

# MAGIC %md
# MAGIC # 4.Table Setup

# COMMAND ----------

# MAGIC %fs cp dbfs:/FileStore/tables/serviceAccountFile.json file:/home/ubuntu/

# COMMAND ----------

ls /home/ubuntu/

# COMMAND ----------

# Install required libraries in cluster settings
import os
from google.cloud import bigquery
from pyspark.sql import SQLContext
import pandas as pd


os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/home/ubuntu/serviceAccountFile.json"
os.environ["PYTHONWARNINGS"]="ignore"

class DataController:
    def __init__(self):
        self.vAR_bqclient = bigquery.Client()
        
    def Read_Bigquery_Table(self):
        vAR_fact_query_string = """
        select * from `flydubai-338806.airline_data.DSAI_FACT_TRANSACT_REVENUE_TABLE`
        """
        vAR_dim_product_query = """select * from `flydubai-338806.airline_data.DSAI_DIM_PRODUCT`"""
        vAR_dim_customer_type = """select * from `flydubai-338806.airline_data.DSAI_DIM_CUSTOMER_TYPE`"""
        vAR_dim_location = """select * from `flydubai-338806.airline_data.DSAI_DIM_LOCATION`"""
        vAR_dim_price = """select * from `flydubai-338806.airline_data.DSAI_DIM_PRICE`"""
        vAR_dim_promotion = """select * from `flydubai-338806.airline_data.DSAI_DIM_PROMOTION`"""
        vAR_dim_currency = """select * from `flydubai-338806.airline_data.DSAI_DIM_CURRENCY`"""
        vAR_dim_flight = """select * from `flydubai-338806.airline_data.DSAI_DIM_FLIGHT`"""
        vAR_fact_dataframe = (
            self.vAR_bqclient.query(vAR_fact_query_string)
            .result()
            .to_dataframe(
                create_bqstorage_client=True,
            )
        )
        vAR_dim_product_df = (
            self.vAR_bqclient.query(vAR_dim_product_query)
            .result()
            .to_dataframe(
                create_bqstorage_client=True,
            )
        )
        vAR_dim_customer_df = (
            self.vAR_bqclient.query(vAR_dim_customer_type)
            .result()
            .to_dataframe(
                create_bqstorage_client=True,
            )
        )
        vAR_dim_location_df = (
            self.vAR_bqclient.query(vAR_dim_location)
            .result()
            .to_dataframe(
                create_bqstorage_client=True,
            )
        )
        vAR_dim_price_df = (
            self.vAR_bqclient.query(vAR_dim_price)
            .result()
            .to_dataframe(
                create_bqstorage_client=True,
            )
        )
        vAR_dim_promotion_df = (
            self.vAR_bqclient.query(vAR_dim_promotion)
            .result()
            .to_dataframe(
                create_bqstorage_client=True,
            )
        )
        vAR_dim_currency_df = (
            self.vAR_bqclient.query(vAR_dim_currency)
            .result()
            .to_dataframe(
                create_bqstorage_client=True,
            )
        )
        vAR_dim_flight_df = (
            self.vAR_bqclient.query(vAR_dim_flight)
            .result()
            .to_dataframe(
                create_bqstorage_client=True,
            )
        )
#         vAR_fact_dataframe = vAR_fact_dataframe.drop(columns = ["model_accuracy","accuracy_probability"],axis=1)
        return vAR_fact_dataframe,vAR_dim_product_df,vAR_dim_customer_df,vAR_dim_location_df,vAR_dim_price_df,vAR_dim_promotion_df,vAR_dim_currency_df,vAR_dim_flight_df
    
    def Convert_To_Spark_DF(self,vAR_dataframe):
        vAR_spark_df = spark.createDataFrame(vAR_dataframe)
        return vAR_spark_df
    
    def Display_Schema_Structure(self,vAR_spark_df):
        display(vAR_spark_df.printSchema())
        
    def Write_Revenue_Table(self,vAR_spark_df):
        vAR_table_name = 'DSAI_fact_transaction_revenue'
        vAR_spark_df.write.format("delta").mode("overwrite").saveAsTable(vAR_table_name)
        return vAR_table_name
    def Write_Product_Table(self,vAR_spark_df):
        vAR_table_name = 'DSAI_DIM_PRODUCT'
        vAR_spark_df.write.format("delta").mode("overwrite").saveAsTable(vAR_table_name)
        return vAR_table_name
    def Write_Customer_Table(self,vAR_spark_df):
        vAR_table_name = 'DSAI_DIM_CUSTOMER'
        vAR_spark_df.write.format("delta").mode("overwrite").saveAsTable(vAR_table_name)
        return vAR_table_name
    def Write_Location_Table(self,vAR_spark_df):
        vAR_table_name = 'DSAI_DIM_Location'
        vAR_spark_df.write.format("delta").mode("overwrite").saveAsTable(vAR_table_name)
        return vAR_table_name
    def Write_Price_Table(self,vAR_spark_df):
        vAR_table_name = 'DSAI_DIM_Price'
        vAR_spark_df.write.format("delta").mode("overwrite").saveAsTable(vAR_table_name)
        return vAR_table_name
    def Write_Promotion_Table(self,vAR_spark_df):
        vAR_table_name = 'DSAI_DIM_Promotion'
        vAR_spark_df.write.format("delta").mode("overwrite").saveAsTable(vAR_table_name)
        return vAR_table_name
    def Write_Currency_Table(self,vAR_spark_df):
        vAR_table_name = 'DSAI_DIM_Currency'
        vAR_spark_df.write.format("delta").mode("overwrite").saveAsTable(vAR_table_name)
        return vAR_table_name
    def Write_Flight_Table(self,vAR_spark_df):
        vAR_table_name = 'DSAI_DIM_Flight'
        vAR_spark_df.write.format("delta").mode("overwrite").saveAsTable(vAR_table_name)
        return vAR_table_name
    def Preview_Table(self,vAR_table_name):
        vAR_preview_data = sqlContext.sql("select * from "+vAR_table_name +" limit 50")
        display(vAR_preview_data)
    def Table_Details(self,vAR_table_name):
        display(sqlContext.sql('Describe EXTENDED ' +vAR_table_name))
    def Table_History(self,vAR_table_name):
        display(sqlContext.sql('Describe HISTORY ' +vAR_table_name))

# COMMAND ----------

if __name__=='__main__':
    try:
        vAR_data_obj = DataController()
        print('*'*45+'Step 1: Object Initialized'+'*'*45+'\n')
        
        vAR_pd_dataframe,vAR_dim_product_df,vAR_dim_customer_df,vAR_dim_location_df,vAR_dim_price_df,vAR_dim_promotion_df,vAR_dim_currency_df,vAR_dim_flight_df = vAR_data_obj.Read_Bigquery_Table()
        print('*'*35+'Step 2: Successfully Read Data From Bigquery'+'*'*35+'\n')
        
        vAR_spark_dataframe = vAR_data_obj.Convert_To_Spark_DF(vAR_pd_dataframe)
        
        vAR_dim_product_spark_df = vAR_data_obj.Convert_To_Spark_DF(vAR_dim_product_df)
        
        vAR_dim_customer_spark_df = vAR_data_obj.Convert_To_Spark_DF(vAR_dim_customer_df)
        
        vAR_dim_location_spark_df = vAR_data_obj.Convert_To_Spark_DF(vAR_dim_location_df)
        
        vAR_dim_price_spark_df = vAR_data_obj.Convert_To_Spark_DF(vAR_dim_price_df)
        
        vAR_dim_promotion_spark_df = vAR_data_obj.Convert_To_Spark_DF(vAR_dim_promotion_df)
        
        vAR_dim_currency_spark_df = vAR_data_obj.Convert_To_Spark_DF(vAR_dim_currency_df)
        
        vAR_dim_flight_spark_df = vAR_data_obj.Convert_To_Spark_DF(vAR_dim_flight_df)
        
        print('*'*25+'Step 3: Pandas Dataframe successfully converted into spark dataframe'+'*'*25+'\n')
        
        
        vAR_fact_table_name = vAR_data_obj.Write_Revenue_Table(vAR_spark_dataframe)
        print('*'*25+'Step 4: Fact Delta table created successfully - from spark dataframe'+'*'*25+'\n')
        
        vAR_dim_table_name = vAR_data_obj.Write_Product_Table(vAR_dim_product_spark_df)
        vAR_dim_table_name = vAR_data_obj.Write_Customer_Table(vAR_dim_customer_spark_df)
        vAR_dim_table_name = vAR_data_obj.Write_Location_Table(vAR_dim_location_spark_df)
        vAR_dim_table_name = vAR_data_obj.Write_Price_Table(vAR_dim_price_spark_df)
        vAR_dim_table_name = vAR_data_obj.Write_Promotion_Table(vAR_dim_promotion_spark_df)
        vAR_dim_table_name = vAR_data_obj.Write_Currency_Table(vAR_dim_currency_spark_df)
        vAR_dim_table_name = vAR_data_obj.Write_Flight_Table(vAR_dim_flight_spark_df)
        print('*'*25+'Step 4: Dimension Delta tables created successfully - from spark dataframe'+'*'*25+'\n')
        
        print('*'*45+'Step 5: Preview Table'+'*'*45+'\n')
        vAR_data_obj.Preview_Table(vAR_fact_table_name)
        
        print('*'*45+'Step 6: Table Details'+'*'*45+'\n')
        vAR_data_obj.Table_Details(vAR_fact_table_name)
        
        print('*'*45+'Step 7: History of Table'+'*'*45+'\n')
        vAR_data_obj.Table_History(vAR_fact_table_name)
        
    except BaseException as e:
        print('In Error Block - ',str(e))

# COMMAND ----------

# MAGIC %sql select * from DSAI_fact_transaction_revenue where data_category='ACTUAL'

# COMMAND ----------

# MAGIC %md
# MAGIC # 5.Demonstrating Lakehouse Functionalities

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.1 ACID Property

# COMMAND ----------

# MAGIC %md 
# MAGIC Below is the query to find the meta data of the table

# COMMAND ----------

# MAGIC %sql describe extended dsai_fact_transaction_revenue

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.1.1 Why are ACID transactions a good thing to have?
# MAGIC    &emsp;&emsp;&emsp;ACID transactions ensure the highest possible data reliability and integrity. They ensure that your data never falls into an inconsistent state because of an operation that only partially completes. For example, without ACID transactions, if you were writing some data to a database table, but the power went out unexpectedly, it's possible that only some of your data would have been saved, while some of it would not. Now your database is in an inconsistent state that is very difficult and time-consuming to recover from.

# COMMAND ----------

# MAGIC %md
# MAGIC Below is the transaction log path for all the transactions made in dsai_fact_transaction_revenue_table

# COMMAND ----------

# MAGIC %fs ls dbfs:/user/hive/warehouse/dsai_revenue_management.db/dsai_fact_transaction_revenue/_delta_log/

# COMMAND ----------

# MAGIC %fs head dbfs:/user/hive/warehouse/dsai_revenue_management.db/dsai_fact_transaction_revenue/_delta_log/00000000000000000000.json

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.2 Unified Batch and Streaming Source and Sink
# MAGIC    &emsp;&emsp;&emsp;In real time, there may be mulitple sources of data to work with any usecase. Here, we already have data from Bigquery and now it's time to extract data from google cloud storage bucket. After this data extraction, we can combine this data into existing one. Then we can perform data cleansing processes.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.2.1 ETL Setup
# MAGIC 
# MAGIC  &emsp;&emsp;&emsp;&emsp;- Here We are Creating Table with only required columns and some columns are derived from Dimension tables. Also, transforming date column into string.

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- select sum(revenue),month,year,subquery.data_category from (
# MAGIC create or replace table dsai_fact_transaction_revenue_table as
# MAGIC select rev.id,rev.data_category,data_source,year,source.location_name as source,dest.location_name as destination,
# MAGIC f.description as flight,price.description as price_type,pro.description as promocode,
# MAGIC cust.description as customer_type,p.description as product_type,source_wind,destination_wind,source_humidity,destination_humidity,to_timestamp(substr(cast(date as string),0,19)) as date_time,revenue,cur.description
# MAGIC from dsai_fact_transaction_revenue rev
# MAGIC join dsai_dim_location source on source.location_id=rev.source_id
# MAGIC join dsai_dim_location dest on dest.location_id=rev.destination_id
# MAGIC join dsai_dim_customer cust on cust.id=rev.customer_type_id
# MAGIC join dsai_dim_product p on p.product_id=rev.product_type_id
# MAGIC join dsai_dim_flight f on f.flight_id=rev.flight_id
# MAGIC join dsai_dim_price price on price.price_id=rev.price_type_id
# MAGIC join dsai_dim_promotion pro on pro.price_id=rev.promocode_id
# MAGIC join dsai_dim_currency cur on cur.currency_id=rev.currency_id
# MAGIC 
# MAGIC -- ) subquery group by month,year,subquery.data_category order by month,year

# COMMAND ----------

# MAGIC %sql select * from dsai_fact_transaction_revenue_table where data_category='ACTUAL'

# COMMAND ----------

# vAR_external_source_df = pd.read_csv('gs://flydubai-airline-data/timeseries-data/FLYDUBAI_DATA_PLAN.csv')
vAR_external_source_df = pd.read_csv('s3://airline-data-bucket/timeseries-data/FLYDUBAI_DATA_PLAN.csv')
vAR_external_source_df['date_time'] = pd.to_datetime(vAR_external_source_df['date_time'])
vAR_spark_df = spark.createDataFrame(vAR_external_source_df)
vAR_spark_df.write.format("delta").mode("overwrite").saveAsTable('dsai_fact_transaction_revenue_external_table')

# COMMAND ----------

# MAGIC %sql 
# MAGIC create or replace table dsai_fact_revenue_table as
# MAGIC select * from dsai_fact_transaction_revenue_external_table where data_category='ACTUAL'
# MAGIC union all
# MAGIC select * from dsai_fact_transaction_revenue_table  where data_category='ACTUAL';

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from dsai_fact_transaction_revenue_table;
# MAGIC select count(*) from dsai_fact_transaction_revenue_external_table;
# MAGIC select count(*) from dsai_fact_revenue_table;

# COMMAND ----------

# MAGIC %md
# MAGIC ##5.3 Schema Enforcement and Evolution

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.3.1 What Is Schema Enforcement?
# MAGIC Schema enforcement, also known as schema validation, is a safeguard in Delta Lake that ensures data quality by rejecting writes to a table that do not match the table’s schema. Like the front desk manager at a busy restaurant that only accepts reservations, it checks to see whether each column in data inserted into the table is on its list of expected columns (in other words, whether each one has a “reservation”), and rejects any writes with columns that aren’t on the list.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.3.2 How Does Schema Enforcement Work?
# MAGIC Delta Lake uses schema validation on write, which means that all new writes to a table are checked for compatibility with the target table’s schema at write time. If the schema is not compatible, Delta Lake cancels the transaction altogether (no data is written), and raises an exception to let the user know about the mismatch.

# COMMAND ----------

# MAGIC %sql show columns from dsai_fact_transaction_revenue_table;

# COMMAND ----------

sample_df = spark.sql("select id,data_category as data_categ,year,revenue from dsai_fact_transaction_revenue_table limit 5")

# COMMAND ----------

sample_df.write.format("delta").mode("append").save("dbfs:/user/hive/warehouse/dsai_revenue_management.db/dsai_fact_transaction_revenue_external_table")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.3.3 What Is Schema Evolution?
# MAGIC Schema evolution is a feature that allows users to easily change a table’s current schema to accommodate data that is changing over time. Most commonly, it’s used when performing an append or overwrite operation, to automatically adapt the schema to include one or more new columns.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.3.4 How Does Schema Evolution Work?
# MAGIC Following up on the example from the previous section, developers can easily use schema evolution to add the new columns that were previously rejected due to a schema mismatch. Schema evolution is activated by adding  .option('mergeSchema', 'true') to your .write or .writeStream Spark command.

# COMMAND ----------

sample_df.write.format("delta").option("mergeSchema", "true").mode("append").save("dbfs:/user/hive/warehouse/dsai_revenue_management.db/dsai_fact_transaction_revenue_external_table")

# COMMAND ----------

# MAGIC %sql select * from dsai_fact_transaction_revenue_external_table

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.4 Time Travel
# MAGIC As you write into a Delta table or directory, every operation is automatically versioned. You can access the different versions of the data.

# COMMAND ----------

# MAGIC %sql describe history dsai_fact_transaction_revenue_external_table

# COMMAND ----------

# MAGIC %sql select count(*) from dsai_fact_transaction_revenue_external_table version as of 0

# COMMAND ----------

# MAGIC %sql select count(*) from dsai_fact_transaction_revenue_external_table version as of 1

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.5 Deletes and Updates

# COMMAND ----------

# MAGIC %sql select * from dsai_fact_transaction_revenue_external_table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW revenue_upserts(id, data_category, year, source,destination,customer_type,source_wind) AS VALUES
# MAGIC   ("b1838d63-1ec7-4434-8a7e-cdf8d9725a56", "PLAN",2022,"MAA","DXB","RARE FLYER",'0.58'),
# MAGIC   ("00000001",  "PLAN",2022,"MAA","DXB","RARE FLYER",'0.58');

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO dsai_fact_transaction_revenue_external_table original
# MAGIC USING revenue_upserts updates
# MAGIC ON original.id=updates.id
# MAGIC WHEN MATCHED
# MAGIC   THEN UPDATE SET 
# MAGIC     id = updates.id,
# MAGIC     data_category = updates.data_category,
# MAGIC     year = updates.year,
# MAGIC     source = updates.source,
# MAGIC     destination = updates.destination,
# MAGIC     customer_type = updates.customer_type,
# MAGIC     source_wind = updates.source_wind
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (id, data_category, year,source,destination,customer_type,source_wind) values (updates.id,updates.data_category,updates.year,updates.source,updates.destination,updates.customer_type,updates.source_wind);

# COMMAND ----------

# MAGIC %sql describe history dsai_fact_transaction_revenue_external_table

# COMMAND ----------

# MAGIC %md
# MAGIC ##5.6 Open format
# MAGIC 
# MAGIC All data in Delta Lake is stored in open Apache Parquet format, allowing data to be read by any compatible reader. APIs are open and compatible with Apache Spark. With Delta Lake on Databricks, you have access to a vast open source ecosystem and avoid data lock-in from proprietary formats.

# COMMAND ----------

# MAGIC %fs ls dbfs:/user/hive/warehouse/dsai_revenue_management.db/dsai_fact_transaction_revenue_external_table/

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.7 Audit History

# COMMAND ----------

# MAGIC %sql describe history dsai_fact_transaction_revenue_external_table

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.8 Scalable Metadata Management

# COMMAND ----------

# MAGIC %sql describe EXTENDED dsai_fact_transaction_revenue_external_table;
# MAGIC select *,_metadata from dsai_fact_transaction_revenue_external_table;

# COMMAND ----------

# MAGIC %sql show columns from dsai_fact_revenue_table

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.9 Other Databricks Functionalities

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.9.1 VACCUM
# MAGIC Recursively vacuum directories associated with the Delta table. VACUUM removes all files from the table directory that are not managed by Delta, as well as data files that are no longer in the latest state of the transaction log for the table and are older than a retention threshold. VACUUM will skip all directories that begin with an _, which includes the _delta_log (partitioning your table on a column that begins with an _ is an exception to this rule; VACUUM scans all valid partitions included in the target Delta table). Delta table data files are deleted according to the time they have been logically removed from Delta’s transaction log + retention hours, not their modification timestamps on the storage system. The default threshold is 7 days.

# COMMAND ----------

# MAGIC %sql describe history dsai_fact_transaction_revenue_external_table

# COMMAND ----------

# MAGIC %sql VACUUM dsai_fact_transaction_revenue_external_table retain 12 hours

# COMMAND ----------

# MAGIC %sql describe history dsai_fact_transaction_revenue_external_table

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.9.2 RESTORE
# MAGIC 
# MAGIC You can restore a Delta table to its earlier state by using the RESTORE command. A Delta table internally maintains historic versions of the table that enable it to be restored to an earlier state. A version corresponding to the earlier state or a timestamp of when the earlier state was created are supported as options by the RESTORE command.

# COMMAND ----------

# MAGIC %sql RESTORE table dsai_fact_transaction_revenue_external_table to version as of 0

# COMMAND ----------

# MAGIC %sql select count(*) from dsai_fact_transaction_revenue_external_table

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.9.3 OPTIMIZE
# MAGIC 
# MAGIC To improve query speed, Delta Lake on Databricks supports the ability to optimize the layout of data stored in cloud storage. Delta Lake on Databricks supports two layout algorithms: bin-packing and Z-Ordering.
# MAGIC 
# MAGIC Readers of Delta tables use snapshot isolation, which means that they are not interrupted when OPTIMIZE removes unnecessary files from the transaction log. OPTIMIZE makes no data related changes to the table, so a read before and after an OPTIMIZE has the same results. Performing OPTIMIZE on a table that is a streaming source does not affect any current or future streams that treat this table as a source. OPTIMIZE returns the file statistics (min, max, total, and so on) for the files removed and the files added by the operation. Optimize stats also contains the Z-Ordering statistics, the number of batches, and partitions optimized.

# COMMAND ----------

# MAGIC %sql describe extended dsai_fact_transaction_revenue_external_table

# COMMAND ----------

# MAGIC %sql OPTIMIZE dsai_fact_transaction_revenue_external_table

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.9.4 Z-Ordering
# MAGIC 
# MAGIC Z-Ordering is a technique to colocate related information in the same set of files. This co-locality is automatically used by Delta Lake on Databricks data-skipping algorithms. This behavior dramatically reduces the amount of data that Delta Lake on Databricks needs to read.
# MAGIC 
# MAGIC Z-Ordering aims to produce evenly-balanced data files with respect to the number of tuples, but not necessarily data size on disk. The two measures are most often correlated, but there can be situations when that is not the case, leading to skew in optimize task times.

# COMMAND ----------

# MAGIC %sql OPTIMIZE dsai_fact_transaction_revenue_external_table ZORDER BY (customer_type)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.9.5 Clone
# MAGIC 
# MAGIC Clones a source Delta table to a target destination at a specific version. A clone can be either deep or shallow: deep clones copy over the data from the source and shallow clones do not.
# MAGIC 
# MAGIC If you specify SHALLOW CLONE Databricks will make a copy of the source table’s definition, but refer to the source table’s files. When you specify DEEP CLONE (default) Databricks will make a complete, independent copy of the source table.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Perform shallow clone
# MAGIC CREATE OR REPLACE TABLE dsai_external_shallow_clone SHALLOW CLONE dsai_fact_transaction_revenue_external_table;

# COMMAND ----------

# MAGIC %sql describe extended dsai_external_shallow_clone

# COMMAND ----------

# MAGIC %fs ls dbfs:/user/hive/warehouse/dsai_revenue_management.db/dsai_external_shallow_clone

# COMMAND ----------

# MAGIC %fs ls dbfs:/user/hive/warehouse/dsai_revenue_management.db/dsai_fact_transaction_revenue_external_table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Perform Deep clone
# MAGIC CREATE OR REPLACE TABLE dsai_external_deep_clone CLONE dsai_fact_transaction_revenue_external_table;

# COMMAND ----------

# MAGIC %fs ls dbfs:/user/hive/warehouse/dsai_revenue_management.db/dsai_external_deep_clone

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.9.6 Cache
# MAGIC 
# MAGIC The Delta cache accelerates data reads by creating copies of remote files in nodes’ local storage using a fast intermediate data format. The data is cached automatically whenever a file has to be fetched from a remote location. Successive reads of the same data are then performed locally, which results in significantly improved reading speed.
# MAGIC 
# MAGIC The Delta cache works for all Parquet files and is not limited to Delta Lake format files. The Delta cache supports reading Parquet files in Amazon S3, DBFS, HDFS, Azure Blob storage, Azure Data Lake Storage Gen1, and Azure Data Lake Storage Gen2. It does not support other storage formats such as CSV, JSON, and ORC.

# COMMAND ----------

# MAGIC %sql CACHE select * from dsai_fact_transaction_revenue_external_table where customer_type='RARE FLYER'

# COMMAND ----------

# MAGIC %sql select * from dsai_fact_transaction_revenue_external_table where customer_type='RARE FLYER'

# COMMAND ----------

# MAGIC %md
# MAGIC As you can see in the below screenshot, the cache hit ratio will increase when we try to access the cached data again.

# COMMAND ----------

displayHTML("<img src ='/files/Cache_Statistics.PNG'>")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.9.6 Serverless Compute
# MAGIC 
# MAGIC With the Serverless compute version of the Databricks platform architecture, the compute layer exists in the Databricks cloud account rather than the customer’s cloud account.
# MAGIC 
# MAGIC As of the current release, Serverless compute is supported for use with Databricks SQL. Admins can create Serverless SQL warehouses (formerly SQL endpoints) that enable instant compute and are managed by Databricks. Serverless SQL warehouses use compute clusters in the Databricks AWS account. Use them with Databricks SQL queries just like you normally would with the original customer-hosted SQL warehouses, which are now called Classic SQL warehouses.
# MAGIC 
# MAGIC Databricks changed the name from SQL endpoint to SQL warehouse because, in the industry, endpoint refers to either a remote computing device that communicates with a network that it’s connected to, or an entry point to a cloud service. A data warehouse is a data management system that stores current and historical data from multiple sources in a business friendly manner for easier insights and reporting. SQL warehouse accurately describes the full capabilities of this compute resource.

# COMMAND ----------

# MAGIC %md
# MAGIC # 6. Best Practices And Recommendations

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 6.1 Medallion Architecure

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### What is a medallion architecture?
# MAGIC A medallion architecture is a data design pattern used to logically organize data in a lakehouse, with the goal of incrementally and progressively improving the structure and quality of data as it flows through each layer of the architecture (from Bronze ⇒ Silver ⇒ Gold layer tables). Medallion architectures are sometimes also referred to as "multi-hop" architectures.

# COMMAND ----------

displayHTML("<img src ='/files/delta_lake_medallion_architecture_2.jpg'>")

# COMMAND ----------

# MAGIC %md
# MAGIC # 7. Major Hadoop Ecosystem Tools used in Databricks 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC * HDFS
# MAGIC * HIVE
# MAGIC * ZOO KEEPER
# MAGIC * YARN
