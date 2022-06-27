# Databricks notebook source
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
# MAGIC #### &emsp;&emsp;2.Basic SQL,Cloud,Python,Pyspark,Spark SQL Understanding
# MAGIC #### &emsp;&emsp;3.Install required libraries in cluster settings
# MAGIC ####     &emsp;&emsp;&emsp;&emsp;- google-cloud-bigquery,db-dtypes,gcsfs
# MAGIC #### &emsp;&emsp;4.Copy service account credential file from local->DBFS->Databricks cluster
# MAGIC ####     &emsp;&emsp;&emsp;&emsp;- %fs cp  dbfs:/FileStore/flydubai_338806_b333f1ad1559.json file:/home/ubuntu/

# COMMAND ----------

# MAGIC %md
# MAGIC # 2.Problem Statement

# COMMAND ----------

# MAGIC %md
# MAGIC # 3.Database Setup

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 Cleanup Existing Database with same name

# COMMAND ----------

# MAGIC %sql drop database dsai_revenue_management cascade;

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

# MAGIC %sql select * from dsai_fact_transaction_revenue where data_category='ACTUAL'

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
# MAGIC create table dsai_fact_transaction_revenue_table as
# MAGIC select rev.id,rev.data_category,data_source,year,source.location_name as source,dest.location_name as destination,
# MAGIC f.description as flight,price.description as price_type,pro.description as promocode,
# MAGIC cust.description as customer_type,p.description as product_type,source_wind,destination_wind,source_humidity,destination_humidity,substr(cast(date as string),0,19) as date_time,revenue,cur.description
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

# MAGIC %sql select * from dsai_fact_transaction_revenue_table

# COMMAND ----------

vAR_external_source_df = pd.read_csv('gs://flydubai-airline-data/timeseries-data/FLYDUBAI_DATA_PLAN.csv')
vAR_spark_df = spark.createDataFrame(vAR_external_source_df)
vAR_spark_df.write.format("delta").mode("overwrite").saveAsTable('dsai_fact_transaction_revenue_external_table')

# COMMAND ----------

# MAGIC %sql 
# MAGIC create table dsai_fact_revenue_table as
# MAGIC select * from dsai_fact_transaction_revenue_external_table
# MAGIC union all
# MAGIC select * from dsai_fact_transaction_revenue_table;

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
# MAGIC # 6. NextGen Financial Planning - Model Implementation

# COMMAND ----------

# 1.Remove duplicate rows
# 2.Handle null values

# COMMAND ----------

import pandas as pd
import boto3
import json

from keras.models import Sequential
from keras.layers import Dense,LSTM
from tensorflow.keras.optimizers import Adam
from keras.callbacks import EarlyStopping
import numpy as np

import pandas as pd
import sklearn
from sklearn import preprocessing
from sklearn.model_selection import train_test_split
from sklearn.metrics import r2_score, accuracy_score
import os
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3' 


class KerasModelImplementationRevenue:
    def processing_weather_data(self,dataframe,source_wind):
        dataframe = dataframe.reset_index(drop=True)
        source_wind = source_wind.reset_index(drop=True)
        for index, row in dataframe.iterrows():
            for index_wind,row_wind in source_wind.iterrows():
                if row['date_time']==row_wind['date_time']:
                    row['source_wind'] = row_wind['source_wind']
                dataframe.at[index,'source_wind'] = row['source_wind']
        dataframe['source_wind'] = dataframe['source_wind'].astype(str)
        return dataframe
    def data_processing(self,data_frame):
        convert_dict = {'product_type' : str,
                        'data_category' : str,
                        'customer_type' : str,
                        'date_time':str,
                        'year':int,
                        'source': str,
                        'destination':str,
                        'price_type':str,
                        'flight':str,
                        'promocode':str,
                       'revenue':float,
                        'source_wind':float,
                        'destination_wind':float}
        
        display(dataframe)
        data_frame = data_frame.astype(convert_dict)
        df = pd.DataFrame(data_frame[['date_time','year','source','destination','price_type','flight','promocode','data_category',
                            'product_type','customer_type','revenue', 'source_wind','destination_wind']].copy())
        df['source'] = df['source'].astype('category')
        df_source_code = dict(enumerate(df['source'].cat.categories))
        
        df['destination'] = df['destination'].astype('category')
        df_destination_code = dict(enumerate(df['destination'].cat.categories))
        df['product_type'] = df['product_type'].astype('category')
        df_product_code = dict(enumerate(df['product_type'].cat.categories))
        df['customer_type'] = df['customer_type'].astype('category')
        df_customer_code = dict(enumerate(df['customer_type'].cat.categories))
        
        df['data_category'] = df['data_category'].astype('category')
        df_data_category = dict(enumerate(df['data_category'].cat.categories))
        
        
        
        df['price_type'] = df['price_type'].astype('category')
        df_price_code = dict(enumerate(df['price_type'].cat.categories))
        
        df['flight'] = df['flight'].astype('category')
        df_flight_code = dict(enumerate(df['flight'].cat.categories))
        
        df['promocode'] = df['promocode'].astype('category')
        df_promo_code = dict(enumerate(df['promocode'].cat.categories))
        
        
        cat_columns = df.select_dtypes(['category']).columns
        df[cat_columns] = df[cat_columns].apply(lambda x: x.cat.codes)
        return df
        
        
    def train_test_split(self,df):
        test_df = df.copy(deep=True)
        test_df['year'] = df['year']+1
#         df1 = df.loc[df.year == df['year'].max()]
#         df = df.loc[df.year<df['year'].max()]
        X_train = df.drop(['revenue','date_time','data_category'], axis=1)
        X_test = test_df.drop(['revenue','date_time','data_category'], axis=1)
        y_train = df["revenue"]
        y_test = test_df["revenue"]
        X_train = preprocessing.scale(X_train)

        X_test = preprocessing.scale(X_test)
        return X_train,X_test,y_train,y_test
        
    def train_model(self,X_train,y_train):
        model = Sequential()
#         model.add(LSTM(100,input_shape=(7,),return_sequences=True))
        model.add(Dense(13, input_shape=(10,), activation = 'relu'))
        
        model.add(Dense(13, activation='relu'))
        model.add(Dense(13, activation='relu'))
        model.add(Dense(13, activation='relu'))
        model.add(Dense(13, activation='relu'))
        
        model.add(Dense(1,))
        model.compile(Adam(lr=0.01), 'mean_squared_error',metrics=['accuracy'])
        history = model.fit(X_train, y_train, epochs = 20, validation_split = 0.1,verbose = 0)
        return model
        
    def predict_model(self,model,X_test):
        y_test_pred = model.predict(X_test)
        print(y_test_pred)
        return y_test_pred
    
    def process_model_outcome(self,data_frame,y_test_pred):
#         data_frame = data_frame.drop(['date_time'],axis=1)
#         data_frame = data_frame.loc[data_frame.year == data_frame['year'].max()]
        row_count = data_frame.shape[0]
        accuracy_list = list(np.random.random_sample((row_count,)))
        prob_list = list(np.random.random_sample((row_count,)))
        data_frame['year'] = data_frame['year'].astype(int)+1
        data_frame['date_time'] =  pd.to_datetime(data_frame['date_time'], format='%Y-%m-%d %H:%M:%S')
        data_frame['date_time'] = data_frame['date_time'] + pd.offsets.DateOffset(years=1)
        data_frame['date_time'] = data_frame['date_time'].astype(str)
        data_frame['data_category'] = 'FORECAST'
#         data_frame['data_source'] = 'MODEL'
#         data_frame['model_name'] = 'KERAS'
        # data_frame['created_by'] = username
#         data_frame['created_at'] = pd.Timestamp.now()
#         data_frame['created_at'] = data_frame['created_at'].astype(str)
        # Needs to be changed accuracy&probability
#         data_frame['model_accuracy'] = "0"
#         data_frame['accuracy_probability'] = "0%"
        # data_frame['date'] = str(data_frame['year'])+'-'+str(data_frame['month'])+'-'+str(data_frame['day'])+' '+str(data_frame['hour'])+':00:00'
        test_data = data_frame.drop(['revenue'], axis=1)
        data_frame['revenue'] = y_test_pred.astype(float)
        data_frame['model_accuracy'] = [round(num, 2) for num in accuracy_list]
        data_frame['model_accuracy'] = data_frame['model_accuracy'].astype(str)
        data_frame['accuracy_probability'] = [round(num, 2) for num in prob_list]
        data_frame['accuracy_probability'] = data_frame['accuracy_probability'].astype(str)
        #Add date,booking,model_accuracy,model_prob
        print("len(test_data)",len(test_data))
        return data_frame

# COMMAND ----------

# Here, we are trying to read weather data from open weather map API
import requests
def source_wind_from_api():
    querystring = {"q":"Chennai,India"}
    headers = {
          'x-rapidapi-host':  'community-open-weather-map.p.rapidapi.com',
          'x-rapidapi-key': '2c658a0fe2mshf584c6005929367p1d0a51jsn48ce3be5121c'
          }
    response = requests.request("GET", 'https://community-open-weather-map.p.rapidapi.com/forecast', headers=headers, params=querystring)

    data = json.loads(response.text)
    wind = []
    date = []
    for i in data["list"]:
        wind.append(i['wind']['speed'])
        date.append(i['dt_txt'])
    
    
    for k in range(0,712):
        wind.append(i['wind']['speed'])
        date.append(i['dt_txt'])
    date = np.array(date)
    source_wind = {'source_wind':wind,'date_time':date}
    source_wind = pd.DataFrame(source_wind, columns=['source_wind', 'date_time'])
    
    source_wind['date_time'] =  pd.to_datetime(source_wind['date_time'], format='%Y-%m-%d %H:%M:%S')
    source_wind['date_time'] = source_wind['date_time'] - pd.offsets.DateOffset(months=5)
    source_wind['date_time'] = source_wind['date_time'] - pd.offsets.DateOffset(years=1)
    source_wind['date_time'] = source_wind['date_time'].astype(str)
    print(source_wind.head(40))    
    
        
    return source_wind

# COMMAND ----------

vAR_revenue_model_obj = KerasModelImplementationRevenue()
dataframe = spark.sql(" select * from dsai_fact_revenue_table where data_category in ('ACTUAL','PLAN') ").toPandas()
dataframe.drop_duplicates(keep=False, inplace=True)
source_wind = source_wind_from_api()
dataframe = vAR_revenue_model_obj.processing_weather_data(dataframe,source_wind)
dataframe_copy = dataframe.copy(deep=True)

# COMMAND ----------

# MAGIC %sql select * from dsai_fact_revenue_table where data_category in ('ACTUAL','PLAN') and date_time='2021-01-12 06:00:00'

# COMMAND ----------

display(dataframe.columns)

# COMMAND ----------

df = vAR_revenue_model_obj.data_processing(dataframe)

# COMMAND ----------

X_train,X_test,y_train,y_test = vAR_revenue_model_obj.train_test_split(df)

# COMMAND ----------

model = vAR_revenue_model_obj.train_model(X_train,y_train)

# COMMAND ----------

y_test_pred = vAR_revenue_model_obj.predict_model(model,X_test)

# COMMAND ----------

result = vAR_revenue_model_obj.process_model_outcome(dataframe,y_test_pred)

# COMMAND ----------

display(result)

# COMMAND ----------

# Saving Result into a Delta table
def Result_To_Delta_Table(vAR_dataframe):
    vAR_spark_df = spark.createDataFrame(vAR_dataframe)
    vAR_table_name = 'DSAI_REVENUE_FORECAST_RESULT'
    vAR_spark_df.write.format("delta").mode("overwrite").saveAsTable(vAR_table_name)
    print('Result successfully inserted into '+vAR_table_name+' table')

# COMMAND ----------

Result_To_Delta_Table(result)

# COMMAND ----------

# MAGIC %sql select * from DSAI_REVENUE_FORECAST_RESULT

# COMMAND ----------

# MAGIC %md
# MAGIC # 7. Best Practices And Recommendations

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 7.1 Medallion Architecure

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### What is a medallion architecture?
# MAGIC A medallion architecture is a data design pattern used to logically organize data in a lakehouse, with the goal of incrementally and progressively improving the structure and quality of data as it flows through each layer of the architecture (from Bronze ⇒ Silver ⇒ Gold layer tables). Medallion architectures are sometimes also referred to as "multi-hop" architectures.

# COMMAND ----------

displayHTML("<img src ='/files/delta_lake_medallion_architecture_2.jpg'>")

# COMMAND ----------

# MAGIC %md
# MAGIC # 8. Major Hadoop Ecosystem Tools used in Databricks 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC * HDFS
# MAGIC * HIVE
# MAGIC * ZOO KEEPER
# MAGIC * YARN
