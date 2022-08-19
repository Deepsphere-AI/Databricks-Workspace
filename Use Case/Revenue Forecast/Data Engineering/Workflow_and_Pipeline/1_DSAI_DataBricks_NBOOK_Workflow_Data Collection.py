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

# MAGIC %sql
# MAGIC show databases;

# COMMAND ----------

# MAGIC %sql
# MAGIC use dsai_revenue_management_pipeline

# COMMAND ----------

# MAGIC %fs cp dbfs:/FileStore/tables/serviceAccountFile.json file:/home/ubuntu/

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
        
#         print('*'*45+'Step 5: Preview Table'+'*'*45+'\n')
#         vAR_data_obj.Preview_Table(vAR_fact_table_name)
        
#         print('*'*45+'Step 6: Table Details'+'*'*45+'\n')
#         vAR_data_obj.Table_Details(vAR_fact_table_name)
        
#         print('*'*45+'Step 7: History of Table'+'*'*45+'\n')
#         vAR_data_obj.Table_History(vAR_fact_table_name)
        
    except BaseException as e:
        print('In Error Block - ',str(e))

# COMMAND ----------


