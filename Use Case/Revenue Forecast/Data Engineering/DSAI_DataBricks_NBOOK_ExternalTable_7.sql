-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Copyright (c) DeepSphere.AI 2022
-- MAGIC 
-- MAGIC #### All rights reserved
-- MAGIC 
-- MAGIC ##### We are sharing this notebook for learning and research, and the idea behind us sharing the source code is to 
-- MAGIC ##### stimulate ideas and thoughts for the learners to develop their Databricks knowledge.
-- MAGIC 
-- MAGIC ##### Author: # DeepSphere.AI | deepsphere.ai | dsschoolofai.com | info@deepsphere.ai
-- MAGIC 
-- MAGIC ##### Release: Initial release

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 1. Managed Table OR Internal Table
-- MAGIC To create a managed table with SQL, run a CREATE TABLE command without a LOCATION clause. To delete a managed table with SQL, use the DROP TABLE statement. When a managed table is dropped, its underlying data is deleted from your cloud tenant. The only supported format for managed tables is Delta.
-- MAGIC ### 2. Unmanaged Table OR External Table
-- MAGIC External tables are tables whose data is stored in a storage location outside of the managed storage location, and are not fully managed by Unity Catalog. When you run DROP TABLE on an external table, Unity Catalog does not delete the underlying data. You can manage privileges on external tables and use them in queries in the the same way as managed tables. To create an external table with SQL, specify a LOCATION path in your CREATE TABLE statement. External tables can use the following file formats:
-- MAGIC DELTA
-- MAGIC CSV
-- MAGIC JSON
-- MAGIC AVRO
-- MAGIC PARQUET
-- MAGIC ORC
-- MAGIC TEXT

-- COMMAND ----------

-- Execute Below Query to Get existing databases
show databases

-- COMMAND ----------

use dsai_revenue_management_schema

-- COMMAND ----------

show tables

-- COMMAND ----------

select * from dsai_dim_product

-- COMMAND ----------

describe extended dsai_dim_product

-- COMMAND ----------



-- COMMAND ----------

-- Creating an external table in dbfs path
CREATE TABLE IF NOT EXISTS dsai_dim_product_external
LOCATION '/external_location'
-- Location is a dbfs path
AS SELECT * from dsai_dim_product;

-- COMMAND ----------

describe extended dsai_dim_product_external

-- COMMAND ----------

show tables

-- COMMAND ----------

select * from dsai_dim_product_external

-- COMMAND ----------

drop table dsai_dim_product_external

-- COMMAND ----------

-- Even after dropped a table, we will be able to retrieve the table data from it's location
select * from delta.`dbfs:/external_location`

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC show tables

-- COMMAND ----------

select * from dsai_dim_customer

-- COMMAND ----------

describe extended dsai_dim_customer

-- COMMAND ----------

drop table dsai_dim_customer

-- COMMAND ----------

-- Since dsai_dim_customer is a managed table, once the table is dropped we will not be able to retrieve the data
select * from delta.`dbfs:/user/hive/warehouse/dsai_revenue_management_schema.db/dsai_dim_customer`

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 2. Creating External table in S3

-- COMMAND ----------

-- Creating an external table in s3 path
CREATE TABLE IF NOT EXISTS dsai_dim_product_external_s3
LOCATION 's3://airline-data-bucket/external-table/'
AS SELECT * from dsai_dim_product;

-- COMMAND ----------

select * from dsai_dim_product_external_s3

-- COMMAND ----------

describe extended dsai_dim_product_external_s3 

-- COMMAND ----------

drop table dsai_dim_product_external_s3

-- COMMAND ----------

select * from delta.`s3://airline-data-bucket/external-table/`

-- COMMAND ----------


