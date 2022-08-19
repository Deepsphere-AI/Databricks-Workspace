-- Databricks notebook source
-- MAGIC %md
-- MAGIC 
-- MAGIC 
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

Use dsai_revenue_management_pipeline;

-- COMMAND ----------

-- Create some default value for promocode or discount
create or replace view dsai_fact_revenue_view as
select source,destination,flight,price_Type,
-- 5% will be discount applicable when Discount Fare for price type
case when price_type='DISCOUNT FARE' then ((5/100)*revenue) end as revenue_with_discount
,promocode,customer_type,product_type,
source_wind,destination_wind,source_humidity,destination_humidity,revenue as revenue_without_discount
from dsai_revenue_management_pipeline.dsai_fact_revenue_table

-- COMMAND ----------


