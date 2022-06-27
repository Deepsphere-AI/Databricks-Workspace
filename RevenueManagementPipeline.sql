-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### 1. Create Transaction Table with fact and dimension tables

-- COMMAND ----------

create live table dsai_fact_transaction_revenue_table as
select rev.id,rev.data_category,data_source,year,source.location_name as source,dest.location_name as destination,
f.description as flight,price.description as price_type,pro.description as promocode,
cust.description as customer_type,p.description as product_type,source_wind,destination_wind,source_humidity,destination_humidity,substr(cast(date as string),0,19) as date_time,revenue,cur.description
from DSAI_Revenue_Management.dsai_fact_transaction_revenue rev
join DSAI_Revenue_Management.dsai_dim_location source on source.location_id=rev.source_id
join DSAI_Revenue_Management.dsai_dim_location dest on dest.location_id=rev.destination_id
join DSAI_Revenue_Management.dsai_dim_customer cust on cust.id=rev.customer_type_id
join DSAI_Revenue_Management.dsai_dim_product p on p.product_id=rev.product_type_id
join DSAI_Revenue_Management.dsai_dim_flight f on f.flight_id=rev.flight_id
join DSAI_Revenue_Management.dsai_dim_price price on price.price_id=rev.price_type_id
join DSAI_Revenue_Management.dsai_dim_promotion pro on pro.price_id=rev.promocode_id
join DSAI_Revenue_Management.dsai_dim_currency cur on cur.currency_id=rev.currency_id

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 2. Read External source as table

-- COMMAND ----------

CREATE LIVE TABLE dsai_fact_transaction_revenue_external_table
AS SELECT * FROM csv.`s3://airline-data-bucket/timeseries-data/FLYDUBAI_DATA_PLAN.csv`

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### 3. Create downstream table from above tables

-- COMMAND ----------

create live table dsai_fact_revenue_table as
select * from dsai_fact_transaction_revenue_external_table
union all
select * from dsai_fact_transaction_revenue_table;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import pandas as pd
-- MAGIC data = pd.read_csv('s3://airline-data-bucket/timeseries-data/FLYDUBAI_DATA_PLAN.csv')
-- MAGIC data.head()
