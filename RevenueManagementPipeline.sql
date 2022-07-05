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
TBLPROPERTIES ("headers" = "true")
AS SELECT * FROM csv.`/FileStore/tables/FLYDUBAI_DATA_PLAN.csv`
-- `gs://flydubai-airline-data/timeseries-data/FLYDUBAI_DATA_PLAN.csv`
-- `s3://airline-data-bucket/timeseries-data/FLYDUBAI_DATA_PLAN.csv`

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### 3. Create downstream table from above tables

-- COMMAND ----------

create live table dsai_fact_revenue_table as
select 
-- id, data_category, data_source, year, source, destination, flight, price_type, promocode, customer_type, product_type, source_wind, destination_wind, source_humidity, destination_humidity, date_time, revenue, description
_c0 as id,_c1 as data_category,_c2 as data_source,
_c3 as year,_c4 as source,_c5 as destination,_c6 as flight,
_c7 as price_type,_c8 as promocode,_c9 as customer_type,_c10 as product_type,
_c11 as source_wind,_c12 as destination_wind,_c13 as source_humidity,_c14 as destination_humidity,
_c15 as date_time,_c16 as revenue,_c17 as description
from live.dsai_fact_transaction_revenue_external_table  where _c1 in ('ACTUAL')
union all
select 
id, data_category, data_source, year, source, destination, flight, price_type, promocode, customer_type, product_type, source_wind, destination_wind, source_humidity, destination_humidity, date_time, revenue, description
from live.dsai_fact_transaction_revenue_table  where data_category in ('ACTUAL');

-- COMMAND ----------



-- COMMAND ----------

-- # import pandas as pd
-- # data = pd.read_csv('s3://airline-data-bucket/timeseries-data/FLYDUBAI_DATA_PLAN.csv')
-- # data.head()
