-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Ingest raw clickstream data

-- COMMAND ----------

CREATE LIVE TABLE clickstream_raw
COMMENT "The raw wikipedia click stream dataset, ingested from /databricks-datasets."
AS SELECT * FROM json.`/databricks-datasets/wikipedia-datasets/data-001/clickstream/raw-uncompressed-json/2015_2_clickstream.json`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Clean and prepare data

-- COMMAND ----------

CREATE LIVE TABLE clickstream_clean(
  CONSTRAINT valid_current_page EXPECT (current_page_title IS NOT NULL),
  CONSTRAINT valid_count EXPECT (click_count > 0) ON VIOLATION FAIL UPDATE
)
COMMENT "Wikipedia clickstream data cleaned and prepared for analysis."
AS SELECT
  curr_title AS current_page_title,
  CAST(n AS INT) AS click_count,
  prev_title AS previous_page_title
FROM live.clickstream_raw

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Top referring pages

-- COMMAND ----------

CREATE LIVE TABLE top_spark_referers
COMMENT "A table containing the top pages linking to the Apache Spark page."
AS SELECT
  previous_page_title as referrer,
  click_count
FROM live.clickstream_clean
WHERE current_page_title = 'Apache_Spark'
ORDER BY click_count DESC
LIMIT 10

-- COMMAND ----------

-- select * from testdb.clickstream_raw;

-- COMMAND ----------

-- select * from testdb.top_spark_referers

-- COMMAND ----------

-- select * from table_changes('testdb.top_spark_referers',1)

-- COMMAND ----------


