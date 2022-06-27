# Databricks notebook source
# MAGIC %md
# MAGIC ## 1. ETL Challenges Without Delta Live Tables
# MAGIC 
# MAGIC ##### As we begin to scale to enrich our analytics environment with additional data sources to empower new insights, ETL complexity multiplies exponentially, and the following challenges cause these pipelines to become extremely brittle:  
# MAGIC 
# MAGIC 
# MAGIC * Error handling and recovery is laborious due to no clear dependencies between tables
# MAGIC * Data quality is poor, as enforcing and monitoring constraints is a manual process
# MAGIC * Data lineage cannot be traced, or heavy implementation is needed at best
# MAGIC * Observability at the granular, individual batch/stream level is impossible
# MAGIC * Difficult to account for batch and streaming within a unified pipeline
# MAGIC 
# MAGIC Reference : https://databricks.com/discover/pages/getting-started-with-delta-live-tables

# COMMAND ----------


