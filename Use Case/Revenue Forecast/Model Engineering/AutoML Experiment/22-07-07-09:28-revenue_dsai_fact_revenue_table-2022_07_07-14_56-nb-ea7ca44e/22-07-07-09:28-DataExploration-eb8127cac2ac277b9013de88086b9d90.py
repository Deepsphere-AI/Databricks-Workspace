# Databricks notebook source
# MAGIC %md
# MAGIC # Data Exploration
# MAGIC This notebook performs exploratory data analysis on the dataset.
# MAGIC To expand on the analysis, attach this notebook to the **AutoML Cluster** cluster and rerun it.
# MAGIC - Explore completed trials in the [MLflow experiment](#mlflow/experiments/1777570645248666/s?orderByKey=metrics.%60val_smape%60&orderByAsc=true)
# MAGIC - Navigate to the parent notebook [here](#notebook/1777570645248656) (If you launched the AutoML experiment using the Experiments UI, this link isn't very useful.)
# MAGIC 
# MAGIC Runtime Version: _10.5.x-cpu-ml-scala2.12_

# COMMAND ----------

import os
import uuid
import pandas as pd
import shutil
import databricks.automl_runtime
import pyspark.pandas as ps

from mlflow.tracking import MlflowClient

ps.options.plotting.backend = "matplotlib"

# Download input data from mlflow into a pyspark.pandas DataFrame
# create temp directory to download data
exp_temp_dir = os.path.join("/dbfs/tmp", str(uuid.uuid4())[:8])
os.makedirs(exp_temp_dir)

# download the artifact and read it
exp_client = MlflowClient()
exp_data_path = exp_client.download_artifacts("81da440400d24a3b83acf9d4785d7cb9", "data", exp_temp_dir)
exp_file_path = os.path.join(exp_data_path, "training_data")
exp_file_path  = "file://" + exp_file_path

df = ps.from_pandas(pd.read_parquet(exp_file_path)).spark.cache()

target_col = "revenue"
time_col = "date_time"
id_cols = ["source", "destination", "price_type", "promocode", "customer_type", "product_type", "source_wind", "destination_wind", "source_humidity"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Time column Analysis

# COMMAND ----------

# MAGIC %md
# MAGIC Show the time range for the time series

# COMMAND ----------

df_time_range = df.groupby(id_cols).agg(min=(time_col, "min"), max=(time_col, "max"))
display(df_time_range.reset_index())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Target Value Analysis

# COMMAND ----------

# MAGIC %md
# MAGIC Time series target value status

# COMMAND ----------

selected_cols = id_cols + [target_col]
target_stats_df = df[selected_cols].groupby(id_cols).describe()
display(target_stats_df.reset_index())

# COMMAND ----------

# MAGIC %md
# MAGIC Check the number of missing values in the target column.

# COMMAND ----------

def num_nulls(x):
  num_nulls = x.isnull().sum()
  return pd.Series(num_nulls)

null_stats_df = df[selected_cols].groupby(id_cols).apply(num_nulls)[target_col]
display(null_stats_df.to_frame().reset_index())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Visualize the Data

# COMMAND ----------

# Select one id from id columns
idx = df[id_cols].to_pandas().astype(str).agg('-'.join, axis=1).unique()[0] # change index here to see other identities
idx_list = idx.split("-")
df_sub = df.loc[(df["source"] == idx_list[0])&(df["destination"] == idx_list[1])&(df["price_type"] == idx_list[2])&(df["promocode"] == idx_list[3])&(df["customer_type"] == idx_list[4])&(df["product_type"] == idx_list[5])&(df["source_wind"] == idx_list[6])&(df["destination_wind"] == idx_list[7])&(df["source_humidity"] == idx_list[8])]

df_sub = df_sub.filter(items=[time_col, target_col])
df_sub.set_index(time_col, inplace=True)
df_sub[target_col] = df_sub[target_col].astype("float")

fig = df_sub.plot()

# COMMAND ----------

# delete the temp data
shutil.rmtree(exp_temp_dir)
