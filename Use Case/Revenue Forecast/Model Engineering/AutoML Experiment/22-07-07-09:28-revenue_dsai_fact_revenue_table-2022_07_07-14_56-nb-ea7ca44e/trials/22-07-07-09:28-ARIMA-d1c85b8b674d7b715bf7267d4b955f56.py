# Databricks notebook source
# MAGIC %md
# MAGIC # ARIMA training
# MAGIC This is an auto-generated notebook. To reproduce these results, attach this notebook to the **AutoML Cluster** cluster and rerun it.
# MAGIC - Compare trials in the [MLflow experiment](#mlflow/experiments/1777570645248666/s?orderByKey=metrics.%60val_smape%60&orderByAsc=true)
# MAGIC - Navigate to the parent notebook [here](#notebook/1777570645248656) (If you launched the AutoML experiment using the Experiments UI, this link isn't very useful.)
# MAGIC - Clone this notebook into your project folder by selecting **File > Clone** in the notebook toolbar.
# MAGIC 
# MAGIC Runtime Version: _10.5.x-cpu-ml-scala2.12_

# COMMAND ----------

import mlflow
import databricks.automl_runtime

target_col = "revenue"
time_col = "date_time"
unit = "days"

id_cols = ["source", "destination", "price_type", "promocode", "customer_type", "product_type", "source_wind", "destination_wind", "source_humidity"]

horizon = 2

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Data

# COMMAND ----------

from mlflow.tracking import MlflowClient
import os
import uuid
import shutil
import pandas as pd
import pyspark.pandas as ps

# Create temp directory to download input data from MLflow
input_temp_dir = os.path.join("/dbfs/tmp/", str(uuid.uuid4())[:8])
os.makedirs(input_temp_dir)

# Download the artifact and read it into a pandas DataFrame
input_client = MlflowClient()
input_data_path = input_client.download_artifacts("81da440400d24a3b83acf9d4785d7cb9", "data", input_temp_dir)

input_file_path = os.path.join(input_data_path, "training_data")
input_file_path = "file://" + input_file_path
df_loaded = ps.from_pandas(pd.read_parquet(input_file_path))

# Preview data
df_loaded.head(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Train ARIMA model
# MAGIC - Log relevant metrics to MLflow to track runs
# MAGIC - All the runs are logged under [this MLflow experiment](#mlflow/experiments/1777570645248666/s?orderByKey=metrics.%60val_smape%60&orderByAsc=true)
# MAGIC - Change the model parameters and re-run the training cell to log a different trial to the MLflow experiment

# COMMAND ----------

# Define the search space of seasonal period m
seasonal_periods = [1, 7]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Aggregate data by `id_col` and `time_col`
# MAGIC Group the data by `id_col` and `time_col`, and take average if there are multiple `target_col` values in the same group.

# COMMAND ----------

group_cols = [time_col] + id_cols
df_aggregation = df_loaded \
  .groupby(group_cols) \
  .agg(y=(target_col, "avg")) \
  .reset_index() \
  .rename(columns={ time_col : "ds" })

df_aggregation = df_aggregation.assign(ts_id=lambda x:x["source"].astype(str)+"-"+x["destination"].astype(str)+"-"+x["price_type"].astype(str)+"-"+x["promocode"].astype(str)+"-"+x["customer_type"].astype(str)+"-"+x["product_type"].astype(str)+"-"+x["source_wind"].astype(str)+"-"+x["destination_wind"].astype(str)+"-"+x["source_humidity"].astype(str))

df_aggregation.head()

# COMMAND ----------

from pyspark.sql.types import *

result_columns = ["ts_id", "pickled_model", "start_time", "end_time", "mse",
                  "rmse", "mae", "mape", "mdape", "smape", "coverage"]
result_schema = StructType([
  StructField("ts_id", StringType()),
  StructField("pickled_model", BinaryType()),
  StructField("start_time", TimestampType()),
  StructField("end_time", TimestampType()),
  StructField("mse", FloatType()),
  StructField("rmse", FloatType()),
  StructField("mae", FloatType()),
  StructField("mape", FloatType()),
  StructField("mdape", FloatType()),
  StructField("smape", FloatType()),
  StructField("coverage", FloatType())
  ])

def arima_training(history_pd):
  from databricks.automl_runtime.forecast.pmdarima.training import ArimaEstimator

  arima_estim = ArimaEstimator(horizon=horizon, frequency_unit=unit, metric="smape",
                              seasonal_periods=seasonal_periods, num_folds=2)

  results_pd = arima_estim.fit(history_pd)
  results_pd["ts_id"] = str(history_pd["ts_id"].iloc[0])
  results_pd["start_time"] = pd.Timestamp(history_pd["ds"].min())
  results_pd["end_time"] = pd.Timestamp(history_pd["ds"].max())
 
  return results_pd[result_columns]

def train_with_fail_safe(df):
  try:
    return arima_training(df)
  except Exception as e:
    print(f"Encountered an exception while training timeseries: {repr(e)}")
    return pd.DataFrame(columns=result_columns)

# COMMAND ----------

import mlflow
from databricks.automl_runtime.forecast.pmdarima.model import MultiSeriesArimaModel, mlflow_arima_log_model

with mlflow.start_run(experiment_id="1777570645248666", run_name="ARIMA") as mlflow_run:
  mlflow.set_tag("estimator_name", "ARIMA")

  arima_results = (df_aggregation.to_spark().repartition(sc.defaultParallelism, id_cols)
    .groupby(id_cols).applyInPandas(train_with_fail_safe, result_schema)).cache().to_pandas_on_spark()
   
  # Check whether every time series's model is trained
  ts_models_trained = set(arima_results["ts_id"].unique().to_list())
  ts_ids = set(df_aggregation["ts_id"].unique().tolist())

  if len(ts_models_trained) == 0:
    raise Exception("Trial unable to train models for any identities. Please check the training cell for error details")

  if ts_ids != ts_models_trained:
    mlflow.log_param("partial_model", True)
    print(f"WARNING: Models not trained for the following identities: {ts_ids.difference(ts_models_trained)}")
 
  # Log metrics to mlflow
  metric_names = ["mse", "rmse", "mae", "mape", "mdape", "smape", "coverage"]
  avg_metrics = arima_results[metric_names].mean().to_frame(name="mean_metrics").reset_index()
  avg_metrics["index"] = "val_" + avg_metrics["index"].astype(str)
  avg_metrics.set_index("index", inplace=True)
  mlflow.log_metrics(avg_metrics.to_dict()["mean_metrics"])

  # Save the model to mlflow
  pickled_model = arima_results[["ts_id", "pickled_model"]].to_pandas().set_index("ts_id").to_dict()["pickled_model"]
  start_time = arima_results[["ts_id", "start_time"]].to_pandas().set_index("ts_id").to_dict()["start_time"]
  end_time = arima_results[["ts_id", "end_time"]].to_pandas().set_index("ts_id").to_dict()["end_time"]
  arima_model = MultiSeriesArimaModel(pickled_model, horizon, unit, start_time, end_time, time_col, id_cols)

  # Generate sample input dataframe
  sample_input = df_loaded.tail(5).to_pandas()
  sample_input[time_col] = pd.to_datetime(sample_input[time_col])
  sample_input.drop(columns=[target_col], inplace=True)

  mlflow_arima_log_model(arima_model, sample_input=sample_input)

# COMMAND ----------

avg_metrics

# COMMAND ----------

# MAGIC %md
# MAGIC ## Analyze the predicted results

# COMMAND ----------

# Load the model
run_id = mlflow_run.info.run_id
loaded_model = mlflow.pyfunc.load_model(f"runs:/{run_id}/model")

# COMMAND ----------

# Predict future with the default horizon
forecast_pd = loaded_model._model_impl.python_model.predict_timeseries()

# COMMAND ----------



# COMMAND ----------

from databricks.automl_runtime.forecast.pmdarima.utils import plot

# Choose a random id from `ts_id` for plot
id_ = set(forecast_pd["ts_id"]).pop()
forecast_pd_plot = forecast_pd[forecast_pd["ts_id"] == id_]
history_pd_plot = df_aggregation[df_aggregation["ts_id"] == id_].to_pandas()
# When visualizing, we ignore the first d (differencing order) points of the prediction results
# because it is impossible for ARIMA to predict the first d values
d = loaded_model._model_impl.python_model.model(id_).order[1]
fig = plot(history_pd_plot[d:], forecast_pd_plot[d:])
fig

# COMMAND ----------

# MAGIC %md
# MAGIC ## Show the predicted results

# COMMAND ----------

predict_cols = ["ds", "ts_id", "yhat"]
forecast_pd = forecast_pd.reset_index()
display(forecast_pd[predict_cols].tail(horizon))

# COMMAND ----------

import json

# Fetch the run-id of the current job
exp_tags = json.loads(
    dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson()
).get('tags')

exp_data = {
    "workflow_run_id": exp_tags.get("runId", ""),
    "mlflow_run_id": mlflow_run.info.run_id
}

dbutils.notebook.exit(json.dumps(exp_data))
