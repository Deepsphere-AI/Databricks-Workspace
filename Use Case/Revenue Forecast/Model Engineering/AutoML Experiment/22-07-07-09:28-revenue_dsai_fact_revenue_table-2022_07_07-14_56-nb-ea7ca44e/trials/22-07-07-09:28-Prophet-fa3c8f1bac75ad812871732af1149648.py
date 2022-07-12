# Databricks notebook source
# MAGIC %md
# MAGIC # Prophet training
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
# MAGIC ## Train Prophet model
# MAGIC - Log relevant metrics to MLflow to track runs
# MAGIC - All the runs are logged under [this MLflow experiment](#mlflow/experiments/1777570645248666/s?orderByKey=metrics.%60val_smape%60&orderByAsc=true)
# MAGIC - Change the model parameters and re-run the training cell to log a different trial to the MLflow experiment

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

import logging

# disable informational messages from prophet
logging.getLogger("py4j").setLevel(logging.WARNING)

# COMMAND ----------

from pyspark.sql.types import *

result_columns = ["ts_id", "model_json", "prophet_params", "start_time", "mse",
                  "rmse", "mae", "mape", "mdape", "smape", "coverage"]
result_schema = StructType([
  StructField("ts_id", StringType()),
  StructField("model_json", StringType()),
  StructField("prophet_params", StringType()),
  StructField("start_time", TimestampType()),
  StructField("mse", FloatType()),
  StructField("rmse", FloatType()),
  StructField("mae", FloatType()),
  StructField("mape", FloatType()),
  StructField("mdape", FloatType()),
  StructField("smape", FloatType()),
  StructField("coverage", FloatType())
  ])

def prophet_training(history_pd):
  from hyperopt import hp
  from databricks.automl_runtime.forecast.prophet.forecast import ProphetHyperoptEstimator

  seasonality_mode = ["additive", "multiplicative"]
  search_space =  {
    "changepoint_prior_scale": hp.loguniform("changepoint_prior_scale", -6.9, -0.69),
    "seasonality_prior_scale": hp.loguniform("seasonality_prior_scale", -6.9, 2.3),
    "holidays_prior_scale": hp.loguniform("holidays_prior_scale", -6.9, 2.3),
    "seasonality_mode": hp.choice("seasonality_mode", seasonality_mode)
  }
  country_holidays="US"
  run_parallel = False
 
  hyperopt_estim = ProphetHyperoptEstimator(horizon=horizon, frequency_unit=unit, metric="smape",interval_width=0.8,
                   country_holidays=country_holidays, search_space=search_space, num_folds=2, max_eval=1, trial_timeout=7047,
                   random_state=323224161, is_parallel=run_parallel)

  results_pd = hyperopt_estim.fit(history_pd)
  results_pd["ts_id"] = str(history_pd["ts_id"].iloc[0])
  results_pd["start_time"] = pd.Timestamp(history_pd["ds"].min())
 
  return results_pd[result_columns]

def train_with_fail_safe(df):
  try:
    return prophet_training(df)
  except Exception as e:
    print(f"Encountered an exception while training timeseries: {repr(e)}")
    return pd.DataFrame(columns=result_columns)

# COMMAND ----------

import mlflow
from databricks.automl_runtime.forecast.prophet.model import mlflow_prophet_log_model, MultiSeriesProphetModel

with mlflow.start_run(experiment_id="1777570645248666", run_name="PROPHET") as mlflow_run:
  mlflow.set_tag("estimator_name", "Prophet")
  mlflow.log_param("holiday_country", "US")
  mlflow.log_param("interval_width", 0.8)

  forecast_results = (df_aggregation.to_spark().repartition(sc.defaultParallelism, id_cols)
    .groupby(id_cols).applyInPandas(train_with_fail_safe, result_schema)).cache().to_pandas_on_spark()
   
  # Check whether every time series's model is trained
  ts_models_trained = set(forecast_results["ts_id"].unique().to_list())
  ts_ids = set(df_aggregation["ts_id"].unique().tolist())

  if len(ts_models_trained) == 0:
    raise Exception("Trial unable to train models for any identities. Please check the training cell for error details")

  if ts_ids != ts_models_trained:
    mlflow.log_param("partial_model", True)
    print(f"WARNING: Models not trained for the following identities: {ts_ids.difference(ts_models_trained)}")
 
  # Log the metrics to mlflow
  avg_metrics = forecast_results[["mse", "rmse", "mae", "mape", "mdape", "smape", "coverage"]].mean().to_frame(name="mean_metrics").reset_index()
  avg_metrics["index"] = "val_" + avg_metrics["index"].astype(str)
  avg_metrics.set_index("index", inplace=True)
  mlflow.log_metrics(avg_metrics.to_dict()["mean_metrics"])

  # Create mlflow prophet model
  model_json = forecast_results[["ts_id", "model_json"]].to_pandas().set_index("ts_id").to_dict()["model_json"]
  start_time = forecast_results[["ts_id", "start_time"]].to_pandas().set_index("ts_id").to_dict()["start_time"]
  prophet_model = MultiSeriesProphetModel(model_json, start_time, "2021-03-29 06:00:00", horizon, unit, time_col, id_cols)

  # Generate sample input dataframe
  sample_input = df_loaded.head(1).to_pandas()
  sample_input[time_col] = pd.to_datetime(sample_input[time_col])
  sample_input.drop(columns=[target_col], inplace=True)

  mlflow_prophet_log_model(prophet_model, sample_input=sample_input)

# COMMAND ----------

forecast_results.head(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Analyze the predicted results

# COMMAND ----------

# Load the model
run_id = mlflow_run.info.run_id
loaded_model = mlflow.pyfunc.load_model(f"runs:/{run_id}/model")

# COMMAND ----------

model = loaded_model._model_impl.python_model
col_types = [StructField(f"{n}", FloatType()) for n in model.get_reserved_cols()]
col_types.append(StructField("ds",TimestampType()))
col_types.append(StructField("ts_id",StringType()))
result_schema = StructType(col_types)

ids = ps.DataFrame(model._model_json.keys(), columns=["ts_id"])
forecast_pd = ids.to_spark().groupby("ts_id").applyInPandas(lambda df: model.model_predict(df), result_schema).cache().to_pandas_on_spark().set_index("ts_id")

# COMMAND ----------



# COMMAND ----------

# Plotly plots is turned off by default because it takes up a lot of storage.
# Set this flag to True and re-run the notebook to see the interactive plots with plotly
use_plotly = False

# COMMAND ----------

# Choose a random id from `ts_id` for plot
id = set(forecast_pd.index.to_list()).pop()
# Get the prophet model for this id
model = loaded_model._model_impl.python_model.model(id)
predict_pd = forecast_pd.loc[id].to_pandas()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Plot the forecast with change points and trend

# COMMAND ----------

from prophet.plot import add_changepoints_to_plot, plot_plotly

if use_plotly:
    fig = plot_plotly(model, predict_pd, changepoints=True, trend=True, figsize=(1200, 600))
else:
    fig = model.plot(predict_pd)
    a = add_changepoints_to_plot(fig.gca(), model, predict_pd)
fig

# COMMAND ----------

# MAGIC %md
# MAGIC ### Plot the forecast components

# COMMAND ----------

from prophet.plot import plot_components_plotly
if use_plotly:
    fig = plot_components_plotly(model, predict_pd, figsize=(900, 400))
    fig.show()
else:
    fig = model.plot_components(predict_pd)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Show the predicted results

# COMMAND ----------

predict_cols = ["ds", "ts_id", "yhat"]
forecast_pd = forecast_pd.reset_index()
display(forecast_pd[predict_cols].tail(horizon))
