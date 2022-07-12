# Databricks notebook source
# MAGIC %md
# MAGIC # AutoML Notebook
# MAGIC Welcome to the parent notebook for AutoML. This notebook is used to start all the trials when you run AutoML from the Experiments UI. However, you typically do not need to modify or rerun this notebook. Instead, you should go to the AutoML Experiment, which has links to the data exploration notebook and each trial notebook.

# COMMAND ----------

dbutils.widgets.text("experiment_id", "")
dbutils.widgets.text("target_col", "")
dbutils.widgets.text("evaluation_metric", "")
dbutils.widgets.text("data_dir", "")
dbutils.widgets.text("timeout_minutes", "")
dbutils.widgets.text("max_trials", "")
dbutils.widgets.text("horizon", "")
dbutils.widgets.text("frequency", "")
dbutils.widgets.text("time_col", "")
dbutils.widgets.text("identity_col", "")
dbutils.widgets.text("output_database", "")
dbutils.widgets.text("dataset", "")
dbutils.widgets.text("automl_url", "")
dbutils.widgets.text("job_user", "")
dbutils.widgets.text("problem_type", "")
dbutils.widgets.text("exclude_frameworks", "")
dbutils.widgets.text("exclude_columns", "")

# COMMAND ----------

automl_url = dbutils.widgets.get("automl_url")

# COMMAND ----------

if automl_url != "":
    %pip install "$automl_url"

# COMMAND ----------

import databricks.automl
import mlflow
import pyspark

# COMMAND ----------

experiment_id = dbutils.widgets.get("experiment_id")
target_col = dbutils.widgets.get("target_col")
evaluation_metric = dbutils.widgets.get("evaluation_metric")
data_dir = dbutils.widgets.get("data_dir")
timeout_minutes = dbutils.widgets.get("timeout_minutes")
max_trials = dbutils.widgets.get("max_trials")
horizon = dbutils.widgets.get("horizon")
frequency = dbutils.widgets.get("frequency")
time_col = dbutils.widgets.get("time_col")
identity_col = dbutils.widgets.get("identity_col")
output_database = dbutils.widgets.get("output_database")
dataset = dbutils.widgets.get("dataset")
job_user = dbutils.widgets.get("job_user")
problem_type = dbutils.widgets.get("problem_type").lower()
exclude_frameworks = dbutils.widgets.get("exclude_frameworks")
exclude_columns = dbutils.widgets.get("exclude_columns")

kwargs = {
"target_col": target_col,
"home_dir": f"/Users/{job_user}",
"metric": evaluation_metric,
"time_col": time_col
}

if max_trials:
  kwargs["max_trials"] = int(max_trials)

if timeout_minutes:
  kwargs["timeout_minutes"] = int(timeout_minutes)

if data_dir:
  kwargs["data_dir"] = data_dir

if exclude_frameworks:
  kwargs["exclude_frameworks"] = exclude_frameworks.split(",")
if exclude_columns:
  kwargs["exclude_columns"] = exclude_columns.split(",")

if problem_type == "forecasting":
  kwargs["horizon"] = int(horizon)
  kwargs["frequency"] = frequency
  if identity_col:
    kwargs["identity_col"] = identity_col.split(",")
  if output_database:
    kwargs["output_database"] = output_database
kwargs

# COMMAND ----------

import json

experiment = mlflow.get_experiment(experiment_id)
kwargs["experiment"] = experiment
if "_databricks_automl.imputers" in experiment.tags:
    imputers = json.loads(experiment.tags["_databricks_automl.imputers"])
    if imputers:
        kwargs["imputers"] = imputers
experiment

# COMMAND ----------

spark = pyspark.sql.session.SparkSession.builder.getOrCreate()
df = spark.table(dataset)
kwargs["dataset"] = df
df

# COMMAND ----------

if problem_type == "classification":
  classifier = databricks.automl.classifier.Classifier(context_type=databricks.automl.ContextType.DATABRICKS)
  classifier.fit(**kwargs)
elif problem_type == "regression":
  regressor = databricks.automl.regressor.Regressor(context_type=databricks.automl.ContextType.DATABRICKS)
  regressor.fit(**kwargs)
elif problem_type == "forecasting":
  from databricks.automl.forecast import Forecast
  from mlflow.tracking import MlflowClient

  forecastor = Forecast(context_type=databricks.automl.ContextType.DATABRICKS)
  summary = forecastor.fit(**kwargs)

  if output_database:
    client = MlflowClient()
    client.set_experiment_tag(experiment_id, "_databricks_automl.output_table_name", summary.output_table_name)
else:
  raise ValueError(f"Unknown problem_type '{problem_type}'.")
