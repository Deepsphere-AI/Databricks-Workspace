# Databricks notebook source
# MAGIC %md
# MAGIC ### MLflow Model
# MAGIC ##### An MLflow Model is a standard format for packaging machine learning models that can be used in a variety of downstream tools—for example, batch inference on Apache Spark or real-time serving through a REST API. The format defines a convention that lets you save a model in different flavors (python-function, pytorch, sklearn, and so on), that can be understood by different model serving and inference platforms.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Importing Libraries

# COMMAND ----------

import mlflow
import json
import pandas as pd
import numpy as np
from prophet import Prophet, serialize
from prophet.diagnostics import cross_validation, performance_metrics

import os
os.environ['DATABRICKS_TOKEN'] = 'dapi8f4cfed33b682a6bc4aad9aabc90ee7d'
# os.environ['MLFLOW_EXPERIMENT_ID']='2152951960190165'
# os.environ['MLFLOW_EXPERIMENT_NAME']='Test2'
ARTIFACT_PATH = 'mlflow_saved_model'

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Data Load and Preprocessing 

# COMMAND ----------

dataframe = spark.sql(" select * from dsai_revenue_management.dsai_fact_revenue_table ").toPandas()

# COMMAND ----------

dataframe.head()

# COMMAND ----------

dataframe = dataframe[['source','destination','customer_type','product_type','source_wind','destination_wind','revenue','date_time']]

# COMMAND ----------

dataframe.rename(columns = {'date_time':'ds','revenue':'y'}, inplace = True)

# COMMAND ----------

dataframe.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Model Training

# COMMAND ----------

with mlflow.start_run(experiment_id='2152951960190165'):

    model = Prophet()
    model.add_regressor('source_wind')
    model.add_regressor('destination_wind')
    
    model.fit(dataframe)
    
    
#     params = extract_params(model)

#     metric_keys = ["mse", "rmse", "mae", "mape", "mdape", "smape", "coverage"]
#     metrics_raw = cross_validation(
#         model=model,
#         horizon="10 days",
#         period="180 days",
#         initial="710 days",
#         parallel="threads",
#         disable_tqdm=True,
#     )
#     cv_metrics = performance_metrics(metrics_raw)
#     metrics = {k: cv_metrics[k].mean() for k in metric_keys}

#     print(f"Logged Metrics: \n{json.dumps(metrics, indent=2)}")
#     print(f"Logged Params: \n{json.dumps(params, indent=2)}")

    mlflow.prophet.log_model(model, artifact_path=ARTIFACT_PATH)
#     mlflow.log_params(params)
#     mlflow.log_metrics(metrics)
#     model_uri = mlflow.get_artifact_uri(ARTIFACT_PATH)
#     print(f"Model artifact logged to: {model_uri}")

# def extract_params(pr_model):
#     return {attr: getattr(pr_model, attr) for attr in serialize.SIMPLE_ATTRIBUTES}

# loaded_model = mlflow.prophet.load_model(model_uri)

# forecast = loaded_model.predict(loaded_model.make_future_dataframe(60))

# print(f"forecast:\n${forecast.head(30)}")

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Access Deployed Model

# COMMAND ----------

import requests
import numpy as np
import pandas as pd

def create_tf_serving_json(data):
  return {'inputs': {name: data[name].tolist() for name in data.keys()} if isinstance(data, dict) else data.tolist()}

def score_model(dataset):
  url = 'https://dbc-83ecb7c8-5bb1.cloud.databricks.com/model/ProphetCustomModel/2/invocations'
  headers = {'Authorization': f'Bearer {os.environ.get("DATABRICKS_TOKEN")}'}
  data_json = dataset.to_dict(orient='split') if isinstance(dataset, pd.DataFrame) else create_tf_serving_json(dataset)
  response = requests.request(method='POST', headers=headers, url=url, json=data_json)
  if response.status_code != 200:
    raise Exception(f'Request failed with status {response.status_code}, {response.text}')
  return response.json()

# COMMAND ----------

dataset = pd.DataFrame([{"ds":"2022-02-26 06:00:00","source_wind":0.87,"destination_wind":3.24},{"ds":"2022-02-26 12:00:00","source_wind":0.87,"destination_wind":3.24}])

# COMMAND ----------

result = score_model(dataset)

# COMMAND ----------

print(result)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reference: https://github.com/mlflow/mlflow/blob/master/examples/prophet/train.py

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


