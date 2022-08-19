# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ### Copyright (c) DeepSphere.AI 2022
# MAGIC 
# MAGIC #### All rights reserved
# MAGIC 
# MAGIC ##### We are sharing this notebook for learning and research, and the idea behind us sharing the source code is to 
# MAGIC ##### stimulate ideas and thoughts for the learners to develop their Databricks knowledge.
# MAGIC 
# MAGIC ##### Author: # DeepSphere.AI | deepsphere.ai | dsschoolofai.com | info@deepsphere.ai
# MAGIC 
# MAGIC ##### Release: Initial release

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. What is a feature store?
# MAGIC A feature store is a centralized repository that enables data scientists to find and share features and also ensures that the same code used to compute the feature values is used for model training and inference.
# MAGIC 
# MAGIC Machine learning uses existing data to build a model to predict future outcomes. In almost all cases, the raw data requires preprocessing and transformation before it can be used to build a model. This process is called featurization or feature engineering, and the outputs of this process are called features - the building blocks of the model.
# MAGIC 
# MAGIC Developing features is complex and time-consuming. An additional complication is that for machine learning, the featurization calculations need to be done for model training, and then again when the model is used to make predictions. These implementations may not be done by the same team or using the same code environment, which can lead to delays and errors. Also, different teams in an organization will often have similar feature needs but may not be aware of work that other teams have done. A feature store is designed to address these problems.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Create a database for feature tables

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS dsai_feature_store;

# COMMAND ----------

# MAGIC %sql
# MAGIC Use dsai_feature_store;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists dsai_fact_revenue_feature_table

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Create a feature table in Databricks Feature Store

# COMMAND ----------

from databricks.feature_store import FeatureStoreClient
vAR_fs = FeatureStoreClient()

# COMMAND ----------

from pyspark.sql import SQLContext


def ReadRevenueFeature():
    return sqlContext.sql('select id,source_humidity from dsai_revenue_management.dsai_fact_revenue_table')

def ReadRevenueTable():
    return sqlContext.sql('select * from dsai_revenue_management.dsai_fact_revenue_table')

vAR_revenue_df = ReadRevenueFeature()



vAR_feature_table = vAR_fs.create_table(
  name='dsai_feature_store.dsai_fact_revenue_feature_table',
  schema=vAR_revenue_df.schema,
   primary_keys='id',
  description='Revenue features'
)


vAR_fs.write_table(
  name='dsai_feature_store.dsai_fact_revenue_feature_table',
  df = vAR_revenue_df,
  mode = 'overwrite'
)


# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Create Feature Lookup for model training

# COMMAND ----------

from databricks.feature_store import FeatureLookup
import mlflow

revenue_features_table = "dsai_feature_store.dsai_fact_revenue_feature_table"

revenue_feature_lookups = [
    FeatureLookup( 
      table_name = revenue_features_table,
      feature_name = "source_humidity",
      lookup_key = ["id"],
    )
]

# COMMAND ----------

revenue_feature_lookups

# COMMAND ----------

# MAGIC %md
# MAGIC Read Few columns for model training

# COMMAND ----------

revenue_data = ReadRevenueTable()
revenue_data = revenue_data['id','destination_humidity','revenue']
display(revenue_data)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Create Training Dataset using Feature Store Client

# COMMAND ----------

# End any existing runs (in the case this notebook is being run for a second time)
# mlflow.end_run()

# Start an mlflow run, which is needed for the feature store to log the model
# mlflow.start_run() 

# Since the rounded timestamp columns would likely cause the model to overfit the data 
# unless additional feature engineering was performed, exclude them to avoid training on them.

# Create the training set that includes the raw input data merged with corresponding features from both feature tables
training_set = vAR_fs.create_training_set(
  revenue_data,
  feature_lookups = revenue_feature_lookups,
  label = "revenue",
    exclude_columns=['id']
)

# Load the TrainingSet into a dataframe which can be passed into sklearn for training a model
training_df = training_set.load_df()

# COMMAND ----------

display(training_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. Model Training

# COMMAND ----------

from sklearn.model_selection import train_test_split
from mlflow.tracking import MlflowClient
import lightgbm as lgb
import mlflow.lightgbm
from mlflow.models.signature import infer_signature

features_and_label = training_df.columns

# Collect data into a Pandas array for training
data = training_df.toPandas()[features_and_label]

train, test = train_test_split(data, random_state=123)
X_train = train.drop(["revenue"], axis=1)
X_test = test.drop(["revenue"], axis=1)
y_train = train.revenue
y_test = test.revenue

mlflow.lightgbm.autolog()
train_lgb_dataset = lgb.Dataset(X_train, label=y_train.values)
test_lgb_dataset = lgb.Dataset(X_test, label=y_test.values)
print(type(train_lgb_dataset))
param = {"num_leaves": 32, "objective": "regression", "metric": "rmse"}
num_rounds = 100

# Train a lightGBM model
model = lgb.train(
  param, train_lgb_dataset, num_rounds
)


# COMMAND ----------

# MAGIC %md
# MAGIC ### 7. Log the trained model with MLflow and package it with feature lookup information

# COMMAND ----------

# Log the trained model with MLflow and package it with feature lookup information. 
vAR_fs.log_model(
  model,
  artifact_path="model_packaged",
  flavor=mlflow.lightgbm,
  training_set=training_set,
  registered_model_name="revenue_model_feature_store"
)

# COMMAND ----------

# MAGIC %md
# MAGIC Note :You can now use the model to make predictions on new data.For batch use cases, the model automatically retrieves the features it needs from Feature Store.

# COMMAND ----------

# MAGIC %md ###8. Scoring: Batch Inference

# COMMAND ----------

# MAGIC %md Suppose another data scientist now wants to apply this model to a different batch of data.

# COMMAND ----------

# Get the model URI
# latest_model_version = get_latest_model_version("revenue_model_feature_store")
model_uri = f"models:/revenue_model_feature_store/5"

# Call score_batch to get the predictions from the model
with_predictions = vAR_fs.score_batch(model_uri, revenue_data)

# COMMAND ----------

display(with_predictions)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9. Real-time serving use cases, publish the features to an online store.
# MAGIC 
# MAGIC At inference time, the model reads pre-computed features from the online feature store and joins them with the data provided in the client request to the model serving endpoint.

# COMMAND ----------

# MAGIC %md
# MAGIC Add Computed features into online store for real-time prediction

# COMMAND ----------

# MAGIC %md
# MAGIC ### When model enabled for real-time API serving
# MAGIC ##### Error Message : Cannot provision this model version due to a failure to retrieve online store metadata due to error: RESOURCE_DOES_NOT_EXIST: No suitable online store found for feature table 'dsai_feature_store.dsai_fact_revenue_feature_table', which is required by model 'revenue_model_feature_store' version '4'. Online store requirements: - Must be one of: Amazon DynamoDB, Amazon RDS MySQL, Amazon Aurora (MySQL-compatible) - Must contain all required features: source_humidity - Must be published with read-only credentials using read_secret_prefix Please check that the feature table has been published to an online store which meets the above requirements. For more information, see https://docs.databricks.com/applications/machine-learning/feature-store/feature-tables.html#enable-databricks-hosted-mlflow-models-to-look-up-features-from-online-stores

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reference Links:
# MAGIC https://docs.databricks.com/applications/machine-learning/feature-store/feature-tables.html#register-an-existing-delta-table-as-a-feature-table
# MAGIC https://docs.databricks.com/applications/machine-learning/feature-store/workflow-overview-and-notebook.html
# MAGIC https://docs.databricks.com/applications/machine-learning/feature-store/online-feature-stores.html

# COMMAND ----------



# COMMAND ----------


