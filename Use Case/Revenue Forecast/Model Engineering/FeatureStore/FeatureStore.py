# Databricks notebook source
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
# MAGIC drop table dsai_fact_revenue_feature_table

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Create a feature table in Databricks Feature Store

# COMMAND ----------

from databricks.feature_store import FeatureStoreClient
vAR_fs = FeatureStoreClient()

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC from pyspark.sql import SQLContext
# MAGIC 
# MAGIC 
# MAGIC def ReadRevenueFeature():
# MAGIC     return sqlContext.sql('select id,source_humidity from dsai_revenue_management.dsai_fact_revenue_table')
# MAGIC 
# MAGIC def ReadRevenueTable():
# MAGIC     return sqlContext.sql('select * from dsai_revenue_management.dsai_fact_revenue_table')
# MAGIC 
# MAGIC vAR_revenue_df = ReadRevenueFeature()
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC vAR_feature_table = vAR_fs.create_table(
# MAGIC   name='dsai_feature_store.dsai_fact_revenue_feature_table',
# MAGIC   schema=vAR_revenue_df.schema,
# MAGIC    primary_keys='id',
# MAGIC   description='Revenue features'
# MAGIC )
# MAGIC 
# MAGIC 
# MAGIC vAR_fs.write_table(
# MAGIC   name='dsai_feature_store.dsai_fact_revenue_feature_table',
# MAGIC   df = vAR_revenue_df,
# MAGIC   mode = 'overwrite'
# MAGIC )
# MAGIC # Created feature table 'dsai_feature_store.dsai_fact_revenue_view'

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Create Feature Lookup for model training

# COMMAND ----------

# MAGIC %python
# MAGIC from databricks.feature_store import FeatureLookup
# MAGIC import mlflow
# MAGIC 
# MAGIC revenue_features_table = "dsai_feature_store.dsai_fact_revenue_feature_table"
# MAGIC 
# MAGIC revenue_feature_lookups = [
# MAGIC     FeatureLookup( 
# MAGIC       table_name = revenue_features_table,
# MAGIC       feature_name = "source_humidity",
# MAGIC       lookup_key = ["id"],
# MAGIC     )
# MAGIC ]

# COMMAND ----------

revenue_feature_lookups

# COMMAND ----------

# MAGIC %md
# MAGIC Read Few columns for model training

# COMMAND ----------

# MAGIC %python
# MAGIC revenue_data = ReadRevenueTable()
# MAGIC revenue_data = revenue_data['id','destination_humidity','revenue']
# MAGIC # revenue_data = revenue_data.withColumnRenamed("revenue", "y").withColumnRenamed("date_time", "ds")
# MAGIC display(revenue_data)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Create Training Dataset using Feature Store Client

# COMMAND ----------

# MAGIC %python
# MAGIC # End any existing runs (in the case this notebook is being run for a second time)
# MAGIC mlflow.end_run()
# MAGIC 
# MAGIC # Start an mlflow run, which is needed for the feature store to log the model
# MAGIC mlflow.start_run() 
# MAGIC 
# MAGIC # Since the rounded timestamp columns would likely cause the model to overfit the data 
# MAGIC # unless additional feature engineering was performed, exclude them to avoid training on them.
# MAGIC 
# MAGIC # Create the training set that includes the raw input data merged with corresponding features from both feature tables
# MAGIC training_set = vAR_fs.create_training_set(
# MAGIC   revenue_data,
# MAGIC   feature_lookups = revenue_feature_lookups,
# MAGIC   label = "revenue",
# MAGIC     exclude_columns=['id']
# MAGIC )
# MAGIC 
# MAGIC # Load the TrainingSet into a dataframe which can be passed into sklearn for training a model
# MAGIC training_df = training_set.load_df()

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
model_uri = f"models:/revenue_model_feature_store/2"

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

import datetime
from databricks.feature_store.online_store_spec import AmazonDynamoDBSpec
# from databricks.feature_store import FeatureStoreClient
# vAR_fs = FeatureStoreClient()

# do not pass `write_secret_prefix` if you intend to use the instance profile attached to the cluster.
online_store = AmazonDynamoDBSpec(
  region='us-west-1',
#   read_secret_prefix='read_scope/dynamodb'
#   write_secret_prefix='<write_scope>/<prefix>'
)

vAR_fs.publish_table(
  name='dsai_feature_store.dsai_fact_revenue_feature_table',
  online_store=online_store,
  mode='merge'
)

# COMMAND ----------

# MAGIC %md
# MAGIC Even after publishing data into Amazon Dynamodb, it was throwing the same error message.

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %md https://docs.databricks.com/applications/machine-learning/feature-store/workflow-overview-and-notebook.html

# COMMAND ----------

# MAGIC %md https://docs.databricks.com/applications/machine-learning/feature-store/online-feature-stores.html

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### Reference Link:
# MAGIC https://docs.databricks.com/applications/machine-learning/feature-store/feature-tables.html#register-an-existing-delta-table-as-a-feature-table

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


