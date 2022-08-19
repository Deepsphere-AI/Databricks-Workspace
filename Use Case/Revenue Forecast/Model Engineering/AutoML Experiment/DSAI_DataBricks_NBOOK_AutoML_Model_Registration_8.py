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

model_name = 'revenue_prophet_model'

import mlflow

# The below run id is auto ml experiment run id
run_id = '9c64a684b960474fa0ed652393e4918f'
# The default path where the MLflow autologging function stores the Tensorflow Keras model
artifact_path = "model"
model_uri = "runs:/{run_id}/{artifact_path}".format(run_id=run_id, artifact_path=artifact_path)

model_details = mlflow.register_model(model_uri=model_uri, name=model_name)

# COMMAND ----------

# MAGIC %md
# MAGIC Note: Once model registered, we can enable serving for real-time inference prediction using the model

# COMMAND ----------


