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
# MAGIC ### 1.Generic API Client Code to Access the Model

# COMMAND ----------


import os
import requests
import numpy as np
import pandas as pd
os.environ['DATABRICKS_TOKEN'] = 'dapi8f4cfed33b682a6bc4aad9aabc90ee7d'
def create_tf_serving_json(data):
  return {'inputs': {name: data[name].tolist() for name in data.keys()} if isinstance(data, dict) else data.tolist()}

def score_model(dataset):
  url = 'https://dbc-83ecb7c8-5bb1.cloud.databricks.com/model/Custom_keras_mlflow_model_new/2/invocations'
  headers = {'Authorization': f'Bearer {os.environ.get("DATABRICKS_TOKEN")}'}
  data_json = dataset.to_dict(orient='split') if isinstance(dataset, pd.DataFrame) else create_tf_serving_json(dataset)
  response = requests.request(method='POST', headers=headers, url=url, json=data_json)
  if response.status_code != 200:
    raise Exception(f'Request failed with status {response.status_code}, {response.text}')
  return response.json()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Test Data to predict model outcome

# COMMAND ----------

dataset = [{"source":"MAA","destination":"DXB","customer_type":"FREQUENT FLYER","product_type":"ECONOMY","source_wind":0.32,"year":2022,
     "destination_wind":0.54,"price_type":"DISCOUNT FARE","flight":"FLIGHT102","promocode":"PROMOCODE"}]

# COMMAND ----------

# MAGIC %md
# MAGIC #### Convert the test dataset into Dataframe

# COMMAND ----------

dataset = pd.DataFrame(dataset)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Preprocess the input data

# COMMAND ----------

from sklearn import preprocessing
def data_preprocessing(data_frame):
        convert_dict = {'product_type' : str,
                        'customer_type' : str,
                        'year':int,
                        'source': str,
                        'destination':str,
                        'price_type':str,
                        'flight':str,
                        'promocode':str,
                        'source_wind':float,
                        'destination_wind':float}
        
        data_frame = data_frame.astype(convert_dict)
        df = pd.DataFrame(data_frame[['year','source','destination','price_type','flight','promocode',
                            'product_type','customer_type', 'source_wind','destination_wind']].copy())
        df['source'] = df['source'].astype('category')
        df_source_code = dict(enumerate(df['source'].cat.categories))
        
        df['destination'] = df['destination'].astype('category')
        df_destination_code = dict(enumerate(df['destination'].cat.categories))
        df['product_type'] = df['product_type'].astype('category')
        df_product_code = dict(enumerate(df['product_type'].cat.categories))
        df['customer_type'] = df['customer_type'].astype('category')
        df_customer_code = dict(enumerate(df['customer_type'].cat.categories))
        
        
        
        
        df['price_type'] = df['price_type'].astype('category')
        df_price_code = dict(enumerate(df['price_type'].cat.categories))
        
        df['flight'] = df['flight'].astype('category')
        df_flight_code = dict(enumerate(df['flight'].cat.categories))
        
        df['promocode'] = df['promocode'].astype('category')
        df_promo_code = dict(enumerate(df['promocode'].cat.categories))
        
        
        cat_columns = df.select_dtypes(['category']).columns
        df[cat_columns] = df[cat_columns].apply(lambda x: x.cat.codes)
        df = preprocessing.scale(df)
        return df

# COMMAND ----------

data = data_preprocessing(dataset)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Calling the API method

# COMMAND ----------

result = score_model(data)

# COMMAND ----------

print(result)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


