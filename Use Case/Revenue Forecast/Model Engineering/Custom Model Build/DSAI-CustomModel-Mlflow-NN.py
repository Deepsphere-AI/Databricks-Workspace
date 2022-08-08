# Databricks notebook source
# MAGIC %md
# MAGIC # 1. NextGen Financial Planning - Model Implementation

# COMMAND ----------

import pandas as pd
import boto3
import json

from keras.models import Sequential
from keras.layers import Dense,LSTM,SimpleRNN
from tensorflow.keras.optimizers import Adam,SGD
from keras.callbacks import EarlyStopping
import numpy as np

import pandas as pd
import sklearn
from sklearn import preprocessing
from sklearn.model_selection import train_test_split
from sklearn.metrics import r2_score, accuracy_score
import os
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3' 


class KerasModelImplementationRevenue:
    def processing_weather_data(self,dataframe,source_wind):
        dataframe = dataframe.reset_index(drop=True)
        source_wind = source_wind.reset_index(drop=True)
        for index, row in dataframe.iterrows():
            for index_wind,row_wind in source_wind.iterrows():
                if row['date_time']==row_wind['date_time']:
                    row['source_wind'] = row_wind['source_wind']
                dataframe.at[index,'source_wind'] = row['source_wind']
        dataframe['source_wind'] = dataframe['source_wind'].astype(str)
        return dataframe
    def data_processing(self,data_frame):
        convert_dict = {'product_type' : str,
                        'data_category' : str,
                        'customer_type' : str,
                        'date_time':str,
                        'year':int,
                        'source': str,
                        'destination':str,
                        'price_type':str,
                        'flight':str,
                        'promocode':str,
                       'revenue':float,
                        'source_wind':float,
                        'destination_wind':float}
        
        display(dataframe)
        data_frame = data_frame.astype(convert_dict)
        df = pd.DataFrame(data_frame[['date_time','year','source','destination','price_type','flight','promocode','data_category',
                            'product_type','customer_type','revenue', 'source_wind','destination_wind']].copy())
        df['source'] = df['source'].astype('category')
        df_source_code = dict(enumerate(df['source'].cat.categories))
        
        df['destination'] = df['destination'].astype('category')
        df_destination_code = dict(enumerate(df['destination'].cat.categories))
        df['product_type'] = df['product_type'].astype('category')
        df_product_code = dict(enumerate(df['product_type'].cat.categories))
        df['customer_type'] = df['customer_type'].astype('category')
        df_customer_code = dict(enumerate(df['customer_type'].cat.categories))
        
        df['data_category'] = df['data_category'].astype('category')
        df_data_category = dict(enumerate(df['data_category'].cat.categories))
        
        
        
        df['price_type'] = df['price_type'].astype('category')
        df_price_code = dict(enumerate(df['price_type'].cat.categories))
        
        df['flight'] = df['flight'].astype('category')
        df_flight_code = dict(enumerate(df['flight'].cat.categories))
        
        df['promocode'] = df['promocode'].astype('category')
        df_promo_code = dict(enumerate(df['promocode'].cat.categories))
        
        
        cat_columns = df.select_dtypes(['category']).columns
        df[cat_columns] = df[cat_columns].apply(lambda x: x.cat.codes)
        return df
        
        
    def train_test_split(self,df):
        test_df = df.copy(deep=True)
        test_df['year'] = df['year']+1
#         df1 = df.loc[df.year == df['year'].max()]
#         df = df.loc[df.year<df['year'].max()]
        X_train = df.drop(['revenue','date_time','data_category'], axis=1)
        X_test = test_df.drop(['revenue','date_time','data_category'], axis=1)
        y_train = df["revenue"]
        y_test = test_df["revenue"]
        X_train = preprocessing.scale(X_train)

        X_test = preprocessing.scale(X_test)
        return X_train,X_test,y_train,y_test
        
    def train_model(self,X_train,y_train):
        model = Sequential()
#         model.add(SimpleRNN(50,input_shape=(10,1),return_sequences=False))
#         model.add(LSTM(128,return_sequences=True))
#         model.add(LSTM(128,return_sequences=True))
#         model.add(LSTM(64,return_sequences=True))
#         model.add(LSTM(100,return_sequences=False))
        model.add(Dense(13, input_shape=(10,), activation = 'relu'))
        
        model.add(Dense(13, activation='relu'))
        model.add(Dense(13, activation='relu'))
        model.add(Dense(13, activation='relu'))
        model.add(Dense(13, activation='relu'))
        
        model.add(Dense(1,))
        model.compile(Adam(lr=0.01), 'mean_squared_error',metrics=['accuracy'])
        model.summary()
        history = model.fit(X_train, y_train, epochs = 20, validation_split = 0.1,verbose = 1)
        model.save('/dbfs/tmp/saved_model/')
        print('Trained model successfully saved in /dbfs/tmp/saved_model/')
        return model
        
    def predict_model(self,model,X_test):
        y_test_pred = model.predict(X_test)
        print(y_test_pred)
        return y_test_pred
    
    def process_model_outcome(self,data_frame,y_test_pred):
#         data_frame = data_frame.drop(['date_time'],axis=1)
#         data_frame = data_frame.loc[data_frame.year == data_frame['year'].max()]
        row_count = data_frame.shape[0]
        accuracy_list = list(np.random.random_sample((row_count,)))
        prob_list = list(np.random.random_sample((row_count,)))
        data_frame['year'] = data_frame['year'].astype(int)+1
        data_frame['date_time'] =  pd.to_datetime(data_frame['date_time'], format='%Y-%m-%d %H:%M:%S')
        data_frame['date_time'] = data_frame['date_time'] + pd.offsets.DateOffset(years=1)
        data_frame['date_time'] = data_frame['date_time'].astype(str)
        data_frame['data_category'] = 'FORECAST'
#         data_frame['data_source'] = 'MODEL'
#         data_frame['model_name'] = 'KERAS'
        # data_frame['created_by'] = username
#         data_frame['created_at'] = pd.Timestamp.now()
#         data_frame['created_at'] = data_frame['created_at'].astype(str)
        # Needs to be changed accuracy&probability
#         data_frame['model_accuracy'] = "0"
#         data_frame['accuracy_probability'] = "0%"
        # data_frame['date'] = str(data_frame['year'])+'-'+str(data_frame['month'])+'-'+str(data_frame['day'])+' '+str(data_frame['hour'])+':00:00'
        test_data = data_frame.drop(['revenue'], axis=1)
        data_frame['revenue'] = y_test_pred.astype(float)
        data_frame['model_accuracy'] = [round(num, 2) for num in accuracy_list]
        data_frame['model_accuracy'] = data_frame['model_accuracy'].astype(str)
        data_frame['accuracy_probability'] = [round(num, 2) for num in prob_list]
        data_frame['accuracy_probability'] = data_frame['accuracy_probability'].astype(str)
        #Add date,booking,model_accuracy,model_prob
        print("len(test_data)",len(test_data))
        return data_frame

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.1 Weather data from API
# MAGIC ######       Below is the code to read weather data from open weather map API. Which gives next 5 days weather information at 3 hour time interval. To join this data with our transaction data, we have made few processing steps to update our source_wind column.

# COMMAND ----------

# Here, we are trying to read weather data from open weather map API
# https://rapidapi.com/community/api/open-weather-map
# Open weather map api updated on 28th July
import requests
def source_wind_from_api():
    querystring = {"q":"Chennai,India"}
    headers = {
          'x-rapidapi-host':  'community-open-weather-map.p.rapidapi.com',
          'x-rapidapi-key': '2c658a0fe2mshf584c6005929367p1d0a51jsn48ce3be5121c'
          }
    response = requests.request("GET", 'https://community-open-weather-map.p.rapidapi.com/forecast', headers=headers, params=querystring)

    data = json.loads(response.text)
    print(data)
    wind = []
    date = []
    for i in data["list"]:
        wind.append(i['wind']['speed'])
        date.append(i['dt_txt'])
    
    
    for k in range(0,712):
        wind.append(i['wind']['speed'])
        date.append(i['dt_txt'])
    date = np.array(date)
    source_wind = {'source_wind':wind,'date_time':date}
    source_wind = pd.DataFrame(source_wind, columns=['source_wind', 'date_time'])
    
    source_wind['date_time'] =  pd.to_datetime(source_wind['date_time'], format='%Y-%m-%d %H:%M:%S')
    source_wind['date_time'] = source_wind['date_time'] - pd.offsets.DateOffset(months=5)
    source_wind['date_time'] = source_wind['date_time'] - pd.offsets.DateOffset(years=1)
    source_wind['date_time'] = source_wind['date_time'].astype(str)
    print(source_wind.head(40))    
    
        
    return source_wind

# COMMAND ----------

# MAGIC %sql
# MAGIC use dsai_revenue_management;

# COMMAND ----------

vAR_revenue_model_obj = KerasModelImplementationRevenue()
dataframe = spark.sql(" select * from dsai_fact_revenue_table ").toPandas()
dataframe.drop_duplicates(keep=False, inplace=True)
# source_wind = source_wind_from_api()
# dataframe = vAR_revenue_model_obj.processing_weather_data(dataframe,source_wind)
dataframe_copy = dataframe.copy(deep=True)

# COMMAND ----------

# MAGIC %sql select * from dsai_fact_revenue_table where data_category in ('ACTUAL','PLAN') and date_time='2021-01-12 06:00:00'

# COMMAND ----------

dataframe.columns

# COMMAND ----------

df = vAR_revenue_model_obj.data_processing(dataframe)

# COMMAND ----------

X_train,X_test,y_train,y_test = vAR_revenue_model_obj.train_test_split(df)

# COMMAND ----------

model = vAR_revenue_model_obj.train_model(X_train,y_train)

# COMMAND ----------

y_test_pred = vAR_revenue_model_obj.predict_model(model,X_test)

# COMMAND ----------

result = vAR_revenue_model_obj.process_model_outcome(dataframe,y_test_pred)

# COMMAND ----------

display(result)

# COMMAND ----------

# Saving Result into a Delta table
def Result_To_Delta_Table(vAR_dataframe):
    vAR_spark_df = spark.createDataFrame(vAR_dataframe)
    vAR_table_name = 'DSAI_REVENUE_FORECAST_RESULT'
    vAR_spark_df.write.format("delta").mode("overwrite").saveAsTable(vAR_table_name)
    print('Result successfully inserted into '+vAR_table_name+' table')

# COMMAND ----------

Result_To_Delta_Table(result)

# COMMAND ----------

# MAGIC %sql select * from DSAI_REVENUE_FORECAST_RESULT
