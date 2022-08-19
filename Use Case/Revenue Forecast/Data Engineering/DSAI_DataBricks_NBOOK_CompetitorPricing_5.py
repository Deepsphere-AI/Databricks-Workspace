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

import requests
import json
import pandas as pd
from datetime import datetime as dt


def ReadFlightPriceAPI():
    vAR_url = "https://priceline-com-provider.p.rapidapi.com/v1/flights/search"

    vAR_date = dt.now().date().replace(day=dt.now().date().day+1)

    vAR_querystring = {"itinerary_type":"ONE_WAY","class_type":"ECO","location_arrival":"MAA","date_departure":str(vAR_date),"location_departure":"DXB","sort_order":"PRICE","number_of_stops":"2","price_max":"1000000","number_of_passengers":"2","duration_max":"10000","price_min":"100","date_departure_return":"2022-12-16"}

    vAR_headers = {
        "X-RapidAPI-Key": "2c658a0fe2mshf584c6005929367p1d0a51jsn48ce3be5121c",
        "X-RapidAPI-Host": "priceline-com-provider.p.rapidapi.com"
    }

    vAR_response = requests.request("GET", vAR_url, headers=vAR_headers, params=vAR_querystring)


    vAR_json_response = json.loads(vAR_response.text)
    print(vAR_json_response['pricedItinerary'][0])

    vAR_result_dict = {}
    vAR_result_dict_list = []

    for idx,list_item in enumerate(json_res['pricedItinerary']):
        vAR_result_dict = {'DepartureDate':vAR_querystring['date_departure'],'Airline':vAR_json_response['pricedItinerary'][idx]['pricingInfo']['ticketingAirline'],
                          'SourceLocation':vAR_querystring['location_departure'],'DestinationLocation':vAR_querystring['location_arrival'],
                           'FlightId':vAR_json_response['pricedItinerary'][idx]['slice'][0]['uniqueSliceId'],
                          'AvailableSeats':vAR_json_response['pricedItinerary'][idx]['numSeats'],'SingleTicketPrice_in_USD':vAR_json_response['pricedItinerary'][idx]['pricingInfo']['totalFare']} 
        vAR_result_dict_list.append(vAR_result_dict)
    return vAR_result_dict_list
    
    
def JsonToTable(vAR_result_dict_list):
    vAR_result_df = pd.DataFrame(vAR_result_dict_list).drop_duplicates(ignore_index=True)
    vAR_spark_df = spark.createDataFrame(vAR_result_df)
    display(vAR_spark_df)
    vAR_table_name = 'dsai_revenue_management.DSAI_flight_pricing'
    vAR_spark_df.write.format("delta").mode("overwrite").saveAsTable(vAR_table_name)
    print('Successfully table created from the dataframe')

# COMMAND ----------

if __name__=='__main__':
    vAR_result_df = ReadFlightPriceAPI()
    JsonToTable(vAR_result_df)

# COMMAND ----------


