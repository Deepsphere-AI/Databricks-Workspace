# Databricks notebook source
import requests
import json
import pandas as pd
from datetime import datetime as dt

url = "https://priceline-com-provider.p.rapidapi.com/v1/flights/search"


date = dt.now().date().replace(day=dt.now().date().day+1)

querystring = {"itinerary_type":"ONE_WAY","class_type":"ECO","location_arrival":"MAA","date_departure":str(date),"location_departure":"DXB","sort_order":"PRICE","number_of_stops":"2","price_max":"1000000","number_of_passengers":"2","duration_max":"10000","price_min":"100","date_departure_return":"2022-12-16"}

headers = {
	"X-RapidAPI-Key": "2c658a0fe2mshf584c6005929367p1d0a51jsn48ce3be5121c",
	"X-RapidAPI-Host": "priceline-com-provider.p.rapidapi.com"
}

response = requests.request("GET", url, headers=headers, params=querystring)


json_res = json.loads(response.text)
print(json_res['pricedItinerary'][0])

vAR_result_dict = {}
vAR_result_dict_list = []

for idx,list_item in enumerate(json_res['pricedItinerary']):
    vAR_result_dict = {'DepartureDate':querystring['date_departure'],'Airline':json_res['pricedItinerary'][idx]['pricingInfo']['ticketingAirline'],
                      'SourceLocation':querystring['location_departure'],'DestinationLocation':querystring['location_arrival'],
                       'FlightId':json_res['pricedItinerary'][idx]['slice'][0]['uniqueSliceId'],
                      'AvailableSeats':json_res['pricedItinerary'][idx]['numSeats'],'SingleTicketPrice_in_USD':json_res['pricedItinerary'][idx]['pricingInfo']['totalFare']} 
    vAR_result_dict_list.append(vAR_result_dict)
    
vAR_result_df = pd.DataFrame(vAR_result_dict_list).drop_duplicates(ignore_index=True)
    

vAR_spark_df = spark.createDataFrame(vAR_result_df)
display(vAR_spark_df)
vAR_table_name = 'dsai_revenue_management.DSAI_flight_pricing'
vAR_spark_df.write.format("delta").mode("overwrite").saveAsTable(vAR_table_name)

# COMMAND ----------


