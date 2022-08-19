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


import tweepy
import configparser
import pandas as pd


vAR_api_key = # api key
vAR_api_key_secret = # api key secret

vAR_access_token = # access token
vAR_access_token_secret = # access token secret


class Linstener(tweepy.Stream):

    tweets = []
    limit = 100
    def on_status(self, status):
        self.tweets.append(status)
        # print(status.user.screen_name + ": " + status.text)

        if len(self.tweets) == self.limit:
            self.disconnect()


if __name__=='__main__':


    vAR_stream_tweet = Linstener(vAR_api_key, vAR_api_key_secret, vAR_access_token, vAR_access_token_secret)

    # stream by keywords
    vAR_keywords = ['#travel','#flight']

    vAR_stream_tweet.filter(track=vAR_keywords)
    # create DataFrame

    vAR_columns = ['User', 'Tweet','Location','CreatedAt','Language']
    vAR_data = []

    for vAR_tweet in vAR_stream_tweet.tweets:
        if not vAR_tweet.truncated:
            vAR_data.append([vAR_tweet.user.screen_name, vAR_tweet.text,vAR_tweet.user.location,vAR_tweet.created_at,vAR_tweet.lang])
        else:
            vAR_data.append([vAR_tweet.user.screen_name, vAR_tweet.extended_tweet['full_text'],vAR_tweet.user.location,vAR_tweet.created_at,vAR_tweet.lang])

    vAR_df = pd.DataFrame(vAR_data, columns=vAR_columns)

    display(vAR_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Reference : https://github.com/mehranshakarami/AI_Spectrum/blob/main/2022/Twitter_API/twitter_data_stream.py

# COMMAND ----------



# COMMAND ----------


