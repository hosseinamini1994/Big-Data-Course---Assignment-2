# imports
import mongoengine
import pymongo
import csv
import json
import pandas as pd
import sys, getopt, pprint
from pymongo import MongoClient



# create connection
client = pymongo.mongoclient("mongodb://localhost:27017")

# read and show .csv file
df = pd.read_csv("austin_weather.csv")
df.head()

# show size of table
df.shape
data = df.to_dic(orient = "records")

# create database and show list of database
db = client["schoolDB"]
print(db)

# create table and insert data from csv file
db.austinWeather.insert_many(data)









myquery={ "Date" : "1/1/2014"}
mydoc=mycol.find(myquery)
for x in mydoc:
    print(x)