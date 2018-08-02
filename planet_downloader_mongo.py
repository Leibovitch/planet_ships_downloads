import json
import os
import requests
from requests.auth import HTTPBasicAuth
import datetime
import time
from dateutil import parser
import sys
import multiprocessing
import threading
import re
import pymongo
from retrying import retry
from pymongo import MongoClient

# get number of cores for the multi download ability
num_cores = multiprocessing.cpu_count()

max_threads = num_cores * 500

# using semaphore to bound the number of active download threads
pool_sema = threading.BoundedSemaphore(value = num_cores * 5)

# load custom filter from file
config = json.load(open(sys.argv[1]))

stats_endpoint_request = config["stats_endpoint_request"]

# Get the Planet API key from envitoment varible
planet_api_key = os.environ['PL_API_KEY']

#setup db
client = MongoClient('mongodb://localhost:27017/')
db = client.planet
ships_collection = db.ships
cursor_collection = db.cursors
# ships_collection.create_index([('updateTime', pymongo.ASCENDING)], unique=True)

# Init the session object
session = requests.Session()
session.auth = (planet_api_key, '')

def save_ships(features):
  for feature in features:
    ships_collection.insert_one(feature)

  # ships_collection.insert_many(features)

def save_cursor(cur):
  cursor_collection.upsert({"_id": "cursor", "value": cur})

# This function return the next cursor 
def fetch_page(cursor):
  result = \
    session.post(
      'https://api.planet.com/collections/v1/collections/ships0327/search/continue',
      json={ "cursor": cursor }).json()

  save_ships(result["features"])
  # save_cursor(result["cursor"])
  return cursor


query = True

# This is what is needed to execute the first cursor.
first_cursor = \
  session.post(
    'https://api.planet.com/collections/v1/collections/ships0327/search/create',
    json=stats_endpoint_request)

cursor = first_cursor.json()["cursor"]

while (query):
  print("cursor", cursor)

  if not cursor:
    query = False

  else:
    cursor = fetch_page(cursor)

time.sleep(600)
