import json
import os
import requests
import sys
import multiprocessing
import threading
import re
import pymongo
from pymongo import MongoClient

# get number of cores for the multi download ability
num_cores = multiprocessing.cpu_count()

max_threads = num_cores * 5

# using semaphore to bound the number of active download threads
pool_sema = threading.BoundedSemaphore(value = num_cores * 5)

# load custom filter from file
config = json.load(open(sys.argv[1]))

stats_endpoint_request = config["stats_endpoint_request"]

# Get the Planet API key from envitoment varible
planet_api_key = os.environ['PL_API_KEY']

# Setup db
client = MongoClient('mongodb://localhost:27017/')
db = client.planet
ships_collection = db.ships
cursor_collection = db.cursors
# ships_collection.create_index([('updateTime', pymongo.ASCENDING)], unique=True)

session = requests.Session()
session.auth = (planet_api_key, '')

count = 0

def save_ships(features):
  for feature in features:
    ships_collection.update({ "_id": feature["id"] }, feature, upsert = True)

  global count
  count += len(features)

def save_cursor(cur):
  cursor_collection.update({ "_id": "cursor" }, { "value": cur }, upsert = True)

def fetch_page(cursor):
  response = session.post(
    'https://api.planet.com/collections/v1/collections/ships0327/search/continue',
    json={ "cursor": cursor })

  if response.status_code == 200:
    result = response.json()
    save_ships(result["features"])
    # save_cursor(result["cursor"])
    return result["cursor"]

  elif response.status_code == 204:
    return None

  else:
    print("Unexpected response: ", response.text)
    return None


# This is what is needed to execute the first cursor.
first_cursor = \
  session.post(
    'https://api.planet.com/collections/v1/collections/ships0327/search/create',
    json=stats_endpoint_request)

query = True
cursor = first_cursor.json()["cursor"]

while True:
  if cursor:
    cursor = fetch_page(cursor)

  else:
    print("Done fetching.")
    break

  print("Fetched so far: ", count)
